//
// Created by rrzhang on 2020/4/15.
//

#include <iostream>
#include <cassert>
#include <cstring>
#include <unistd.h>
#include <thread>
#include <vector>
#include "config.h"

#include "leveldb/db.h"
#include "server/buffer/buffer.h"
#include "server/buffer/lru_index.h"
#include "util/string_util.h"
#include "util/profiler.h"
#include "util/numbercomparator.h"

using namespace dbx1000;
using namespace std;

/// 默认 buffer 大小为总数据量的 1/8
uint64_t pool_size = g_synth_table_size / 8L * 16384L;
size_t row_size = 16384;                    // 16 KB

/// 校验 buffer 正确性
void Check(
#ifdef USE_MEMORY_DB
                MemoryDB* db
#else
               leveldb::DB *db
#endif
        ) {
    cout << "checking whether the buffer is executed successfully..." << endl;
    std::unordered_map<uint64_t, std::string> check_map;        /// for compare
    int num_item = 10240;
    Buffer* buffer = new Buffer(num_item / 8 * row_size, row_size, db);

    /// put
    RandNum_generator rng(0, 255);
    for(int i = 0; i < num_item; i++) {
        string str = StringUtil::Random_string(rng, row_size);
        check_map.insert(std::pair<uint64_t, std::string>(i, str));
        buffer->BufferPut(i, str.data(), row_size);
    }

    /// check
    for(int i = 0; i < num_item; i++) {
        char buf[row_size];
        buffer->BufferGet(i, buf, row_size);
        assert(0 == memcmp(check_map[i].data(), buf, row_size));
    }

    delete buffer;
    cout << "checked." << endl;
}

void WarmupDB(Buffer* buffer) {
    Profiler profiler;

    profiler.Start();
    for(uint64_t i = 0; i < g_synth_table_size; i++) {
//        if(0 == i % 1000 ) { cout << i << endl; }
        string str(row_size, 0);
        buffer->BufferPut(i, str.data(), row_size);
    }
    profiler.End();
    cout << "total items : " << g_synth_table_size << ", warmup time : " << profiler.Micros() << " micros." << endl;
}



size_t bench_size = g_synth_table_size / 800;
void SimpleTest(Buffer* buffer){
    Profiler profiler;

    /// 单线程读
    profiler.Start();
    for(int i = 0; i < bench_size; i++) {
        char buf[row_size];
        int a = rand() % g_synth_table_size;
        buffer->BufferGet(a, buf, row_size);
    }
    profiler.End();
    cout << "single thread read items : " << bench_size << ", time : " << profiler.Micros() << " micros." << endl;

    /// 单线程写
    profiler.Clear();
    profiler.Start();
    char buf[row_size];
    memset(buf, 0, row_size);
    for(int i = 0; i < bench_size; i++) {
        int a = rand() % g_synth_table_size;
        buffer->BufferPut(a, buf, row_size);
    }
    profiler.End();
    cout << "single thread write items : " << bench_size << ", time : " << profiler.Micros() << " micros." << endl;


    size_t num_threads = 5;
    /// 多线程读
    profiler.Clear();
    profiler.Start();
    std::vector<std::thread> read_threads;
    for(int i = 0; i < num_threads; i++) {
        read_threads.emplace_back(std::thread([&] {
            srand(i);
            for (int i = 0; i < bench_size / num_threads; i++) {
                char buf[row_size];
                int a = rand() % g_synth_table_size;
                buffer->BufferGet(a, buf, row_size);
            }
        }));
    }
    /// 等待读完成
    for(int i = 0; i < num_threads; i++){
        read_threads[i].join();
    }
    profiler.End();
    cout << num_threads << " threads, each thread read items : " << bench_size / num_threads << ", time : " << profiler.Micros() << " micros." << endl;

    /// 多线程写
    profiler.Clear();
    profiler.Start();
    std::vector<std::thread> write_threads;
    for(int i = 0; i < num_threads; i++) {
        write_threads.emplace_back(std::thread([&] {
            srand(num_threads - i -1);
            for (int i = 0; i < bench_size / num_threads; i++) {
                int a = rand() % g_synth_table_size;
                buffer->BufferPut(a, buf, row_size);
            }
        }));
    }
    /// 等待写完成
    for(int i = 0; i < num_threads; i++){
        write_threads[i].join();
    }
    profiler.End();
    cout << num_threads << " threads, each thread write items : " << bench_size / num_threads << ", time : " << profiler.Micros() << " micros." << endl;
}

int main(){
    Buffer* buffer;
#ifdef USE_MEMORY_DB
    MemoryDB* db = new MemoryDB();
#else
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.comparator = new dbx1000::NumberComparatorImpl();
    leveldb::Status status = leveldb::DB::Open(options, g_db_path, &db);
    assert(status.ok());
    leveldb::Iterator *iter = db->NewIterator(leveldb::ReadOptions());
    iter->SeekToFirst();
//    assert(iter->Valid());
#endif

    /// 必要时，删除 leveldb 目录
    std::string cmd = "rm -rf " + g_db_path;
    int rc = system(cmd.data());
    cout << "rmdir rc : " << rc << endl;

    /// 验证 buffer 正确性
    Check(db);

    buffer = new Buffer(pool_size, row_size, db);
    cout << "buffer inited" << endl;

    /// 初始化 db
    WarmupDB(buffer);
    /// 读写测试
    SimpleTest(buffer);


    delete buffer;
    while(1) {}         /// 方便 linux top 查看占用内存大小

    return 0;
}