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

size_t bench_size = DB_NUM_ITEM / 1000;

uint64_t pool_size = DB_NUM_ITEM / 8L * 16384L;
size_t row_size = 16384;                    // 16 KB

/// 校验 buffer 正确性
void Check() {
    cout << "checking whether the buffer is executed successfully..." << endl;
    std::unordered_map<uint64_t, std::string> check_map;
    int num_item = 10240;
    Buffer* buffer = new Buffer(num_item * row_size / 10, row_size);

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
    for(uint64_t i = 0; i < DB_NUM_ITEM; i++) {
//        if(0 == i % 1000 ) { cout << i << endl; }
        string str(row_size, 0);
        buffer->BufferPut(i, str.data(), row_size);
    }
    profiler.End();
    cout << "total items : " << DB_NUM_ITEM << ", warmup time : " << profiler.Micros() << " micros." << endl;
}

void SimpleTest(Buffer* buffer){
    Profiler profiler;

    /// 单线程读
    profiler.Start();
    for(int i = 0; i < bench_size; i++) {
        char buf[row_size];
        int a = rand() % DB_NUM_ITEM;
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
        int a = rand() % DB_NUM_ITEM;
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
                int a = rand() % DB_NUM_ITEM;
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
                int a = rand() % DB_NUM_ITEM;
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
    /// 必要时，删除 leveldb 目录
    std::string cmd = "rm -rf " + g_db_path;
    int rc = system(cmd.data());
    cout << "rmdir rc : " << rc << endl;

    /// 验证 buffer 正确性
    Check();

    Buffer* buffer = new Buffer(pool_size, row_size);
    cout << "buffer inited" << endl;

    /// 初始化 db
    WarmupDB(buffer);
    /// 读写测试
    SimpleTest(buffer);


    delete buffer;

    while(1) {}

    return 0;
}