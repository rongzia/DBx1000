//
// Created by rrzhang on 2020/4/15.
//

#include <iostream>
#include <cassert>
#include <cstring>
#include <unistd.h>
#include <thread>
#include <vector>
#include "buffer.h"
#include "lru_index.h"
#include "leveldb/db.h"
#include "../util/string_util.h"
#include "../util/profiler.h"
#include "../util/numbercomparator.h"

using namespace dbx1000;
using namespace std;

size_t bench_size = db_num_item_/1000;

//int pool_size = 6553600000/10*9;
int pool_size = 65536000;
size_t row_size = 65536;

string db_path("/home/zhangrongrong/dbx1000_leveldb");


void Print_unordered_map(unordered_map<uint64_t, string> row_map) {
    std::cout << "unordered_map : {" << std::endl;
    int count = 0;
    for(auto& iter : row_map) {
        if (count % 10 == 0) { std::cout << "    "; }
        std::cout << ", {key:" << iter.first << ", row:" << iter.second << "}";
        if (count++ % 10 == 0) { std::cout << std::endl; }
    }
    std::cout << std::endl << "}" << std::endl;
}

void Warmup_leveldb(leveldb::DB *db) {
    std::unique_ptr<Profiler> profiler_total(new Profiler());
    std::unique_ptr<Profiler> profiler_gen_string(new Profiler());
    std::unique_ptr<Profiler> profiler_buffer_put(new Profiler());

    profiler_total->Start();
    for(uint64_t i = 0; i < db_num_item_; i++) {
//        if(0 == i % pool_item ) { cout << i << endl; }
//        profiler_gen_string->Start();
//        string str = StringUtil::Random_string(rng, row_size);
//        profiler_gen_string->End();

//        profiler_buffer_put->Start();
        string str(row_size, 0);
        db->Put(leveldb::WriteOptions(), to_string(i), str);
//        profiler_buffer_put->End();
    }

    profiler_total->End();
//    cout << "gen string time : " << profiler_gen_string->Micros() << " micros." << endl;
//    cout << "buffer put time : " << profiler_buffer_put->Micros() << " micros." << endl;
    cout << "total      time : " << profiler_total->Micros() << " micros." << endl;

    /// some check
    leveldb::Iterator *iter = db->NewIterator(leveldb::ReadOptions());
    size_t count = 0;
    iter->SeekToFirst();
    while (iter->Valid()) {
        assert(count == std::stoi(iter->key().ToString()));
        assert(row_size == iter->value().size());
        iter->Next();
        count++;
    }
}

void SimpleTest(Buffer* buffer){
    std::unique_ptr<Profiler> profiler(new Profiler());

    /// 单线程读
    profiler->Start();
    for(int i = 0; i < bench_size; i++) {
        char buf[row_size];
        int a = rand() % db_num_item_;
        buffer->BufferGet(a, buf, row_size);
    }
    profiler->End();
    cout << "single thread read time  : " << profiler->Micros() << " micros." << endl;

    /// 单线程写
    profiler->Clear();
    profiler->Start();
    char buf[row_size];
    memset(buf, 0, row_size);
    for(int i = 0; i < bench_size; i++) {
        int a = rand() % db_num_item_;
        buffer->BufferPut(a, buf, row_size);
    }
    profiler->End();
    cout << "single thread write time : " << profiler->Micros() << " micros." << endl;


    size_t num_threads = 5;
    /// 读
    std::unique_ptr<Profiler> profiler_read(new Profiler());
    profiler_read->Start();
    std::vector<std::thread> read_threads;
    for(int i = 0; i < num_threads; i++) {
        read_threads.emplace_back(std::thread([&] {
            srand(i);
            for (int i = 0; i < bench_size / num_threads; i++) {
                char buf[row_size];
                int a = rand() % db_num_item_;
                buffer->BufferGet(a, buf, row_size);
            }
        }));
    }
    /// 等待读完成
    for(int i = 0; i < num_threads; i++){
        read_threads[i].join();
    }
    profiler_read->End();
    cout << num_threads << " thread read, each thread read db_size/num_threads times   : " << profiler_read->Micros() << " micros." << endl;

    /// 写
    std::unique_ptr<Profiler> profiler_write(new Profiler());
    profiler_write->Start();
    std::vector<std::thread> write_threads;
    for(int i = 0; i < num_threads; i++) {
        write_threads.emplace_back(std::thread([&] {
            srand(num_threads - i -1);
            for (int i = 0; i < bench_size / num_threads; i++) {
                int a = rand() % db_num_item_;
                buffer->BufferPut(a, buf, row_size);
            }
        }));
    }
    /// 等待写完成
    for(int i = 0; i < num_threads; i++){
        write_threads[i].join();
    }
    profiler_write->End();
    cout << num_threads << " thread write, each thread write db_size/num_threads times : " << profiler_write->Micros() << " micros." << endl;
}

int main(){
//#define WARMUP_LEVELDB 1
#ifdef WARMUP_LEVELDB
    /// 必要时，删除 leveldb 目录
    int rc = system("rm -rf /home/zhangrongrong/dbx1000_leveldb");
    cout << "rmdir rc : " << rc << endl;
#endif

    std::unique_ptr<Profiler> profiler(new Profiler());
    std::unique_ptr<Buffer> buffer(new Buffer(pool_size, row_size, db_path));
    cout << "buffer inited" << endl;

#ifdef WARMUP_LEVELDB
    Warmup_leveldb(buffer->db_.get());
#endif

    SimpleTest(buffer.get());

    return 0;
}