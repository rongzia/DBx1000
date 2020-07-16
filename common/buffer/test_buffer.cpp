//
// Created by rrzhang on 2020/6/2.
//


#include <iostream>
#include <cstring>
#include <vector>
#include <thread>
#include <mutex>
#include <cassert>
#include "buffer.h"

#include "config.h"
#include "util/profiler.h"

using namespace std;

/**
 * 初始化 buffer 为 10000 个 uint64 = 0, 10 个线程同时对每个 item++, 最后验证是否正确
 */
#define buffer_item_num (1024L * 100)
#define page_size sizeof(uint64_t)

void Test_Buffer() {
    dbx1000::Buffer *buffer = new dbx1000::Buffer(page_size * buffer_item_num, page_size);
    dbx1000::Profiler profiler;


    profiler.Start();
    uint64_t a = 0;
    for (uint64_t i = 0; i < buffer_item_num; i++) {
        buffer->BufferPut(i, &a, page_size);
    }/// warmup buffer
    profiler.End();
    cout << "warmup time : " << profiler.Micros() << endl;

    profiler.Clear();
    profiler.Start();
    vector<thread> threads_write;
    mutex mtx;
    /// write
    for (int i = 0; i < 10; i++) {
        threads_write.emplace_back(thread(
                [&]() {
                    for (uint64_t j = 0; j < buffer_item_num; j++) {
                        mtx.lock();
                        uint64_t a;
//                        assert(0 == buffer->BufferGetWithLock(j, &a, page_size));
                        assert(0 == buffer->BufferGet(j, &a, page_size));
                        a++;
//                        assert(0 == buffer->BufferPutWithLock(j, (void*)&a, page_size));
                        assert(0 == buffer->BufferPut(j, (void*)&a, page_size));
                        mtx.unlock();
                    }
                }
        ));
    }
    for (int i = 0; i < 10; i++) {
        threads_write[i].join();
    }
    profiler.End();
    cout << "write  time : " << profiler.Micros() << endl;


    profiler.Clear();
    profiler.Start();
    /// read
    vector<thread> threads_read;
    for (int i = 0; i < 10; i++) {
        threads_read.emplace_back(thread(
                [&]() {
                    for (uint64_t j = 0; j < buffer_item_num; j++) {
//                        mtx.lock();
                        uint64_t a;
//                        buffer->BufferGetWithLock(j, &a, page_size);
                        buffer->BufferGet(j, &a, page_size);
                        assert(10 == a);
//                        mtx.unlock();
                    }
                }
        ));
    }
    for (int i = 0; i < 10; i++) {
        threads_read[i].join();
    }
    profiler.End();
    cout << "read   time : " << profiler.Micros() << endl;

    delete buffer;
}

int main(){
    Test_Buffer();
}