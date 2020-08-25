//
// Created by rrzhang on 2020/7/16.
//

#include <iostream>
#include <cstring>
#include <vector>
#include <thread>
#include <mutex>
#include <cassert>

#include "lru.h"
#include "lru_index.h"
#include "common/storage/tablespace/page.h"
#include "util/profiler.h"
#include "buffer_def.h"

using namespace std;
using namespace dbx1000;

void Test_lru(){
    dbx1000::LruIndex *lruIndex = new dbx1000::LruIndex();
    dbx1000::Profiler profiler;

    profiler.Clear();
    profiler.Start();
    vector<thread> threads;
    int thread_num = 10;
    int total_item = 100000;
    mutex mtx;

    /// IndexPut, 分片插入，每个线程插入 0-total_item 的一部分，
    for (int i = 0; i < thread_num; i++) {
        threads.emplace_back(thread(
                [total_item, thread_num, i, &lruIndex, &mtx]() {
                    for (int j = i*total_item/thread_num; j < (i+1)*total_item/thread_num; j++) {
#ifdef USE_TBB
                        lruIndex->IndexPut(j, new dbx1000::PageNode());
#else
                        mtx.lock();
                        lruIndex->IndexPut(j, new dbx1000::PageNode());
                        mtx.unlock();
#endif
                    }
                }
        ));
    }
    for (auto& th:threads) { th.join(); }
    profiler.End();
    cout << "IndexPut  time : " << profiler.Micros() << endl;
    cout << "after IndexPut, lru_index's size: " << lruIndex->lru_map_.size() << endl;

    /// IndexGet, 每个线程读取全部的 item
    threads.clear();
    profiler.Clear();
    profiler.Start();
    for (int i = 0; i < thread_num; i++) {
        threads.emplace_back(thread(
                [total_item, thread_num, i, &lruIndex, &mtx]() {
                    for (int j = 0; j < total_item; j++) {
                        lruIndex->IndexGet(j, new dbx1000::LruIndexFlag());
                    }
                }
        ));
    }
    for (auto& th:threads) { th.join(); }
    profiler.End();
    cout << "IndexGet  time : " << profiler.Micros() << endl;


    /// IndexDelete, 分片删除，每个线程删除 0-total_item 的一部分
    threads.clear();
    profiler.Clear();
    profiler.Start();
    for (int i = 0; i < thread_num; i++) {
        threads.emplace_back(thread(
                [total_item, thread_num, i, &lruIndex, &mtx]() {
                    for (int j = i*total_item/thread_num; j < (i+1)*total_item/thread_num; j++) {
#ifdef USE_TBB
                        lruIndex->IndexDelete(j);
#else
                        mtx.lock();
                        lruIndex->IndexDelete(j);
                        mtx.unlock();
#endif
                    }
                }
        ));
    }
    for (auto& th:threads) { th.join(); }
    profiler.End();
    cout << "IndexDelete  time : " << profiler.Micros() << endl;
    cout << "after IndexDelete, lru_index's size: " << lruIndex->lru_map_.size() << endl;



    delete lruIndex;
}
int main(){

    Test_lru();
    return 0;
}

