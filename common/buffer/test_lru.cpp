//
// Created by rrzhang on 2020/7/15.
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

using namespace std;


/// to test cas
struct Node { int value; Node* next; };
std::atomic<Node*> list_head (nullptr);
void append (int val) {     // append an element to the list
  Node* oldHead = list_head;
  Node* newNode = new Node {val,oldHead};

  // what follows is equivalent to: list_head = newNode, but in a thread-safe way:
  while (!list_head.compare_exchange_weak(oldHead,newNode)) {
    newNode->next = oldHead;
  }
}
int test ()
{
  // spawn 10 threads to fill the linked list:
  std::vector<std::thread> threads;
  for (int i=0; i<10000; ++i) threads.push_back(std::thread(append,i));
  for (auto& th : threads) th.join();

  // print contents:
  int a = 0;
  for (Node* it = list_head; it!=nullptr; it=it->next){
//    std::cout << ' ' << it->value;
    a++;
  }
  std::cout << a <<'\n';

  // cleanup:
  Node* it; while (it=list_head) {list_head=it->next; delete it;}

  return 0;
}
/// to test cas






void Test_lru(){
    dbx1000::LRU *lru = new dbx1000::LRU();
    dbx1000::LruIndex *lruIndex = new dbx1000::LruIndex();
    dbx1000::Profiler profiler;                             /// 计时器

    profiler.Clear();
    profiler.Start();
    vector<thread> threads;
    int thread_num = 10;
    int total_item = 100000;
    mutex mtx;
    /// Prepend, 分片插入，每个线程插入 0-total_item 的一部分
    for (int i = 0; i < thread_num; i++) {
        threads.emplace_back(thread(
                [total_item, thread_num, i, &lru, &mtx, &lruIndex]() {
                    for (int j = i*total_item/thread_num; j < (i+1)*total_item/thread_num; j++) {
                        char *p = new char[8];
                        memcpy(p, &j, 8);
                        dbx1000::PageNode* pageNode1 = new dbx1000::PageNode(p);
                        pageNode1->page_->set_page_id(j);
#ifdef USE_MUTEX
                        lru->Prepend(pageNode1);
#else
                        mtx.lock();
                        lru->Prepend(pageNode1);
                        mtx.unlock();
#endif // USE_MUTEX
#ifdef USE_TBB
                        lruIndex->IndexPut(pageNode1->page_->page_id(), pageNode1);
#else
                        mtx.lock();
                        lruIndex->IndexPut(pageNode1->page_->page_id(), pageNode1);
                        mtx.unlock();
#endif // USE_TBB
                    }
                }
        ));
    }
    for (auto& th:threads) { th.join(); }
    profiler.End();
    cout << "Prepend  time : " << profiler.Micros() << endl;
    lru->Check();



    /// Get
    threads.clear();
    profiler.Clear();
    profiler.Start();
    for (int i = 0; i < thread_num; i++) {
        threads.emplace_back(thread(
                [total_item, thread_num, i, &lru, &mtx, &lruIndex]() {
                    for (int j = 0; j < total_item; j++) {
                        dbx1000::PageNode* pageNode = lruIndex->IndexGet(j, new dbx1000::LruIndexFlag);
                        if(pageNode != nullptr) {
#ifdef USE_MUTEX
                            lru->Get(pageNode);
#else
                            mtx.lock();
                            lru->Get(pageNode);
                            mtx.unlock();
#endif // USE_MUTEX
                        }
                    }
                }
        ));
    }
    for (auto& th:threads) { th.join(); }
    profiler.End();
    cout << "Get      time : " << profiler.Micros() << endl;
    lru->Check();



    /// Popback
    threads.clear();
    profiler.Clear();
    profiler.Start();
    for (int i = 0; i < thread_num; i++) {
        threads.emplace_back(thread(
                [total_item, thread_num, i, &lru, &mtx, &lruIndex]() {
                    for (int j = i*total_item/thread_num; j < (i+1)*total_item/thread_num; j++) {
#ifdef USE_MUTEX
                        dbx1000::PageNode* pageNode = lru->Popback();
#else
                        mtx.lock();
                        dbx1000::PageNode* pageNode = lru->Popback();
                        mtx.unlock();
#endif // USE_MUTEX
#ifdef USE_TBB
                        lruIndex->IndexDelete(pageNode->page_->page_id());
#else
                        mtx.lock();
                        lruIndex->IndexDelete(pageNode->page_->page_id());
                        mtx.unlock();
#endif
                    }
                }
        ));
    }
    for (auto& th:threads) { th.join(); }
    profiler.End();
    cout << "Popback  time : " << profiler.Micros() << endl;
    lru->Check();


    delete lru;
}
int main(){

    Test_lru();
//    test();
    return 0;
}