#define TBB_PREVIEW_CONCURRENT_LRU_CACHE 1
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_lru_cache.h>
#include <iostream>
#include <thread>
#include <atomic>
#include <vector>
#include "../include/rr/concurrent_lru_cache.h"
#include "../include/rr/profiler.h"
using namespace std;

int global_num_thd = 28;
int global_batch = 10000;

void simple_test() {
    rr::LRUCache cache(10 * PAGE_SIZE, PAGE_SIZE);

    for (uint64_t key = 0; key < 13; key++) {
        rr::lru_cache::Page* page = nullptr;
        cache.GetNewPage(page);
        assert(page);
        page->set_key(key); page->set_id(key);
        cache.Write(page->key(), page);
        // cache.Write(&page);
    }
    
    cache.Print();
}

void multi_thd_read_after_write() {
    int batch = global_batch;
    int num_thd = global_num_thd;
    rr::LRUCache* cache = new rr::LRUCache(batch * num_thd / 1.1 * 10, 10);
    // rr::LRUCache* cache = new rr::LRUCache(batch * num_thd * 10, 10);

    auto Write = [&cache, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++)
        {
            rr::lru_cache::Page* page = nullptr;
            bool res = cache->GetNewPage(page);
            assert(res);
            page->set_key(i); page->set_id(i);
            // bool is_new = cache->Write(page.key_, page.ptr_, thd_id);
            bool is_new = cache->Write(i, page, thd_id);
            // cout << i << endl;
        }
    };
    auto Read = [&cache, num_thd, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page page;
            // page.set_key(i);
            // bool find = cache->Read(page.key_, page.ptr_, thd_id);
            bool find = cache->Read(i, &page, thd_id);
            if(find) { page.PageUnref(); }
            // cout << i << endl;
        }
    };

    rr::Profiler profiler;
    profiler.Start();
    std::vector<std::thread> v_thread;
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread.emplace_back(Write, thd);
    }
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread[thd].join();
    }
    profiler.End();
    cout << "ConcurrentLinkedHashMap write time: " << profiler.Micros() << endl;

    v_thread.clear();
    profiler.Clear();
    profiler.Start();
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread.emplace_back(Read, thd);
    }
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread[thd].join();
    }
    profiler.End();
    cout << "ConcurrentLinkedHashMap read  time: " << profiler.Micros() << endl;

    delete cache;
}


int main()
{
    simple_test();
    multi_thd_read_after_write();
    return 0;
}
// g++ lru_cache_test_ptr.cpp -o lru_cache_test_ptr.exe -ltbb -lpthread -g