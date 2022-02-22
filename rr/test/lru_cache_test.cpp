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


class A {
public:
    size_t* a_{ 0 };
    virtual size_t Ref() = 0;
    virtual size_t RefSize() = 0;
    virtual void Delete() {
        cout << "A::Delete()" << endl;
        delete this;
    }
    ~A() { cout << "~A()" << endl; delete a_; a_ = nullptr; }
};

class B : public A {
public:
    size_t* b_;
    virtual size_t Ref() { (*b_)++; }
    virtual size_t RefSize() { return (*b_); }
    void Delete() {
        cout << "B::Delete()" << endl;
        // delete this;
    }
    ~B() { cout << "~B()" << endl; delete b_; b_ = nullptr; }
};
void test() {
    B* b = new B();
    A* a = b;
    a->Delete();
}


void simple_test() {
    rr::LRUCache* cache = new rr::LRUCache(10 * PAGE_SIZE, PAGE_SIZE);

    for (uint64_t key = 0; key < 13; key++) {
        rr::lru_cache::Page* page = nullptr;
        cache->GetNewPage(page);
        assert(page);
        page->set_key(key); page->set_id(key);
            rr::lru_cache::Page* prior = nullptr;
        bool is_new = cache->Write(page->key(), page, prior);
        if(is_new) { page->Unref(); } else { prior->Unref(); }
    }

    cache->Print();
    cout << __FILE__ << ", " << __LINE__ << endl;
    delete cache;
}

void multi_thd_read_after_write() {
    cout << __FILE__ << ", " << __LINE__ << endl;
    int batch = global_batch;
    int num_thd = global_num_thd;
    // rr::LRUCache* cache = new rr::LRUCache(batch * num_thd / 2 * PAGE_SIZE, PAGE_SIZE);
    rr::LRUCache* cache = new rr::LRUCache(batch * num_thd * PAGE_SIZE, PAGE_SIZE);

    auto Write = [&cache, num_thd, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++)
        // for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page* page = nullptr;
            bool res = cache->GetNewPage(page);
            assert(res);
            page->set_key(i); page->set_id(i);
            rr::lru_cache::Page* prior = nullptr;
            bool is_new = cache->Write(i, page, prior, thd_id);
            if(is_new) { page->Unref(); } else { prior->Unref(); }
            // cout << i << endl;
        }
    };
    auto Read = [&cache, num_thd, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page* page = nullptr;
            // page.set_key(i);
            bool find = cache->Read(i, page, thd_id);
            if (find) { page->Unref(); }
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

void multi_thd_read_while_write() {
    cout << __FILE__ << ", " << __LINE__ << endl;
    int batch = global_batch;
    int num_thd = global_num_thd;
    // rr::LRUCache* cache = new rr::LRUCache(batch * num_thd / 2 * PAGE_SIZE, PAGE_SIZE);
    rr::LRUCache* cache = new rr::LRUCache(batch * num_thd * PAGE_SIZE, PAGE_SIZE);

    auto Write = [&cache, num_thd, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++)
        // for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page* page = nullptr;
            bool res = cache->GetNewPage(page);
            assert(res);
            page->set_key(i); page->set_id(i);
            rr::lru_cache::Page* prior = nullptr;
            bool is_new = cache->Write(i, page, prior, thd_id);
            if(is_new) { page->Unref(); } else { prior->Unref(); }
            // cout << i << endl;
        }
    // cout << __FILE__ << ", " << __LINE__ << endl;
    };
    auto Read = [&cache, num_thd, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page* page = nullptr;
            bool find = cache->Read(i, page, thd_id);
            if (find) { page->Unref(); }
    // cout << __FILE__ << ", " << __LINE__ << ", " << i << endl;
            // cout << i << endl;
        }
    // cout << __FILE__ << ", " << __LINE__ << endl;
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
        v_thread.emplace_back(Read, thd);
    }
    // cache->PrintRef();
    for (auto thd = 0; thd < num_thd * 2; thd++)
    {
        v_thread[thd].join();
    }
    profiler.End();
    cout << __FILE__ << ", " << __LINE__ << endl;
    cout << "ConcurrentLinkedHashMap total time: " << profiler.Micros() << endl;
    delete cache;
    cout << __FILE__ << ", " << __LINE__ << endl;
}


int main()
{
    // test();
    simple_test();
    multi_thd_read_after_write();
    multi_thd_read_while_write();
    return 0;
}
// g++ lru_cache_test.cpp -o lru_cache_test.exe -ltbb -lpthread -g