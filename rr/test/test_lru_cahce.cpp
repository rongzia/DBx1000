#define TBB_PREVIEW_CONCURRENT_LRU_CACHE 1
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_lru_cache.h>
#include <iostream>
#include <thread>
#include <atomic>
#include <vector>
#include <cmath>
#include "../include/rr/concurrent_lru_cache.h"
#include "../include/rr/profiler.h"
#include "../include/rr/random.h"
using namespace std;

int global_num_thd = 20;
size_t global_batch = 10000;


void simple_test() {
    rr::LRUCache* cache = new rr::LRUCache(10 * PAGE_SIZE, PAGE_SIZE);

    for (uint64_t key = 0; key < 13; key++) {
        rr::lru_cache::Page* page = nullptr;
        rr::lru_cache::Page* prior = nullptr;
        cache->GetNewPage(page);
        assert(page);
        page->page_id_ = key;
        bool is_new = cache->Write(key, page, prior);
        if (is_new) {
            size_t before = page->Unref(); 
            std::cout << __LINE__ << "\t, page/unref: " << page->page_id_ << "/" << before-1 << std::endl;
        }
        else { prior->Unref(); }
    }

    // cache->Print();
    // cout << __FILE__ << ", " << __LINE__ << endl;
    delete cache;
}

void read_after_write(double den = 1, bool with_thd_id = false) {
    // cout << "read_after_write     , " << __FILE__ << ", " << __LINE__ << endl;
    int batch = global_batch;
    int num_thd = global_num_thd;
    rr::LRUCache* cache = new rr::LRUCache(batch * num_thd / den * PAGE_SIZE, PAGE_SIZE);


    auto Write1 = [&cache, num_thd, batch, with_thd_id](int thd_id) -> void
    {
        int th = with_thd_id ? thd_id : 0;
        
        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++) {
            rr::lru_cache::Page* page = nullptr;
            rr::lru_cache::Page* prior = nullptr;
            bool res = cache->GetNewPage(page);
            assert(res); assert(page);
            page->page_id_ = i;
            bool is_new = cache->Write(i, page, prior, th);
            if (is_new) { page->Unref(); }
            else { prior->Unref(); }

            if(i == (thd_id + 1) * batch - 1) {
                // std::cout << "thread: " << thd_id << " done." << std::endl;
            }
        }
    };

    auto Write2 = [&cache, num_thd, batch, with_thd_id](int thd_id) -> void
    {
        int th = with_thd_id ? thd_id : 0;
        
        for (int i = 0; i < num_thd * batch; i++) {
            rr::lru_cache::Page* page = nullptr;
            rr::lru_cache::Page* prior = nullptr;
            bool res = cache->GetNewPage(page);
            assert(res); assert(page);
            page->page_id_ = i;
            bool is_new = cache->Write(i, page, prior, th);
            if (is_new) { page->Unref(); /*cout << i << endl;*/ }
            else { prior->Unref(); }

            

            if(i == num_thd * batch - 1) {
                // std::cout << "thread: " << thd_id << " done." << std::endl;
            }
        }
    };

    auto Read = [&cache, num_thd, batch, with_thd_id](int thd_id) -> void
    {
        int th = with_thd_id ? thd_id : 0;
        
        for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page* page = nullptr;
            bool find = cache->Read(i, page, th);
            if (find) { page->Unref(); }

            if(i == num_thd * batch - 1) {
                // std::cout << "thread: " << thd_id << " done." << std::endl;
            }
        }
    };

    rr::Profiler profiler;
    profiler.Start();
    std::vector<std::thread> v_thread;
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread.emplace_back(Write1, thd);
    }
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread[thd].join();
    }
    profiler.End();
    cout << "read_after_write write1 time: " << profiler.Micros() << " us" << endl;


    v_thread.clear();
    profiler.Clear();
    profiler.Start();
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread.emplace_back(Write2, thd);
    }
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread[thd].join();
    }
    profiler.End();
    cout << "read_after_write write2 time: " << profiler.Micros() << " us" << endl;


    v_thread.clear();
    profiler.Clear();
    profiler.Start();
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread.emplace_back(Read, thd);
    }
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread[thd].join();
    }
    profiler.End();
    cout << "read_after_write read   time: " << profiler.Micros() << " us" << endl;
    delete cache;
}


void read_while_write(double den = 1, bool with_thd_id = false, bool with_delete = false) {
    size_t batch = global_batch;
    int num_thd = global_num_thd;
    size_t total_bytes = batch * num_thd / den * PAGE_SIZE;
    rr::LRUCache* cache = new rr::LRUCache(total_bytes, PAGE_SIZE);
    std::atomic<size_t> write_size{0}, read_size{0}, delete_size{0};

    auto Write = [&cache, num_thd, batch, &write_size, with_thd_id](int thd_id) -> void
    {
        int th = with_thd_id ? thd_id : 0;

        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++)
        {
            rr::lru_cache::Page* page = nullptr;
            rr::lru_cache::Page* prior = nullptr;
            bool res = cache->GetNewPage(page);
            assert(res); assert(page);
            page->page_id_ = i;
            bool is_new = cache->Write(i, page, prior, th);
            if (is_new) { page->Unref(); }
            else { prior->Unref(); }
            write_size.fetch_add(1);
        }
    };
    auto Read = [&cache, num_thd, batch, &read_size, with_thd_id](int thd_id) -> void
    {
        int th = with_thd_id ? thd_id : 0;

        for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page* page = nullptr;
            bool find = cache->Read(i, page, th);
            if (!find) {
                rr::lru_cache::Page* temp = nullptr;
                rr::lru_cache::Page* prior = nullptr;
                cache->GetNewPage(temp);
                assert(temp);
                temp->page_id_ = i;
                bool is_new = cache->Write(i, temp, prior, th);
                if (is_new) { temp->Unref();  }
                else { prior->Unref(); }
            }
            if (find) { page->Unref(); }
            read_size.fetch_add(1);
        }
    };
    auto Delete = [&cache, num_thd, batch, &delete_size, with_thd_id](int thd_id) -> void
    {
        int th = with_thd_id ? thd_id : 0;

        for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page* prior = nullptr;
            bool res = cache->Delete(i, th);
            delete_size.fetch_add(1);
        }
    };

    rr::Profiler profiler;
    profiler.Start();
    std::vector<std::thread> v_thread;
    for (auto thd = 0; thd < num_thd; thd++) { v_thread.emplace_back(Write, thd); }
    for (auto thd = 0; thd < num_thd; thd++) { v_thread.emplace_back(Read, thd); }
    if(with_delete) { for (auto thd = 0; thd < num_thd; thd++) { v_thread.emplace_back(Delete, thd); }}
// /* debug */
// while(1) {
//     this_thread::sleep_for(chrono::seconds(1));
//     cache->Print();
//     std::cout << "write_size/read_size/delete_size: " << write_size << "/" << read_size << "/" << delete_size << std::endl;
// }
    if (!with_delete) { for (auto thd = 0; thd < num_thd * 2; thd++) { v_thread[thd].join(); } }
    else { for (auto thd = 0; thd < num_thd * 3; thd++) { v_thread[thd].join(); } }

    profiler.End();
    cout << "read_while_write total time: " << profiler.Micros() << " us" << endl;
    delete cache;
}
















double g_zipf_theta = 0.9;
uint64_t the_n = 0;
double denom = 0;

drand48_data buffer;

double zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++)
        sum += pow(1.0 / i, theta);
    return sum;
}

void calculateDenom(size_t max_num) {
    assert(the_n == 0);
    the_n = max_num - 1;
    denom = zeta(the_n, g_zipf_theta);
    cout << "calculateDenom OK!" << endl;
}

double zeta_2_theta = zeta(2, g_zipf_theta);

uint64_t zipf(uint64_t n, double theta) {
    assert(the_n == n);
    assert(theta == g_zipf_theta);
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / n, 1 - theta)) / (1 - zeta_2_theta / zetan);
    double u;
    drand48_r(&buffer, &u);
    double uz = u * zetan;
    if (uz < 1) return 1;
    if (uz < 1 + pow(0.5, theta)) return 2;
    return 1 + (uint64_t)(n * pow(eta * u - eta + 1, alpha));
}


enum class TYPE { uniform, ycsb_05, ycsb_06, ycsb_07, ycsb_08, ycsb_09, ycsb_099 };

// 有个 bug, 当调用 Warmup 且 后面不指定线程 thd_id 时, 会卡在 GetNewPage
void lru_test(TYPE type, double den = 1) {
    // cout << "lru_test, " << __FILE__ << ", " << __LINE__ << endl;
    int batch = global_batch;
    int num_thd = global_num_thd;
    rr::LRUCache* cache = new rr::LRUCache(batch / den * PAGE_SIZE, PAGE_SIZE);

    auto Warmup = [&cache, batch, type]() -> void
    {
        rr::RandNum_generator rng(0, batch - 1);
        bool has_res;
        for (int i = 0; i < batch; i++)
        {
            uint64_t key;
            if (type == TYPE::uniform) { key = rng.nextNum(); }
            else if (type == TYPE::ycsb_05) { key = zipf(batch - 1, 0.5); }
            else if (type == TYPE::ycsb_06) { key = zipf(batch - 1, 0.6); }
            else if (type == TYPE::ycsb_07) { key = zipf(batch - 1, 0.7); }
            else if (type == TYPE::ycsb_08) { key = zipf(batch - 1, 0.8); }
            else if (type == TYPE::ycsb_09) { key = zipf(batch - 1, 0.9); }
            else if (type == TYPE::ycsb_099) { key = zipf(batch - 1, 0.99); }

            rr::lru_cache::Page* p = nullptr;
            bool res = cache->Read(key, p);
            if (!res) {
                rr::lru_cache::Page* page = nullptr;
                rr::lru_cache::Page* prior = nullptr;
                cache->GetNewPage(page);
                assert(page);
                page->page_id_ = key;
                bool is_new = cache->Write(key, page, prior);
                if (is_new) { page->Unref(); }
                else { prior->Unref(); }
            }
            else {
                p->Unref();
            }
        }
    };

    Warmup();
    Warmup();

    // cout << "Warmup done!" << endl;
    // cout << "cache free size: " << cache->free_list_size_.load() << endl;


    auto Read = [&cache, batch, type](int thd_id) -> void
    {
        bool has_res;
        rr::RandNum_generator rng(0, batch - 1);
        for (int i = 0; i < batch; i++)
        {
            uint64_t key;
            if (type == TYPE::uniform) { key = rng.nextNum(); }
            else if (type == TYPE::ycsb_05) { key = zipf(batch - 1, 0.5); }
            else if (type == TYPE::ycsb_06) { key = zipf(batch - 1, 0.6); }
            else if (type == TYPE::ycsb_07) { key = zipf(batch - 1, 0.7); }
            else if (type == TYPE::ycsb_08) { key = zipf(batch - 1, 0.8); }
            else if (type == TYPE::ycsb_09) { key = zipf(batch - 1, 0.9); }
            else if (type == TYPE::ycsb_099) { key = zipf(batch - 1, 0.99); }

            rr::lru_cache::Page* p = nullptr;
            bool find = cache->Read(key, p, thd_id);
            // bool find = cache->Read(key, p);
            if (!find) {
                rr::lru_cache::Page* page = nullptr;
                rr::lru_cache::Page* prior = nullptr;
                cache->GetNewPage(page);
                assert(page);
                page->page_id_ = key;
                bool is_new = cache->Write(key, page, prior, thd_id);
                // bool is_new = cache->Write(key, page, prior);
                if (is_new) { page->Unref(); }
                else { prior->Unref(); }
            }
            else { p->Unref(); }
        }
    };

    // cout << __FILE__ << ", " << __LINE__ << endl;
    rr::Profiler profiler;
    profiler.Start();
    std::vector<std::thread> v_thread;
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread.emplace_back(Read, thd);
    }
    // cache->PrintRef();
    // cache->cmap_->check();
    // cout << cache->cmap_->deleted_size_ << endl;
    // cout << cache->free_list_size_ << endl;
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread[thd].join();
    }
    profiler.End();
    cout << "lru_test time: " << profiler.Micros() << " us" << endl;

    v_thread.clear();

    delete cache;
    cout << "lru_test done, " << __FILE__ << ", " << __LINE__ << endl << endl;
}





int main()
{
    simple_test();
    cout << endl << endl << endl;
    // 正确性
    read_after_write(1, false);
    read_after_write(1, true);
    cout << __FILE__ << ", " << __LINE__ << endl;
    read_after_write(2, false);
    read_after_write(2, true);

    cout << endl << endl << endl;
    cout << __FILE__ << ", " << __LINE__ << endl;
    read_while_write(1, false, false);
    read_while_write(1, true, false);
    cout << __FILE__ << ", " << __LINE__ << endl;
    read_while_write(2, false, false);
    read_while_write(2, true, false);

    cout << endl << endl << endl;
    cout << __FILE__ << ", " << __LINE__ << endl;
    read_while_write(1, false, true);
    read_while_write(1, true, true);
    read_while_write(2, false, true);
    read_while_write(2, true, true);

    // cout << endl << endl << endl;

    calculateDenom(global_batch);
    lru_test(TYPE::uniform, 1);
    lru_test(TYPE::uniform, 5);
    lru_test(TYPE::ycsb_09, 1);
    lru_test(TYPE::ycsb_09, 5);
    return 0;
}
// g++ test_lru_cahce.cpp -o test_lru_cahce.exe -ltbb -lpthread -g