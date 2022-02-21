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
using namespace std;

int global_num_thd = 28;
int global_batch = 100;


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


void calculateDenom(size_t max_num)
{
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
    double eta = (1 - pow(2.0 / n, 1 - theta)) /
        (1 - zeta_2_theta / zetan);
    double u;
    drand48_r(&buffer, &u);
    double uz = u * zetan;
    if (uz < 1) return 1;
    if (uz < 1 + pow(0.5, theta)) return 2;
    return 1 + (uint64_t)(n * pow(eta * u - eta + 1, alpha));
cout << __FILE__ << ", " << __LINE__ << endl;
}





void multi_thd_read_after_warmup(int fenmu) {
    int batch = global_batch;
    int num_thd = global_num_thd;
    rr::LRUCache* cache = new rr::LRUCache(batch / fenmu * PAGE_SIZE, PAGE_SIZE);

    auto Warmup = [&cache, batch]() -> void
    {
        bool has_res;
        for (int i = 0; i < global_batch; i++)
        {
            // uint64_t key = zipf(global_batch - 1, g_zipf_theta);
            uint64_t key = i;
            rr::lru_cache::Page page;
            page.set_key(i);
            bool res = cache->Read(&page);
            if (!res) {
                cache->GetNewPage(&page);
                page.set_id(i);
                cache->Write(key, page.ptr_);
            }
        }
    };

    Warmup();
    cache->Print();
    Warmup();

    cout << "Warmup done!" << endl;
    cout << "cache free size: " << cache->free_list_size_.load() << endl;

    // auto Read = [&cache, batch](int thd_id) -> void
    // {
    //     bool has_res;
    //     for (int i = 0; i < batch; i++)
    //     {
    //         uint64_t key = zipf(global_batch - 1, g_zipf_theta);
    //         rr::lru_cache::Page page;
    //         page.key_ = key;
    //         bool find = cache->Read(page, thd_id);
    //         if (!find) {
    //             cache->GetNewPage(page);
    //             page.key_ = key;
    //             cache->Write(key, page.ptr_, thd_id);
    //         }
    //     }
    // };

    // rr::Profiler profiler;
    // profiler.Start();
    // std::vector<std::thread> v_thread;
    // for (auto thd = 0; thd < num_thd; thd++)
    // {
    //     v_thread.emplace_back(Read, thd);
    // }
    // for (auto thd = 0; thd < num_thd; thd++)
    // {
    //     v_thread[thd].join();
    // }
    // profiler.End();
    // cout << "ConcurrentLinkedHashMap time: " << profiler.Micros() << endl;

    // v_thread.clear();

    delete cache;
}


int main()
{
    calculateDenom(global_batch);
    multi_thd_read_after_warmup(1);
    return 0;
}
// g++ lru_cache_test2.cpp -o lru_cache_test2.exe -ltbb -lpthread -g