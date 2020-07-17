//
// Created by rrzhang on 2020/7/17.
//

#include <tbb/concurrent_lru_cache.h>

int main() {
    tbb::concurrent_lru_cache<int, char *> lruCache(100);
    lruCache.mo

    return 0;
}