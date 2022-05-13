#ifndef LOCK_SERVER
#define LOCK_SERVER

#include <mutex>
#include <condition_variable>
#include <tbb/concurrent_unordered_map.h>
#include <map>

#include "config.h"
#include "global.h"

class workload;

namespace rdb {
    struct DirectoryItem {
        DirectoryItem();
        int item_owner;
        std::mutex mtx;
        std::condition_variable cv;
    };

    class LockServer {
    public:
        ~LockServer();
        void Init();
        workload* m_wl;


        std::map<TABLES, tbb::concurrent_unordered_map<uint64_t, DirectoryItem*>> directories_;
    };
} // rdb

#endif // LOCK_SERVER