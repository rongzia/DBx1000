//
// Created by rrzhang on 2020/6/3.
//

#ifndef DBX1000_MANAGER_LOCK_SERVICE_H
#define DBX1000_MANAGER_LOCK_SERVICE_H

#include <cstdint>
#include <string>
#include <map>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include "common/global.h"

class workload;

namespace dbx1000 {

    class Buffer;
    class Index;
    class Page;
    class TableSpace;
    class LockTable;
//    class Stats;
    class LockServiceClient;
    class Buffer;
    class SharedDiskClient;

    /// lock service 仅作为锁的协调，不缓存锁，所以没有采用 common/lock_table 下的锁表
    /// LockNode_Service 仅仅记录某个 page 被哪个 instance 持有，默认 -1 （没有任何节点持有）
    class LockNode_Service{
    public:
        LockNode_Service();
        int write_ins_id;
        std::mutex mtx;
    };

    /// lock service 仅作为锁的协调，不缓存锁
    class ManagerLockService {
    public:
        ManagerLockService();
        ~ManagerLockService();
        ManagerLockService(const ManagerLockService&) = delete;
        ManagerLockService &operator=(const ManagerLockService&) = delete;

        RC LockRemote(uint64_t ins_id, uint64_t page_id, char *page_buf, size_t count);

        uint64_t GetNextTs(uint64_t thread_id);

        struct InstanceInfo {
            int instance_id;
            std::string host;
            bool init_done;
            LockServiceClient* lock_service_client;
        };

        /// getter and setter
        bool init_done();
        void set_buffer_manager_id(int id)      { this->buffer_manager_id_ = id; }
        std::map<int, std::string> &hosts_map() { return this->hosts_map_; }
        InstanceInfo* instances()               { return this->instances_; }
        void set_instance_i(int instance_id);
//        LockTable* lock_table()                 { return this->lock_table_; }
        Buffer* buffer()                        { return this->buffer_; }
        Index* index()                          { return this->index_; }

    private:
        bool init_done_;
        int buffer_manager_id_;
        std::map<int, std::string> hosts_map_;
        InstanceInfo* instances_;                                               /// 记录各个 instance 的信息
        std::unordered_map<uint64_t , LockNode_Service*> lock_service_table_;     /// 该 table 并不是真正的锁表，而是记录哪个 page 锁被哪个节点持有

        std::atomic<uint64_t> timestamp_;

        workload* m_workload_;
        Buffer * buffer_;
        TableSpace* table_space_;
        Index* index_;
        SharedDiskClient * shared_disk_client_;
//        Stats* stats_;
    };
}


#endif //DBX1000_MANAGER_LOCK_SERVICE_H
