//
// Created by rrzhang on 2020/6/3.
//

#ifndef DBX1000_GLOBAL_LOCK_H
#define DBX1000_GLOBAL_LOCK_H

#include <cstdint>
#include <string>
#include <map>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <condition_variable>
#include "common/global.h"
#include "common/mystats.h"

class workload;

namespace dbx1000 {

    class Buffer;
    class Index;
    class Page;
    class TableSpace;
    class LockTable;

    namespace global_lock_service {
        class GlobalLockServiceClient;
    }
    class Buffer;
    class SharedDiskClient;

    namespace global_lock {
        /// lock service 仅作为锁的协调，不缓存锁，所以没有采用 common/lock_table 下的锁表
        /// LockNode_Service 仅仅记录某个 page 被哪个 instance 持有，默认 -1 （没有任何节点持有）
        class LockNode {
        public:
            LockNode();

            int write_ins_id;
            std::mutex mtx;
            std::condition_variable cv;
        };

        /// global lock 仅作为锁的协调，不缓存锁
        class GlobalLock {
        public:
            GlobalLock();
            ~GlobalLock();
            GlobalLock(const GlobalLock &) = delete;
            GlobalLock &operator=(const GlobalLock &) = delete;

            RC LockRemote(uint64_t ins_id, TABLES table, uint64_t item_id, char *buf, size_t count);

            uint64_t GetNextTs(uint64_t thread_id);

            struct InstanceInfo {
                int instance_id;
                std::string host;
                bool init_done;
                dbx1000::Stats stats;
                bool instance_run_done;
                global_lock_service::GlobalLockServiceClient *global_lock_service_client;
            };

            /// getter and setter
            bool init_done();
            int lock_service_id() { return this->lock_service_id_; }
            void set_lock_service_id(int id) { this->lock_service_id_ = id; }
            std::map<int, std::string> &hosts_map() { return this->hosts_map_; }
            InstanceInfo *instances() { return this->instances_; }
            void set_instance_i(int instance_id);

//            Buffer *buffer() { return this->buffer_; }
//            Index *index() { return this->index_; }
            int test_num_;

        private:
            bool init_done_;
            int lock_service_id_;
            std::map<int, std::string> hosts_map_;
            InstanceInfo *instances_;                                               /// 记录各个 instance 的信息
//            std::unordered_map<uint64_t, LockNode *> lock_service_table_;

            std::map<TABLES, std::unordered_map<uint64_t, LockNode *>> lock_tables_; /// 该 table 并不是真正的锁表，而是记录哪个 page 锁被哪个节点持有

            std::atomic<uint64_t> timestamp_;

            workload *m_workload_;
//            Buffer *buffer_;
//            TableSpace *table_space_;
//            Index *index_;
//            SharedDiskClient *shared_disk_client_;
        };
    }
}


#endif //DBX1000_GLOBAL_LOCK_H
