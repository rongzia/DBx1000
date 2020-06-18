//
// Created by rrzhang on 2020/6/3.
//

#ifndef DBX1000_MANAGER_LOCK_SERVER_H
#define DBX1000_MANAGER_LOCK_SERVER_H

#include <cstdint>
#include <string>
#include <map>
#include <atomic>

class workload;

namespace dbx1000 {

    class Buffer;
    class Index;
    class Page;
    class TableSpace;
//    class ServerLockTable;
    class LockTable;
//    class Stats;
    class BufferManagerClient;
    class Buffer;
    class SharedDiskClient;



    class ManagerServer {
    public:
        ManagerServer();
        ~ManagerServer();
        ManagerServer(const ManagerServer&) = delete;
        ManagerServer &operator=(const ManagerServer&) = delete;

        uint64_t GetNextTs(uint64_t thread_id);

        struct InstanceInfo {
            int instance_id;
            std::string host;
            bool init_done;
            BufferManagerClient* buffer_manager_client;
        };

        /// getter and setter
        bool init_done();
        void set_buffer_manager_id(int id);
        std::map<int, std::string> &hosts_map();
        InstanceInfo* instances();
        void set_instance_i(int instance_id);
        LockTable* lock_table();
        Buffer* buffer() { return this->buffer_; };
        Index* index() { return this->index_; };


        int test_num;

    private:
        bool init_done_;
        int buffer_manager_id_;
        std::map<int, std::string> hosts_map_;
        InstanceInfo* instances_;

        std::atomic<uint64_t> timestamp_;

//        workload* m_workload_;
//        Buffer * buffer_;
//        TableSpace* table_space_;
//        Index* index_;
//        ServerLockTable* lock_table_;
        workload* m_workload_;
        Buffer * buffer_;
        TableSpace* table_space_;
        Index* index_;
        LockTable* lock_table_;
        SharedDiskClient * shared_disk_client_;
//        Stats* stats_;
    };
}


#endif //DBX1000_MANAGER_LOCK_SERVER_H
