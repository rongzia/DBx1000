//
// Created by rrzhang on 2020/4/27.
//

#ifndef DBX1000_MANAGER_CLIENT_H
#define DBX1000_MANAGER_CLIENT_H

#include <cstdint>
#include <string>
#include "common/workload/wl.h"

class txn_man;

class workload;

class Query_queue;
class Page_mvcc;

namespace dbx1000 {
    class BufferManagerClient;
    class Buffer;
    class Index;
    class LockTable;

    class ManagerClient {
    public:
        ManagerClient();
        ~ManagerClient();
        void Init();
        void InitBufferForTest();
        uint64_t GetNextTs(uint64_t thread_id);
        void add_ts(uint64_t thread_id, uint64_t ts);

        /// getter and setter
        Query_queue* query_queue();
        BufferManagerClient *buffer_manager_rpc_handler();
        std::map<uint64_t, Page_mvcc*> mvcc_map();
    private:
        bool init_done_;
        int instance_id_;
        std::atomic<uint64_t> timestamp_;
        uint64_t*   all_ts_;
        std::map<uint64_t, Page_mvcc*> mvcc_map_;

        Query_queue* query_queue_;
        workload* m_workload_;
        std::map<int, std::string> host_map_;
        BufferManagerClient *buffer_manager_rpc_handler_;
        Buffer * buffer_;
        Index* index_;
        LockTable* lock_table_;
    };
}

#endif //DBX1000_MANAGER_CLIENT_H
