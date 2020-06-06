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

        /// getter and setter
        Query_queue* query_queue();
        BufferManagerClient *buffer_manager_rpc_handler();
    private:
        bool init_done_;
        int instance_id_;

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
