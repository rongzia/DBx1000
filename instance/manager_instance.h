//
// Created by rrzhang on 2020/4/27.
//

#ifndef DBX1000_MANAGER_INSTANCE_H
#define DBX1000_MANAGER_INSTANCE_H

#include <cstdint>
#include <atomic>
#include <unordered_map>
#include <vector>
#include "common/lock_table/lock_table.h"
#include "common/global.h"
#include "common/mystats.h"

class txn_man;

class workload;

class Query_queue;
class Row_mvcc;

namespace dbx1000 {
    namespace global_lock_service {
        class GlobalLockServiceClient;
    }
    using namespace dbx1000::global_lock_service;
    class LockTable;
    class SharedDiskClient;
    class RowHandler;

    class ManagerInstance {
    public:
        ManagerInstance() = default;
        ~ManagerInstance();

        void ReRun();

        void Init(const std::string &shared_disk_host);
        void InitMvccs();
        void InitLockTables();

        void SetTxnMan(txn_man* m_txn);
        uint64_t GetNextTs(uint64_t thread_id);
        void AddTs(uint64_t, uint64_t);
        uint64_t GetMinTs(uint64_t);

        /// getter and setter
        int instance_id()                                               { return this->instance_id_; }
        void set_init_done(bool init_done)                              { this->init_done_ = init_done; }
        workload* m_workload()                                          { return this->m_workload_;  }
        dbx1000::Stats& stats()                                         { return this->stats_;       }
        RowHandler* row_handler()                                       { return this->row_handler_; }
        Query_queue* query_queue()                                      { return this->query_queue_; }

//    private:
        int instance_id_;
        bool init_done_;
        std::map<int, std::string> host_map_;

        atomic_uint64_t timestamp_;
        uint64_t*   all_ts_;
        std::map<TABLES, unordered_map<uint64_t, std::pair<weak_ptr<Row_mvcc>, volatile bool>> *> mvcc_vectors_;
        txn_man** txn_man_;
        dbx1000::Stats stats_;

        Query_queue* query_queue_;
        workload* m_workload_;
        RowHandler* row_handler_;
        std::map<TABLES, LockTable* > lock_table_;
        global_lock_service::GlobalLockServiceClient *global_lock_service_client_;
        SharedDiskClient * shared_disk_client_;
#ifdef RDB_BUFFER_DIFF_SIZE
        atomic_bool stop_;
#endif // RDB_BUFFER_DIFF_SIZE
    };
}

#endif //DBX1000_MANAGER_INSTANCE_H
