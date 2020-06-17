//
// Created by rrzhang on 2020/4/27.
//

#ifndef DBX1000_MANAGER_INSTANCE_H
#define DBX1000_MANAGER_INSTANCE_H

#include <cstdint>
#include <atomic>
#include <unordered_map>
#include "common/global.h"
#include "common/mystats.h"

class txn_man;

class workload;

class Query_queue;
class Row_mvcc;

namespace dbx1000 {
    class InstanceClient;
    class Buffer;
    class Index;
    class LockTable;
    class Page;
    class RowItem;
    class TableSpace;
    class SharedDiskClient;

    class ManagerInstance {
    public:
        ManagerInstance();
        ~ManagerInstance();

        void Init(const std::string &shared_disk_host);
        void InitBufferForTest();
        void InitMvccs();
        void InitLockTable();

        uint64_t GetNextTs(uint64_t thread_id);
        RC GetRow(uint64_t key, access_t type, txn_man * txn, RowItem *&);
        void ReturnRow(uint64_t key, access_t type, txn_man * txn, RowItem *);
        void SetTxnMan(txn_man* m_txn);
        void AddTs(uint64_t, uint64_t);
        uint64_t GetMinTs(uint64_t);
        bool RowFromDB(RowItem* row);
        bool RowToDB(RowItem* row);

        /// getter and setter
        int instance_id();
        void set_init_done(bool init_done);
        void set_instance_id(int);
        std::map<int, std::string>& host_map();
        Stats stats();
        std::unordered_map<uint64_t, Row_mvcc*> mvcc_map();
        Query_queue* query_queue();
        workload* m_workload();
        Buffer* buffer();
        TableSpace* table_space();
        Index* index();
        LockTable* &lock_table();
        InstanceClient *instance_rpc_handler();
        void set_instance_rpc_handler(InstanceClient*);
        SharedDiskClient * shared_disk_client();
    private:
        bool init_done_;
        int instance_id_;
        std::map<int, std::string> host_map_;

        std::atomic<uint64_t> timestamp_;
        uint64_t*   all_ts_;
        std::unordered_map<uint64_t, Row_mvcc*> mvcc_map_;
        txn_man** txn_man_;
        dbx1000::Stats stats_;

        Query_queue* query_queue_;
        workload* m_workload_;
        Buffer * buffer_;
        TableSpace* table_space_;
        Index* index_;
        LockTable* lock_table_;
        InstanceClient *instance_rpc_handler_;
        SharedDiskClient * shared_disk_client_;
    };
}

#endif //DBX1000_MANAGER_INSTANCE_H
