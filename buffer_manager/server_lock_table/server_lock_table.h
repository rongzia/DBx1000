//
// Created by rrzhang on 2020/6/2.
//

#ifndef DBX1000_SERVER_LOCK_TABLE_H
#define DBX1000_SERVER_LOCK_TABLE_H

#include <unordered_map>
#include <set>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <common/lock_table/lock_table.h>
#include "common/lock_table/lock_table.h"

namespace dbx1000 {

    class ManagerServer;
    struct ServerLockNode {
        ServerLockNode();
        int write_ids;
        std::set<int> read_ids;
        std::mutex mtx;
    };

    class ServerLockTable {
    public:
        ~ServerLockTable();
        ServerLockTable(ManagerServer* manager_server);
        void Init(uint64_t start_page, uint64_t end_page) ;

        bool LockGet(int instance_id, uint64_t page_id, LockMode mode, char *&page_buf, size_t &count);


        /// getter and setter
        std::unordered_map<uint64_t, ServerLockNode*> lock_table();

    private:
        std::unordered_map<uint64_t, ServerLockNode*> lock_table_;
        ManagerServer* manager_server_;
    };
}


#endif // DBX1000_SERVER_LOCK_TABLE_H
