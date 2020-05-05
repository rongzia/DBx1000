//
// Created by rrzhang on 2020/4/27.
//

#ifndef DBX1000_MANAGER_CLIENT_H
#define DBX1000_MANAGER_CLIENT_H

#include <cstdint>

class txn_man;

namespace dbx1000 {
    class ManagerClient {
    public:
//        uint64_t get_next_ts(uint64_t thread_id);
//        void add_ts(uint64_t thread_id, uint64_t ts);
//        uint64_t get_min_ts(uint64_t thread_id = 0);

        void init();

        void SetTxnMan(txn_man* txnMan);
        txn_man* GetTxnMan(uint64_t txn_id);

        txn_man** all_txns_;
    };
}

#endif //DBX1000_MANAGER_CLIENT_H
