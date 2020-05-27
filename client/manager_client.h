//
// Created by rrzhang on 2020/4/27.
//

#ifndef DBX1000_MANAGER_CLIENT_H
#define DBX1000_MANAGER_CLIENT_H

#include <cstdint>

class txn_man;

namespace dbx1000 {
    class ApiTxnClient;

    class ManagerClient {
    public:
        void init();
        void SetTxnMan(txn_man* txnMan);
        txn_man* GetTxnMan(uint64_t txn_id);
        ApiTxnClient* api_txn_client();
        uint64_t row_size();
        void SetRowSize(uint64_t size);

    private:
        txn_man** all_txns_;
        uint64_t row_size_;
        ApiTxnClient* api_txn_client_;

    };
}

#endif //DBX1000_MANAGER_CLIENT_H
