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
    class ApiTxnClient;

    class ManagerClient {
    public:
        static size_t process_cnt_;
        static size_t thread_cnt_;

        void Init();

        void SetTxnMan(txn_man *txnMan);
        txn_man *GetTxnMan(uint64_t txn_id);

        ApiTxnClient *api_txn_client();
        workload *m_wl();
        Query_queue *query_queue();
        std::string host();
//        uint64_t process_id();

    private:
        std::string host_;
//        uint64_t process_id_;
        txn_man **all_txn_mans_;
        ApiTxnClient *api_txn_client_;
        workload *m_wl_;
        Query_queue *query_queue_;
    };
}

#endif //DBX1000_MANAGER_CLIENT_H
