//
// Created by rrzhang on 2020/4/27.
//

#include "manager_client.h"

#include "client/txn/txn.h"

namespace dbx1000 {
    void ManagerClient::init() {
        all_txns_ = new txn_man*[g_thread_cnt]();
    }

    void ManagerClient::SetTxnMan(txn_man *txnMan) { all_txns_[txnMan->get_thd_id()] = txnMan; }

    txn_man* ManagerClient::GetTxnMan(uint64_t thread_id) { return all_txns_[thread_id]; }
}