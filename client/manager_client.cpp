//
// Created by rrzhang on 2020/4/27.
//
#include <fstream>
#include <cassert>
#include "manager_client.h"

#include "json/json.h"
#include "api/api_txn/api_txn.h"
#include "client/txn/txn.h"

namespace dbx1000 {
    void ManagerClient::init() {
        all_txns_ = new txn_man*[g_thread_cnt]();
        Json::Reader reader;
        Json::Value root;
        ifstream in("../config.json", ios::binary);
        assert(true == reader.parse(in, root));
        api_txn_client_ = new ApiTxnClient(root["server"]["ip"].asString() + ":" + root["server"]["port"].asString());
    }

    void ManagerClient::SetTxnMan(txn_man *txnMan) { all_txns_[txnMan->get_thd_id()] = txnMan; }

    txn_man* ManagerClient::GetTxnMan(uint64_t thread_id) { return all_txns_[thread_id]; }

    ApiTxnClient* ManagerClient::api_txn_client() {
        return api_txn_client_;
    }

    uint64_t ManagerClient::row_size() {
        return row_size_;
    }

    void ManagerClient::SetRowSize(uint64_t size) {
        this->row_size_ = size;
    }
}