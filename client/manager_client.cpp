//
// Created by rrzhang on 2020/4/27.
//
#include <fstream>
#include <cassert>
#include "manager_client.h"

#include "config.h"
#include "api/api_txn/api_txn.h"
#include "client/benchmarks/ycsb_query.h"
#include "client/benchmarks/query.h"
#include "client/txn/txn.h"
#include "common/global.h"
#include "common/workload/ycsb_wl.h"
#include "common/workload/wl.h"
#include "json/json.h"

namespace dbx1000 {
     size_t ManagerClient::process_cnt_ = PROCESS_CNT;
     size_t ManagerClient::thread_cnt_  = THREAD_CNT_PER_NODE;

    void ManagerClient::Init() {
        this->host_ = std::string(PROJECT_ROOT_PATH) + "congif.json";
//        this->process_id_ = process_id;

        Json::Value root;
        std::ifstream in(this->host_, std::ifstream::binary);
        in >> root;
        this->all_txn_mans_ = new txn_man*[thread_cnt_]();
        this->api_txn_client_ = new ApiTxnClient(root["server"]["ip"].asString() + ":" + root["server"]["port"].asString());
        this->m_wl_ = new ycsb_wl();
        query_queue_ = new Query_queue();
    }

    void ManagerClient::SetTxnMan(txn_man *txnMan) { all_txn_mans_[txnMan->get_thd_id()] = txnMan; }
    txn_man* ManagerClient::GetTxnMan(uint64_t thread_id) { return all_txn_mans_[thread_id]; }

    ApiTxnClient* ManagerClient::api_txn_client() { return api_txn_client_; }
    workload *ManagerClient::m_wl() { return this->m_wl_; }
    Query_queue *ManagerClient::query_queue() { return this->query_queue_; }
    std::string ManagerClient::host() { return this->host_; }
}