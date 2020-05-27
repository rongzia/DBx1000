//
// Created by rrzhang on 2020/4/26.
//
#include <cassert>
#include <cstring>
#include "manager_server.h"

#include "api/api_cc/api_cc.h"
#include "common/row_item.h"
#include "common/txn_row_man.h"
#include "server/buffer/buffer.h"
#include "server/storage/table.h"
#include "server/storage/catalog.h"
#include "server/workload/ycsb_wl.h"


namespace dbx1000 {

    ManagerServer::ManagerServer() {
        timestamp_ = 1;
	    min_ts_ = 0;
	    all_ts_ = new uint64_t[g_thread_cnt]();
	    all_txns_ = new TxnRowMan[g_thread_cnt]();
        txn_port_ = new std::string[g_thread_cnt]();
        api_con_ctl_clients_ = new ApiConCtlClient*[g_thread_cnt]();
        thread_done_ = new bool[g_thread_cnt]();
        for (uint32_t i = 0; i < g_thread_cnt; i++) {
            all_ts_[i] = UINT64_MAX;
            txn_port_[i] = "";
            thread_done_[i] = false;
        }
        init_wl_done_ = false;
    }
    void ManagerServer::InitWl(ycsb_wl* wl) {
        wl_ = wl;
        for(int i = 0; i < g_thread_cnt; i++) {
            all_txns_[i].cur_row_ = new RowItem(UINT64_MAX, wl_->the_table->get_schema()->get_tuple_size());
        }
        init_wl_done_ = true;
    }

    void ManagerServer::SetTxnReady(uint64_t thread_id, const std::string& host) {
        txn_port_[thread_id] = host;
        api_con_ctl_clients_[thread_id] = new ApiConCtlClient(host);
//        api_con_ctl_clients_[thread_id]->Test();
    }

    bool ManagerServer::AllTxnReady() {
        bool flag = true;
        for(size_t i = 0; i < g_thread_cnt; i++) {
            if("" == txn_port_[i]) { flag = false; }
        }
        return flag;
    }

    bool ManagerServer::AllThreadDone() {
        bool flag = true;
        for(size_t i = 0; i < g_thread_cnt; i++) {
            if(false == thread_done_[i]) { flag = false; }
        }
        return flag;
    }

    uint64_t ManagerServer::get_next_ts(uint64_t thread_id) { return timestamp_.fetch_add(1, std::memory_order_consume); }

    void ManagerServer::add_ts(uint64_t thd_id, uint64_t ts) {
//        assert(ts >= all_ts_[thd_id] ||
//               all_ts_[thd_id] == UINT64_MAX);
        all_ts_[thd_id] = ts;
    }

    uint64_t ManagerServer::GetMinTs(uint64_t thread_id) {
        uint64_t min = UINT64_MAX;
        for (uint32_t i = 0; i < g_thread_cnt; i++)
            if (all_ts_[i] < min)
                min = all_ts_[i];
        if (min > min_ts_)
            min_ts_ = min;
        return min_ts_;
    }

#ifdef WITH_RPC
    TxnRowMan* ManagerServer::SetTxn(Mess_TxnRowMan messTxnRowMan) {
        uint64_t thread_id = messTxnRowMan.thread_id();

        all_txns_[thread_id].thread_id_ = thread_id;
        all_txns_[thread_id].txn_id_    = messTxnRowMan.txn_id();
        all_txns_[thread_id].ts_ready_  = messTxnRowMan.ts_ready();
        assert(messTxnRowMan.has_cur_row());
        all_txns_[thread_id].cur_row_->key_ = messTxnRowMan.cur_row().key();
//        assert(messTxnRowMan.cur_row().row());
        memcpy(all_txns_[thread_id].cur_row_->row_, messTxnRowMan.cur_row().row().data(), messTxnRowMan.cur_row().size());
        all_txns_[thread_id].timestamp_ = messTxnRowMan.timestamp();
        return &all_txns_[thread_id];
    }
#endif // WITH_RPC

    TxnRowMan* ManagerServer::SetTxn(uint64_t thread_id, uint64_t txn_id, bool ts_ready
                                     , uint64_t key, char *row, size_t size, uint64_t timestamp) {
//        TxnRowMan* temp = &all_txns_[thread_id];
        all_txns_[thread_id].thread_id_     = thread_id;
        all_txns_[thread_id].txn_id_        = txn_id;
        all_txns_[thread_id].ts_ready_      = ts_ready;
        all_txns_[thread_id].cur_row_->key_ = key;
        if(size > 0) {
            memcpy(all_txns_[thread_id].cur_row_->row_, row, size);
        }
        all_txns_[thread_id].timestamp_     = timestamp;
        return &all_txns_[thread_id];
    }
}