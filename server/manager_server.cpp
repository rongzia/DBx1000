//
// Created by rrzhang on 2020/4/26.
//
#include <cassert>
#include <common/txn_row_man.h>
#include <cstring>
#include "manager_server.h"

#include "global.h"     /// for g_thread_cnt
#include "row_item.h"
#include "wl.h"
#include "ycsb_wl.h"
#include "table.h"
#include "catalog.h"
#include "buffer.h"


namespace dbx1000 {
    void ManagerServer::init(workload* wl) {
        timestamp_ = 1;
	    min_ts_ = 0;
	    all_ts_ = new uint64_t[g_thread_cnt]();
	    all_txns_ = new TxnRowMan[g_thread_cnt]();
        wl_ = wl;
        for (uint32_t i = 0; i < g_thread_cnt; i++) {
            all_ts_[i] = UINT64_MAX;
            all_txns_[i].cur_row_ = new RowItem(0, ((ycsb_wl*)wl)->the_table->get_schema()->get_tuple_size());
        }
    }

    uint64_t ManagerServer::get_next_ts(uint64_t thread_id) { return timestamp_.fetch_add(1, std::memory_order_consume); }

    void ManagerServer::add_ts(uint64_t thd_id, uint64_t ts) {
        assert(ts >= all_ts_[thd_id] ||
               all_ts_[thd_id] == UINT64_MAX);
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

    TxnRowMan* ManagerServer::SetTxn(Mess_TxnRowMan messTxnRowMan) {
        uint64_t thread_id = messTxnRowMan.thread_id();

        all_txns_[thread_id].thread_id_ = thread_id;
        all_txns_[thread_id].txn_id_    = messTxnRowMan.txn_id();
        all_txns_[thread_id].ts_ready_  = messTxnRowMan.ts_ready();
        assert(messTxnRowMan.has_cur_row());
        all_txns_[thread_id].cur_row_->MergeFrom(messTxnRowMan.cur_row());
        all_txns_[thread_id].timestamp_ = messTxnRowMan.timestamp();
        return &all_txns_[thread_id];
    }

//    void ManagerServer::SetTxn(uint64_t thread_id, uint64_t txn_id, bool ts_ready, RowItem* cur_row, uint64_t timestamp) {
//        TxnRowMan::CopyTxnRowMan(all_txns_[thread_id], thread_id, txn_id, ts_ready, cur_row, timestamp);
//    }
}