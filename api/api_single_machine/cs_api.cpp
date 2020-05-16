//
// Created by rrzhang on 2020/4/23.
//
#ifndef WITH_RPC

#include <memory>
#include <cstring>
#include "cs_api.h"

#include "client/manager_client.h"
#include "common/mystats.h"
#include "common/myhelper.h"
#include "server/concurrency_control/row_mvcc.h"
#include "server/manager_server.h"
#include "server/workload/wl.h"
#include "server/workload/ycsb_wl.h"
#include "util/profiler.h"

namespace dbx1000 {
    int API::count_ = 0;
    std::mutex API::mtx_;
//    workload *API::wl_ = nullptr;

    RC API::get_row(uint64_t key, access_t type, txn_man* txn, int accesses_index) {
//        std::cout << "API::get_row, key:" << key << ", type:" << (type == RD ? "R_REQ" : "P_REQ") << std::endl;
        TsType ts_type = (type == RD || type == SCAN) ? R_REQ : P_REQ;
        uint64_t thread_id = txn->get_thd_id();

        glob_manager_server->SetTxn(thread_id, txn->get_txn_id(), txn->ts_ready, key, nullptr, 0, txn->get_ts());
        assert(txn->accesses[accesses_index]->orig_row->key_ == glob_manager_server->all_txns_[thread_id].cur_row_->key_);
        RC rc = glob_manager_server->row_mvccs_[key]->access(&glob_manager_server->all_txns_[thread_id], ts_type);
        assert(txn->accesses[accesses_index]->orig_row->key_ == glob_manager_server->all_txns_[thread_id].cur_row_->key_);

        if (RCOK == rc) {
            memcpy(txn->accesses[accesses_index]->orig_row->row_
                   , glob_manager_server->all_txns_[thread_id].cur_row_->row_, glob_manager_server->all_txns_[thread_id].cur_row_->size_);
        }
        else if (WAIT == rc) {
//            cout << "API::get_row, type == WAIT" << endl;
            dbx1000::Profiler profiler;
            profiler.Start();
            while (!txn->ts_ready) { PAUSE }
            profiler.End();
            stats.tmp_stats[thread_id]->time_wait += profiler.Nanos();

            txn->ts_ready = true;
            memcpy(txn->accesses[accesses_index]->orig_row->row_
                   , glob_manager_server->all_txns_[thread_id].cur_row_->row_, glob_manager_server->all_txns_[thread_id].cur_row_->size_);
        }
        return rc;

    }

    void API::return_row(uint64_t key, access_t type, txn_man* txn, int accesses_index) {
//    cout << "API::return_row" << endl;
        if(RD == type) { return;}
        else if(SCAN == type) { cout << "SCAN." << endl; return;}

        uint64_t thread_id = txn->get_thd_id();

        RC rc;
        if (type == XP) {
            glob_manager_server->SetTxn(thread_id, txn->get_txn_id(), txn->ts_ready
                    , key
                    , glob_manager_client->all_txns_[thread_id]->accesses[accesses_index]->orig_row->row_
                    , glob_manager_client->all_txns_[thread_id]->accesses[accesses_index]->orig_row->size_
                    , txn->get_ts());
            rc = glob_manager_server->row_mvccs_[key]->access(&glob_manager_server->all_txns_[thread_id], XP_REQ);
        } else if(type == WR) {
            glob_manager_server->SetTxn(thread_id, txn->get_txn_id(), txn->ts_ready
                    , key
                    , glob_manager_client->all_txns_[thread_id]->accesses[accesses_index]->data->row_
                    , glob_manager_client->all_txns_[thread_id]->accesses[accesses_index]->data->size_
                    , txn->get_ts());
            rc = glob_manager_server->row_mvccs_[key]->access(&glob_manager_server->all_txns_[thread_id], W_REQ);
        } else {
            assert(false);
        }
        assert(RCOK == rc);
    }

    void API::set_wl_sim_done() {
        glob_manager_server->wl_->sim_done_.store(true);
        assert(true == glob_manager_server->wl_->sim_done_.load());
    }

    bool API::get_wl_sim_done() {
        return glob_manager_server->wl_->sim_done_.load();
    }

    uint64_t API::get_next_ts(uint64_t thread_id) {
        return glob_manager_server->get_next_ts(thread_id);
    }

    void API::add_ts(uint64_t thread_id, uint64_t ts) {
        glob_manager_server->add_ts(thread_id, ts);
    }

    void API::SetTsReady(TxnRowMan* global_server_txn) {
        assert(true == global_server_txn->ts_ready_);
        uint64_t thread_id = global_server_txn->thread_id_;
        int row_cnt = glob_manager_client->all_txns_[thread_id]->row_cnt;

        glob_manager_client->all_txns_[thread_id]->ts_ready = true;
        memcpy(glob_manager_client->all_txns_[thread_id]->accesses[row_cnt]->orig_row->row_
            , glob_manager_server->all_txns_[thread_id].cur_row_->row_, glob_manager_server->all_txns_[thread_id].cur_row_->size_);
    }
}

#endif // no define WITH_RPC