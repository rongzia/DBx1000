//
// Created by rrzhang on 2020/4/23.
//

#include <client/manager_client.h>
#include <memory>
#include "cs_api.h"
#include "row_mvcc.h"
#include "manager_server.h"
#include "profiler.h"
#include "stats.h"
#include "myhelper.h"
#include "wl.h"
#include "ycsb_wl.h"
#include "cstring"

namespace dbx1000 {
    int API::count_ = 0;
    std::mutex API::mtx_;
//    workload *API::wl_ = nullptr;

    RC API::get_row(uint64_t key, access_t type, txn_man* txn, int accesses_index) {
//        std::cout << "API::get_row, key:" << key << ", type:" << (type == RD ? "R_REQ" : "P_REQ") << std::endl;
        TsType ts_type = (type == RD || type == SCAN) ? R_REQ : P_REQ;
//        cout << "__LINE__" << __LINE__ << endl;
        uint64_t thread_id = txn->get_thd_id();

        TxnRowMan txnRowMan(thread_id, txn->get_txn_id(), txn->ts_ready, new RowItem(key, 0, false), txn->get_ts());
//        cout << "__LINE__" << __LINE__ << endl;
        glob_manager_server->SetTxn(&txnRowMan);
//        cout << "__LINE__" << __LINE__ << endl;
        RC rc = glob_manager_server->row_mvccs_[key]->access(&glob_manager_server->all_txns_[thread_id], ts_type);
//        cout << "__LINE__" << __LINE__ << endl;
        mtx_.lock();
        count_++;
        mtx_.unlock();
//        cout << "__LINE__" << __LINE__ << endl;
        delete txnRowMan.cur_row_;
        cout << "__LINE__" << __LINE__ << endl;
        cout << "accesses_index" << accesses_index << endl;
        cout << "orig_row.key : " << txn->accesses[accesses_index]->orig_row->key_ << endl;
        cout << "orig_row.size_ : " << txn->accesses[accesses_index]->orig_row->size_ << endl;
        cout << "orig_row.row_ : " << txn->accesses[accesses_index]->orig_row->row_ << endl;
        txnRowMan.cur_row_ = txn->accesses[accesses_index]->orig_row;

//        cout << "__LINE__" << __LINE__ << endl;
//        cout << "rc : " << rc << endl;

        if (RCOK == rc) {
//        cout << "__LINE__" << __LINE__ << endl;
            memcpy(txnRowMan.cur_row_->row_, glob_manager_server->all_txns_[thread_id]->cur_row_->row_
                    , glob_manager_server->all_txns_[thread_id]->cur_row_->size_);
//        cout << "__LINE__" << __LINE__ << endl;
        }
        else if (WAIT == rc) {
//        cout << "__LINE__" << __LINE__ << endl;
    cout << "API::get_row WAIT" << endl;
            /// TODO: WAIT
//        cout << "__LINE__" << __LINE__ << endl;
            std::unique_ptr<dbx1000::Profiler> profiler(new dbx1000::Profiler());
            profiler->Start();
//        cout << "__LINE__" << __LINE__ << endl;
            while (!glob_manager_client->all_txns_[txn->get_thd_id()]->ts_ready) { PAUSE }
//        cout << "__LINE__" << __LINE__ << endl;
            profiler->End();
            INC_TMP_STATS(txn->get_thd_id(), time_wait, profiler->Nanos());
//        cout << "__LINE__" << __LINE__ << endl;
            txnRowMan.ts_ready_ = true;
//        cout << "__LINE__" << __LINE__ << endl;
            memcpy(txnRowMan.cur_row_->row_, glob_manager_server->all_txns_[thread_id]->cur_row_->row_
                    , glob_manager_server->all_txns_[thread_id]->cur_row_->size_);
//        cout << "__LINE__" << __LINE__ << endl;
    cout << "out API::get_row WAIT" << endl;
        }
//        std::cout << "out API::get_row" << std::endl;
        return rc;

    }

    void API::return_row(uint64_t key, access_t type, txn_man* txn, int accesses_index) {
//    cout << "API::return_row" << endl;
        RC rc;
//            cout << "return_row::key : " << key
//            << ", orig_row->key_" << txn->accesses[accesses_index]->orig_row->key_
//            << ", data->key_" << txn->accesses[accesses_index]->data->key_ << endl;
        TxnRowMan txnRowMan(txn->get_thd_id(), txn->get_txn_id(), txn->ts_ready, nullptr, txn->get_ts());
        if (type == XP) {
            txnRowMan.cur_row_ = txn->accesses[accesses_index]->orig_row;
            assert(nullptr != txnRowMan.cur_row_);
            glob_manager_server->SetTxn(&txnRowMan);

//            cout << "all_txns_[txnRowMan.thread_id_].key : " << glob_manager_server->all_txns_[txnRowMan.thread_id_]->cur_row_->key_
//            << ", txnRowMan.key : " << txnRowMan.cur_row_->key_ << endl;

            rc = glob_manager_server->row_mvccs_[key]->access(glob_manager_server->all_txns_[txnRowMan.thread_id_], XP_REQ);
            return;
        }
        if (type == WR) {
            assert (type == WR && txn->accesses[accesses_index]->data != nullptr);
            txnRowMan.cur_row_ = txn->accesses[accesses_index]->data;
            glob_manager_server->SetTxn(&txnRowMan);

//            cout << "all_txns_[txnRowMan.thread_id_].key : " << glob_manager_server->all_txns_[txnRowMan.thread_id_]->cur_row_->key_
//            << ", txnRowMan.key : " << txnRowMan.cur_row_->key_ << endl;

            rc = glob_manager_server->row_mvccs_[key]->access(glob_manager_server->all_txns_[txnRowMan.thread_id_], W_REQ);
            return;
        }
        if (type == RD) { return;}
        if (type == SCAN) { cout << "SCAN." << endl; return;}
        assert(false);
        assert(RCOK == rc);
    }

    void API::set_wl_sim_done() {
        glob_manager_server->wl_->sim_done_.exchange(true, std::memory_order_consume);
        assert(true == glob_manager_server->wl_->sim_done_.load(std::memory_order_consume));
    }

    bool API::get_wl_sim_done() {
        return glob_manager_server->wl_->sim_done_.load(std::memory_order_consume);
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

        glob_manager_client->all_txns_[thread_id]->ts_ready = global_server_txn->ts_ready_;
        glob_manager_client->all_txns_[thread_id]->accesses[row_cnt]->orig_row->MergeFrom(*(global_server_txn->cur_row_));
//        RowItem::CopyRowItem(glob_manager_client->all_txns_[thread_id]->accesses[row_cnt]->orig_row, global_server_txn->cur_row_);
    }
}