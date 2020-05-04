//
// Created by rrzhang on 2020/4/23.
//
#include "txn_row_man.h"

#include "row_item.h"

namespace dbx1000 {
    TxnRowMan::TxnRowMan() {}

    TxnRowMan::~TxnRowMan() {}

    TxnRowMan::TxnRowMan(uint64_t thread_id, uint64_t txn_id, bool ts_ready, dbx1000::RowItem* cur_row, uint64_t timestamp) {
        this->thread_id_ = thread_id;
        this->txn_id_    = txn_id;
        this->ts_ready_  = ts_ready;
        this->cur_row_   = cur_row;
        this->timestamp_ = timestamp;
    }

    void TxnRowMan::CopyTxnRowMan(TxnRowMan* to, TxnRowMan* from) {
        assert(nullptr != from->cur_row_);
        assert(nullptr != from->cur_row_ && nullptr != to->cur_row_);
//        assert(nullptr != from->cur_row_->row_ && nullptr != to->cur_row_->row_);

        to->thread_id_ = from->thread_id_;
        to->txn_id_    = from->txn_id_;
        to->ts_ready_  = from->ts_ready_;
        to->cur_row_->MergeFrom(*(from->cur_row_));
        to->timestamp_ = from->timestamp_;
    }

    void TxnRowMan::CopyTxnRowMan(TxnRowMan* to, uint64_t thread_id, uint64_t txn_id, bool ts_ready, RowItem* cur_row, uint64_t timestamp) {
        to->thread_id_ = thread_id;
        to->txn_id_    = txn_id;
        to->ts_ready_  = ts_ready;
        // TODO : if to == nullptr or from == nullptr
        assert(nullptr != to->cur_row_);
        if(nullptr != cur_row) {
//            RowItem::CopyRowItem(to->cur_row_, cur_row);
//            to->cur_row_ = cur_row;
        }
        to->timestamp_ = timestamp;
    }

    void TxnRowMan::PrintTxnRowMan() {
        cout << "TxnRowMan::PrintTxnRowMan" << endl;
        cout << "    thread_id_      : " << this->thread_id_ << endl;
        cout << "    txn_id_         : " << this->txn_id_ << endl;
        cout << "    ts_ready_       : " << this->ts_ready_ << endl;
        cout << "    cur_row_->key_  : " << this->cur_row_->key_ << endl;
        cout << "    cur_row_->size_ : " << this->cur_row_->size_ << endl;
    cout << "    timestamp_      : " << this->timestamp_ << endl;
    }

//    void TxnRowMan::set_txn_id(uint64_t txn_id) { this->txn_id_ = txn_id; }
//
//    ts_t TxnRowMan::get_ts() { return this->timestamp_; }
//
//    uint64_t TxnRowMan::get_thd_id() { return thread_id_; }
}