//
// Created by rrzhang on 2020/5/4.
//

#include "dbx1000_service_util.h"

#include "api.pb.h"
#include "client/txn/txn.h"

namespace dbx1000 {
    void DBx1000ServiceUtil::Set_Mess_RowItem(uint64_t key, char* row, size_t size, Mess_RowItem* messRowItem) {
        messRowItem->set_key(key);
        if(nullptr == row) {
            messRowItem->set_row("");
        } else {
            messRowItem->set_row(row, size);
        }
        messRowItem->set_size((uint64_t)size);
    }

    void DBx1000ServiceUtil::Set_Mess_TxnRowMan(txn_man* txnMan, Mess_RowItem* messRowItem, Mess_TxnRowMan* messTxnRowMan) {
        messTxnRowMan->set_thread_id(txnMan->get_thd_id());
        messTxnRowMan->set_txn_id(txnMan->get_txn_id());
        messTxnRowMan->set_ts_ready(txnMan->ts_ready);
        messTxnRowMan->set_allocated_cur_row(messRowItem);
        messTxnRowMan->set_timestamp(txnMan->get_ts());
    }
}