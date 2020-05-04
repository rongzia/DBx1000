//
// Created by rrzhang on 2020/4/23.
//

#ifndef DBX1000_TXN_ROW_MAN_H
#define DBX1000_TXN_ROW_MAN_H

#include <cstdlib>
#include "global.h"
#include "api.pb.h"

namespace dbx1000 {
    class RowItem;

    class TxnRowMan {
    public:
        TxnRowMan();
        TxnRowMan(uint64_t thread_id, uint64_t txn_id, bool ts_ready, dbx1000::RowItem* cur_row, uint64_t timestamp);
        TxnRowMan(const TxnRowMan &) = delete;
        TxnRowMan &operator=(const TxnRowMan &) = delete;
        ~TxnRowMan();

        static void CopyTxnRowMan(TxnRowMan* to, TxnRowMan* from);
        static void CopyTxnRowMan(TxnRowMan* to, uint64_t thread_id, uint64_t txn_id, bool ts_ready, RowItem* cur_row, uint64_t timestamp);

        void PrintTxnRowMan();

//	    void 			set_txn_id(uint64_t txn_id);
//	    uint64_t 		get_txn_id();

//        ts_t            get_ts();
//        uint64_t        get_thd_id();

        uint64_t        thread_id_;
        uint64_t 		txn_id_;
//        bool volatile ts_ready_;
        bool            ts_ready_;
//        dbx1000::RowItem volatile *cur_row_;
        RowItem*        cur_row_;
        uint64_t        timestamp_;
    };
}


#endif //DBX1000_TXN_ROW_MAN_H
