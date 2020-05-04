//
// Created by rrzhang on 2020/5/4.
//

#ifndef DBX1000_DBX1000SERVICEUTIL_H
#define DBX1000_DBX1000SERVICEUTIL_H

#include <cstdint>
#include <cstdlib>

class txn_man;

namespace dbx1000 {

    class Mess_RowItem;
    class Mess_TxnRowMan;

    class DBx1000ServiceUtil {
    public:
        static void Set_Mess_RowItem(uint64_t key, char *row, size_t size, Mess_RowItem *messRowItem);
        static void Set_Mess_TxnRowMan(txn_man *txnMan, Mess_RowItem* messRowItem, Mess_TxnRowMan *messTxnRowMan);
    };
}


#endif //DBX1000_DBX1000SERVICEUTIL_H
