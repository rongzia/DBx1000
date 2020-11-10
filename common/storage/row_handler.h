//
// Created by rrzhang on 2020/7/13.
//

#ifndef DBX1000_ROW_HANDLER_H
#define DBX1000_ROW_HANDLER_H

#include "common/global.h"


class txn_man;
class row_t;

namespace dbx1000 {

    class RowItem;
    class ManagerInstance;
    class RowHandler {
    public:
        RowHandler(ManagerInstance *);
        RC GetRow(TABLES table, uint64_t key, access_t type, txn_man *txn, row_t *&);
        void ReturnRow(TABLES table, uint64_t key, access_t type, txn_man *txn, row_t *);
//        bool SnapShotReadRow(RowItem *row);
        RC ReadRow(TABLES table, uint64_t key, row_t * row, size_t size);
        RC WriteRow(TABLES table, uint64_t key, row_t * row, size_t size);

    private:
        ManagerInstance *manager_instance_;
    };
}


#endif //DBX1000_ROW_HANDLER_H
