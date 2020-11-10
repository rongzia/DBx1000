//
// Created by rrzhang on 2020/9/26.
//

#ifndef DBX1000_TPCC_TXN_H
#define DBX1000_TPCC_TXN_H

#include "txn.h"

class tpcc_wl;
class tpcc_query;

class tpcc_txn_man : public txn_man
{
public:
    void init(thread_t * h_thd, workload * h_wl, uint64_t part_id);
    RC run_txn(base_query * query);

//    void GetLockTableSharedPtrs(base_query *m_query) override;
    void GetWriteRecordSet(base_query *m_query) override;
//    RC GetWriteRecordLock(base_query *m_query) override;
private:
    tpcc_wl * _wl;
    RC run_payment(tpcc_query * m_query);
    RC run_new_order(tpcc_query * m_query);
    RC run_order_status(tpcc_query * query);
    RC run_delivery(tpcc_query * query);
    RC run_stock_level(tpcc_query * query);
};

#endif //DBX1000_TPCC_TXN_H
