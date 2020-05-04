//
// Created by rrzhang on 2020/4/9.
//

#ifndef DBX1000_YCSB_TXN_H
#define DBX1000_YCSB_TXN_H

#include "txn.h"

class ycsb_query;

class ycsb_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd);
	RC run_txn(base_query * query);
private:
	uint64_t row_cnt;
};


#endif //DBX1000_YCSB_TXN_H
