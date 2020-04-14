//
// Created by rrzhang on 2020/4/9.
//

#ifndef DBX1000_YCSB_TXN_H
#define DBX1000_YCSB_TXN_H

#include "txn.h"

class ycsb_query;
class ycsb_wl;


class ycsb_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id);
	RC run_txn(base_query * query);
private:
	uint64_t row_cnt;
	ycsb_wl * _wl;
};


#endif //DBX1000_YCSB_TXN_H
