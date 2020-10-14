#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "txn.h"

#include "common/lock_table/lock_table.h"

class ycsb_wl;
class ycsb_query;

class ycsb_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
	RC run_txn(base_query * query);

    void GetLockTableSharedPtrs(ycsb_query *m_query);
    std::set<uint64_t> GetWriteRecordSet(ycsb_query *m_query);
    RC GetWriteRecordLock(std::set<uint64_t> &write_record_set, ycsb_query *m_query);
private:
	uint64_t row_cnt;
	ycsb_wl * _wl;
};

#endif
