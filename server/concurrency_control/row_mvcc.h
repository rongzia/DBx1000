#pragma once

#include <cstdlib>
#include "global.h"
#include "row_item.h"
#include "txn_row_man.h"

class table_t;
class Catalog;
class txn_man;

// Only a constant number of versions can be maintained.
// If a request accesses an old version that has been recycled,   
// simply abort the request.


#if CC_ALG == MVCC
class WriteHisEntry {
public:
	bool valid;		// whether the entry contains a valid version
	bool reserved; 	// when valid == false, whether the entry is reserved by a P_REQ 
	ts_t ts;
	dbx1000::RowItem * row;
	~WriteHisEntry() {} /// delete WriteHisEntry 时，不会调用 row 的析构函数
};

class ReqEntry {
public:
	bool valid;
	TsType              type; // P_REQ or R_REQ
	ts_t                ts;        //! 当前事务开始时间
	dbx1000::TxnRowMan* txn;  //! 当前事务
	ts_t                time;      //! 分配出该 ReqEntry 的时间戳
	//! 两个时间戳并不一样，ts 是全局从 0 递增的，time 是 get_sys_clock()
	~ReqEntry() {} /// delete ReqEntry 时，不会调用 txn 的析构函数
};


class Row_mvcc {
public:
//    Row_mvcc(uint64_t key);
    Row_mvcc();
//	void init(dbx1000::RowItem* row_item, size_t row_size);
	void init(dbx1000::RowItem* rowItem);
//	RC access(txn_man * txn, TsType type, row_t * row);
	RC access(dbx1000::TxnRowMan* txn, TsType type);
private:
 	pthread_mutex_t * latch;
	volatile bool blatch;

	uint64_t key_;
//	dbx1000::RowItem* _row;
	size_t row_size_;


	RC conflict(TsType type, ts_t ts, uint64_t thd_id = 0);
	void update_buffer(dbx1000::TxnRowMan* txn, TsType type);
	void buffer_req(TsType type, dbx1000::TxnRowMan* txn, bool served);

//	dbx1000::RowItem* SetLatestRow();
//	dbx1000::RowItem* GetLatestRow();

	// Invariant: all valid entries in _requests have greater ts than any entry in _write_history 
	dbx1000::RowItem* 		_latest_row;
	ts_t			_latest_wts;
	ts_t			_oldest_wts;
	WriteHisEntry * _write_history;
	// the following is a small optimization.
	// the timestamp for the served prewrite request. There should be at most one 
	// served prewrite request. 
	bool  			_exists_prewrite;
	ts_t 			_prewrite_ts;
	uint32_t 		_prewrite_his_id;
	ts_t 			_max_served_rts;

	// _requests only contains pending requests.
	ReqEntry * 		_requests;
	uint32_t 		_his_len;
	uint32_t 		_req_len;
	// Invariant: _num_versions <= 4
	// Invariant: _num_prewrite_reservation <= 2
	uint32_t 		_num_versions;
	
	// list = 0: _write_history
	// list = 1: _requests
	void double_list(uint32_t list);
//	row_t * reserveRow(ts_t ts, txn_man * txn);
//	dbx1000::RowItem* reserveRow(ts_t ts, dbx1000::TxnRowMan* txn);
	dbx1000::RowItem* reserveRow(ts_t ts, uint64_t thread_id);
};

#endif
