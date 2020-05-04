#pragma once 

#include <cstdint>
#include "global.h"
//#include "manager_client.h"


#define ROW_SIZE 100

class workload;
class thread_t;
namespace dbx1000 {
    class RowItem;
}
class table_t;
class base_query;
//class INDEX;

// each thread has a txn_man. 
// a txn_man corresponds to a single transaction.

class Access {
public:
	access_t 	type;
	dbx1000::RowItem* 	orig_row;   /// 读出来的 row
	dbx1000::RowItem* 	data;       /// 脏页
//	dbx1000::RowItem * 	orig_data;
	void cleanup();
};

//! 每个线程持有一个，在 thread_t::run() 中声明，并在 workload::get_txn_man() 中初始化
class txn_man
{
public:
	virtual void init(thread_t * h_thd);
	void release();

	virtual RC 		run_txn(base_query * m_query) = 0;
	int txn_count;

	uint64_t 		get_thd_id();

	void 			set_txn_id(uint64_t txn_id);
	uint64_t 		get_txn_id();

	//! 事务开始时间戳
	void 			set_ts(uint64_t timestamp);
	uint64_t        get_ts();

	dbx1000::RowItem* 		get_row(uint64_t key, access_t type);
	RC 				finish(RC rc);
	void 			cleanup(RC rc);


	thread_t * h_thd;       //! 线程 id, 0...3
//	workload * h_wl;        //! 属于哪个 wl
//	myrand * mrand;
	uint64_t abort_cnt;     //! 事务失败的次数，thread::run() 中用到
	// [TIMESTAMP, MVCC]
	bool volatile 	ts_ready;
	// following are public for OCC
	int 			row_cnt;
	int	 			wr_cnt;
	Access **		accesses;
	int 			num_accesses_alloc;
//	dbx1000::RowItem*        cur_row;

 private:
	uint64_t 		txn_id;
	uint64_t 		timestamp_;                      //! 事务开始时间戳
};
