#pragma once 

#include <cstdint>

class row_t;
class txn_man;

typedef uint64_t ts_t;

class Manager {
public:
	void 			init();
	// returns the next timestamp.
	//! 获取时间戳
	ts_t			get_ts(uint64_t thread_id);     //! thread_id 只有在加锁方式为 TS_CLOCK 下才用的到

	// For MVCC. To calculate the min active ts in the system
	void 			add_ts(uint64_t thd_id, ts_t ts);
	ts_t 			get_min_ts(uint64_t tid = 0);

//	txn_man * 		get_txn_man(int thd_id) { return _all_txns[thd_id]; };
//	void 			set_txn_man(txn_man * txn);
private:
	uint64_t *		timestamp;
	ts_t volatile * volatile * volatile all_ts;
//	txn_man ** 		_all_txns;
	// for MVCC
	volatile ts_t	_last_min_ts_time;
	ts_t			_min_ts;
};
