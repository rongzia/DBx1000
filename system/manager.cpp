#include "manager.h"
#include "row.h"
#include "txn/txn.h"
#include "pthread.h"

void Manager::init() {
	timestamp = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
	*timestamp = 1;
	_last_min_ts_time = 0;
	_min_ts = 0;
	all_ts = (ts_t volatile **) _mm_malloc(sizeof(ts_t *) * g_thread_cnt, 64);
	all_ts = new ts_t*[](); //(ts_t volatile **) _mm_malloc(sizeof(ts_t *) * g_thread_cnt, 64);
	for (uint32_t i = 0; i < g_thread_cnt; i++)
		all_ts[i] = (ts_t *) _mm_malloc(sizeof(ts_t), 64);
}

//! 获取时间戳
uint64_t
Manager::get_ts(uint64_t thread_id) {
	assert(g_ts_alloc == TS_CAS);
	uint64_t time;
	uint64_t starttime = get_sys_clock();
	switch(g_ts_alloc) {        //! TS_ALLOC == TS_CAS
	case TS_CAS :
		time = ATOM_FETCH_ADD((*timestamp), 1);
		break;
	default :
		assert(false);
	}
	//! 此次分配时间的函数，花了多少时间
	INC_STATS(thread_id, time_ts_alloc, get_sys_clock() - starttime);
	return time;
}

//! all_ts 数组记录了每个线程里当前事务的开始时间，
//! 该函数选出所有线程中，最先开始的事务的开始时间
ts_t Manager::get_min_ts(uint64_t tid) {
	uint64_t now = get_sys_clock();
	uint64_t last_time = _last_min_ts_time;
	if (tid == 0 && now - last_time > MIN_TS_INTVL)
	{
		ts_t min = UINT64_MAX;
    		for (uint32_t i = 0; i < g_thread_cnt; i++)
			if (*all_ts[i] < min)
		    	    	min = *all_ts[i];
		if (min > _min_ts)
			_min_ts = min;
	}
	return _min_ts;
}

void Manager::add_ts(uint64_t thd_id, ts_t ts) {
	assert( ts >= *all_ts[thd_id] || 
		*all_ts[thd_id] == UINT64_MAX);
	*all_ts[thd_id] = ts;
}