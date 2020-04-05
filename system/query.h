#pragma once 

#include <memory>
#include <atomic>

#include "global.h"
#include "helper.h"


class workload;
class ycsb_query;
class tpcc_query;
namespace dbx1000{
    class Arena;
}

class base_query {
public:
	virtual void init(uint64_t thd_id, workload * h_wl) = 0;
	uint64_t waiting_time;
	uint64_t part_num;
	uint64_t * part_to_access;
};

// All the querise for a particular thread.
class Query_thd {
public:
	void init(workload * h_wl, int thread_id);
	base_query * get_next_query();
	dbx1000::Arena *GetArena();

	int q_idx;      //! get_next_query 每被调用一次，该值就增加 1
#if WORKLOAD == YCSB
	ycsb_query * queries;
#else 
	tpcc_query * queries;
#endif
//	char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
	drand48_data buffer;
	std::unique_ptr<dbx1000::Arena> arena_;
};

// TODO we assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sofisticated 
// queue model might be implemented.
//! 查询队列，all_queries[g_thread_cnt] 为每个线程都提供了一个查询队列，
//! 每个队列里有 MAX_TXN_PER_PART(100000) 个左右的 query, 每个 query 有 小于等于 16 个 request
//! request 是最小操作单位，有类型(读、写、查询，读写请求单个 row, 查询会请求多个主键连续的 row）、primary、value等
//! 每个 query 里的 requests 是按抓紧排序的
//! 每个线程的 all_queries[thd_id] 里的query 是乱序的，因为 query 里的 requests's primary 是 zipf 随机生成的
class Query_queue {
public:
	void init(workload * h_wl);
	//! 分别初始化每个线程内的 query
	void init_per_thread(int thread_id);
	//! 通过 all_queries[tid]->get_next_query() 获取下一个查询。
	base_query * get_next_query(int thread_id);

//	std::vector<std::unique_ptr<dbx1000::Arena>> arenas_;
private:
    //! 调用 init_per_thread
	static void * threadInitQuery(void * This);

	Query_thd ** all_queries;
	workload * _wl;
//	static int _next_tid; //! [0, g_thread_cnt-1]

	static std::atomic<int> _next_tid;
};
