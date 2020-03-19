#pragma once 

#include "global.h"
#include "helper.h"

class workload;
class ycsb_query;
class tpcc_query;

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
	int q_idx;      //! querie id，自增
#if WORKLOAD == YCSB
	ycsb_query * queries;
#else 
	tpcc_query * queries;
#endif
	char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
	drand48_data buffer;
};

// TODO we assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sofisticated 
// queue model might be implemented.
class Query_queue {
public:
	void init(workload * h_wl);
	//! 分别初始化每个线程内的 query
	void init_per_thread(int thread_id);
	//! 通过 all_queries[tid]->get_next_query() 获取下一个查询。
	base_query * get_next_query(uint64_t thd_id); 
	
private:
    //! 调用 init_per_thread
	static void * threadInitQuery(void * This);

	Query_thd ** all_queries;
	workload * _wl;
	static int _next_tid; //! [0, g_thread_cnt-1]
};
