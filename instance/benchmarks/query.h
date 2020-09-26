#pragma once 

/**
 * Query_queue : 所有的事务线程从 Query_queue 获取 query 请求。
 *      private : Query_thd *[g_thread_cnt]
 * Query_thd : 单个线程持有.
 *      private : ycsb_query [MAX_TXN_PER_PART]
 * ycsb_query : base_query
 *      private : ycsb_request [16], 是按主键排好序的
 *
 */


#include <memory>
#include <atomic>
#include <vector>
#include "util/arena.h"
#include "config.h"

class ycsb_query;
class tpcc_query;
class Query_queue;
class Query_thd;

class base_query {
public:
    virtual void init(uint64_t thd_id, Query_thd* query_thd) = 0;
    uint64_t waiting_time;
    uint64_t part_num;
    uint64_t * part_to_access;
};

// All the querise for a particular thread.
class Query_thd {
public:
	void init(int thread_id);
	base_query * get_next_query();
	int q_idx;                          /// get_next_query() 每被调用一次，该值就增加 1
#if WORKLOAD == YCSB
	ycsb_query * queries;
#else
	tpcc_query * queries;
#endif
//	char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
    drand48_data buffer;

    dbx1000::Arena* arena_;
    ~Query_thd();
};

// TODO we assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sofisticated 
// queue model might be implemented.
class Query_queue {
public:
	void init();
    //! 分别初始化每个线程内的 query
    void init_per_thread(int thread_id);
    //! 通过 all_queries[tid]->get_next_query() 获取下一个查询。
	base_query * get_next_query(int thread_id);

	~Query_queue();

private:
    //! 调用 init_per_thread
	static void * threadInitQuery(void * This);

	Query_thd ** all_queries;
	/*workload * _wl;
    static int _next_tid; //! [0, g_thread_cnt-1]*/
	static std::atomic<int> _next_tid;
};
