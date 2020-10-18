#include <iostream>
#include <memory>
#include <thread>

#include <mm_malloc.h>
#include "query.h"
#include "tpcc_query.h"
#include "ycsb_query.h"
#include "common/workload/tpcc_helper.h"
#include "util/profiler.h"

/*************************************************/
//     class Query_queue
/*************************************************/
std::atomic<int> Query_queue::_next_tid = ATOMIC_VAR_INIT(0);

void
Query_queue::init() {
    std::cout << "Query_queue" << std::endl;
	all_queries = new Query_thd * [g_thread_cnt];
//	_wl = h_wl;
	_next_tid = 0;


#if WORKLOAD == YCSB	
	ycsb_query::calculateDenom();
#elif WORKLOAD == TPCC
	assert(tpcc_buffer != NULL);
#endif

    std::unique_ptr<dbx1000::Profiler> profiler(new dbx1000::Profiler());
    profiler->Start();
    std::vector<std::thread> v_thread;
    for(uint32_t i = 0; i < g_thread_cnt; i++) {
        v_thread.push_back(thread(threadInitQuery, this));
    }
    for(auto &th : v_thread) {
        th.join();
    }
	profiler->End();
	std::cout << "Query Queue Init Time : " << profiler->Millis() << " Millis" << std::endl;
}

void
Query_queue::init_per_thread(int thread_id) {
//    all_queries[thread_id] = (Query_thd *) _mm_malloc(sizeof(Query_thd), 64);
    all_queries[thread_id] = new Query_thd();
    all_queries[thread_id]->queryQueue_ = this;
    all_queries[thread_id]->init(thread_id);
    cout << "Query_queue::init_per_thread" << endl;
}

base_query *
Query_queue::get_next_query(int thread_id) {
    return all_queries[thread_id]->get_next_query();
}

void *
Query_queue::threadInitQuery(void * This) {
	Query_queue * query_queue = (Query_queue *)This;
	int tid = _next_tid.fetch_add(1, std::memory_order_consume);

//	 set cpu affinity, 绑定 CPU 物理核
//	 set_affinity(tid);

	query_queue->init_per_thread(tid);
	return NULL;
}

/*************************************************/
//     class Query_thd
/*************************************************/

void
Query_thd::init(int thread_id) {
    this->arena_ = new dbx1000::Arena();
    uint64_t request_cnt;
    q_idx = 0;
    request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
#if ABORT_BUFFER_ENABLE
    request_cnt += ABORT_BUFFER_SIZE;
#endif
#if WORKLOAD == YCSB
	queries = new (this->arena_->Allocate(sizeof(ycsb_query) * request_cnt))
	        ycsb_query[request_cnt]();
	srand48_r(thread_id + 1, &buffer);
#elif WORKLOAD == TPCC
//	queries = (tpcc_query *) _mm_malloc(sizeof(tpcc_query) * request_cnt, 64);
    queries = new (this->arena_->Allocate(sizeof(tpcc_query) * request_cnt))
            tpcc_query[request_cnt]();
#endif

	for (uint32_t qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB
//		new(&queries[qid]) ycsb_query();
		queries[qid].init(thread_id, this);
#elif WORKLOAD == TPCC
//		new(&queries[qid]) tpcc_query();
		queries[qid].init(thread_id, this);
#endif
	}
	cout << "thread: " << thread_id << " done." << endl;
}

base_query *
Query_thd::get_next_query() {
	base_query * query = &queries[q_idx++];
	return query;
}

Query_thd::~Query_thd() {
    delete this->arena_;
    /// 不需要 delete [] queries， 因为 queries 是在 arena_ 中创建的，由 arena_ 释放空间
//    delete [] queries;
}

Query_queue::~Query_queue() {
    for(uint32_t i = 0; i< g_thread_cnt; i++) { delete all_queries[i]; }
}