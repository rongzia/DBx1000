#include <thread>
#include <memory>
#include "query.h"

#include "ycsb_query.h"
//#include "tpcc_query.h"
//#include "tpcc_helper.h"

#include "util/make_unique.h"
#include "util/arena.h"
#include "util/profiler.h"

/*************************************************/
//     class Query_queue
/*************************************************/
std::atomic<int> Query_queue::_next_tid;

void Query_queue::init() {
	all_queries = new Query_thd * [g_thread_cnt];
    for (int i = 0; i < g_thread_cnt; i++) {
        arenas_.emplace_back(dbx1000::make_unique<dbx1000::Arena>(i));
    }
	_next_tid = 0;
//	_wl = h_wl;

#if WORKLOAD == YCSB	
	ycsb_query::calculateDenom();
#elif WORKLOAD == TPCC
	assert(tpcc_buffer != NULL);
#endif

    std::unique_ptr<dbx1000::Profiler> profiler(new dbx1000::Profiler());
    profiler->Start();
    std::vector<std::thread> v_thread;
    for(int i = 0; i < g_thread_cnt; i++) {
        v_thread.emplace_back(thread(threadInitQuery, this));
    }
    for(int i = 0; i < g_thread_cnt; i++){
        v_thread[i].join();
    }
	profiler->End();
	std::cout << "Query Queue Init Time : " << profiler->Millis() << " Millis" << std::endl;
}

void *Query_queue::threadInitQuery(void * This) {
	Query_queue * query_queue = (Query_queue *)This;
	int tid = _next_tid.fetch_add(1, std::memory_order_consume);
    std::cout << tid << std::endl;
//	 set cpu affinity
//	 set_affinity(tid);
	query_queue->init_per_thread(tid);
	return NULL;
}

void Query_queue::init_per_thread(int thread_id) {
	all_queries[thread_id] = new Query_thd();
	all_queries[thread_id]->init(thread_id, this);
}

base_query *Query_queue::get_next_query(int thread_id) {
	base_query * query = all_queries[thread_id]->get_next_query();
	return query;
}

/*************************************************/
//     class Query_thd
/*************************************************/

void Query_thd::init(int thread_id, Query_queue *queryQueue) {
    q_idx = 0;
    thread_id_ = thread_id;
    queryQueue_ = queryQueue;

	uint64_t request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
#if ABORT_BUFFER_ENABLE
    request_cnt += ABORT_BUFFER_SIZE;
#endif

	std::cout << "request_cnt : " << request_cnt << std::endl;

#if WORKLOAD == YCSB
	queries = new (queryQueue_->arenas_[thread_id_]->Allocate(sizeof(ycsb_query) * request_cnt)) ycsb_query[request_cnt]();
//	srand48_r(thread_id + 1, &buffer);
#elif WORKLOAD == TPCC
	queries = (tpcc_query *) _mm_malloc(sizeof(tpcc_query) * request_cnt, 64);
#endif

	for (uint32_t qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB	
//		new(&queries[qid]) ycsb_query();
		queries[qid].init(thread_id_, this);
#elif WORKLOAD == TPCC
		new(&queries[qid]) tpcc_query();
		queries[qid].init(thread_id, h_wl);
#endif
	}
}

base_query *Query_thd::get_next_query() {
	base_query * query = &queries[q_idx++];
	return query;
}