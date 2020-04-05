#include <sched.h>
#include <thread>
#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

#include "leveldb/db.h"

std::atomic<int> ycsb_wl::next_tid;

ycsb_wl::ycsb_wl(){
    cout << "ycsb_wl::ycsb_wl()" << endl;
}

ycsb_wl::~ycsb_wl(){
    cout << "ycsb_wl::~ycsb_wl()" << endl;
}

RC ycsb_wl::init() {
	workload::init();
	next_tid = 0;
    init_schema(std::string("/home/zhangrongrong/CLionProjects/DBx1000/benchmarks/YCSB_schema.txt"));

	init_table();
	return RCOK;
}

RC ycsb_wl::init_schema(string schema_file) {
	workload::init_schema(schema_file);
	the_table = tables["MAIN_TABLE"];
	the_index = indexes["MAIN_INDEX"];
	return RCOK;
}
//! 数据是分区的，每个区间的 key 都是 0-g_synth_table_size / g_part_cnt，单调增，返回 key 在哪个区间
int
ycsb_wl::key_to_part(uint64_t key) {
	uint64_t rows_per_part = g_synth_table_size / g_part_cnt;
	return key / rows_per_part;
}

RC ycsb_wl::init_table() {
    init_table_parallel();
    return RCOK;
}

// init table in parallel
void ycsb_wl::init_table_parallel() {
    std::vector<std::thread> v_thread;
    for(int i = 0; i < g_init_parallelism; i++) {
        v_thread.push_back(thread(threadInitTable, this));
    }
    for(int i = 0; i < g_init_parallelism; i++){
        v_thread[i].join();
    }
}
//! 初始化单个区间
void * ycsb_wl::init_table_slice() {
    uint32_t tid = next_tid.fetch_add(1, std::memory_order_consume);
	cout << tid << endl;
//	set_affinity(tid);

	RC rc;
	assert(g_synth_table_size % g_init_parallelism == 0);
	assert(tid < g_init_parallelism);
	uint64_t slice_size = g_synth_table_size / g_init_parallelism;
    for (uint64_t key = slice_size * tid; key < slice_size * (tid + 1); key++) {
		row_t * new_row = NULL;
		uint64_t row_id;
		int part_id = key_to_part(key);     //! 区间内 index
		rc = the_table->get_new_row(new_row, part_id, row_id);  //! TODO, row_id 到底返回的啥？
		assert(rc == RCOK);
		uint64_t primary_key = key;
		new_row->set_primary_key(primary_key);
		new_row->set_value(0, &primary_key);    //! 第 0 列是主键

		for (uint32_t fid = 0; fid < the_table->get_schema()->get_field_cnt(); fid ++) {
			char value[11] = "hellohello";
			new_row->set_value(fid, value);
		}

		index_insert(the_index, primary_key, new_row, part_id);
		assert(rc == RCOK);
	}
	return NULL;
}
//! h_thd = 0, 1, 2, 3
RC ycsb_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd){
    txn_manager = new ycsb_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}


