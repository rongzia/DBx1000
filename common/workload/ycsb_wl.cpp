

#include <sched.h>
#include <thread>
#include <server/manager_server.bak.h>

#include "ycsb_wl.h"

#include "leveldb/db.h"
#include "common/global.h"
#include "common/row_item.h"
#include "common/buffer/buffer.h"
#include "common/storage/table.h"
#include "common/storage/catalog.h"
#include "server/concurrency_control/row_mvcc.h"
#include "util/profiler.h"
#include "util/arena.h"

std::atomic<int> ycsb_wl::next_tid;

ycsb_wl::ycsb_wl(){
    cout << "ycsb_wl::ycsb_wl()" << endl;
}

ycsb_wl::~ycsb_wl(){
    cout << "ycsb_wl::~ycsb_wl()" << endl;
}

RC ycsb_wl::init() {
    cout << "ycsb_wl::init()" << endl;
	workload::init();
	next_tid = 0;
    db_has_inited = false;
    init_schema(g_schame_path);

//	init_table();
	return RC::RCOK;
}

RC ycsb_wl::init_schema(string schema_file) {
	workload::init_schema(schema_file);
	the_table = tables["MAIN_TABLE"];
//	the_index = indexes["MAIN_INDEX"];
	return RC::RCOK;
}
//! 数据是分区的，每个区间的 key 都是 0-g_synth_table_size / g_part_cnt，单调增，返回 key 在哪个区间
int
ycsb_wl::key_to_part(uint64_t key) {
	uint64_t rows_per_part = g_synth_table_size / g_part_cnt;
	return key / rows_per_part;
}

RC ycsb_wl::init_table() {
    cout << "db_has_inited : " << db_has_inited << endl;
    cout << "ycsb_wl::init_table, table size:" << g_synth_table_size << endl;
    std::unique_ptr<dbx1000::Profiler> profiler(new dbx1000::Profiler());
    profiler->Start();

    init_table_parallel();
    profiler->End();
    std::cout << "ycsb_wl::init_table, workload Init Time : " << profiler->Millis() << " Millis" << std::endl;
    return RC::RCOK;
}

// init table in parallel
void ycsb_wl::init_table_parallel() {

    std::vector<std::thread> v_thread;
    for(int i = 0; i < g_init_parallelism; i++) {
        v_thread.emplace_back(thread(threadInitTable, this));
    }
    for(int i = 0; i < g_init_parallelism; i++) {
        v_thread[i].join();
    }
}
//! 初始化单个区间
void * ycsb_wl::init_table_slice() {
    uint32_t tid = next_tid.fetch_add(1);

//	cout << tid << endl;
//	set_affinity(tid);      /// 绑定到物理核

	RC rc;
	assert(g_synth_table_size % g_init_parallelism == 0);
	assert(tid < g_init_parallelism);
	uint64_t slice_size = g_synth_table_size / g_init_parallelism;

    uint32_t tuple_size = the_table->get_schema()->get_tuple_size();

    dbx1000::RowItem *rowItem = new dbx1000::RowItem(0, tuple_size);
    memset(rowItem->row_, 'a', tuple_size);
    for (uint64_t key = slice_size * tid; key < slice_size * (tid + 1); key++) {
//		for (uint32_t fid = 0; fid < the_table->get_schema()->get_field_cnt(); fid ++) {
//		    uint64_t field_size = the_table->get_schema()->get_field_size(fid);
//		    memset((char*) rowItem->row_ + fid*field_size, 'a', field_size);
//		}
        rowItem->key_ = key;
#ifdef USE_MEMORY_DB
		buffer_->BufferPut(key, rowItem->row_, tuple_size);
#else
		if(false == db_has_inited) {
            buffer_->BufferPut(key, rowItem->row_, tuple_size);
        }
#endif

//		Row_mvcc *rowMvcc = new (arenas_[tid]->Allocate(sizeof(Row_mvcc)))Row_mvcc();
//		rowMvcc->init(rowItem);
//		glob_manager_server->row_mvccs_mutex_.lock();
//		glob_manager_server->row_mvccs_.insert(std::pair<uint64_t, Row_mvcc*>(key, rowMvcc));
//        glob_manager_server->row_mvccs_mutex_.unlock();
	}
    delete rowItem;
}

