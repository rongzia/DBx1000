

#include <sched.h>
#include <thread>
#include <cstring>

#include "ycsb.h"

#include "common/global.h"
#include "common/buffer/buffer.h"
#include "common/index/index.h"
#include "common/storage/disk/file_io.h"
#include "common/storage/tablespace/page.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/catalog.h"
#include "common/storage/table.h"
#include "util/profiler.h"
#include "util/arena.h"

std::atomic<int> ycsb_wl::next_tid = ATOMIC_VAR_INIT(0);

RC ycsb_wl::init() {
    cout << "ycsb_wl::init()" << endl;
	workload::init();
	next_tid = 0;
    init_schema(g_ycsb_schame_path);

//  init_table_parallel();
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
    /*
    RC rc;
    uint64_t total_row = 0;
    while (true) {
        for (UInt32 part_id = 0; part_id < g_part_cnt; part_id ++) {
            if (total_row > g_synth_table_size)
                goto ins_done;
            row_t * new_row = NULL;
            uint64_t row_id;
            rc = the_table->get_new_row(new_row, part_id, row_id);
            // TODO insertion of last row may fail after the table_size
            // is updated. So never access the last record in a table
            assert(rc == RCOK);
            uint64_t primary_key = total_row;
            new_row->set_primary_key(primary_key);
            new_row->set_value(0, &primary_key);
            Catalog * schema = the_table->get_schema();
            for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
                int field_size = schema->get_field_size(fid);
                char value[field_size];
                for (int i = 0; i < field_size; i++)
                    value[i] = (char)rand() % (1<<8) ;
                new_row->set_value(fid, value);
            }
            itemid_t * m_item =
                    (itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id );
            assert(m_item != NULL);
            m_item->type = DT_row;
            m_item->location = new_row;
            m_item->valid = true;
            uint64_t idx_key = primary_key;
            rc = the_index->index_insert(idx_key, m_item, part_id);
            assert(rc == RCOK);
            total_row ++;
        }
    }
    ins_done:
    printf("[YCSB] Table \"MAIN_TABLE\" initialized.\n");
     */
    return RC::RCOK;

}

// init table in parallel
void ycsb_wl::init_table_parallel() {
    std::vector<std::thread> v_thread;
    for(uint32_t i = 0; i < g_init_parallelism; i++) {
        v_thread.emplace_back(thread(threadInitTable, this));
    }
    for(uint32_t i = 0; i < g_init_parallelism; i++) {
        v_thread[i].join();
    }
}
//! 初始化单个区间
void * ycsb_wl::init_table_slice() {
    /*
    UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
    // set cpu affinity
    set_affinity(tid);

    mem_allocator.register_thread(tid);
    RC rc;
    assert(g_synth_table_size % g_init_parallelism == 0);
    assert(tid < g_init_parallelism);
    while ((UInt32)ATOM_FETCH_ADD(next_tid, 0) < g_init_parallelism) {}
    assert((UInt32)ATOM_FETCH_ADD(next_tid, 0) == g_init_parallelism);
    uint64_t slice_size = g_synth_table_size / g_init_parallelism;
    for (uint64_t key = slice_size * tid;
         key < slice_size * (tid + 1);
         key ++
            ) {
        row_t * new_row = NULL;
        uint64_t row_id;
        int part_id = key_to_part(key);     //! 区间内 index
        rc = the_table->get_new_row(new_row, part_id, row_id);  //! TODO, row_id 到底返回的啥？
        assert(rc == RCOK);
        uint64_t primary_key = key;
        new_row->set_primary_key(primary_key);
        new_row->set_value(0, &primary_key);    //! 第 0 列是主键
        Catalog * schema = the_table->get_schema();

        for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
            char value[6] = "hello";
            new_row->set_value(fid, value);
        }

        itemid_t * m_item =
                (itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id );
        assert(m_item != NULL);
        m_item->type = DT_row;
        m_item->location = new_row;
        m_item->valid = true;
        uint64_t idx_key = primary_key;

        rc = the_index->index_insert(idx_key, m_item, part_id);
        assert(rc == RCOK);
    }
     */
    return NULL;
}
/*
//! h_thd = 0, 1, 2, 3
RC ycsb_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd){
    txn_manager = (ycsb_txn_man *)
            _mm_malloc( sizeof(ycsb_txn_man), 64 );
    new(txn_manager) ycsb_txn_man();
    txn_manager->init(h_thd, this, h_thd->get_thd_id());
    return RCOK;
}*/


ycsb_wl::ycsb_wl(){
    cout << "ycsb_wl::ycsb_wl()" << endl;
}

ycsb_wl::~ycsb_wl(){
    cout << "ycsb_wl::~ycsb_wl()" << endl;
}
