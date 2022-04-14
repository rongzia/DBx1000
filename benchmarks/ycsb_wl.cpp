#include <sched.h>
#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_map_hash.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

ycsb_wl::~ycsb_wl() {}

void ycsb_wl::check() {
	/**
	* tuple_size    = 100
	* rows_per_page = 4096/100 = 40
	* rows_per_thd  = g_synth_table_size / g_init_parallelism = 2^20*10/4 = 2^18*10
	* pages_per_thd = 2^18*10 / 40 = 2^16
	* num_page = g_synth_table_size/rows_per_page+1 = 262145
	*/
	uint64_t rows_per_page = PAGE_SIZE / this->the_table->get_schema()->get_tuple_size();
	uint64_t rows_per_thd  = g_synth_table_size / g_init_parallelism;
	uint64_t pages_per_thd = rows_per_thd / rows_per_page;
	assert(rows_per_page == 40);
	assert(rows_per_thd % rows_per_page == 0);
}

int ycsb_wl::next_tid;

RC ycsb_wl::init() {
	workload::init();
	next_tid = 0;
	// string path = "./benchmarks/YCSB_schema.txt";
	string this_file(__FILE__);
	string path = this_file.substr(0, this_file.length()-11);
	path += "YCSB_schema.txt";
	init_schema( path );

	check();

	uint64_t rows_per_page = (PAGE_SIZE-64) / the_table->get_schema()->get_tuple_size();
	uint64_t rows_per_thd  = g_synth_table_size / g_init_parallelism;
	uint64_t pages_per_thd = rows_per_thd / rows_per_page;
	// lru_cache_ = new rr::lru_cache::LRUCache((g_synth_table_size/rows_per_page+1) * PAGE_SIZE, PAGE_SIZE);
	lru_cache_ = new rr::lru_cache::LRUCache((g_synth_table_size/rows_per_page+1) * PAGE_SIZE / 1, PAGE_SIZE);
	// /* rr::debug */ cout << __FILE__ << ", " << __LINE__ << ", rows_per_page: " << rows_per_page << endl;
	// /* rr::debug */ cout << __FILE__ << ", " << __LINE__ << ", g_synth_table_size: " << g_synth_table_size << ", num_page:" << g_synth_table_size / rows_per_page+1 << ", " << g_synth_table_size / rows_per_page * PAGE_SIZE << endl;
	page_ids_ = new uint64_t[g_init_parallelism];
	for(uint32_t i = 0; i < g_init_parallelism; i++) {
		page_ids_[i] = i * pages_per_thd;
		// /* rr::debug */ cout << page_ids_[i] << endl;
	}

	init_table_parallel();
//	init_table();

	// 多线程需要把几个 index 串起来
	uint64_t slice_size = g_synth_table_size / g_init_parallelism;
	for(uint64_t key = slice_size-1; key < g_synth_table_size-1; key += slice_size) {
		index_item* prev = nullptr;
		index_item* next = nullptr;
		RC rc1 = the_index->index_read(key, prev);
		RC rc2 = the_index->index_read(key+1, next);
		assert(rc1 == RCOK && rc2 == RCOK && prev && next);
		prev->next = next;
		next->prev = prev;
	}

	/*
	// check prev and next in the_index
	for(uint64_t key = 0; key < g_synth_table_size-1; key++) {
		index_item* item = nullptr;
		RC rc = the_index->index_read(key, item);
		assert(rc == RCOK && item);
		assert(item->next->prev == item);
	} */

	return RCOK;
}

RC ycsb_wl::init_schema(string schema_file) {
	workload::init_schema(schema_file);
	the_table = tables["MAIN_TABLE"];  
	// the_index = indexes["MAIN_INDEX"];
	the_index = indexes["MAIN_INDEX"];
	return RCOK;
}
	
int 
ycsb_wl::key_to_part(uint64_t key) {
	uint64_t rows_per_part = g_synth_table_size / g_part_cnt;
	return key / rows_per_part;
}

RC ycsb_wl::init_table() {
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
            // rc = the_index->index_insert(idx_key, m_item, part_id);
            assert(rc == RCOK);
            total_row ++;
        }
    }
ins_done:
    printf("[YCSB] Table \"MAIN_TABLE\" initialized.\n");
    return RCOK;

}

// init table in parallel
void ycsb_wl::init_table_parallel() {
	enable_thread_mem_pool = true;
	pthread_t p_thds[g_init_parallelism - 1];
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitTable, this);
	threadInitTable(this);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}
	enable_thread_mem_pool = false;
	mem_allocator.unregister();
}

void * ycsb_wl::init_table_slice() {
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

	rr::lru_cache::Page* page;
	bool res = lru_cache_->GetNewPage(page); assert(res);
	uint64_t page_id = page_ids_[tid];
	index_item* prev = nullptr;

    auto WritePage = [this, &page, page_id, tid]() -> void
    {
		page->page_id_ = page_id;
		page->Serialize();
		rr::lru_cache::Page* prior = nullptr;
		bool is_new = lru_cache_->Write(page->page_id_, page, prior, tid);
		if (is_new) { page->Unref(); }
		else { prior->Unref(); }
    };

	for (uint64_t key = slice_size * tid; 
			key < slice_size * (tid + 1); 
			key ++
	) {
		row_t * new_row = NULL;
		uint64_t row_id;
		int part_id = key_to_part(key);
		rc = the_table->get_new_row(new_row, part_id, row_id); 
		assert(rc == RCOK);
		uint64_t primary_key = key;
		new_row->set_primary_key(primary_key);
		new_row->set_value(0, &primary_key);
		new_row->manager->wl_ = this;
		Catalog * schema = the_table->get_schema();
		
		for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
			char value[6] = "hello";
			new_row->set_value(fid, value);
		}

		res = page->AddRow(new_row->data, new_row->get_tuple_size());
		if(!res) {
			WritePage();

			res = lru_cache_->GetNewPage(page); assert(res); assert(page->used_size_ == 64);
			page_id++;
			// /* rr::debug */ std::cout << "page id: " << page_id << std::endl;
			res = page->AddRow(new_row->data, new_row->get_tuple_size()); assert(res);
		}

		index_item* idx_item = new index_item(the_table, new_row, page_id, page->used_size_);
		rc = the_index->index_insert(primary_key, idx_item, prev);
		assert(rc == RCOK);
		prev = idx_item;
	}
	WritePage();
	return NULL;
}

RC ycsb_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd){
	txn_manager = (ycsb_txn_man *)
		_mm_malloc( sizeof(ycsb_txn_man), 64 );
	new(txn_manager) ycsb_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}


