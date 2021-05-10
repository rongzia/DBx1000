

#include <sched.h>
#include <thread>
#include <cstring>
#include <iostream>

#include "ycsb.h"

#include "common/global.h"
#include "common/buffer/record_buffer.h"
#include "common/index/index.h"
#include "instance/manager_instance.h"
#include "common/storage/disk/file_io.h"
#include "common/storage/tablespace/page.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/catalog.h"
#include "common/storage/table.h"
#include "common/storage/row.h"
#include "util/profiler.h"
#include "util/arena.h"

std::atomic<int> ycsb_wl::next_tid = ATOMIC_VAR_INIT(0);

RC ycsb_wl::init() {
    cout << "ycsb_wl::init()" << endl;
	workload::init();
//	next_tid = 0;
    init_schema(g_ycsb_schame_path);

//    init_table_parallel();
    init_table();
	return RC::RCOK;
}

RC ycsb_wl::init_schema(string schema_file) {
	workload::init_schema(schema_file);
	the_table = tables["MAIN_TABLE"];
//	the_index = indexes["MAIN_INDEX"];
/////////////// rrzhang ///////////////
#if defined(B_P_L_P) || defined(B_P_L_R)
    tablespaces_[TABLES::MAIN_TABLE] = make_shared<dbx1000::TableSpace>();
    indexes_[TABLES::MAIN_TABLE]     = make_shared<dbx1000::Index>();
#endif
#ifdef RDB_BUFFER_SAME_SIZE
    #ifdef B_P_L_P
    if(this->manager_instance_ && !this->is_server_) {
        buffer_pool_.SetSize(SYNTH_TABLE_SIZE/204 * 0.25);
    }
    #endif // B_P_L_P
#endif // RDB_BUFFER_SANE_SIZE
#ifdef RDB_BUFFER_DIFF_SIZE
    #ifdef B_P_L_P
    if(this->manager_instance_ && !this->is_server_) {
        buffer_pool_.manager_instance_ = manager_instance_;
        manager_instance_->threshold_ = manager_instance_->instance_id_==0 ? 0.1:0.3;
        // manager_instance_->threshold_ = 0.25;
        // manager_instance_->threshold_ = manager_instance_->instance_id_==0 ? 0.4:0.2;
        // manager_instance_->threshold_ = manager_instance_->instance_id_==0 ? 0.7:0.1;
        // manager_instance_->threshold_ = manager_instance_->instance_id_==0 ? 0.82:0.06;
        // manager_instance_->threshold_ = manager_instance_->instance_id_==0 ? 0.91:0.03;
        // manager_instance_->threshold_ = manager_instance_->instance_id_==0 ? 0.97:0.01;
        // manager_instance_->threshold_ = manager_instance_->instance_id_==0 ? 0.991:0.003;
        buffer_pool_.SetSize(SYNTH_TABLE_SIZE/204 * manager_instance_->threshold_);
    }
    #endif // B_P_L_P
#endif // RDB_BUFFER_DIFF_SIZE
    tables_[TABLES::MAIN_TABLE]      = the_table;
/////////////// rrzhang ///////////////
	return RC::RCOK;
}

//! 数据是分区的，每个区间的 key 都是 0-g_synth_table_size / g_part_cnt，单调增，返回 key 在哪个区间
int
ycsb_wl::key_to_part(uint64_t key) {
    uint64_t rows_per_part = g_synth_table_size / g_part_cnt;
    return key / rows_per_part;
}

RC ycsb_wl::init_table() {
////////////// rrzhang //////////////
#if defined(B_P_L_P) || defined(B_P_L_R)
    dbx1000::Page *page = new dbx1000::Page();
    page->Init();
    page->set_page_id(tablespaces_[TABLES::MAIN_TABLE]->GetNextPageId());
#endif
////////////// rrzhang //////////////
    RC rc;
#ifdef NO_CONFLICT
    for(auto i = 0; i < PROCESS_CNT; i++) {
        bool key_in_this_instance = false;
        if (manager_instance_ && i == manager_instance_->instance_id_) { key_in_this_instance = true; }
        for (uint64_t key = i*g_synth_table_size; key < (i+1)*g_synth_table_size; key++) {
            uint64_t total_row = key;
            row_t *new_row = NULL;
            uint64_t row_id;
            rc = the_table->get_new_row(new_row, 0, row_id);
            // TODO insertion of last row may fail after the table_size
            // is updated. So never access the last record in a table
            assert(rc == RC::RCOK);
            uint64_t primary_key = total_row;
            new_row->set_primary_key(primary_key);
            new_row->set_value(0, &primary_key);
            Catalog *schema = the_table->get_schema();
            for (uint32_t fid = 0; fid < schema->get_field_cnt(); fid++) {
                int field_size = schema->get_field_size(fid);
                char value[field_size];
                for (int i = 0; i < field_size; i++)
                    value[i] = (char) rand() % (1 << 8);
                new_row->set_value(fid, value);
            }

#if defined(B_P_L_P) || defined(B_P_L_R)
            if (new_row->get_tuple_size() > (MY_PAGE_SIZE - page->used_size())) {
                page->Serialize();
                if(key_in_this_instance || is_server_ == true) { buffers_[TABLES::MAIN_TABLE]->BufferPut(page->page_id(), page->page_buf(), MY_PAGE_SIZE); }
                page->set_page_id(tablespaces_[TABLES::MAIN_TABLE]->GetNextPageId());
                page->set_used_size(64);
            }
            page->PagePut(page->page_id(), new_row->data, new_row->get_tuple_size());
            dbx1000::IndexItem indexItem(page->page_id(), page->used_size() - new_row->get_tuple_size());
            indexes_[TABLES::MAIN_TABLE]->IndexPut(primary_key, &indexItem);
#else
            buffers_[TABLES::MAIN_TABLE]->BufferPut(primary_key, new_row->data, new_row->get_tuple_size());
#endif
        }
#if defined(B_P_L_P) || defined(B_P_L_R)
        if (page->used_size() > 64) {
            page->Serialize();
            if(key_in_this_instance || is_server_ == true) { buffers_[TABLES::MAIN_TABLE]->BufferPut(page->page_id(), page->page_buf(), MY_PAGE_SIZE); }
        }
#endif
    }

#if defined(B_P_L_P) || defined(B_P_L_R)
    delete page;
#endif
#else // NO_CONFLICT
    for(auto key = 0; key < g_synth_table_size; key++) {
            uint64_t total_row = key;
            row_t *new_row = NULL;
            uint64_t row_id;
            rc = the_table->get_new_row(new_row, 0, row_id);
            // TODO insertion of last row may fail after the table_size
            // is updated. So never access the last record in a table
            assert(rc == RC::RCOK);
            uint64_t primary_key = total_row;
            new_row->set_primary_key(primary_key);
            new_row->set_value(0, &primary_key);
            Catalog *schema = the_table->get_schema();
            for (uint32_t fid = 0; fid < schema->get_field_cnt(); fid++) {
                int field_size = schema->get_field_size(fid);
                char value[field_size];
                for (int i = 0; i < field_size; i++)
                    value[i] = (char) rand() % (1 << 8);
                new_row->set_value(fid, value);
            }

#if defined(B_P_L_P) || defined(B_P_L_R)
            if (new_row->get_tuple_size() > (MY_PAGE_SIZE - page->used_size())) {
                page->Serialize();
                const BufferPool::PageKey pagekey = std::make_pair(TABLES::MAIN_TABLE, page->page_id());
                const BufferPool::PageHandle* handle = buffer_pool_.Put(pagekey, dbx1000::Page(*page));
                buffer_pool_.Release(handle);
                page->set_page_id(tablespaces_[TABLES::MAIN_TABLE]->GetNextPageId());
                page->set_used_size(64);
            }
            page->PagePut(page->page_id(), new_row->data, new_row->get_tuple_size());
            dbx1000::IndexItem indexItem(page->page_id(), page->used_size() - new_row->get_tuple_size());
            indexes_[TABLES::MAIN_TABLE]->IndexPut(primary_key, &indexItem);
#else // defined(B_P_L_P) || defined(B_P_L_R)
            buffers_[TABLES::MAIN_TABLE]->BufferPut(primary_key, new_row->data, new_row->get_tuple_size());
#endif // defined(B_P_L_P) || defined(B_P_L_R)
        }
#if defined(B_P_L_P) || defined(B_P_L_R)
        if (page->used_size() > 64) {
            page->Serialize();
            const BufferPool::PageKey pagekey = std::make_pair(TABLES::MAIN_TABLE, page->page_id());
            const BufferPool::PageHandle* handle = buffer_pool_.Put(pagekey, dbx1000::Page(*page));
            buffer_pool_.Release(handle);
        }
    delete page;
#endif // defined(B_P_L_P) || defined(B_P_L_R)
#endif // NO_CONFLICT

    printf("[YCSB] Table \"MAIN_TABLE\" initialized.\n");
    return RC::RCOK;
}

//// init table in parallel
//void ycsb_wl::init_table_parallel() {
//    enable_thread_mem_pool = true;
//    pthread_t p_thds[g_init_parallelism - 1]; //! p_thds[39]
//    for (UInt32 i = 0; i < g_init_parallelism - 1; i++)
//        pthread_create(&p_thds[i], NULL, threadInitTable, this);    //! 创建线程
//    threadInitTable(this);
//
//    for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
//        int rc = pthread_join(p_thds[i], NULL);
//        if (rc) {
//            printf("ERROR; return code from pthread_join() is %d\n", rc);
//            exit(-1);
//        }
//    }
//    enable_thread_mem_pool = false;
//    mem_allocator.unregister();
//}
////! 初始化单个区间
//void * ycsb_wl::init_table_slice() {
//    UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
//    // set cpu affinity
//    set_affinity(tid);
//
//    mem_allocator.register_thread(tid);
//    RC rc;
//    assert(g_synth_table_size % g_init_parallelism == 0);
//    assert(tid < g_init_parallelism);
//    while ((UInt32)ATOM_FETCH_ADD(next_tid, 0) < g_init_parallelism) {}
//    assert((UInt32)ATOM_FETCH_ADD(next_tid, 0) == g_init_parallelism);
//    uint64_t slice_size = g_synth_table_size / g_init_parallelism;
//    for (uint64_t key = slice_size * tid;
//         key < slice_size * (tid + 1);
//         key ++
//            ) {
//        row_t * new_row = NULL;
//        uint64_t row_id;
//        int part_id = key_to_part(key);     //! 区间内 index
//        rc = the_table->get_new_row(new_row, part_id, row_id);  //! TODO, row_id 到底返回的啥？
//        assert(rc == RCOK);
//        uint64_t primary_key = key;
//        new_row->set_primary_key(primary_key);
//        new_row->set_value(0, &primary_key);    //! 第 0 列是主键
//        Catalog * schema = the_table->get_schema();
//
//        for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
//            char value[6] = "hello";
//            new_row->set_value(fid, value);
//        }
//
//        itemid_t * m_item =
//                (itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id );
//        assert(m_item != NULL);
//        m_item->type = DT_row;
//        m_item->location = new_row;
//        m_item->valid = true;
//        uint64_t idx_key = primary_key;
//
//        rc = the_index->index_insert(idx_key, m_item, part_id);
//        assert(rc == RCOK);
//    }
//    return NULL;
//}
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
