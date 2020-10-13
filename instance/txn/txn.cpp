
#include <cstring>
#include <iostream>
#include "txn.h"

#include "instance/benchmarks/ycsb_query.h"
#include "instance/benchmarks/tpcc_query.h"
#include "instance/concurrency_control/row_mvcc.h"
#include "instance/manager_instance.h"
#include "instance/thread.h"
#include "common/storage/row.h"
#include "common/storage/table.h"
#include "common/storage/catalog.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/tablespace/page.h"
#include "common/storage/row_handler.h"
#include "common/workload/ycsb.h"
#include "common/workload/tpcc.h"
#include "common/workload/tpcc_helper.h"
#include "common/workload/wl.h"
#include "common/mystats.h"
#include "common/myhelper.h"

void txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	this->h_thd = h_thd;
	this->h_wl = h_wl;
	/* pthread_mutex_init(&txn_lock, NULL);
	lock_ready = false;
	ready_part = 0; */
	row_cnt = 0;
	wr_cnt = 0;
	/* insert_cnt = 0; */
	accesses = new Access* [MAX_ROW_PER_TXN]();
	for (int i = 0; i < MAX_ROW_PER_TXN; i++) {
        accesses[i] = nullptr;
    }
	num_accesses_alloc = 0;
	/*
#if CC_ALG == TICTOC || CC_ALG == SILO
	_pre_abort = (g_params["pre_abort"] == "true");
	if (g_params["validation_lock"] == "no-wait")
		_validation_no_wait = true;
	else if (g_params["validation_lock"] == "waiting")
		_validation_no_wait = false;
	else 
		assert(false);
#endif
#if CC_ALG == TICTOC
	_max_wts = 0;
	_write_copy_ptr = (g_params["write_copy_form"] == "ptr");
	_atomic_timestamp = (g_params["atomic_timestamp"] == "true");
#elif CC_ALG == SILO
	_cur_tid = 0;
#endif
	 */

}

void txn_man::set_txn_id(txnid_t txn_id) {
	this->txn_id = txn_id;
}

txnid_t txn_man::get_txn_id() {
	return this->txn_id;
}

workload * txn_man::get_wl() {
	return h_wl;
}

uint64_t txn_man::get_thd_id() {
	return h_thd->get_thd_id();
}

void txn_man::set_ts(ts_t timestamp) {
	this->timestamp = timestamp;
}

ts_t txn_man::get_ts() {
	return this->timestamp;
}

void txn_man::cleanup(RC rc) {
    /*
#if CC_ALG == HEKATON
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
	return;
#endif
     */
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		row_t * orig_r = accesses[rid]->orig_row;
		access_t type = accesses[rid]->type;
		if (type == WR && rc == RC::Abort) {
			type = XP;
		}
    /*
#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
		if (type == RD) {
			accesses[rid]->data = NULL;
			continue;
		}
#endif
     */

		if (ROLL_BACK && type == XP &&
					(CC_ALG == DL_DETECT || 
					CC_ALG == NO_WAIT || 
					CC_ALG == WAIT_DIE)) 
		{
			/* orig_r->return_row(type, this, accesses[rid]->orig_data); */
		} else {
			/* orig_r->return_row(type, this, accesses[rid]->data); */
//            this->h_thd->manager_client_->ReturnRow(accesses[rid]->data->key_, type, this, accesses[rid]->data);
            this->h_thd->manager_client_->row_handler()->ReturnRow(accesses[rid]->table, accesses[rid]->data->get_primary_key(), type, this, accesses[rid]->data);
		}
		/*
#if CC_ALG != TICTOC && CC_ALG != SILO
		accesses[rid]->data = NULL;
#endif
		 */
	}

	/*
	if (rc == RC::Abort) {
		for (UInt32 i = 0; i < insert_cnt; i ++) {
			row_t * row = insert_rows[i];
			assert(g_part_alloc == false);
#if CC_ALG != HSTORE && CC_ALG != OCC
			mem_allocator.free(row->manager, 0);
#endif
			row->free_row();
			mem_allocator.free(row, sizeof(row));
		}
	}
	 */
	row_cnt = 0;
	wr_cnt = 0;
	/* insert_cnt = 0;
#if CC_ALG == DL_DETECT
	dl_detector.clear_dep(get_txn_id());
#endif
	 */
}

row_t * txn_man::get_row(TABLES table, uint64_t key, access_t type) {
    /*
	if (CC_ALG == HSTORE)
		return row;
	uint64_t starttime = get_sys_clock();
     */
    dbx1000::Profiler profiler;
    profiler.Start();
	RC rc = RC::RCOK;
	if (accesses[row_cnt] == nullptr) {
		Access * access = new Access();
		accesses[row_cnt] = access;
		/*
#if (CC_ALG == SILO || CC_ALG == TICTOC)
		access->data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->data->init(MAX_TUPLE_SIZE);
		access->orig_data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->orig_data->init(MAX_TUPLE_SIZE);
#elif (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
		access->orig_data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->orig_data->init(MAX_TUPLE_SIZE);
#endif
		 */
		num_accesses_alloc ++;
	}
	/// accesses[i] 不为 RowItem:row_ 申请空间，读写空间都是指向的 row_mvcc_ 里面的 RowItem
	/* rc = row->get_row(type, this, accesses[ row_cnt ]->data); */
	rc = this->h_thd->manager_client_->row_handler()->GetRow(table, key, type, this, accesses[row_cnt]->data);


	if (rc == RC::Abort) {
		return NULL;
	}
    assert(accesses[row_cnt]->data == this->cur_row);
    assert(accesses[row_cnt]->data->get_primary_key() == key);
    assert(accesses[row_cnt]->data->get_tuple_size() == GetTable(table)->get_schema()->tuple_size);

	accesses[row_cnt]->type = type;
	accesses[row_cnt]->table = table;
	/*
	accesses[row_cnt]->orig_row = row;
#if CC_ALG == TICTOC
	accesses[row_cnt]->wts = last_wts;
	accesses[row_cnt]->rts = last_rts;
#elif CC_ALG == SILO
	accesses[row_cnt]->tid = last_tid;
#elif CC_ALG == HEKATON
	accesses[row_cnt]->history_entry = history_entry;
#endif

#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
	if (type == WR) {
		accesses[row_cnt]->orig_data->table = row->get_table();
		accesses[row_cnt]->orig_data->copy(row);
	}
#endif

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
	if (type == RD)
		row->return_row(type, this, accesses[ row_cnt ]->data);
#endif
	 */
	
	row_cnt ++;
	if (type == WR) {
		wr_cnt ++;
	}
    /*
	uint64_t timespan = get_sys_clock() - starttime;
	INC_TMP_STATS(get_thd_id(), time_man, timespan);
     */
	profiler.End();
	this->h_thd->manager_client_->stats().tmp_stats[h_thd->get_thd_id()]->time_man += profiler.Nanos();
	return accesses[row_cnt - 1]->data;
}
/*
void txn_man::insert_row(row_t * row, table_t * table) {
	if (CC_ALG == HSTORE)
		return;
	assert(insert_cnt < MAX_ROW_PER_TXN);
	insert_rows[insert_cnt ++] = row;
}

itemid_t *
txn_man::index_read(INDEX * index, idx_key_t key, int part_id) {
	uint64_t starttime = get_sys_clock();
	itemid_t * item;
	index->index_read(key, item, part_id, get_thd_id());
	INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
	return item;
}

void 
txn_man::index_read(INDEX * index, idx_key_t key, int part_id, itemid_t *& item) {
	uint64_t starttime = get_sys_clock();
	index->index_read(key, item, part_id, get_thd_id());
	INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
}
*/
RC txn_man::finish(RC rc) {
    dbx1000::Profiler profiler;
    profiler.Start();
    /*
#if CC_ALG == HSTORE
	return RCOK;
#endif
	uint64_t starttime = get_sys_clock();
#if CC_ALG == OCC
	if (rc == RCOK)
		rc = occ_man.validate(this);
	else 
		cleanup(rc);
#elif CC_ALG == TICTOC
	if (rc == RCOK)
		rc = validate_tictoc();
	else 
		cleanup(rc);
#elif CC_ALG == SILO
	if (rc == RCOK)
		rc = validate_silo();
	else 
		cleanup(rc);
#elif CC_ALG == HEKATON
	rc = validate_hekaton(rc);
	cleanup(rc);
#else 
	cleanup(rc);
#endif
     */
	cleanup(rc);
    /*
	uint64_t timespan = get_sys_clock() - starttime;
	INC_TMP_STATS(get_thd_id(), time_man,  timespan);
	INC_STATS(get_thd_id(), time_cleanup,  timespan);
     */
	profiler.End();
	this->h_thd->manager_client_->stats().tmp_stats[h_thd->get_thd_id()]->time_man += profiler.Nanos();
	this->h_thd->manager_client_->stats()._stats[h_thd->get_thd_id()]->time_cleanup += (profiler.Nanos());

    mvcc_maps_.clear();
	return rc;
}
/*
void txn_man::release() {
	for (int i = 0; i < num_accesses_alloc; i++)
		mem_allocator.free(accesses[i], 0);
	mem_allocator.free(accesses, 0);
}
*/

table_t* txn_man::GetTable(TABLES table) {
    if(table == TABLES::WAREHOUSE) { return ((tpcc_wl *)(this->h_thd->manager_client_->m_workload()))->t_warehouse; }
    if(table == TABLES::ITEM) { return ((tpcc_wl *)(this->h_thd->manager_client_->m_workload()))->t_item; }
    if(table == TABLES::DISTRICT) { return ((tpcc_wl *)(this->h_thd->manager_client_->m_workload()))->t_district; }
    if(table == TABLES::CUSTOMER) { return ((tpcc_wl *)(this->h_thd->manager_client_->m_workload()))->t_customer; }
    if(table == TABLES::HISTORY) { return ((tpcc_wl *)(this->h_thd->manager_client_->m_workload()))->t_history; }
    if(table == TABLES::STOCK) { return ((tpcc_wl *)(this->h_thd->manager_client_->m_workload()))->t_stock; }
    if(table == TABLES::NEW_ORDER) { return ((tpcc_wl *)(this->h_thd->manager_client_->m_workload()))->t_neworder; }
    if(table == TABLES::ORDER) { return ((tpcc_wl *)(this->h_thd->manager_client_->m_workload()))->t_order; }
    if(table == TABLES::ORDER_LINE) { return ((tpcc_wl *)(this->h_thd->manager_client_->m_workload()))->t_orderline; }
    if(table == TABLES::MAIN_TABLE) { return ((ycsb_wl *)(this->h_thd->manager_client_->m_workload()))->the_table; }
    else { assert(false); return nullptr; }
}

void txn_man::GetMvccSharedPtr(TABLES table, uint64_t key) {
    auto global_mvcc_map_i = h_thd->manager_client_->mvcc_vectors()->at(table);
    shared_ptr<Row_mvcc> shared_p;
    while(!ATOM_CAS(global_mvcc_map_i->at(key).second, false, true))
        PAUSE
    shared_p = global_mvcc_map_i->at(key).first.lock();
    if(global_mvcc_map_i->at(key).first.expired()) {
        assert(shared_p == nullptr);
        assert(global_mvcc_map_i->at(key).first.use_count() == 0);
        assert(global_mvcc_map_i->at(key).first.expired());
        shared_p = make_shared<Row_mvcc>();
        global_mvcc_map_i->at(key).first = shared_p;
#if WORKLOAD == YCSB
        shared_p->init(table, GetTable(table), key, this->h_thd->manager_client_->record_buffer_);
#elif WORKLOAD == TPCC
        shared_p->init(table, GetTable(table), key, this->h_thd->manager_client_->record_buffer_);
#endif
    } else {
        assert(!global_mvcc_map_i->at(key).first.expired());
        // shared_p = global_mvcc_map_i[key].first.lock();
        assert(shared_p != nullptr);
        assert(global_mvcc_map_i->at(key).first.use_count() > 0);
        assert(!global_mvcc_map_i->at(key).first.expired());
    }
    this->mvcc_maps_[table].insert(make_pair(key, shared_p));
    global_mvcc_map_i->at(key).second = false;
}
