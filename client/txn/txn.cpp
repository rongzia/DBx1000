
#include <cstring>
#include <common/mystats.h>

#include "txn.h"

#include "api/api_single_machine/cs_api.h"
#include "api/api_txn/api_txn.h"
#include "client/benchmarks/ycsb_query.h"
#include "client/thread.h"
#include "common/row_item.h"

void txn_man::init(thread_t * h_thd) {
	this->h_thd = h_thd;
	this->abort_cnt = this->wr_cnt = this->row_cnt = this->num_accesses_alloc = 0;
	this->ts_ready = false;
	this->accesses = new Access* [MAX_ROW_PER_TXN]();

	txn_count = 0;
}

uint64_t txn_man::get_thd_id() { return h_thd->get_thd_id(); }

void txn_man::set_txn_id(uint64_t txn_id) { this->txn_id = txn_id; }
uint64_t txn_man::get_txn_id() { return this->txn_id; }

void txn_man::set_ts(uint64_t timestamp) { this->timestamp_ = timestamp; }
uint64_t txn_man::get_ts() { return this->timestamp_; }

void PrintAccess(Access** pAccess, size_t size) {
    for(size_t i = 0; i < size; i++) {
        cout << "accesses[" << i << "].origin.key : " << pAccess[i]->orig_row->key_ << endl;
        cout << "accesses[" << i << "].data.key : " << pAccess[i]->data->key_ << endl;
    }
}

void txn_man::cleanup(RC rc) {
//    cout << "txn_man::cleanup" << endl;
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
	    if(accesses[rid]->orig_row->key_ != accesses[rid]->data->key_) {
//            PrintAccess(accesses, row_cnt);
	    }
	    assert(accesses[rid]->orig_row->key_ == accesses[rid]->data->key_);
	    assert(accesses[rid]->orig_row->size_ == accesses[rid]->data->size_);
	    assert(nullptr != accesses[rid]->orig_row->row_ && nullptr != accesses[rid]->data->row_);
//	    cout << "txn_man::cleanup, return key : " << accesses[rid]->orig_row->key_ << endl;
//		row_t * orig_r = accesses[rid]->orig_row;
//		dbx1000::RowItem* orig_r = accesses[rid]->orig_row;
		access_t type = accesses[rid]->type;
		if (type == WR && rc == Abort) { type = XP; }


		if (ROLL_BACK && type == XP &&
					(CC_ALG == DL_DETECT || 
					CC_ALG == NO_WAIT || 
					CC_ALG == WAIT_DIE)) 
		{
//			orig_r->return_row(type, this, accesses[rid]->orig_data);
		} else {
//			orig_r->return_row(type, this, accesses[rid]->data);
#ifdef WITH_RPC
            api_txn_client->ReturnRow(accesses[rid]->orig_row->key_, type, this, rid);
#else
            dbx1000::API::return_row(accesses[rid]->orig_row->key_, type, this, rid);
#endif // WITH_RPC
		}
	}
	row_cnt = 0;
	wr_cnt = 0;
}

dbx1000::RowItem* txn_man::get_row(uint64_t key, access_t type) {
//    cout << "txn_man::get_row, key : "  << key << endl;
    uint64_t thread_id = get_thd_id();
    stats.tmp_stats[thread_id]->profiler->Clear();
    stats.tmp_stats[thread_id]->profiler->Start();

	RC rc = RCOK;
	if (accesses[row_cnt] == NULL) {
		accesses[row_cnt] = new Access();
		accesses[row_cnt]->orig_row = new dbx1000::RowItem(key, ROW_SIZE);
		accesses[row_cnt]->data     = new dbx1000::RowItem(key, ROW_SIZE);
		num_accesses_alloc ++;
	} else {
	    accesses[row_cnt]->orig_row->key_ = key;
	    accesses[row_cnt]->data->key_ = key;
	}

	this->ts_ready = false;
#ifdef WITH_RPC
	rc = api_txn_client->GetRow(key, type, this, row_cnt);
#else
    rc = dbx1000::API::get_row(key, type, this, row_cnt);
#endif // WITH_RPC

	if (rc == Abort) {
		return NULL;
	}
	accesses[row_cnt]->type = type;
	assert(accesses[row_cnt]->orig_row->key_ == accesses[row_cnt]->data->key_);
	assert(accesses[row_cnt]->orig_row->size_ == accesses[row_cnt]->data->size_);
	memcpy(accesses[row_cnt]->data->row_, accesses[row_cnt]->orig_row->row_, ROW_SIZE);

	row_cnt ++;
	if (type == WR) { wr_cnt ++; }

    stats.tmp_stats[thread_id]->profiler->End();
	stats.tmp_stats[thread_id]->time_man += stats.tmp_stats[thread_id]->profiler->Nanos();
	if ( RD == type || SCAN == type) {
        return accesses[row_cnt - 1]->orig_row;
	} else {
        return accesses[row_cnt - 1]->data;
    }
}

RC txn_man::finish(RC rc) {
    uint64_t thread_id = this->get_thd_id();
    stats.tmp_stats[thread_id]->profiler->Clear();
    stats.tmp_stats[thread_id]->profiler->Start();

//    PrintAccess(accesses, row_cnt);
	cleanup(rc);

	stats.tmp_stats[thread_id]->time_man += stats.tmp_stats[thread_id]->profiler->Nanos();
	stats._stats[thread_id]->time_cleanup += stats.tmp_stats[thread_id]->profiler->Nanos();
	return rc;
}

/*
void txn_man::release() {
	for (int i = 0; i < num_accesses_alloc; i++) {
//        mem_allocator.free(accesses[i], 0);
        std::free(accesses[i]);
    }
//	mem_allocator.free(accesses, 0);
	std::free(accesses);
}
 */
