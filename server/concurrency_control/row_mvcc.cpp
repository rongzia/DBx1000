
#include <cstring>
#include <chrono>
#include <memory>
#include "row_mvcc.h"
#include "server/manager_server.h"

#include "common/myhelper.h"
#include "api/api_single_machine/cs_api.h"
#include "api/api_cc/api_cc.h"
#include "server/workload/wl.h"
#include "server/workload/ycsb_wl.h"
#include "server/buffer/buffer.h"

#if CC_ALG == MVCC

#define DEBUG1 1

Row_mvcc::Row_mvcc() {}

void Row_mvcc::recycle(uint64_t thread_id){
    ts_t min_ts = glob_manager_server->GetMinTs(thread_id);
    if(_oldest_wts < min_ts) {
		ts_t max_recycle_ts = 0;
		ts_t idx = _his_len;
		for (uint32_t i = 0; i < _his_len; i++) {
			if (_write_history[i].valid
				&& _write_history[i].ts < min_ts
				&& _write_history[i].ts > max_recycle_ts)
			{
				max_recycle_ts = _write_history[i].ts;
				idx = i;
			}
		}

		if (idx != _his_len) {
			_oldest_wts = max_recycle_ts;
			for (uint32_t i = 0; i < _his_len; i++) {
				if (_write_history[i].valid
					&& _write_history[i].ts <= max_recycle_ts)
				{
					_write_history[i].valid = false;
					_write_history[i].reserved = false;
					glob_manager_server->wl_->buffer_->BufferPut(this->key_, _write_history[i].row->row_, _write_history[i].row->size_);
#ifdef DEBUG1
#else
					if(_latest_row == _write_history[i].row) {
					    _latest_row = nullptr;
					}
#endif
					delete _write_history[i].row;
					_write_history[i].row = nullptr;
					assert(nullptr == _write_history[i].row);
					_num_versions --;
				}
			}
		}
    }
}

void Row_mvcc::init(dbx1000::RowItem* rowItem) {
//	_row = row;
	key_ = rowItem->key_;
	row_size_ = rowItem->size_;
	_req_len = _his_len = 4;

	_write_history = new WriteHisEntry[_his_len]();
	_requests = new ReqEntry[_req_len]();
	for (uint32_t i = 0; i < _his_len; i++) {
		_requests[i].valid = false;
		_requests[i].txn = nullptr;
		_write_history[i].valid = false;
		_write_history[i].reserved = false;
		_write_history[i].row = nullptr;
	}
#ifdef DEBUG1
    _latest_row = new dbx1000::RowItem(this->key_, this->row_size_);
	memcpy(_latest_row->row_, rowItem->row_, this->row_size_);
#else
    _latest_row = nullptr;
#endif
	_latest_wts = 0;
	_oldest_wts = 0;

	_num_versions = 0;
	_exists_prewrite = false;
	_max_served_rts = 0;
	
	blatch = false;
//	latch = (pthread_mutex_t *) _mm_malloc(sizeof(pthread_mutex_t), 64);
//	latch = new pthread_mutex_t();
//	pthread_mutex_init(latch, NULL);
}

//! 从 _requests 中选出一个空的 ReqEntry，并给该 ReqEntry 赋上相应的数据
void Row_mvcc::buffer_req(TsType type, dbx1000::TxnRowMan* local_txn, bool served)
{
	uint32_t access_num = 1;    /// access_num 控制 _requests 链表长度不会无限增长，目前只能增长一次（assert(access_num == 1)）
	while (true) {
	    //! for 语句从 _requests 中选出一个空的，若有空的则返回，没有则将 _requests 链表扩展长度
		for (uint32_t i = 0; i < _req_len; i++) {
			// TODO No need to keep all read history.
			// in some sense, only need to keep the served read request with the max ts.
			// 
			if (!_requests[i].valid) {
				_requests[i].valid = true;
				_requests[i].type = type;
				_requests[i].ts = local_txn->timestamp_;
				_requests[i].txn = local_txn;
				_requests[i].time = std::chrono::system_clock::now().time_since_epoch().count();
				return;
			}
		}
		assert(access_num == 1);
		double_list(1);
		access_num ++;
	}
}

//! 将原来的 _write_history 或 _requests 链表扩展成双倍的长度，
//! 扩展后原来的部分不变，后面一半为空
//! 即在原来的链表基础上，在后面增加相同长度的空链表
//! list == 0 or 1，0 表示对 _write_history 扩展，1 表示对 _requests 扩展
void Row_mvcc::double_list(uint32_t list)
{
    cout << "double_list" << endl;
	if (list == 0) {
//		WriteHisEntry * temp = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len * 2, 64);
		WriteHisEntry * temp = new WriteHisEntry[_his_len * 2]();
		for (uint32_t i = 0; i < _his_len; i++) {
			temp[i].valid = _write_history[i].valid;
			temp[i].reserved = _write_history[i].reserved;
			temp[i].ts = _write_history[i].ts;
			temp[i].row = _write_history[i].row;
		}
		for (uint32_t i = _his_len; i < _his_len * 2; i++) {
			temp[i].valid = false;
			temp[i].reserved = false;
			temp[i].row = nullptr;
		}
//		_mm_free(_write_history);
        delete _write_history;   /// TODO
		_write_history = temp;
		_his_len = _his_len * 2;
	} else {
		assert(list == 1);
//		ReqEntry * temp = (ReqEntry *) _mm_malloc(sizeof(ReqEntry) * _req_len * 2, 64);
		ReqEntry * temp = new ReqEntry[_req_len * 2]();
		for (uint32_t i = 0; i < _req_len; i++) {
			temp[i].valid = _requests[i].valid;
			temp[i].type = _requests[i].type;
			temp[i].ts = _requests[i].ts;
			temp[i].txn = _requests[i].txn;
			temp[i].time = _requests[i].time;
		}
		for (uint32_t i = _req_len; i < _req_len * 2; i++) {
            temp[i].valid = false;
        }
//		_mm_free(_requests);
        delete _requests;   /// TODO
		_requests = temp;
		_req_len = _req_len * 2;
	}
}

void Row_mvcc::GetLatestRow(dbx1000::RowItem* temp) {
    if(nullptr == _latest_row) {
        glob_manager_server->wl_->buffer_->BufferGet(this->key_, temp->row_, this->row_size_);
    } else {
        assert(nullptr != _latest_row->row_);
        memcpy(temp->row_, _latest_row->row_, this->row_size_);
    }
}

void PrintRequset(ReqEntry *reqEntry, size_t size) {
    for(size_t i = 0; i< size; i++) {
        if(reqEntry[i].valid) {
            cout << "key_ : " << reqEntry[i].txn->cur_row_->key_ << endl;
        }
    }
}

void PrintWriteHisEntry(WriteHisEntry *writeHisEntry, size_t size) {
    for(size_t i = 0; i< size; i++) {
        if(writeHisEntry[i].valid) {
            cout << "key_ : " << writeHisEntry[i].row->key_ << endl;
        }
    }
}

//RC Row_mvcc::access(dbx1000::TxnRowMan* txn, TsType type, dbx1000::RowItem* row, size_t row_size) {
RC Row_mvcc::access(dbx1000::TxnRowMan* local_txn, TsType type) {
//    cout << "Row_mvcc::access" << endl;
	RC rc = RCOK;
	ts_t ts = local_txn->timestamp_;

	uint64_t thread_id = local_txn->thread_id_;
//	recycle(thread_id);
	dbx1000::Profiler profiler;
	profiler.Start();
    /// 等待获取 row 上的锁，即其他涉及该 row 的事务结束
    while (!ATOM_CAS(blatch, false, true)) { PAUSE }
    profiler.End();
	stats._stats[thread_id]->debug4 += profiler.Nanos();\

	profiler.Clear();
	profiler.Start();

#if DEBUG_CC
	for (uint32_t i = 0; i < _req_len; i++)
		if (_requests[i].valid) {
			assert(_requests[i].ts > _latest_wts);
			if (_exists_prewrite)
				assert(_prewrite_ts < _requests[i].ts);
		}
#endif
	if (type == R_REQ) {
//        cout << "Row_mvcc::access, type : R_REQ" << endl;
		if (ts < _oldest_wts) {
            // the version was already recycled... This should be very rare
            rc = Abort;
        }
		//! 时间戳大于最近写时间戳
		else if (ts > _latest_wts) {
			if (_exists_prewrite && _prewrite_ts < ts)
			{
//			cout << "Row_mvcc::access, type == R_REQ, _exists_prewrite" << endl;
//PrintRequset(_requests, _req_len);
//PrintWriteHisEntry(_write_history, _his_len);
//cout << "current read : " << _row->key_ << endl;
			    //! 之前的写没有完成
				// exists a pending prewrite request before the current read. should wait.
				rc = WAIT;
				buffer_req(R_REQ, local_txn, false);
				local_txn->ts_ready_ = false;
			} else {
			    //! 读最新的 row
				// should just read
				rc = RCOK;
#ifdef DEBUG1
				memcpy(local_txn->cur_row_->row_, _latest_row->row_, row_size_);
#else
				GetLatestRow(local_txn->cur_row_);
#endif
				if (ts > _max_served_rts)
					_max_served_rts = ts;
			}
		}
		//! 时间戳大于最老写时间戳，小于最近写时间戳
		else {
			rc = RCOK;
			// ts is between _oldest_wts and _latest_wts, should find the correct version
			uint32_t the_ts = 0;
		   	uint32_t the_i = _his_len;
		   	//! 这个 for 找出 _write_history 中 (valid && max(_write_history[i].ts ) 的 WriteHisEntry
		   	//! 且这个 WriteHisEntry 满足：_oldest_wts < WriteHisEntry.ts < 当前事务 ts < _latest_wts
	   		for (uint32_t i = 0; i < _his_len; i++) {
		   		if (_write_history[i].valid 
					&& _write_history[i].ts < ts 
			   		&& _write_history[i].ts > the_ts) 
	   			{
		   			the_ts = _write_history[i].ts;
			  		the_i = i;
				}
			}
			if (the_i == _his_len)
#ifdef DEBUG1
				memcpy(local_txn->cur_row_->row_, _latest_row->row_, row_size_);
#else
				GetLatestRow(local_txn->cur_row_);
#endif
   			else 
	   			local_txn->cur_row_ = _write_history[the_i].row;
		}
	} else if (type == P_REQ) {
//        cout << "Row_mvcc::access, type : P_REQ" << endl;
		if (ts < _latest_wts || ts < _max_served_rts || (_exists_prewrite && _prewrite_ts > ts)) {
            rc = Abort;
        }
		else if (_exists_prewrite) {  // _prewrite_ts < ts
			rc = WAIT;
			buffer_req(P_REQ, local_txn, false);
			local_txn->ts_ready_ = false;
		} else {
			rc = RCOK;
			dbx1000::RowItem* res_row = reserveRow(ts, local_txn->thread_id_);
			assert(nullptr != res_row);
			GetLatestRow(local_txn->cur_row_);
			memset(local_txn->cur_row_->row_, 'a', row_size_);
		}
	} else if (type == W_REQ) {
//        cout << "Row_mvcc::access, type : W_REQ" << endl;
		rc = RCOK;
		assert(ts > _latest_wts);
//		assert(row == _write_history[_prewrite_his_id].row);
        assert(local_txn->cur_row_->key_ == this->key_);
		_write_history[_prewrite_his_id].valid = true;
		_write_history[_prewrite_his_id].ts = ts;
		_latest_wts = ts;
		assert(nullptr != _write_history[_prewrite_his_id].row);
		assert(nullptr != local_txn->cur_row_);
        _write_history[_prewrite_his_id].row->MergeFrom(*local_txn->cur_row_);
#ifdef DEBUG1
        memcpy(_latest_row->row_, local_txn->cur_row_->row_, this->row_size_);
#else
		_latest_row = _write_history[_prewrite_his_id].row;
#endif
		_exists_prewrite = false;
		_num_versions ++;
		update_buffer(local_txn, W_REQ);
	} else if (type == XP_REQ) {
//        cout << "Row_mvcc::access, type : XP_REQ" << endl;
		rc = RCOK;
        assert(local_txn->cur_row_->key_ == this->key_);
		_write_history[_prewrite_his_id].valid = false;
		_write_history[_prewrite_his_id].reserved = false;
		_exists_prewrite = false;
		update_buffer(local_txn, XP_REQ);
	} else {
        assert(false);
    }

	profiler.End();
	stats._stats[thread_id]->debug3 += profiler.Nanos();

	if (g_central_man) {
//        glob_manager->release_row(_row);
    }
	else {
        blatch = false;
        //pthread_mutex_unlock( latch );
    }
//    std::cout << "out Row_mvcc::access" << std::endl;
	return rc;
}


//! 从 _write_history 中分配一个 WriteHisEntry，
//! 该 WriteHisEntry 满足 valid == false && reserved == false，且尽可能 WriteHisEntry.row != NULL(为了避免额外申请内存的操作)
//! in : ts 为当前事务的开始时间
//! out : WriteHisEntry.row，预留给后面的写操作。（该 WriteHisEntry's valid == false && reserved == true ）
//dbx1000::RowItem* Row_mvcc::reserveRow(ts_t ts, dbx1000::TxnRowMan* txn)
dbx1000::RowItem* Row_mvcc::reserveRow(ts_t ts, uint64_t thread_id)
{
	assert(!_exists_prewrite);
	
	// Garbage Collection
	//! 所有已经开始事务的最先开始的时间
	ts_t min_ts = glob_manager_server->GetMinTs(thread_id);
	/// 版本回收，凡是时间戳小于 min_ts 的版本，都应当写入存储
	if (_oldest_wts < min_ts && 
		_num_versions == _his_len)
	{
		ts_t max_recycle_ts = 0;
		ts_t idx = _his_len;
		/// 该 for 挑出最接近 min_ts 的一个版本
		for (uint32_t i = 0; i < _his_len; i++) {
			if (_write_history[i].valid
				&& _write_history[i].ts < min_ts
				&& _write_history[i].ts > max_recycle_ts)		
			{
				max_recycle_ts = _write_history[i].ts;
				idx = i;
			}
		}
		// some entries can be garbage collected.
		/// 所有小于等于 max_recycle_ts 的行，都被写回储存，空间被回收
		if (idx != _his_len) {
//			dbx1000::RowItem* temp = _row;
//			_row = _write_history[idx].row;
//			_write_history[idx].row = temp;
			_oldest_wts = max_recycle_ts;
			for (uint32_t i = 0; i < _his_len; i++) {
				if (_write_history[i].valid
					&& _write_history[i].ts <= max_recycle_ts)
				{
					_write_history[i].valid = false;
					_write_history[i].reserved = false;
					glob_manager_server->wl_->buffer_->BufferPut(this->key_, _write_history[i].row->row_, _write_history[i].row->size_);
#ifdef DEBUG1
#else
					if(_latest_row == _write_history[i].row) {
					    _latest_row = nullptr;
					}
#endif
                    delete _write_history[i].row;
					_write_history[i].row = nullptr;
					assert(nullptr == _write_history[i].row);
					_num_versions --;
				}
			}
		}
	}
	
#if DEBUG_CC
	uint32_t his_size = 0;
	uint64_t max_ts = 0;
	for (uint32_t i = 0; i < _his_len; i++) 
		if (_write_history[i].valid) {
			his_size ++;
			if (_write_history[i].ts > max_ts)
				max_ts = _write_history[i].ts;
		}
	assert(his_size == _num_versions);
	if (_num_versions > 0)
		assert(max_ts == _latest_wts);
#endif
	uint32_t idx = _his_len;
	// _write_history is not full, find an unused entry for P_REQ.
	if (_num_versions < _his_len) {
	    //! 尽量挑 valid == false && reserved == false && row != NULL
	    //! 实在不行就挑最后一个 valid == false && reserved == false
		for (uint32_t i = 0; i < _his_len; i++) {
			if (!_write_history[i].valid 
				&& !_write_history[i].reserved 
				&& _write_history[i].row != nullptr)
			{
				idx = i;
				break;
			}
			else if (!_write_history[i].valid 
				 	 && !_write_history[i].reserved)
				idx = i;
		}
		assert(idx < _his_len);
	}
//	dbx1000::RowItem* row;
	if (idx == _his_len) { 
		if (_his_len >= g_thread_cnt) {
			// all entries are taken. recycle the oldest version if _his_len is too long already
			ts_t min_ts2 = UINT64_MAX;
			for (uint32_t i = 0; i < _his_len; i++) {
				if (_write_history[i].valid && _write_history[i].ts < min_ts2) {
					min_ts2 = _write_history[i].ts;
					idx = i;
				}
			}
			assert(min_ts2 > _oldest_wts);
			assert(nullptr != _write_history[idx].row);
			glob_manager_server->wl_->buffer_->BufferPut(this->key_, _write_history[idx].row->row_, _write_history[idx].row->size_);
//			row = _write_history[idx].row;
//			_row = _write_history[idx].row;
//			_write_history[idx].row = row;
			_oldest_wts = min_ts2;
			_num_versions --;
		} else {
			// double the history size. 
			double_list(0);
			_prewrite_ts = ts;
#if DEBUG_CC
			for (uint32_t i = 0; i < _his_len / 2; i++)
				assert(_write_history[i].valid);
			assert(!_write_history[_his_len / 2].valid);
#endif
			idx = _his_len / 2;
		}
	} 
	assert(idx != _his_len);
	// some entries are not taken. But the row of that entry is NULL.
	if (nullptr == _write_history[idx].row) {
		_write_history[idx].row = new dbx1000::RowItem(this->key_, row_size_);
	}
	_write_history[idx].valid = false;
	_write_history[idx].reserved = true;
	_write_history[idx].ts = ts;
	_exists_prewrite = true;
	assert(nullptr != _write_history[idx].row->row_);
	_prewrite_his_id = idx;
	_prewrite_ts = ts;
	return _write_history[idx].row;
}

void Row_mvcc::update_buffer(dbx1000::TxnRowMan* local_txn, TsType type) {
	// the current txn performs WR or XP.
	// immediate following R_REQ and P_REQ should return.
	ts_t ts = local_txn->timestamp_;
	// figure out the ts for the next pending P_REQ
	ts_t next_pre_ts = UINT64_MAX ;
	//! 找出时间最老的 非读请求，记录到 next_pre_ts 里
	for (uint32_t i = 0; i < _req_len; i++) {
        if (_requests[i].valid && _requests[i].type == P_REQ
            && _requests[i].ts > ts
            && _requests[i].ts < next_pre_ts) {
            next_pre_ts = _requests[i].ts;
        }
    }
	// return all pending quests between txn->ts and next_pre_ts
	for (uint32_t i = 0; i < _req_len; i++)	{
		if (_requests[i].valid)	
			assert(_requests[i].ts > ts);
		// return pending R_REQ 
		if (_requests[i].valid && _requests[i].type == R_REQ && _requests[i].ts < next_pre_ts) {
			if (_requests[i].ts > _max_served_rts)
				_max_served_rts = _requests[i].ts;
#ifdef DEBUG1
			memcpy(_requests[i].txn->cur_row_->row_, _latest_row->row_, this->row_size_);
#else
			GetLatestRow(_requests[i].txn->cur_row_);
#endif
			_requests[i].txn->ts_ready_ = true;
#ifdef WITH_RPC
			api_con_ctl_client->SetTsReady(_requests[i].txn);
#else
			dbx1000::API::SetTsReady(_requests[i].txn);
#endif // WITH_RPC
			_requests[i].valid = false;
		}
		// return one pending P_REQ
		else if (_requests[i].valid && _requests[i].ts == next_pre_ts) {
			assert(_requests[i].type == P_REQ);
			dbx1000::RowItem* res_row = reserveRow(_requests[i].ts, local_txn->thread_id_);
			assert(res_row);
//			res_row->copy(_latest_row);
#ifdef DEBUG1
			memcpy(_requests[i].txn->cur_row_->row_, _latest_row->row_, this->row_size_);
#else
			GetLatestRow(_requests[i].txn->cur_row_);
#endif
			_requests[i].txn->ts_ready_ = true;
#ifdef WITH_RPC
			api_con_ctl_client->SetTsReady(_requests[i].txn);
#else
			dbx1000::API::SetTsReady(_requests[i].txn);
#endif // WITH_RPC
			_requests[i].valid = false;
		}
	}
}

#endif
