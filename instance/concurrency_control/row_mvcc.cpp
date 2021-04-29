#include <cassert>
#include <cstring>
#include "row_mvcc.h"

#include "common/buffer/buffer.h"
#include "common/buffer/record_buffer.h"
#include "common/index/index.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/tablespace/page.h"
#include "common/storage/catalog.h"
#include "common/storage/row.h"
#include "common/storage/row_handler.h"
#include "common/storage/table.h"
#include "common/workload/ycsb.h"
#include "common/workload/wl.h"
#include "common/myhelper.h"
#include "instance/txn/txn.h"
#include "instance/manager_instance.h"
#include "instance/thread.h"
#include "common/lock_table/lock_table.h"


#if CC_ALG == MVCC

Row_mvcc::~Row_mvcc() {
    /// 刷 page 至 buffer
	RC rc = RC::RCOK;
	if(_latest_row != _row) {
    	RC rc = managerInstance_->row_handler_->WriteRow(table_, key_, _latest_row, size_);
	}
    assert(RC::RCOK == rc);

	for(uint32_t i = 0; i < _his_len; i++) {
		if(_write_history[i].row != nullptr) {
			delete _write_history[i].row;
			_write_history[i].row = nullptr;
		}
	}
	delete [] _requests;
	delete [] _write_history;
	delete _row;
	_requests = nullptr;
	_write_history = nullptr;
    _row = nullptr;
//	if(key_ % 1000000 == 0) {cout << "Row_mvcc::~Row_mvcc: " << key_ << endl;}
}

void Row_mvcc::init(dbx1000::ManagerInstance* managerInstance, TABLES table, uint64_t key) {
	/* _row = row; */
	this->managerInstance_ = managerInstance;
	this->table_ = table;
	this->key_ = key;
    this->size_ = managerInstance_->m_workload_->tables_[table_]->get_schema()->get_tuple_size();

    uint64_t row_id;
    managerInstance_->m_workload_->tables_[table_]->get_new_row(this->_row, 0, row_id);
    RC rc = managerInstance_->row_handler_->ReadRow(table_, key_, _row, size_);
//    RC rc =	workload_->buffers_[table_]->BufferGet(key_, _row->data, size_);
    _row->set_primary_key(key_);
    assert(RC::RCOK == rc);
	_his_len = 4;
	_req_len = _his_len;

	_write_history = new WriteHisEntry[_his_len]();
	_requests = new ReqEntry[_req_len]();
	for (uint32_t i = 0; i < _his_len; i++) {
		_requests[i].valid = false;
		_write_history[i].valid = false;
		_write_history[i].reserved = false;
		_write_history[i].row = nullptr;
	}
	/* _latest_row = _row; */
	_latest_row = this->_row;
	_latest_wts = 0;
	_oldest_wts = 0;

	_num_versions = 0;
	_exists_prewrite = false;
	_max_served_rts = 0;
	
	blatch = false;
	/*
	latch = (pthread_mutex_t *) _mm_malloc(sizeof(pthread_mutex_t), 64);
	pthread_mutex_init(latch, NULL);
	 */
}

//! 从 _requests 中选出一个空的 ReqEntry，并给该 ReqEntry 赋上相应的数据
void Row_mvcc::buffer_req(TsType type, txn_man * txn, bool served)
{
	uint32_t access_num = 1;
	while (true) {
	    //! for 语句从 _requests 中选出一个空的，若有空的则返回，没有则将 _requests 链表扩展长度
		for (uint32_t i = 0; i < _req_len; i++) {
			// TODO No need to keep all read history.
			// in some sense, only need to keep the served read request with the max ts.
			// 
			if (!_requests[i].valid) {
				_requests[i].valid = true;
				_requests[i].type = type;
				_requests[i].ts = txn->get_ts();
				_requests[i].txn = txn;
				_requests[i].time = std::chrono::system_clock::now().time_since_epoch().count();
				return;
			}
		}
		assert(access_num == 1);
		double_list(1);
		access_num ++;      //! access_num 控制 _requests 链表长度不会无限增长，目前只能增长一次（assert(access_num == 1)）
	}
}

//! 将原来的 _write_history 或 _requests 链表扩展成双倍的长度，
//! 扩展后原来的部分不变，后面一半为空
//! 即在原来的链表基础上，在后面增加相同长度的空链表
//! list == 0 or 1，0 表示对 _write_history 扩展，1 表示对 _requests 扩展
void 
Row_mvcc::double_list(uint32_t list)
{
	if (list == 0) {
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
			temp[i].row = NULL;
		}
		/* _mm_free(_write_history); */
        delete [] _write_history;  /// 不会调用 _write_history[i].row 的析构函数 
		_write_history = temp;
		_his_len = _his_len * 2;
	} else {
		assert(list == 1);
		ReqEntry * temp = new ReqEntry[_req_len * 2]();
		for (uint32_t i = 0; i < _req_len; i++) {
			temp[i].valid = _requests[i].valid;
			temp[i].type = _requests[i].type;
			temp[i].ts = _requests[i].ts;
			temp[i].txn = _requests[i].txn;
			temp[i].time = _requests[i].time;
		}
		for (uint32_t i = _req_len; i < _req_len * 2; i++) 
			temp[i].valid = false;
		/* _mm_free(_requests); */
        delete [] _requests;  /// TODO : 是否会释放 txn_man?
		_requests = temp;
		_req_len = _req_len * 2;
	}
}

row_t* Row_mvcc::GetRow(uint64_t key) {
    row_t* new_row;
    uint64_t row_id;
    managerInstance_->m_workload_->tables_[table_]->get_new_row(new_row, 0, row_id);
    new_row->set_primary_key(key);
    new_row->set_value(0, &key);
    return new_row;
    // bool res = txn->h_thd->manager_client_->row_handler()->SnapShotReadRow(this->_row);
    // assert(res);
}
void Row_mvcc::CheckLatestRow(){
    if(nullptr == _latest_row || _latest_row == _row) {
        return;
    }
    bool flag = false;
    uint32_t idx = _his_len;
    for(uint32_t i = 0; i < _his_len; i++){
        if(_latest_row == _write_history[i].row){
            idx = i;
            flag = true;
            break;
        }
    }
    assert(true == flag);
    assert(idx < _his_len);
    assert(_write_history[idx].valid);
    assert(_write_history[idx].reserved);
}

RC Row_mvcc::access(txn_man * txn, TsType type, row_t * row) {
	RC rc = RC::RCOK;
	ts_t ts = txn->get_ts();
	dbx1000::Profiler profiler;
	profiler.Start();
	/*
uint64_t t1 = get_sys_clock();
	if (g_central_man)      //! g_central_man == false
		glob_manager->lock_row(_row);
	else
	 */
	    //! 等待获取 row 上的锁，即其他涉及该 row 的事务结束
		while (!ATOM_CAS(blatch, false, true))
			PAUSE
    profiler.End();
	txn->h_thd->manager_client_->stats_._stats[txn->get_thd_id()]->debug4 += profiler.Nanos();

	profiler.Clear();
	profiler.Start();
	/*
		//pthread_mutex_lock( latch );
uint64_t t2 = get_sys_clock();
INC_STATS(txn->get_thd_id(), debug4, t2 - t1);
	 */

#if DEBUG_CC
	for (uint32_t i = 0; i < _req_len; i++)
		if (_requests[i].valid) {
			assert(_requests[i].ts > _latest_wts);
			if (_exists_prewrite)
				assert(_prewrite_ts < _requests[i].ts);
		}
#endif
	if (type == R_REQ) {
		if (ts < _oldest_wts)
			// the version was already recycled... This should be very rare
			rc = RC::Abort;
		//! 时间戳大于最近写时间戳
		else if (ts > _latest_wts) {
			if (_exists_prewrite && _prewrite_ts < ts)
			{
			    //! 之前的写没有完成
				// exists a pending prewrite request before the current read. should wait.
				rc = RC::WAIT;
				buffer_req(R_REQ, txn, false);
				txn->ts_ready = false;
			} else {
			    //! 读最新的 row
				// should just read
				rc = RC::RCOK;
				txn->cur_row = _latest_row;
				if (ts > _max_served_rts)
					_max_served_rts = ts;
			}
		}
		//! 时间戳大于最老写时间戳，小于最近写时间戳
		else {
			rc = RC::RCOK;
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
	   		// 当历史不存在时，从 buffer 读
			if (the_i == _his_len) {
                /* txn->cur_row = _row; */
                txn->cur_row = _row;
            }
   			else {
                txn->cur_row = _write_history[the_i].row;
            }
		}
	} else if (type == P_REQ) {
		if (ts < _latest_wts || ts < _max_served_rts || (_exists_prewrite && _prewrite_ts > ts))
			rc = RC::Abort;
		else if (_exists_prewrite) {  // _prewrite_ts < ts
			rc = RC::WAIT;
			buffer_req(P_REQ, txn, false);
			txn->ts_ready = false;
		} else {
			rc = RC:: RCOK;
			row_t * res_row = reserveRow(ts, txn);
			assert(res_row);
			res_row->copy(_latest_row);
			txn->cur_row = res_row;
		}
	} else if (type == W_REQ) {
		rc = RC::RCOK;
		if(ts <= _latest_wts) {
		    cout <<"ts:" <<ts << ", _latest_wts:" << _latest_wts << endl;
		}
		assert(ts > _latest_wts);
		assert(row == _write_history[_prewrite_his_id].row);
		_write_history[_prewrite_his_id].valid = true;
		_write_history[_prewrite_his_id].ts = ts;
		_latest_wts = ts;
		_latest_row = row;
		_exists_prewrite = false;
		_num_versions ++;
		update_buffer(txn, W_REQ);
	} else if (type == XP_REQ) {
		assert(row == _write_history[_prewrite_his_id].row);
		_write_history[_prewrite_his_id].valid = false;
		_write_history[_prewrite_his_id].reserved = false;
		_exists_prewrite = false;
		update_buffer(txn, XP_REQ);
	} else 
		assert(false);
	profiler.End();
	txn->h_thd->manager_client_->stats_._stats[txn->get_thd_id()]->debug3 += profiler.Nanos();
	/*
INC_STATS(txn->get_thd_id(), debug3, get_sys_clock() - t2);
	if (g_central_man)
		glob_manager->release_row(_row);
	else
	 */
		blatch = false;
		//pthread_mutex_unlock( latch );	
		
	return rc;
}

//! 从 _write_history 中分配一个 WriteHisEntry，
//! 该 WriteHisEntry 满足 valid == false && reserved == false，且尽可能 WriteHisEntry.row != NULL(为了避免额外申请内存的操作)
//! in : ts 为当前事务的开始时间
//! out : WriteHisEntry.row，预留给后面的写操作。（该 WriteHisEntry's valid == false && reserved == true ）
row_t *
Row_mvcc::reserveRow(ts_t ts, txn_man * txn)
{
	assert(!_exists_prewrite);
	
	// Garbage Collection
	//! 所有已经开始事务的最先开始的时间
	/* ts_t min_ts = glob_manager->get_min_ts(txn->get_thd_id()); */
	ts_t min_ts = txn->h_thd->manager_client_->GetMinTs(txn->get_thd_id());
	/// 版本回收，凡是时间戳小于 min_ts 的版本，都应当写入 buffer
	if (_oldest_wts < min_ts && 
		_num_versions == _his_len)
	{
//	    PrintWriteHistory(min_ts); // test
		ts_t max_recycle_ts = 0;
		ts_t idx = _his_len;
		/// 该 for 挑出最接近（不等于） min_ts 的一个版本
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
		if (idx != _his_len) {
		    /*
			row_t * temp = _row;
			_row = _write_history[idx].row;
			_write_history[idx].row = temp;
		     */
		    row_t* temp = _row;
			_row = _write_history[idx].row;
			_write_history[idx].row = temp;
			_oldest_wts = max_recycle_ts;
			for (uint32_t i = 0; i < _his_len; i++) {
				if (_write_history[i].valid
					&& _write_history[i].ts <= max_recycle_ts)
				{
					_write_history[i].valid = false;
					_write_history[i].reserved = false;
					assert(_write_history[i].row);
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
	    //! 尽量挑 valid == true && reserved == true && row != NULL
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

	if (idx == _his_len) { 
		if (_his_len >= g_thread_cnt) {
			// all entries are taken. recycle the oldest version if _his_len is too long already
			ts_t temp_min_ts = UINT64_MAX;
			for (uint32_t i = 0; i < _his_len; i++) {
				if (_write_history[i].valid && _write_history[i].ts < temp_min_ts) {
					temp_min_ts = _write_history[i].ts;
					idx = i;
				}
			}
			assert(temp_min_ts > _oldest_wts);
			assert(_write_history[idx].row);
			/*
			row = _row;
			_row = _write_history[idx].row;
			_write_history[idx].row = row;
			 */
		    row_t* temp = _row;
			_row = _write_history[idx].row;
			_write_history[idx].row = temp;
			_oldest_wts = temp_min_ts;
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
	if (!_write_history[idx].row) {
		_write_history[idx].row = GetRow(_row->get_primary_key());
		/* _write_history[idx].row->init(MAX_TUPLE_SIZE); */
	}
	_write_history[idx].valid = false;
	_write_history[idx].reserved = true;
	_write_history[idx].ts = ts;
	assert(_write_history[idx].row);
	assert(_write_history[idx].row->get_primary_key() == this->key_);
	assert(_write_history[idx].row->get_tuple_size() == this->size_);
	_exists_prewrite = true;
	_prewrite_his_id = idx;
	_prewrite_ts = ts;
	return _write_history[idx].row;
}

void Row_mvcc::update_buffer(txn_man * txn, TsType type) {
	// the current txn performs WR or XP.
	// immediate following R_REQ and P_REQ should return.
	ts_t ts = txn->get_ts();
	// figure out the ts for the next pending P_REQ
	ts_t next_pre_ts = UINT64_MAX ;
	//! 找出时间最老的 非读请求，记录到 next_pre_ts 里
	for (uint32_t i = 0; i < _req_len; i++)	
		if (_requests[i].valid && _requests[i].type == P_REQ
			&& _requests[i].ts > ts
			&& _requests[i].ts < next_pre_ts)
			next_pre_ts = _requests[i].ts;
	// return all pending quests between txn->ts and next_pre_ts
	for (uint32_t i = 0; i < _req_len; i++)	{
		if (_requests[i].valid)	
			assert(_requests[i].ts > ts);
		// return pending R_REQ 
		if (_requests[i].valid && _requests[i].type == R_REQ && _requests[i].ts < next_pre_ts) {
			if (_requests[i].ts > _max_served_rts)
				_max_served_rts = _requests[i].ts;
			_requests[i].valid = false;
			_requests[i].txn->cur_row = _latest_row;
			_requests[i].txn->ts_ready = true;
		}
		// return one pending P_REQ
		else if (_requests[i].valid && _requests[i].ts == next_pre_ts) {
			assert(_requests[i].type == P_REQ);
			row_t * res_row = reserveRow(_requests[i].ts, txn);
			assert(res_row);
			res_row->copy(_latest_row);
			_requests[i].valid = false;
			_requests[i].txn->cur_row = res_row;
			_requests[i].txn->ts_ready = true;
		}
	}
}

#endif
