#include <cstring>
#include <set>
#include <vector>
#include <thread>
#include "ycsb_txn.h"

#include "common/buffer/buffer.h"
#include "common/index/index.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/catalog.h"
#include "common/storage/table.h"
#include "common/workload/ycsb_wl.h"
#include "common/myhelper.h"
#include "instance/benchmarks/ycsb_query.h"
#include "instance/thread.h"
#include "shared_disk/shared_disk_service.h"
#include "rpc_handler/instance_handler.h"

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (ycsb_wl *) h_wl;
}

ycsb_query* gen_query(){
    ycsb_query * m_query = new ycsb_query();
    m_query->requests = new ycsb_request[g_req_per_query]();
    m_query->request_cnt = 4;
    for(int i = 0; i< 4; i++) {
        m_query->requests[i].key = i*203;
        m_query->requests[i].rtype = WR;
    }
    return m_query;
}

RC ycsb_txn_man::run_txn(base_query * query) {
//    cout << "instance " << h_thd->manager_client_->instance_id() << ", thread " << this->get_thd_id() << ", txn " << this->txn_id << " start." << endl;
	RC rc;
	ycsb_query * m_query = (ycsb_query *) query;
//	ycsb_query * m_query = gen_query();
	rc = CheckWriteLockSet(m_query);
	if(rc == RC::Abort) { return rc; }
	/* ycsb_wl * wl = (ycsb_wl *) h_wl;
	itemid_t * m_item = NULL; */
  	row_cnt = 0;

	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		/* int part_id = wl->key_to_part( req->key ); */      //! 分区数为 1，part_id == 0
		//! finish_req、iteration 是为 req->rtype==SCAN 准备的，扫描需要读 SCAN_LEN 个 item，
		//! while 虽然为 SCAN 提供了 SCAN_LEN 次读，但是每次请求的 key 是一样的，并没有对操作 [key, key + SCAN_LEN]
		bool finish_req = false;
		uint32_t iteration = 0;
		while ( !finish_req ) {
		    /*
			if (iteration == 0) {
				m_item = index_read(_wl->the_index, req->key, part_id);
			} 
#if INDEX_STRUCT == IDX_BTREE
			else {
				_wl->the_index->index_next(get_thd_id(), m_item);
				if (m_item == NULL)
					break;
			}
#endif
			row_t * row = ((row_t *)m_item->location);
		     */
			dbx1000::RowItem * row_local;
			access_t type = req->rtype;
			
//			row_local = get_row(req->key, type);
//            assert(h_thd->manager_client_->instance_id() < PROCESS_CNT);
//            assert(req->key < g_synth_table_size/32);
//            uint64_t read_key = req->key + g_synth_table_size/32*h_thd->manager_client_->instance_id();
//            assert(read_key < g_synth_table_size);
//			row_local = get_row(read_key , type);


			row_local = get_row(req->key , type);
			if (row_local == NULL) {
				rc = RC::Abort;
				goto final;
			}
			size_t size = _wl->the_table->get_schema()->tuple_size;
			assert(row_local->size_ == size);

			// Computation //
			// Only do computation when there are more than 1 requests.
            if (m_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == SCAN) {
//                  for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
						int fid = 0;
						char * data = row_local->row_;
						__attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 10]);
//                  }
                } else {
                    assert(req->rtype == WR);
//					for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
						int fid = 0;
						char * data = row_local->row_;
//						*(uint64_t *)(&data[fid * 10]) = 0;
                        memcpy(&data[size - sizeof(uint64_t)], &timestamp, sizeof(uint64_t));
//					}
                } 
            }


			iteration ++;
			if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len)
				finish_req = true;
		}
	}
	rc = RC::RCOK;
final:
	rc = finish(rc);




//	ycsb_query * m_query = (ycsb_query *) query;
	std::set<uint64_t> write_page_set;
    for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
        dbx1000::IndexItem indexItem;
//        this->h_thd->manager_client_->index()->IndexGet(req->key + g_synth_table_size/32*h_thd->manager_client_->instance_id(), &indexItem);
        this->h_thd->manager_client_->index()->IndexGet(req->key, &indexItem);
        if(req->rtype == WR){
            write_page_set.insert(indexItem.page_id_);
        }
    }

    dbx1000::LockTable* lockTable =  this->h_thd->manager_client_->lock_table();
    for(auto iter:write_page_set) {
        lockTable->RemoveThread(iter, this->get_thd_id());
    }
//    cout << "instance " << h_thd->manager_client_->instance_id() << ", thread " << this->get_thd_id() << ", txn " << this->txn_id << " end." << endl;


	return rc;
}

RC ycsb_txn_man::CheckWriteLockSet(ycsb_query * m_query){
//	ycsb_query * m_query = (ycsb_query *) query;
	std::set<uint64_t> write_page_set;
    for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
        dbx1000::IndexItem indexItem;
//        this->h_thd->manager_client_->index()->IndexGet(req->key + g_synth_table_size/32*h_thd->manager_client_->instance_id(), &indexItem);
        this->h_thd->manager_client_->index()->IndexGet(req->key, &indexItem);
        if(req->rtype == WR){
            write_page_set.insert(indexItem.page_id_);
        }
    }

    auto print_set = [&](){
        cout << "instance " << h_thd->manager_client_->instance_id() << ", page_id : ";
        for(auto iter : write_page_set) {
           cout << iter << " ";
        }cout << endl;
    };
//    print_set();


    dbx1000::LockTable* lockTable =  this->h_thd->manager_client_->lock_table();

    auto has_invalid_req = [&](){
        for(auto iter : write_page_set) {
            if(this->h_thd->manager_client_->lock_table()->lock_table()[iter]->invalid_req){ return true; }
//            else { lockTable->AddThread(iter, this->get_thd_id()); }
        }
        return false;
    };
    while(has_invalid_req()) { std::this_thread::yield(); }

//    cout << "instance " << h_thd->manager_client_->instance_id() << ", thread " << this->get_thd_id() << ", txn " << this->txn_id << " get lock." << endl;
/**/
dbx1000::Profiler profiler;
profiler.Start();
    std::vector<thread> lock_remote_thread;
    for(auto iter : write_page_set) {
        if(lockTable->lock_table()[iter]->lock_mode == dbx1000::LockMode::O) {
            bool flag = ATOM_CAS(lockTable->lock_table()[iter]->lock_remoting, false, true);
            if(flag) {
                assert(true == lockTable->lock_table()[iter]->lock_remoting);
//                lock_remote_thread.emplace_back(thread([iter, lockTable, this]() {
                    char page_buf[MY_PAGE_SIZE];
//                    cout << "instance " << h_thd->manager_client_->instance_id() << ", thread " << this->get_thd_id() << ", txn " << this->txn_id << " remote get lock." << endl;
                    RC rc = this->h_thd->manager_client_->instance_rpc_handler()->LockRemote(
                            this->h_thd->manager_client_->instance_id(), iter, dbx1000::LockMode::X, page_buf, MY_PAGE_SIZE);
                    assert(rc == RC::RCOK);
//                    lockTable->lock_table()[iter]->mtx.lock();
//                    if(lockTable->lock_table()[iter]->lock_mode != dbx1000::LockMode::O){
//                      cout << "thread : " << get_thd_id() << " change lock : " << iter << " lockTable->lock_table()[iter]->lock_mode != dbx1000::LockMode::O" << endl;
//                    }
                    assert(lockTable->lock_table()[iter]->lock_mode == dbx1000::LockMode::O);
                    lockTable->lock_table()[iter]->lock_mode = dbx1000::LockMode::P;
//                    cout << "thread : " << get_thd_id() << " change lock : " << iter << " to P" << endl;
//                    lockTable->lock_table()[iter]->mtx.unlock();
//                  this->h_thd->manager_client_->buffer()->BufferPut(iter, page_buf, MY_PAGE_SIZE);
                    lockTable->lock_table()[iter]->lock_remoting = false;
//                }
//                ));
            } else {
                assert(true == lockTable->lock_table()[iter]->lock_remoting);
                while(lockTable->lock_table()[iter]->lock_mode == dbx1000::LockMode::O) {
//                    cout << "thread waiting..." << endl; std::this_thread::yield();
                    }
                assert(lockTable->lock_table()[iter]->lock_mode != dbx1000::LockMode::O);
            }
        }
    }

    for(auto &iter : lock_remote_thread){
        iter.join();
    }
    /// some check
    for(auto iter : write_page_set) {
        assert(lockTable->lock_table()[iter]->lock_mode != dbx1000::LockMode::O);
//        if(lockTable->lock_table()[iter]->lock_mode != dbx1000::LockMode::O) {
            lockTable->AddThread(iter, this->get_thd_id());
//        }
    }
    profiler.End();
    this->h_thd->manager_client_->stats()._stats[this->get_thd_id()]->debug6 += profiler.Nanos();
    RC rc = RC::RCOK;

    return rc;
}

