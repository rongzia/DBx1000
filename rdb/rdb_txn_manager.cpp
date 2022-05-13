
#include <brpc/channel.h>

#include "rdb_txn_manager.h"

#include "global.h"
#include "system/txn.h"
#include "benchmarks/ycsb_query.h"
#include "benchmarks/tpcc_query.h"
#include "benchmarks/ycsb.h"
#include "benchmarks/tpcc.h"
#include "storage/index_map_hash.h"
#include "rdb/lock_service/lock_service.pb.h"
#include "rdb/instance_lock_manager/ins_client_manager.h"


namespace rdb {

    void RDB_Txn_Manager::Init() {
        txn_mans_ = new txn_man * [g_thread_cnt];
        record_map_ = new std::map<rdb::map_key, access_t, rdb::map_comp>[g_thread_cnt];
        page_map_ = new std::map<rdb::map_key, access_t, rdb::map_comp>[g_thread_cnt];
    }

    void RDB_Txn_Manager::AddTxn(txn_man* txn, uint64_t thd_id) {
        txn_mans_[thd_id] = txn;
    }

#ifdef YCSB
    void RDB_Txn_Manager::ParseQuery(base_query* m_query, uint64_t thd_id) {
        ycsb_query* query = (ycsb_query*)m_query;
        workload* h_wl = txn_mans_[thd_id]->get_wl();
        std::map<rdb::map_key, access_t>::reverse_iterator m_record;
        std::map<rdb::map_key, access_t>::reverse_iterator m_page;
        for (uint32_t rid = 0; rid < query->request_cnt; rid++) {
            ycsb_request* req = &query->requests[rid];
            m_record = record_map_[thd_id].rbegin();
            m_page = page_map_[thd_id].rbegin();

            if (rid == 0) {
                assert(record_map_[thd_id].size() == 0);
                assert(page_map_[thd_id].size() == 0);
                assert(m_record == record_map_[thd_id].rend());
                assert(m_page == page_map_[thd_id].rend());
            }
            else {
                assert(m_record->first.first == TABLES::MAIN_TABLE);
                assert(m_record->first.second < req->key);
                assert(m_page != page_map_[thd_id].rend());
            }

            record_map_[thd_id].insert(make_pair(std::make_pair(TABLES::MAIN_TABLE, req->key), req->rtype));

            index_item* idx_item = txn_mans_[thd_id]->index_read(((ycsb_wl*)h_wl)->the_index, req->key);
            assert(idx_item);
            if (m_page != page_map_[thd_id].rend() && m_page->first.first == TABLES::MAIN_TABLE && m_page->first.second == idx_item->page_id_
                && m_page->second < req->rtype) {
                page_map_[thd_id].erase(m_page->first);
            }
            else if (m_page != page_map_[thd_id].rend() && m_page->first.first == TABLES::MAIN_TABLE && m_page->first.second == idx_item->page_id_
                && m_page->second >= req->rtype) {
                continue;
            }
            // some check
            // m_page = page_map_[thd_id].rbegin();
            // if (m_page != page_map_[thd_id].rend()) {
            //     if (m_page->first.second >= idx_item->page_id_) {
            //         cout << m_page->first.second << ", " << m_page->second << endl;
            //         cout << idx_item->page_id_ << ", " << req->rtype << endl;
            //     }
            //     assert(m_page->first.second < idx_item->page_id_);
            // }
            page_map_[thd_id].insert(make_pair(std::make_pair(TABLES::MAIN_TABLE, idx_item->page_id_), req->rtype));
        }

        // check
        {
            // std::map<rdb::map_key, access_t>::iterator curr, next;
            // curr = record_map_[thd_id].begin(); curr++;  next = curr; curr = record_map_[thd_id].begin();
            // for (curr = record_map_[thd_id].begin(); next != record_map_[thd_id].end();) {
            //     if (curr->first.first == next->first.first) {
            //         assert(curr->first.second < next->first.second);
            //     }
            //     curr++; next++;
            // }

            // curr = page_map_[thd_id].begin(); curr++;  next = curr; curr = page_map_[thd_id].begin();
            // for (curr = page_map_[thd_id].begin(); next != page_map_[thd_id].end();) {
            //     if (curr->first.first == next->first.first) {
            //         assert(curr->first.second < next->first.second);
            //     }
            //     curr++; next++;
            // }
        }
    }

    void RDB_Txn_Manager::ClearQuery(uint64_t thd_id) {
        std::map<rdb::map_key, access_t>::iterator it_record;
        std::map<rdb::map_key, access_t>::iterator it_page;


        // for (it_record = page_map_[thd_id].begin(); it_record != page_map_[thd_id].end(); it_record++) {
        // }

        for (it_page = page_map_[thd_id].begin(); it_page != page_map_[thd_id].end(); it_page++) {
            LockTable::const_accessor const_acc;
            bool res = lock_table_->lock_table_map_.find(const_acc, it_page->first);
            assert(res);
            const_acc->second->LockItemBase::ClearTxn(txn_mans_[thd_id]);
        }
        page_map_[thd_id].clear();
        record_map_[thd_id].clear();
    }
#endif // YCSB

    RC RDB_Txn_Manager::GetLock(uint64_t thd_id, uint64_t item_id, access_t access, size_t count) {
        rdb::GetLockRequest request;
        rdb::GetLockReply reply;
        brpc::Controller cntl;
    }

    RC RDB_Txn_Manager::GetLocks(uint64_t thd_id) {
        std::map<rdb::map_key, access_t>::iterator it_record;
        std::map<rdb::map_key, access_t>::iterator it_page;
        rdb::GetLocksRequest request;
        rdb::GetLocksReply reply;
        brpc::Controller cntl;

        request.set_instance_id(rdb::ins_client_manager_->ins_id_);

        // fill request
        for (it_page = page_map_[thd_id].begin(); it_page != page_map_[thd_id].end(); it_page++) {
            LockTable::const_accessor const_acc;
            bool res = lock_table_->lock_table_map_.find(const_acc, it_page->first);
            assert(res);

            remote_lock_flag flag = const_acc->second->AddTxn(txn_mans_[thd_id]);

            assert(flag != remote_lock_flag::Undefined);
            if (flag == remote_lock_flag::Nothing) {
                rdb::GetLockItem* item = request.add_req_items();
                item->set_table(TABLES_2_RpcTABLES(it_page->first.first));
                item->set_item_id(it_page->first.second);
                item->set_req_mode(access_t_2_RpcLM(it_page->second));
                item->set_allocated_buf(nullptr);
                item->set_count(PAGE_SIZE);
            }
        }
        /**
         * @brief debug
         *
         *
        txn_mutex_.lock();
        cout << "thd_id: " << thd_id << ", txn: " << txn_mans_[thd_id]->get_txn_id() << ", pages: ";
        for(it_page = page_map_[thd_id].begin(); it_page != page_map_[thd_id].end(); it_page++) {
            cout << it_page->first.second << ", ";
        }
        cout << "remoteget: ";
        for(int i =0; i < request.req_items_size(); i++) {
            cout << request.req_items(i).item_id() << ", ";
        }
        cout << endl;
        txn_mutex_.unlock();
        // cout << __FILE__ << ", " << __LINE__ << endl;
        */

        // cntl.set_timeout_ms(100);
        rdb::ins_client_manager_->GetLocks(&cntl, &request, &reply);

        if (!cntl.Failed()) {
            assert(request.req_items_size() == reply.ret_items_size());
            for (int i = 0; i < reply.ret_items_size(); i++) {
                RC rc = RpcRC_2_RC(reply.ret_items(i).rc());
                TABLES table = RpcTABLES_2_TABLES(reply.ret_items(i).table());
                uint64_t page_id = reply.ret_items(i).item_id();

                LockTable::const_accessor const_acc;
                bool res = lock_table_->lock_table_map_.find(const_acc, make_pair(table, page_id));
                assert(res);
                while (!__sync_bool_compare_and_swap(&const_acc->second->latch_, false, true)) { PAUSE }
                // some debug
                if (const_acc->second->remote_lock_flag_ != remote_lock_flag::Processing) {
                    cout << remote_lock_flag_2_string(const_acc->second->remote_lock_flag_) << endl;
                }
                assert(const_acc->second->remote_lock_flag_ == remote_lock_flag::Processing);
                // end some debug

                if (rc == RC::RCOK) const_acc->second->remote_lock_flag_ = remote_lock_flag::Succ;
                else if (rc == RC::Abort /*including timeout*/) const_acc->second->remote_lock_flag_ = remote_lock_flag::Abort;
                else { assert(false); }
                const_acc->second->latch_ = false;

                // write buffer
            }
        }
        else {
            // assert(false);
            for (int i = 0; i < request.req_items_size(); i++) {
                TABLES table = RpcTABLES_2_TABLES(request.req_items(i).table());
                uint64_t page_id = request.req_items(i).item_id();

                LockTable::const_accessor const_acc;
                bool res = lock_table_->lock_table_map_.find(const_acc, make_pair(table, page_id));
                assert(res);
                while (!__sync_bool_compare_and_swap(&const_acc->second->latch_, false, true)) { PAUSE }
                // some debug
                if (const_acc->second->remote_lock_flag_ != remote_lock_flag::Processing) {
                    cout << remote_lock_flag_2_string(const_acc->second->remote_lock_flag_) << endl;
                }
                assert(const_acc->second->remote_lock_flag_ == remote_lock_flag::Processing);
                // end some debug
                const_acc->second->remote_lock_flag_ = remote_lock_flag::Abort;
                const_acc->second->latch_ = false;
            }
        }

        // wait all lock need by this txn return, both rpc by local thread and other thread
        for (it_page = page_map_[thd_id].begin(); it_page != page_map_[thd_id].end(); it_page++) {
            while (1) {
                LockTable::const_accessor const_acc;
                bool res = lock_table_->lock_table_map_.find(const_acc, it_page->first);
                assert(res);

                remote_lock_flag flag = const_acc->second->remote_lock_flag_;
                assert(flag != remote_lock_flag::Nothing && flag != remote_lock_flag::Undefined);

                if (flag == remote_lock_flag::Processing) {
                    // cout << "continue; " << __FILE__ << ", " << __LINE__ << endl;
                    continue;
                }
                else if (flag == remote_lock_flag::Abort) return RC::Abort;
                else if (flag == remote_lock_flag::Succ) {
                    // cout << "break; " << __FILE__ << ", " << __LINE__ << endl;
                    const_acc->second->lock_mode_ = lock_mode::P;
                    break;
                }
                else {
                    cout << remote_lock_flag_2_string(flag) << __LINE__ << endl;
                    assert(false);
                }
            }
        }
    }

    void RDB_Txn_Manager::CheckLocks(uint64_t thd_id) {
        std::map<rdb::map_key, access_t>::iterator it_record;
        std::map<rdb::map_key, access_t>::iterator it_page;

        for (it_page = page_map_[thd_id].begin(); it_page != page_map_[thd_id].end(); it_page++) {
            LockTable::const_accessor const_acc;
            bool res = lock_table_->lock_table_map_.find(const_acc, it_page->first);
            assert(res);
            assert(const_acc->second->lock_mode_ != lock_mode::O);
        }
    }

} // rdb