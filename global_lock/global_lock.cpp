//
// Created by rrzhang on 2020/6/3.
//
#include <unistd.h>
#include <fstream>
#include <cassert>
#include "global_lock.h"

#include "common/index/index.h"
#include "common/lock_table/lock_table.h"
#include "common/storage/disk/file_io.h"
#include "common/storage/tablespace/page.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/catalog.h"
#include "common/storage/table.h"
#include "common/storage/row.h"
#include "common/workload/ycsb.h"
#include "common/workload/tpcc.h"
#include "common/workload/tpcc_helper.h"
#include "common/workload/wl.h"
#include "common/global.h"
#include "common/myhelper.h"
#include "common/mystats.h"
#include "json/json.h"
#include "global_lock_service.h"
#include "shared_disk_service.h"
#include "config.h"


namespace dbx1000 {
    namespace global_lock {
        LockNode::LockNode() {
            write_ins_id = -1;
        }


        GlobalLock::~GlobalLock() {
//        delete buffer_;
//        delete lock_table_;
        }

        GlobalLock::GlobalLock() {
#ifdef REAL_STOP
            stop_flag_.clear();
#endif // REAL_STOP
            stats_.Clear();
#ifdef WARMUP
            warmup_done_ = new bool[PROCESS_CNT]();
            is_warmup_done_ = false;
#endif // WARMUP
            this->init_done_ = false;
            this->instances_ = new InstanceInfo[PROCESS_CNT]();
            for (int i = 0; i < PROCESS_CNT; i++) {
                instances_[i].instance_id = -1;
                instances_[i].init_done = false;
                instances_[i].stats.init();
                instances_[i].instance_run_done = false;
#ifdef WARMUP
                warmup_done_[i] = false;
#endif // WARMUP
            }

            timestamp_ = ATOMIC_VAR_INIT(1);
#if WORKLOAD == YCSB
            this->m_workload_ = new ycsb_wl();
#elif WORKLOAD == TPCC
            this->m_workload_ = new tpcc_wl();
#endif
            m_workload_->is_server_ = true;
            m_workload_->init();

            /// 初始化 lock_service_table_
#if WORKLOAD == YCSB
#if defined(B_P_L_P)
            IndexItem indexItem;
#ifdef NO_CONFLICT
            for (uint64_t key = 0; key < SYNTH_TABLE_SIZE*PROCESS_CNT; key++) {
#else // NO_CONFLICT
            for (uint64_t key = 0; key < SYNTH_TABLE_SIZE; key++) {
#endif // NO_CONFLICT
                m_workload_->indexes_[TABLES::MAIN_TABLE]->IndexGet(key, &indexItem);
                if(this->lock_tables_[TABLES::MAIN_TABLE].find(indexItem.page_id_) == lock_tables_[TABLES::MAIN_TABLE].end()) {
                    this->lock_tables_[TABLES::MAIN_TABLE].insert(make_pair(indexItem.page_id_, new LockNode()));
                }
            }
#elif defined(B_R_L_R) || defined(B_M_L_R) || defined(B_P_L_R)
#ifdef NO_CONFLICT
            for (uint64_t key = 0; key < SYNTH_TABLE_SIZE*PROCESS_CNT; key++) {
#else // NO_CONFLICT
            for (uint64_t key = 0; key < SYNTH_TABLE_SIZE; key++) {
#endif // NO_CONFLICT
                this->lock_tables_[TABLES::MAIN_TABLE].insert(make_pair(key, new LockNode()));
            }
#else // B_L
        assert(false);
#endif // B_L
#elif WORKLOAD == TPCC
#if defined(B_P_L_P)
            IndexItem indexItem;
            for (uint32_t i = 1; i <= g_max_items; i++) {
                m_workload_->indexes_[TABLES::ITEM]->IndexGet(i, &indexItem);
                if (lock_tables_[TABLES::ITEM].find(indexItem.page_id_) == lock_tables_[TABLES::ITEM].end()) {
                    this->lock_tables_[TABLES::ITEM].insert(make_pair(indexItem.page_id_, new LockNode()));
                }
            }
            for (uint64_t wh_id = 1; wh_id <= NUM_WH; wh_id++) {
                m_workload_->indexes_[TABLES::WAREHOUSE]->IndexGet(wh_id, &indexItem);
                if (lock_tables_[TABLES::WAREHOUSE].find(indexItem.page_id_) == lock_tables_[TABLES::WAREHOUSE].end()) {
                    this->lock_tables_[TABLES::WAREHOUSE].insert(make_pair(indexItem.page_id_, new LockNode()));
                }
                for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
                    m_workload_->indexes_[TABLES::DISTRICT]->IndexGet(distKey(did, wh_id), &indexItem);
                    if (lock_tables_[TABLES::DISTRICT].find(indexItem.page_id_) == lock_tables_[TABLES::DISTRICT].end()) {
                        this->lock_tables_[TABLES::DISTRICT].insert(make_pair(indexItem.page_id_, new LockNode()));
                    }
                    for (uint32_t cid = 1; cid <= g_cust_per_dist; cid++) {
                        m_workload_->indexes_[TABLES::CUSTOMER]->IndexGet(custKey(cid, did, wh_id), &indexItem);
                        if (lock_tables_[TABLES::CUSTOMER].find(indexItem.page_id_) == lock_tables_[TABLES::CUSTOMER].end()) {
                            this->lock_tables_[TABLES::CUSTOMER].insert(make_pair(indexItem.page_id_, new LockNode()));
                        }
                    }
                }
                for (uint32_t sid = 1; sid <= g_max_items; sid++) {
                    m_workload_->indexes_[TABLES::STOCK]->IndexGet(stockKey(sid, wh_id), &indexItem);
                    if (lock_tables_[TABLES::STOCK].find(indexItem.page_id_) == lock_tables_[TABLES::STOCK].end()) {
                        this->lock_tables_[TABLES::STOCK].insert(make_pair(indexItem.page_id_, new LockNode()));
                    }
                }
            }
#elif defined(B_R_L_R) || defined(B_M_L_R) || defined(B_P_L_R)
//            for (uint32_t i = 1; i <= g_max_items; i++) {
//                this->lock_tables_[TABLES::ITEM].insert(make_pair(i, new LockNode()));
//            }
            for(uint64_t wh_id = 1; wh_id <= NUM_WH; wh_id++) {
                this->lock_tables_[TABLES::WAREHOUSE].insert(make_pair(wh_id, new LockNode()));
                for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
                    this->lock_tables_[TABLES::DISTRICT].insert(make_pair(distKey(did, wh_id), new LockNode()));
                    for (uint32_t cid = 1; cid <= g_cust_per_dist; cid++) {
                        this->lock_tables_[TABLES::CUSTOMER].insert(make_pair(custKey(cid, did, wh_id), new LockNode()));
                    }
                }
                for (uint32_t sid = 1; sid <= g_max_items; sid++) {
                    this->lock_tables_[TABLES::STOCK].insert(make_pair(stockKey(sid, wh_id), new LockNode()));
                }
            }
#else // B_L
        assert(false);
#endif // B_L
#endif // TPCC
        }

        /**
         * 线程 ins_id 需要获取 page_id 的锁，本地 lock_service_table_ 记录了拥有该 page 锁的节点，需要先时其他节点的锁失效，
         * 同时把其他节点最新的 page_buf 拿过来，然后返回锁权限和 page_buf 给 ins_id
         * ， 当然，也有可能 lock_service_table_[page_id] = -1，即没有任何其他节点拥有该锁权限
         * @param ins_id
         * @param page_id
         * @param page_buf
         * @param count, 要是当前节点申请的锁不在任何节点中，那么直接 count = 0,表示没有返回数据
         * @return
         */
        RC GlobalLock::LockRemote(uint64_t ins_id, TABLES table, uint64_t item_id, char *buf, size_t count) {
            Profiler profiler1;
            profiler1.Start();
            auto iter = lock_tables_[table].find(item_id);
            assert(lock_tables_[table].end() != iter);
            if (lock_tables_[table].end() == iter) { assert(false); return RC::Abort; }

            RC rc = RC::RCOK;
            std::unique_lock<std::mutex> lck(iter->second->mtx);
            profiler1.End();
            this->stats_.total_global_lock_time_.fetch_add(profiler1.Nanos());
            if (iter->second->write_ins_id >= 0) {
#if ((WORKLOAD == YCSB) && defined(NO_CONFLICT)) // || ((WORKLOAD == TPCC) && (PROCESS_CNT == NUM_WH_NODE))
                    cout << ins_id << " want to invalid " << iter->second->write_ins_id << ", table: " << MyHelper::TABLESToInt(table) << ", page id : " << item_id << endl;
#endif
                Profiler profiler2;
                profiler2.Start();
                uint64_t invld_time = 0;
                rc = instances_[iter->second->write_ins_id].global_lock_service_client->Invalid(table, item_id, buf, count, invld_time);
                profiler2.End();
                this->stats_.total_global_invalid_time_.fetch_add(profiler2.Nanos());
                this->stats_.total_global_invalid_count_.fetch_add(1);
                this->stats_.total_ins_invalid_time_.fetch_add(invld_time);
                if (rc == RC::TIME_OUT) {
#if ((WORKLOAD == YCSB) && defined(NO_CONFLICT)) // || ((WORKLOAD == TPCC) && (PROCESS_CNT == NUM_WH_NODE))
                        cout << ins_id << " invaild " << iter->second->write_ins_id << ", table: " << MyHelper::TABLESToInt(table) << ", page: " << item_id << " time out" << endl;
#endif
                }
                if (rc == RC::Abort) {
#if ((WORKLOAD == YCSB) && defined(NO_CONFLICT)) // || ((WORKLOAD == TPCC) && (PROCESS_CNT == NUM_WH_NODE))
                        cout << ins_id << " invaild " << iter->second->write_ins_id << ", table: " << MyHelper::TABLESToInt(table) << ", page: " << item_id << " Abort" << endl;
#endif
                }
                if (rc == RC::RCOK) {
#if ((WORKLOAD == YCSB) && defined(NO_CONFLICT)) // || ((WORKLOAD == TPCC) && (PROCESS_CNT == NUM_WH_NODE))
                        cout << ins_id << " invalid " << iter->second->write_ins_id << ", table: " << MyHelper::TABLESToInt(table) << ", page: " << item_id << " success" << endl;
#endif
#if defined(B_P_L_P)
                    const BufferPool::PageKey pagekey = std::make_pair(table, item_id);
                    Page page(buf);
                    const BufferPool::PageHandle* handle = this->m_workload_->buffer_pool_.Put(pagekey, std::move(page));
                    this->m_workload_->buffer_pool_.Release(handle);
#elif defined(B_P_L_R)
                    row_t* row = new row_t();
                    row->set_primary_key(item_id);
                    row->init(this->m_workload_->tables_[table], 0, item_id);
                    assert(count == row->get_tuple_size());
                    memcpy(row->data, buf, count);
                    
        dbx1000::IndexItem indexItem;
        this->m_workload_->indexes_[table]->IndexGet(item_id, &indexItem);

        auto pagekey = std::make_pair(table, indexItem.page_id_);
        const BufferPool::PageHandle* handle_read = this->m_workload_->buffer_pool_.Get(pagekey);
        #ifdef CLREAR_BUF
        if(handle_read == NULL || handle_read == nullptr) {
            handle_read = this->m_workload_->buffer_pool_.PutIfNotExist(pagekey);
        }
        #endif // CLREAR_BUF
        if(!handle_read) {cout << "page id: " << indexItem.page_id_ << endl;}
        assert(handle_read);
        // assert(indexItem.page_id_ == handle_read->value.page_id());

        const BufferPool::PageHandle* handle_write = this->m_workload_->buffer_pool_.Put(pagekey, Page(handle_read->value.page_buf_read()));
        this->m_workload_->buffer_pool_.Release(handle_read);
        this->m_workload_->buffer_pool_.Release(handle_write);
                    delete row;
#elif defined(B_M_L_R) || defined(B_R_L_R)
                    const RowBufferPool::RowKey rowkey = std::make_pair(table, item_id);
                    row_t row;
                    row.set_primary_key(item_id);
                    row.init(this->m_workload_->tables_[table], 0, item_id);
                    const RowBufferPool::RowHandle* handle = this->m_workload_->buffer_pool_.Put(rowkey, std::move(row));
                    this->m_workload_->buffer_pool_.Release(handle);
#else  //B_L
                    assert(false);
#endif //B_L
                    iter->second->write_ins_id = ins_id;
                }
            } else {
                iter->second->write_ins_id = ins_id;
                if (count > 0) {
#if defined(B_P_L_P)
                    const BufferPool::PageKey pagekey = std::make_pair(table, item_id);
                    const BufferPool::PageHandle* handle = this->m_workload_->buffer_pool_.Get(pagekey);
                    memcpy(buf, handle->value.page_buf_read(), count);
                    this->m_workload_->buffer_pool_.Release(handle);
#elif defined(B_M_L_R) || defined(B_R_L_R) || defined(B_P_L_R)
                    // do nothing, no need init buf
#else  //B_L
                    assert(false);
#endif //B_L
                }
//                 cout << ins_id << " get page id : " << page_id << " success" << endl;
            }
            return rc;
        }
#ifdef DB2
        RC GlobalLock::LockRemote_DB2(uint64_t ins_id, TABLES table, uint64_t item_id, char *buf, size_t count) {
            Profiler profiler;
            profiler.Start();
            auto iter = lock_tables_[table].find(item_id);
            assert(lock_tables_[table].end() != iter);
            if (lock_tables_[table].end() == iter) { assert(false); return RC::Abort; }

            RC rc = RC::RCOK;
            std::unique_lock<std::mutex> lck(iter->second->mtx);
            if(iter->second->cv.wait_for(lck, std::chrono::seconds(1), [iter](){return iter->second->write_ins_id == -1;})) {
                profiler.End();
                this->stats_.total_global_lock_time_.fetch_add(profiler.Nanos());
                iter->second->write_ins_id = ins_id;
                if (count > 0) {
                    m_workload_->buffers_[table]->BufferGet(item_id, buf, count);
                }
            } else {
                rc = RC::TIME_OUT;
            }
            return rc;
        }

        RC GlobalLock::Unlock(uint64_t ins_id, TABLES table, uint64_t item_id, char *buf, size_t count) {
            auto iter = lock_tables_[table].find(item_id);
            assert(lock_tables_[table].end() != iter);
            if (lock_tables_[table].end() == iter) { assert(false); return RC::Abort; }

            RC rc = RC::RCOK;
            std::unique_lock<std::mutex> lck(iter->second->mtx);
            
            // assert(iter->second->write_ins_id == ins_id);
            iter->second->write_ins_id = -1;
            m_workload_->buffers_[table]->BufferPut(item_id, buf, count);
            iter->second->cv.notify_all();
            
            return rc;
        }
#endif // DB2
        uint64_t GlobalLock::GetNextTs(uint64_t thread_id) { return timestamp_.fetch_add(1); }


        void GlobalLock::set_instance_i(int instance_id) {
            cout << "GlobalLock::set_instance_i, instance_id : " << instance_id << endl;
            assert(instance_id >= 0 && instance_id < PROCESS_CNT);
            assert(instances_[instance_id].init_done == false);
            instances_[instance_id].instance_id = instance_id;
            instances_[instance_id].host = hosts_map_[instance_id];
            instances_[instance_id].global_lock_service_client = new global_lock_service::GlobalLockServiceClient(instances_[instance_id].host);
            instances_[instance_id].init_done = true;
            cout << "GlobalLock::set_instance_i, instances_[instance_id].host : " << instances_[instance_id].host << endl;
        }


        bool GlobalLock::init_done() {
            for (int i = 0; i < PROCESS_CNT; i++) {
                if (instances_[i].init_done == false) { return false; }
            }
            init_done_ = true;
            return init_done_;
        }
    }
}