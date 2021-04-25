//
// Created by rrzhang on 2020/4/27.
//
#include <fstream>
#include <cassert>
#include <cstdint>
#include "manager_instance.h"

#include "common/buffer/record_buffer.h"
#include "common/index/index.h"
#include "common/lock_table/lock_table.h"
#include "common/storage/disk/file_io.h"
#include "common/storage/tablespace/page.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/catalog.h"
#include "common/storage/row_handler.h"
#include "common/storage/table.h"
#include "common/workload/ycsb.h"
#include "common/workload/tpcc.h"
#include "common/workload/tpcc_helper.h"
#include "common/workload/wl.h"
#include "common/global.h"
#include "common/myhelper.h"
#include "common/mystats.h"
#include "json/json.h"
#include "instance/benchmarks/ycsb_query.h"
#include "instance/benchmarks/query.h"
#include "instance/concurrency_control/row_mvcc.h"
#include "instance/txn/ycsb_txn.h"
#include "instance/txn/txn.h"
#include "instance/thread.h"
#include "global_lock_service.h"
#include "shared_disk_service.h"
#include "config.h"


namespace dbx1000 {

    ManagerInstance::~ManagerInstance() {
        delete all_ts_;
        delete query_queue_;
        delete m_workload_;
        delete row_handler_;
        for(auto &iter : lock_table_) {
            delete iter.second;
        }
        delete global_lock_service_client_;
    }

    void ManagerInstance::ReRun(){
        delete this->query_queue_;
        query_queue_ = nullptr;
        this->query_queue_ = new Query_queue();
        this->query_queue_->managerInstance_ = this;
        query_queue_->init();

        stats_.clear();
        for(auto i = 0; i < g_thread_cnt; i++) {
            txn_man_[i] = nullptr;
            all_ts_[i] = UINT64_MAX;
        }

        timestamp_.store(0);

#ifdef KEY_COUNT
        keyCounter_->Clear();
#endif // KEY_COUNT
    }

    // 调用之前确保 parser_host 被调用，因为 instance_id_，host_map_ 需要先初始化
    void ManagerInstance::Init(const std::string &shared_disk_host) {
        timestamp_ = ATOMIC_VAR_INIT(1);
        this->all_ts_ = new uint64_t[g_thread_cnt]();
        for (uint32_t i = 0; i < g_thread_cnt; i++) { all_ts_[i] = UINT64_MAX; }
        this->txn_man_ = new txn_man *[g_thread_cnt]();
        stats_.init();

        // init workload
#if WORKLOAD == YCSB
        this->m_workload_ = new ycsb_wl();
#elif WORKLOAD == TPCC
        this->m_workload_ = new tpcc_wl();
#endif
        m_workload_->manager_instance_ = this;
        m_workload_->is_server_ = false;
        m_workload_->init();

        // init query
        this->query_queue_ = new Query_queue();
        query_queue_->managerInstance_ = this;
        query_queue_->init();

        row_handler_ = new RowHandler(this);

        // init mvcc
#if WORKLOAD == YCSB
        mvcc_vectors_.insert(make_pair(TABLES::MAIN_TABLE, new unordered_map<uint64_t, std::pair<weak_ptr<Row_mvcc>, volatile bool>>()));
#elif WORKLOAD == TPCC
        mvcc_vectors_.insert(make_pair(TABLES::WAREHOUSE, new unordered_map<uint64_t, std::pair<weak_ptr<Row_mvcc>, volatile bool>>()));
        mvcc_vectors_.insert(make_pair(TABLES::DISTRICT, new unordered_map<uint64_t, std::pair<weak_ptr<Row_mvcc>, volatile bool>>()));
        mvcc_vectors_.insert(make_pair(TABLES::CUSTOMER, new unordered_map<uint64_t, std::pair<weak_ptr<Row_mvcc>, volatile bool>>()));
        mvcc_vectors_.insert(make_pair(TABLES::HISTORY, new unordered_map<uint64_t, std::pair<weak_ptr<Row_mvcc>, volatile bool>>()));
        mvcc_vectors_.insert(make_pair(TABLES::NEW_ORDER, new unordered_map<uint64_t, std::pair<weak_ptr<Row_mvcc>, volatile bool>>()));
        mvcc_vectors_.insert(make_pair(TABLES::ORDER, new unordered_map<uint64_t, std::pair<weak_ptr<Row_mvcc>, volatile bool>>()));
        mvcc_vectors_.insert(make_pair(TABLES::ORDER_LINE, new unordered_map<uint64_t, std::pair<weak_ptr<Row_mvcc>, volatile bool>>()));
        mvcc_vectors_.insert(make_pair(TABLES::ITEM, new unordered_map<uint64_t, std::pair<weak_ptr<Row_mvcc>, volatile bool>>()));
        mvcc_vectors_.insert(make_pair(TABLES::STOCK, new unordered_map<uint64_t, std::pair<weak_ptr<Row_mvcc>, volatile bool>>()));
#endif

        InitMvccs();    // mvcc_map_ 在 m_workload_ 初始化后才能初始化

        // instance 缓存不够时，直接刷盘会把过时的数据刷到 DB，
        // 应该把缓存调大，避免缓存不够时刷旧版本
//        this->shared_disk_client_ = new SharedDiskClient(shared_disk_host);

        InitLockTables();
#ifdef KEY_COUNT
        keyCounter_ = new KeyCounter();
#endif // KEY_COUNT
    }

#if WORKLOAD == YCSB
    void ManagerInstance::InitMvccs() {
        std::cout << "ManagerInstance::InitMvccs" << std::endl;
        Profiler profiler;
        profiler.Start();
#ifdef NO_CONFLICT
        for(uint64_t key = g_synth_table_size*this->instance_id_; key < g_synth_table_size*(this->instance_id_+1); key++) {
#else // NO_CONFLICT
        for(uint64_t key = 0; key < g_synth_table_size; key++) {
#endif // NO_CONFLICT
            mvcc_vectors_[TABLES::MAIN_TABLE]->insert(make_pair(key, make_pair(weak_ptr<Row_mvcc>(), false)));
        }
        profiler.End();
        std::cout << "ManagerInstance::InitMvccs done. time : " << profiler.Millis() << " millis." << std::endl;
    }
#endif  // WORKLOAD == YCSB

#if WORKLOAD == TPCC
    void ManagerInstance::InitMvccs() {
        std::cout << "ManagerInstance::InitMvccs" << std::endl;
        Profiler profiler;
        profiler.Start();
        uint32_t tuple_size;
        for (uint32_t i = 1; i <= g_max_items; i++) {
            mvcc_vectors_[TABLES::ITEM]->insert(make_pair(i, make_pair(shared_ptr<Row_mvcc>(), false)));
        }
        for(uint64_t wh_id = 1; wh_id <= NUM_WH; wh_id++) {
            mvcc_vectors_[TABLES::WAREHOUSE]->insert(make_pair(wh_id, make_pair(shared_ptr<Row_mvcc>(), false)));
            for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
                mvcc_vectors_[TABLES::DISTRICT]->insert(make_pair(distKey(did, wh_id), make_pair(shared_ptr<Row_mvcc>(), false)));
                for (uint32_t cid = 1; cid <= g_cust_per_dist; cid++) {
                    mvcc_vectors_[TABLES::CUSTOMER]->insert(make_pair(custKey(cid, did, wh_id), make_pair(shared_ptr<Row_mvcc>(), false)));
                }
            }
            for (uint32_t sid = 1; sid <= g_max_items; sid++) {
                mvcc_vectors_[TABLES::STOCK]->insert(make_pair(stockKey(sid, wh_id), make_pair(shared_ptr<Row_mvcc>(), false)));
            }
        }
        profiler.End();
        std::cout << "ManagerInstance::InitMvccs done. time : " << profiler.Millis() << " millis." << std::endl;
    }
#endif // WORKLOAD == TPCC


#if WORKLOAD == YCSB
    void ManagerInstance::InitLockTables() {
        IndexItem indexItem;
        this->lock_table_[TABLES::MAIN_TABLE] = new LockTable(TABLES::MAIN_TABLE, this->instance_id(), this);
#ifdef NO_CONFLICT
        for(uint64_t key = g_synth_table_size*this->instance_id_; key < g_synth_table_size*(this->instance_id_+1); key++) {
#else // NO_CONFLICT
        for(uint64_t key = 0; key < g_synth_table_size; key++) {
#endif // NO_CONFLICT
#ifdef B_P_L_P
            m_workload_->indexes_[TABLES::MAIN_TABLE]->IndexGet(key, &indexItem);
            if(lock_table_[TABLES::MAIN_TABLE]->lock_table_.find(indexItem.page_id_) == lock_table_[TABLES::MAIN_TABLE]->lock_table_.end()) {
                lock_table_[TABLES::MAIN_TABLE]->lock_table_[indexItem.page_id_] = make_shared<LockNode>();
                lock_table_[TABLES::MAIN_TABLE]->lock_table_[indexItem.page_id_]->Init(this->instance_id_, LockMode::O);
            }
#else // B_P_L_P
            lock_table_[TABLES::MAIN_TABLE]->lock_table_[key] = make_shared<LockNode>();
            lock_table_[TABLES::MAIN_TABLE]->lock_table_[key]->Init(this->instance_id_, LockMode::O);
#endif // B_P_L_P
        }
    }
#endif // WORKLOAD == YCSB

#if WORKLOAD == TPCC
    void ManagerInstance::InitLockTables() {
        this->lock_table_[TABLES::WAREHOUSE] = new LockTable(TABLES::WAREHOUSE, this->instance_id(), this);
        this->lock_table_[TABLES::DISTRICT] = new LockTable(TABLES::DISTRICT, this->instance_id(), this);
        this->lock_table_[TABLES::CUSTOMER] = new LockTable(TABLES::CUSTOMER, this->instance_id(), this);
//        this->lock_table_[TABLES::HISTORY] = new LockTable(TABLES::HISTORY, this->instance_id(), 0, NUM_WH + 1, this);
//        this->lock_table_[TABLES::NEW_ORDER] = new LockTable(TABLES::NEW_ORDER, this->instance_id(), 0, NUM_WH + 1, this);
//        this->lock_table_[TABLES::ORDER] = new LockTable(TABLES::ORDER, this->instance_id(), 0, NUM_WH + 1, this);
//        this->lock_table_[TABLES::ORDER_LINE] = new LockTable(TABLES::ORDER_LINE, this->instance_id(), 0, NUM_WH + 1, this);
        this->lock_table_[TABLES::ITEM] = new LockTable(TABLES::ITEM, this->instance_id(), this);
        this->lock_table_[TABLES::STOCK] = new LockTable(TABLES::STOCK, this->instance_id(), this);

#ifdef B_P_L_P
        IndexItem indexItem;
        for (uint32_t i = 1; i <= g_max_items; i++) {
            m_workload_->indexes_[TABLES::ITEM]->IndexGet(i, &indexItem);
            if (lock_table_[TABLES::ITEM]->lock_table_.find(indexItem.page_id_) == lock_table_[TABLES::ITEM]->lock_table_.end()) {
                lock_table_[TABLES::ITEM]->lock_table_[indexItem.page_id_] = make_shared<LockNode>();
                lock_table_[TABLES::ITEM]->lock_table_[indexItem.page_id_]->Init(this->instance_id_, LockMode::O);
            }
        }
        for (uint64_t wh_id = 1; wh_id <= NUM_WH; wh_id++) {
            m_workload_->indexes_[TABLES::WAREHOUSE]->IndexGet(wh_id, &indexItem);
            if (lock_table_[TABLES::WAREHOUSE]->lock_table_.find(indexItem.page_id_) == lock_table_[TABLES::WAREHOUSE]->lock_table_.end()) {
                lock_table_[TABLES::WAREHOUSE]->lock_table_[indexItem.page_id_] = make_shared<LockNode>();
            }
            for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
                m_workload_->indexes_[TABLES::DISTRICT]->IndexGet(distKey(did, wh_id), &indexItem);
                if (lock_table_[TABLES::DISTRICT]->lock_table_.find(indexItem.page_id_) ==
                    lock_table_[TABLES::DISTRICT]->lock_table_.end()) {
                    lock_table_[TABLES::DISTRICT]->lock_table_[indexItem.page_id_] = make_shared<LockNode>();
                    lock_table_[TABLES::DISTRICT]->lock_table_[indexItem.page_id_]->Init(this->instance_id_, LockMode::O);
                }
                for (uint32_t cid = 1; cid <= g_cust_per_dist; cid++) {
                    m_workload_->indexes_[TABLES::CUSTOMER]->IndexGet(custKey(cid, did, wh_id), &indexItem);
                    if (lock_table_[TABLES::CUSTOMER]->lock_table_.find(indexItem.page_id_) ==
                        lock_table_[TABLES::CUSTOMER]->lock_table_.end()) {
                        lock_table_[TABLES::CUSTOMER]->lock_table_[indexItem.page_id_] = make_shared<LockNode>();
                        lock_table_[TABLES::CUSTOMER]->lock_table_[indexItem.page_id_]->Init(this->instance_id_, LockMode::O);
                    }
                }
            }
            for (uint32_t sid = 1; sid <= g_max_items; sid++) {
                m_workload_->indexes_[TABLES::STOCK]->IndexGet(stockKey(sid, wh_id), &indexItem);
                if (lock_table_[TABLES::STOCK]->lock_table_.find(indexItem.page_id_) == lock_table_[TABLES::STOCK]->lock_table_.end()) {
                    lock_table_[TABLES::STOCK]->lock_table_[indexItem.page_id_] = make_shared<LockNode>();
                    lock_table_[TABLES::STOCK]->lock_table_[indexItem.page_id_]->Init(this->instance_id_, LockMode::O);
                }
            }
        }
#else // B_P_L_P
        for (uint32_t i = 0; i <= g_max_items; i++) {
            lock_table_[TABLES::ITEM]->lock_table_[i] = make_shared<LockNode>();
        }
        for (uint64_t wh_id = 0; wh_id <= NUM_WH; wh_id++) {
            lock_table_[TABLES::WAREHOUSE]->lock_table_[wh_id] = make_shared<LockNode>();
            for (uint64_t did = 0; did <= DIST_PER_WARE; did++) {
                lock_table_[TABLES::DISTRICT]->lock_table_[distKey(did, wh_id)] = make_shared<LockNode>();
                lock_table_[TABLES::DISTRICT]->lock_table_[distKey(did, wh_id)]->Init(this->instance_id_, LockMode::O);
                for (uint32_t cid = 0; cid <= g_cust_per_dist; cid++) {
                    lock_table_[TABLES::CUSTOMER]->lock_table_[custKey(cid, did, wh_id)] = make_shared<LockNode>();
                    lock_table_[TABLES::CUSTOMER]->lock_table_[custKey(cid, did, wh_id)]->Init(this->instance_id_, LockMode::O);
                }
            }
            for (uint32_t sid = 0; sid <= g_max_items; sid++) {
                lock_table_[TABLES::STOCK]->lock_table_[stockKey(sid, wh_id)] = make_shared<LockNode>();
                lock_table_[TABLES::STOCK]->lock_table_[stockKey(sid, wh_id)]->Init(this->instance_id_, LockMode::O);
            }
        }
#endif // B_P_L_P
    }
#endif // TPCC


    uint64_t ManagerInstance::GetNextTs(uint64_t thread_id) {
//        return instance_rpc_handler_->GetNextTs();
        return timestamp_.fetch_add(1);
    }

    void ManagerInstance::SetTxnMan(txn_man *m_txn) { txn_man_[m_txn->get_thd_id()] = m_txn; }

    void ManagerInstance::AddTs(uint64_t thread_id, uint64_t ts) { all_ts_[thread_id] = ts; }

    uint64_t ManagerInstance::GetMinTs(uint64_t thread_id) {
        uint64_t min_ts = UINT64_MAX;
        for (uint32_t thd_id = 0; thd_id < g_thread_cnt; thd_id++) {
            if (min_ts > all_ts_[thd_id]) {
                min_ts = all_ts_[thd_id];
            }
        }
        return min_ts;
    }
}