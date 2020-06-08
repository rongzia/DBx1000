//
// Created by rrzhang on 2020/4/27.
//
#include <fstream>
#include <cassert>
#include <cstdint>
#include "manager_client.h"

#include "common/buffer/buffer.h"
#include "common/index/index.h"
#include "common/lock_table/lock_table.h"
#include "common/storage/disk/file_io.h"
#include "common/storage/tablespace/page.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/catalog.h"
#include "common/storage/table.h"
#include "common/workload/ycsb_wl.h"
#include "common/workload/wl.h"
#include "common/global.h"
#include "common/myhelper.h"
#include "common/mystats.h"
#include "instance/benchmarks/ycsb_query.h"
#include "instance/benchmarks/query.h"
#include "instance/concurrency_control/row_mvcc.h"
#include "instance/txn/ycsb_txn.h"
#include "instance/txn/txn.h"
#include "instance/thread.h"
#include "json/json.h"
#include "config.h"


namespace dbx1000 {

    ManagerClient::~ManagerClient() {
        delete query_queue_;
        delete m_workload_;
        delete buffer_manager_rpc_handler_;
        delete buffer_;
    }

    ManagerClient::ManagerClient() {
        init_done_ = false;

        timestamp_ = 0;
        all_ts_ = new uint64_t[g_thread_cnt]();
        txn_man_ = new txn_man *[g_thread_cnt]();
        for (int i = 0; i < g_thread_cnt; i++) {
            all_ts_[i] = UINT64_MAX;
            txn_man_[i] = new ycsb_txn_man();
        }
        stats_.init();

        query_queue_ = new Query_queue();
        query_queue_->init();
        m_workload_ = new ycsb_wl();
        m_workload_->init();


        InitMvccs();
        table_space_ = new TableSpace(((ycsb_wl *) m_workload_)->the_table->get_table_name());
        index_ = new Index(((ycsb_wl *) m_workload_)->the_table->get_table_name() + "_INDEX");
        table_space_->DeSerialize();
        index_->DeSerialize();
        buffer_ = new Buffer(table_space_->GetLastPageId() * MY_PAGE_SIZE, MY_PAGE_SIZE, this);
        lock_table_ = new LockTable();
        lock_table_->Init(0, table_space_->GetLastPageId());
        // TODO :
//        server_rpc_handler_ = new Server
    }

    void ManagerClient::InitBufferForTest() {
        char buf[PAGE_SIZE];
        for (uint64_t page_id = 0; page_id < table_space_->GetLastPageId(); page_id++) {
            buffer_->BufferGet(page_id, buf, PAGE_SIZE);
        }
    }

    void ManagerClient::InitMvccs() {
        std::cout << "ManagerClient::InitMvccs" << std::endl;
        for (uint64_t  key = 0; key < g_synth_table_size; key++) {
            mvcc_map_.insert(std::pair<uint64_t, Row_mvcc *>(key, new Row_mvcc()));
            mvcc_map_[key]->init(key, ((ycsb_wl *) m_workload_)->the_table->get_schema()->tuple_size);
        }
    }
    void ManagerClient::InitLockTable() {
        for (uint64_t key = 0; key < table_space_->GetLastPageId(); key++) {
            lock_table_->lock_table_[key]->instance_id = this->instance_id_;
            lock_table_->lock_table_[key]->valid = true;
        }
    }

    uint64_t ManagerClient::GetNextTs(uint64_t thread_id) { return timestamp_.fetch_add(1, std::memory_order_consume); }

    RC ManagerClient::GetRow(uint64_t key, access_t type, txn_man *txn, RowItem *&row) {
        RC rc = RC::RCOK;
        uint64_t thd_id = txn->get_thd_id();
        TsType ts_type = (type == RD) ? R_REQ : P_REQ;
//        rc = this->manager->access(txn, ts_type, row);
        rc = this->mvcc_map_[key]->access(txn, ts_type, row);
        if (rc == RC::RCOK) {
            row = txn->cur_row;
        } else if (rc == RC::WAIT) {
            dbx1000::Profiler profiler;
            profiler.Start();
            while (!txn->ts_ready) {PAUSE}
            profiler.End();
            this->stats_.tmp_stats[thd_id]->time_wait += profiler.Nanos();
            txn->ts_ready = true;
            row = txn->cur_row;
        }
        return rc;
    }

    void ManagerClient::ReturnRow(uint64_t key, access_t type, txn_man *txn, RowItem *row) {
        RC rc = RC::RCOK;
#if CC_ALG == MVCC
        if (type == XP) {
            mvcc_map_[key]->access(txn, XP_REQ, row);
        } else if (type == WR) {
            assert (type == WR && row != NULL);
            mvcc_map_[key]->access(txn, W_REQ, row);
            assert(rc == RC::RCOK);
        }
#endif
    }

    void ManagerClient::SetTxnMan(txn_man *m_txn) { txn_man_[m_txn->get_thd_id()] = m_txn; }

    void ManagerClient::AddTs(uint64_t thread_id, uint64_t ts) { all_ts_[thread_id] = ts; }

    uint64_t ManagerClient::GetMinTs(uint64_t threa_id) {
        uint64_t min_ts = UINT64_MAX;
        for (uint32_t thd_id = 0; thd_id < g_thread_cnt; thd_id++) {
            if (min_ts > all_ts_[thd_id]) {
                min_ts = all_ts_[thd_id];
            }
        }
        return min_ts;
    }


    void ManagerClient::set_instance_id(int instance_id) { this->instance_id_ = instance_id; }
    std::map<int, std::string> &ManagerClient::host_map() { return this->host_map_; }
    Stats ManagerClient::stats() { return this->stats_; }
    Query_queue *ManagerClient::query_queue() { return this->query_queue_; }
    workload* ManagerClient::m_workload() { return this->m_workload_; }
    Index* ManagerClient::index() { return this->index_; }
    LockTable* ManagerClient::lock_table() { return this->lock_table_; }

    BufferManagerClient *ManagerClient::buffer_manager_rpc_handler() { return this->buffer_manager_rpc_handler_; }

    std::map<uint64_t, Row_mvcc *> ManagerClient::mvcc_map() { return this->mvcc_map_; }


}