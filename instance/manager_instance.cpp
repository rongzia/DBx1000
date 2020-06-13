//
// Created by rrzhang on 2020/4/27.
//
#include <fstream>
#include <cassert>
#include <cstdint>
#include "manager_instance.h"

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
#include "shared_disk/shared_disk_service.h"
#include "json/json.h"
#include "config.h"


namespace dbx1000 {

    ManagerInstance::~ManagerInstance() {
        delete all_ts_;
        for(auto &iter:mvcc_map_) {
            delete iter.second;
        }
        delete query_queue_;
        delete m_workload_;
        delete buffer_;
        index_->Serialize();
        table_space_->Serialize();
        delete table_space_;
        delete index_;
        delete lock_table_;
//        delete buffer_manager_rpc_handler_;
    }

    ManagerInstance::ManagerInstance(const std::string &shared_disk_host) {
        this->init_done_ = false;

        this->timestamp_ = 1;
        this->all_ts_ = new uint64_t[g_thread_cnt]();
        this->txn_man_ = new txn_man *[g_thread_cnt]();
        for (int i = 0; i < g_thread_cnt; i++) {
            all_ts_[i] = UINT64_MAX;
        }
        stats_.init();

        this->query_queue_ = new Query_queue();
        query_queue_->init();
        this->m_workload_ = new ycsb_wl();
        m_workload_->init();

        this->shared_disk_client_ = new SharedDiskClient(shared_disk_host);

        InitMvccs();
        this->table_space_ = new TableSpace(((ycsb_wl *) m_workload_)->the_table->get_table_name());
        this->index_ = new Index(((ycsb_wl *) m_workload_)->the_table->get_table_name() + "_INDEX");
        table_space_->DeSerialize();
        index_->DeSerialize();

        this->buffer_ = new Buffer(table_space_->GetLastPageId() * MY_PAGE_SIZE, MY_PAGE_SIZE, shared_disk_client_);
        this->lock_table_ = new LockTable();
        lock_table_->Init(0, table_space_->GetLastPageId() + 1);
        InitLockTable();
        // TODO :
//        server_rpc_handler_ = new Server
    }

    void ManagerInstance::InitBufferForTest() {
        char buf[PAGE_SIZE];
        for (uint64_t page_id = 0; page_id < table_space_->GetLastPageId(); page_id++) {
            buffer_->BufferGet(page_id, buf, PAGE_SIZE);
        }
    }

    void ManagerInstance::InitMvccs() {
        std::cout << "ManagerInstance::InitMvccs" << std::endl;
        Profiler profiler;
        profiler.Start();
        for (uint64_t  key = 0; key < g_synth_table_size; key++) {
            mvcc_map_.insert(std::pair<uint64_t, Row_mvcc *>(key, new Row_mvcc()));
            mvcc_map_[key]->init(key, ((ycsb_wl *) m_workload_)->the_table->get_schema()->tuple_size, this);
        }
        profiler.End();
        std::cout << "ManagerInstance::InitMvccs done. time : " << profiler.Millis() << " millis." << std::endl;
    }
    void ManagerInstance::InitLockTable() {
        std::cout << "ManagerInstance::InitLockTable" << std::endl;
        for(auto &iter : lock_table_->lock_table()) {
            iter.second->instance_id = this->instance_id_;
//            iter.second->valid = true;
        }
        std::cout << "ManagerInstance::InitLockTable done." << std::endl;
    }

    uint64_t ManagerInstance::GetNextTs(uint64_t thread_id) { return timestamp_.fetch_add(1); }

    RC ManagerInstance::GetRow(uint64_t key, access_t type, txn_man *txn, RowItem *&row) {
        RC rc = RC::RCOK;
        uint64_t thd_id = txn->get_thd_id();
        TsType ts_type = (type == RD) ? R_REQ : P_REQ;
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

    void ManagerInstance::ReturnRow(uint64_t key, access_t type, txn_man *txn, RowItem *row) {
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

    void ManagerInstance::SetTxnMan(txn_man *m_txn) { txn_man_[m_txn->get_thd_id()] = m_txn; }

    void ManagerInstance::AddTs(uint64_t thread_id, uint64_t ts) { all_ts_[thread_id] = ts; }

    uint64_t ManagerInstance::GetMinTs(uint64_t threa_id) {
        uint64_t min_ts = UINT64_MAX;
        for (uint32_t thd_id = 0; thd_id < g_thread_cnt; thd_id++) {
            if (min_ts > all_ts_[thd_id]) {
                min_ts = all_ts_[thd_id];
            }
        }
        return min_ts;
    }

    bool ManagerInstance::RowFromDB(RowItem* row) {
        dbx1000::Page* page = new dbx1000::Page(new char[MY_PAGE_SIZE]);
        dbx1000::IndexItem indexItem;
        this->index_->IndexGet(row->key_, &indexItem);
        bool rc = this->lock_table_->Lock(indexItem.page_id_, dbx1000::LockMode::S);
        assert(true == rc);
        this->buffer_->BufferGet(indexItem.page_id_, page->page_buf(), MY_PAGE_SIZE);
        page->Deserialize();

        assert(row->size_ == ((ycsb_wl *) (this->m_workload_))->the_table->get_schema()->get_tuple_size());
        memcpy(row->row_, &page->page_buf()[indexItem.page_location_], row->size_);
//        {
//            /// 验证 key
//            uint64_t temp_key;
//            memcpy(&temp_key, row->row_, sizeof(uint64_t));           /// 读 key
//            assert(row->key_ == temp_key);
//        }
        assert(true == this->lock_table_->UnLock(indexItem.page_id_));
    }
    bool ManagerInstance::RowToDB(RowItem *row) {
        dbx1000::Page* page = new dbx1000::Page(new char[MY_PAGE_SIZE]);
        dbx1000::IndexItem indexItem;
        this->index_->IndexGet(row->key_, &indexItem);
        bool rc = this->lock_table_->Lock(indexItem.page_id_, dbx1000::LockMode::X);
        assert(true == rc);
        this->buffer_->BufferGet(indexItem.page_id_, page->page_buf(), MY_PAGE_SIZE);
        page->Deserialize();

        assert(row->size_ == ((ycsb_wl *) (this->m_workload_))->the_table->get_schema()->get_tuple_size());
        memcpy(&page->page_buf()[indexItem.page_location_], row->row_, row->size_);
        this->buffer_->BufferPut(indexItem.page_id_, page->Serialize()->page_buf(), MY_PAGE_SIZE);
        assert(true == this->lock_table_->UnLock(indexItem.page_id_));
    }


    int ManagerInstance::instance_id() { return this->instance_id(); }
    void ManagerInstance::set_init_done(bool init_done) { this->init_done_ = init_done; }
    void ManagerInstance::set_instance_id(int instance_id) { this->instance_id_ = instance_id; }
    std::map<int, std::string> &ManagerInstance::host_map() { return this->host_map_; }
    Stats ManagerInstance::stats() { return this->stats_; }
    Query_queue *ManagerInstance::query_queue() { return this->query_queue_; }
    workload* ManagerInstance::m_workload() { return this->m_workload_; }
    Buffer* ManagerInstance::buffer() { return this->buffer_; }
    TableSpace* ManagerInstance::table_space() { return this->table_space_; }
    Index* ManagerInstance::index() { return this->index_; }
    LockTable* ManagerInstance::lock_table() { return this->lock_table_; }

    InstanceClient *ManagerInstance::instance_rpc_handler() { return this->instance_rpc_handler_; }

    std::map<uint64_t, Row_mvcc *> ManagerInstance::mvcc_map() { return this->mvcc_map_; }


}