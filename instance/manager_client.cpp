//
// Created by rrzhang on 2020/4/27.
//
#include <fstream>
#include <cassert>
#include <cstdint>
#include "manager_client.h"

#include "config.h"
#include "api/api_txn/api_txn.h"
#include "instance/benchmarks/ycsb_query.h"
#include "instance/benchmarks/query.h"
#include "instance/txn/txn.h"
#include "common/buffer/buffer.h"
#include "common/index/index.h"
#include "common/lock_table/lock_table.h"
#include "common/storage/table.h"
#include "common/workload/ycsb_wl.h"
#include "common/workload/wl.h"
#include "common/global.h"
#include "json/json.h"

namespace dbx1000 {

    ManagerClient::ManagerClient() {
        delete query_queue_;
        delete m_workload_;
        delete buffer_manager_rpc_handler_;
        delete buffer_;
    }

    ManagerClient::~ManagerClient() {
        init_done_ = false;
        query_queue_ = new Query_queue();
        m_workload_ = new ycsb_wl();
        m_workload_->init();
        buffer_ = new Buffer(FILE_SIZE / 10, MY_PAGE_SIZE, this);
        index_ = new Index(((ycsb_wl*)m_workload_)->the_table->get_table_name() + "INDEX");
        index_->DeSerialize();
        lock_table_ = new LockTable();
        // TODO : init lock table
        // TODO :
//        server_rpc_handler_ = new Server
    }

    void ManagerClient::InitBufferForTest() {
        char buf[PAGE_SIZE];
        for(uint64_t page_id = 0; page_id < FILE_SIZE / PAGE_SIZE; page_id++ ) {
            buffer_->BufferGet(page_id, buf, PAGE_SIZE);
        }
    }

    Query_queue* ManagerClient::query_queue() { return this->query_queue_; }
    BufferManagerClient* ManagerClient::buffer_manager_rpc_handler() { return this->buffer_manager_rpc_handler_; }
}