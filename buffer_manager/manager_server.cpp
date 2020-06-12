////
//// Created by rrzhang on 2020/6/3.
////
//#include <unistd.h>
//#include <fstream>
//#include <cassert>
//#include "manager_server.h"
//
//#include "common/buffer/buffer.h"
//#include "common/index/index.h"
//#include "common/lock_table/lock_table.h"
//#include "common/storage/disk/file_io.h"
//#include "common/storage/tablespace/page.h"
//#include "common/storage/tablespace/row_item.h"
//#include "common/storage/tablespace/tablespace.h"
//#include "common/storage/catalog.h"
//#include "common/storage/table.h"
//#include "common/workload/ycsb_wl.h"
//#include "common/workload/wl.h"
//#include "common/global.h"
//#include "common/myhelper.h"
//#include "common/mystats.h"
//#include "json/json.h"
//#include "config.h"
//
//#include "server_lock_table/server_lock_table.h"
//
//namespace dbx1000 {
//    ManagerServer::~ManagerServer() {
//        delete buffer_;
//        delete lock_table_;
//    }
//
//    ManagerServer::ManagerServer() {
//        this->init_done_ = false;
//        this->instances_ = new InstanceInfo[PROCESS_CNT]();
//
//        this->m_workload_ = new ycsb_wl();
//        m_workload_->init();
//        this->table_space_ = new TableSpace(((ycsb_wl *) m_workload_)->the_table->get_table_name());
//        this->index_ = new Index(((ycsb_wl *) m_workload_)->the_table->get_table_name() + "_INDEX");
//        table_space_->DeSerialize();
//        index_->DeSerialize();
//        this->buffer_ = new Buffer(table_space_->GetLastPageId() * MY_PAGE_SIZE, MY_PAGE_SIZE);
//        this->lock_table_ = new ServerLockTable();
//        lock_table_->Init(0, table_space_->GetLastPageId() + 1);
//    }
//
//
//    void ManagerServer::set_instances_i(InstanceInfo *instance) {
//        uint16_t ins_id = instance->instance_id;
//        this->instances_[ins_id].instance_id = ins_id;
////        this->instances_[ins_id].thread_num = instance->thread_num;
//        this->instances_[ins_id].host = instance->host;
//        this->instances_[ins_id].init_done = instance->init_done;
//        // TODO:
////        this->instances_[ins_id].instance_rpc_handler = new InstanceClient();
//    }
//
//
//        ManagerServer::InstanceInfo* ManagerServer::instances(){ return this->instances_; }
//        ServerLockTable* ManagerServer::lock_table() { return this->lock_table_; }
//
//}