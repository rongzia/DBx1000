//
// Created by rrzhang on 2020/6/3.
//
#include <unistd.h>
#include <fstream>
#include <cassert>
#include "manager_lock_server.h"

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
#include "json/json.h"
#include "lock_server/lock_server_table/lock_server_table.h"
#include "rpc_handler/lock_service_handler.h"
#include "shared_disk/shared_disk_service.h"
#include "config.h"


namespace dbx1000 {
    ManagerServer::~ManagerServer() {
//        delete buffer_;
        delete lock_table_;
    }

    ManagerServer::ManagerServer() {
        this->init_done_ = false;
        this->instances_ = new InstanceInfo[PROCESS_CNT]();
        for(int i = 0; i< PROCESS_CNT; i++) { instances_[i].instance_id = -1; instances_[i].init_done = false; }

        this->m_workload_ = new ycsb_wl();
        m_workload_->init();
        this->shared_disk_client_ = new SharedDiskClient(SHARED_DISK_HOST);
        this->table_space_ = new TableSpace(((ycsb_wl *) m_workload_)->the_table->get_table_name());
        this->index_ = new Index(((ycsb_wl *) m_workload_)->the_table->get_table_name() + "_INDEX");
        table_space_->DeSerialize();
        index_->DeSerialize();


        this->buffer_ = new Buffer(ITEM_NUM_PER_FILE * MY_PAGE_SIZE, MY_PAGE_SIZE, shared_disk_client_);
        this->lock_table_ = new LockTable();

        lock_table_->Init(0, table_space_->GetLastPageId() + 1, -1);

        for(auto iter : lock_table_->lock_table() ){ iter.second->lock_mode = LockMode::P; }
        for(uint64_t page_id = 0; page_id < table_space_->GetLastPageId() + 1; page_id ++){
            char page_buf[MY_PAGE_SIZE];
            buffer_->BufferGet(page_id, page_buf, MY_PAGE_SIZE);
        }
        test_num = 0;
    }

    uint64_t ManagerServer::GetNextTs(uint64_t thread_id) { return timestamp_.fetch_add(1); }
    void ManagerServer::set_instance_i(int instance_id){
        cout << "ManagerServer::set_instance_i, instance_id : " << instance_id << endl;
        assert(instance_id >= 0 && instance_id < PROCESS_CNT);
        assert(instances_[instance_id].init_done == false);
        instances_[instance_id].init_done = true;
        instances_[instance_id].instance_id = instance_id;
        instances_[instance_id].host = hosts_map_[instance_id];
        cout << "ManagerServer::set_instance_i, instances_[instance_id].host : " << instances_[instance_id].host << endl;
        instances_[instance_id].buffer_manager_client = new BufferManagerClient(instances_[instance_id].host);
    }


    bool ManagerServer::init_done() {
//        for(int i = 0; i < PROCESS_CNT; i++) {
        for(int i = 0; i < PROCESS_CNT; i++) {
            if(instances_[i].init_done == false){
                return false;
            }
        }
        init_done_ = true;
        return init_done_;
    }
    void ManagerServer::set_buffer_manager_id(int id) { this->buffer_manager_id_ = id; }
    std::map<int, std::string> &ManagerServer::hosts_map() { return this->hosts_map_; }
    ManagerServer::InstanceInfo* ManagerServer::instances(){ return this->instances_; }
    LockTable* ManagerServer::lock_table() { return this->lock_table_; }
}