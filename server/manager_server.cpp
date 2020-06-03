//
// Created by rrzhang on 2020/6/3.
//
#include <unistd.h>
#include "manager_server.h"

#include "common/global.h"
#include "common/buffer/buffer.h"
#include "common/index/index.h"
#include "common/lock_table/lock_table.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/table.h"
#include "common/storage/catalog.h"
#include "common/workload/ycsb_wl.h"

namespace dbx1000 {
//        Buffer* buffer_;
//        Index* index_;
//        LockTable* lock_table_;
//        TableSpace* table_space_;
//        workload* m_workload_;
//        Stats* stats_;
//
//        bool init_done_;
//        std::string host_;
//        InstanceInfo* instances_;
    bool ManagerServer::CheckDB() {
        std::string db_dir(DB_PREFIX);
        std::string table_name = ((ycsb_wl *) m_workload_)->the_table->get_table_name();
        if (access(db_dir.data(), F_OK) < 0
            || access((db_dir + table_name).data(), F_OK) < 0
            || access((db_dir + table_name + "_INDEX").data(), F_OK) < 0
                ) {
            return false;
        }
        this->table_space_->DeSerialize(db_dir + table_name);
        if (table_space_->page_size() != PAGE_SIZE
            || table_space_->row_size() != ((ycsb_wl *) m_workload_)->the_table->get_schema()->get_tuple_size()
                ) {
            return false;
        }
        return true;
    }

    ManagerServer::ManagerServer() {
        this->init_done_ = false;
        this->instances_ = new InstanceInfo[PROCESS_CNT]();
        this->buffer_ = new Buffer(FILE_SIZE, PAGE_SIZE);
        this->index_ = new Index();
        this->lock_table_ = new LockTable();
        this->table_space_ = new TableSpace();
        this->m_workload_ = new ycsb_wl();

        bool db_ok = CheckDB();
        if(true == db_ok) {
            init_done_ = true;
        } else {
            m_workload_->init_table();
        }

    }


    void ManagerServer::set_instances_i(InstanceInfo *instance) {
        uint16_t ins_id = instance->instance_id;
        this->instances_[ins_id].instance_id = ins_id;
        this->instances_[ins_id].thread_num = instance->thread_num;
        this->instances_[ins_id].host = instance->host;
        this->instances_[ins_id].init_done = instance->init_done;
        // TODO:
        this->instances_[ins_id].instance_rpc_handler = new InstanceClient();
    }

}