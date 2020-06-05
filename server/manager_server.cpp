//
// Created by rrzhang on 2020/6/3.
//
#include <unistd.h>
#include <fstream>
#include <cassert>
#include "manager_server.h"

#include "common/global.h"
#include "common/buffer/buffer.h"
#include "common/index/index.h"
#include "common/lock_table/lock_table.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/table.h"
#include "common/storage/catalog.h"
#include "common/workload/ycsb_wl.h"
#include "json/json.h"

namespace dbx1000 {
    ManagerServer::~ManagerServer() {
        delete buffer_;
        delete m_workload_;
        delete table_space_;
        delete index_;
        delete lock_table_;
        delete stats_;
    }

    bool ManagerServer::CheckDB() {
        std::string db_dir(DB_PREFIX);
        std::string table_name = ((ycsb_wl *) m_workload_)->the_table->get_table_name();

        if (access(db_dir.data(), F_OK) < 0
            || access((db_dir + table_name).data(), F_OK) < 0
            || access((db_dir + table_name + "_INDEX").data(), F_OK) < 0
                ) {
            cout <<__LINE__ << endl;
            return false;
        }
            cout <<__LINE__ << endl;
        this->table_space_->DeSerialize();
        this->index_->DeSerialize();
            cout <<__LINE__ << endl;
            cout << "table page_size : " <<table_space_->page_size() << endl;
            cout << "table row_size : " <<table_space_->row_size() << endl;
        if (table_space_->page_size() != MY_PAGE_SIZE
            || table_space_->row_size() != ((ycsb_wl *) m_workload_)->the_table->get_schema()->get_tuple_size()
                ) {
            cout <<__LINE__ << endl;
            return false;
        }
            cout <<__LINE__ << endl;
        return true;
    }

    ManagerServer::ManagerServer() {
        this->init_done_ = false;
        this->instances_ = new InstanceInfo[PROCESS_CNT]();
        this->buffer_ = new Buffer(FILE_SIZE, MY_PAGE_SIZE);
        this->m_workload_ = new ycsb_wl();
        m_workload_->init();
        this->table_space_ = new TableSpace(((ycsb_wl*)m_workload_)->the_table->get_table_name());
        this->index_ = new Index(((ycsb_wl*)m_workload_)->index_name_);
        m_workload_->table_space_ = this->table_space_;
        m_workload_->index_ = this->index_;
        this->lock_table_ = new LockTable();
        this->stats_ = new Stats();


        std::ifstream in("../config.json", std::ios::out | std::ios::binary);
        assert(in.is_open());
        Json::Value root;
        in >> root;
        this->host_ = root["server"]["ip"].asString() + ":" + root["server"]["port"].asString();
        in.close();

        bool db_ok = CheckDB();
        if(true == db_ok) {
            cout <<__LINE__ << endl;
            init_done_ = true;
        } else {
            cout <<__LINE__ << endl;
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
//        this->instances_[ins_id].instance_rpc_handler = new InstanceClient();
    }

}