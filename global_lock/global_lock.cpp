//
// Created by rrzhang on 2020/6/3.
//
#include <unistd.h>
#include <fstream>
#include <cassert>
#include "global_lock.h"

#include "common/buffer/buffer.h"
#include "common/index/index.h"
#include "common/lock_table/lock_table.h"
#include "common/storage/disk/file_io.h"
#include "common/storage/tablespace/page.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/catalog.h"
#include "common/storage/table.h"
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
            this->init_done_ = false;
            this->instances_ = new InstanceInfo[PROCESS_CNT]();
            for (int i = 0; i < PROCESS_CNT; i++) {
                instances_[i].instance_id = -1;
                instances_[i].init_done = false;
                instances_[i].stats.init();
                instances_[i].instance_run_done = false;
            }

            timestamp_ = ATOMIC_VAR_INIT(1);
#if WORKLOAD == YCSB
            this->m_workload_ = new ycsb_wl();
#elif WORKLOAD == TPCC
            this->m_workload_ = new tpcc_wl();
#endif
            m_workload_->init();
//            this->shared_disk_client_ = new SharedDiskClient(SHARED_DISK_HOST);
//            this->table_space_ = new TableSpace(((ycsb_wl *) m_workload_)->the_table->get_table_name());
//            this->index_ = new Index(((ycsb_wl *) m_workload_)->the_table->get_table_name() + "_INDEX");
//            table_space_->DeSerialize();
//            index_->DeSerialize();
//
//
//            this->buffer_ = new Buffer(FILE_SIZE, MY_PAGE_SIZE, shared_disk_client_);
            /// 初始化 lock_service_table_
#if WORKLOAD == YCSB
            for (uint64_t page_id = 0; page_id < SYNTH_TABLE_SIZE; page_id++) {
                this->lock_tables_[TABLES::MAIN_TABLE].insert(make_pair(page_id, new LockNode()));
            }
#elif WORKLOAD == TPCC
            for (uint32_t i = 1; i <= g_max_items; i++) {
                this->lock_tables_[TABLES::ITEM].insert(make_pair(i, new LockNode()));
            }
            for(uint64_t key = 1; key <= NUM_WH; key++) {
                this->lock_tables_[TABLES::WAREHOUSE].insert(make_pair(key, new LockNode()));
            }
            for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
                this->lock_tables_[TABLES::DISTRICT].insert(make_pair(did, new LockNode()));
            }
            for (uint32_t sid = 1; sid <= g_max_items; sid++) {
                this->lock_tables_[TABLES::STOCK].insert(make_pair(sid, new LockNode()));
            }
            for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
                for (uint32_t cid = 1; cid <= g_cust_per_dist; cid++) {
                    this->lock_tables_[TABLES::CUSTOMER].insert(make_pair(did * cid, new LockNode()));
                }
                for (uint32_t oid = 1; oid <= g_cust_per_dist; oid++) { }
                for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) { }
            }
#endif
        }

        /**
         * 线程 ins_id 需要获取 page_id 的锁，本地 lock_service_table_ 记录了拥有该 page 锁的节点，需要先时其他节点的锁失效，
         * 同时把其他节点最新的 page_buf 拿过来，然后返回锁权限和 page_buf 给 ins_id
         * ， 当然，也有可能 lock_service_table_[page_id] = -1，即没有任何其他节点拥有该锁权限
         * @param ins_id
         * @param page_id
         * @param page_buf
         * @param count
         * @return
         */
        RC GlobalLock::LockRemote(uint64_t ins_id, TABLES table, uint64_t item_id, char *buf, size_t count) {
            auto iter = lock_tables_[table].find(item_id);
            if (lock_tables_[table].end() == iter) { assert(false); return RC::Abort; }

            RC rc = RC::RCOK;
            std::unique_lock<std::mutex> lck(iter->second->mtx);
            if (iter->second->write_ins_id >= 0) {
//                    cout << ins_id << " want to invalid " << iter->second->write_ins_id << ", page id : " << page_id << endl;
                rc = instances_[iter->second->write_ins_id].global_lock_service_client->Invalid(table, item_id, buf, count);

                if (rc == RC::TIME_OUT) {
//                        cout << ins_id << " invaild " << iter->second->write_ins_id << ", page: " << page_id << " time out" << endl;
                }
                if (rc == RC::Abort) {
//                        cout << ins_id << " invaild " << iter->second->write_ins_id << ", page: " << page_id << " Abort" << endl;
                }
                if (rc == RC::RCOK) {
//                        cout << ins_id << " invalid " << iter->second->write_ins_id << ", page: " << page_id << " success" << endl;
                    iter->second->write_ins_id = ins_id;
                }
            } else {
                iter->second->write_ins_id = ins_id;
                if (count > 0) {
//                    this->buffer_->BufferGet(table, item_id, buf, count);
                }
//                 cout << ins_id << " get page id : " << page_id << " success" << endl;
            }
                return rc;
        }

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