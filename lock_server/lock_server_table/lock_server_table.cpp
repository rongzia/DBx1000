////
//// Created by rrzhang on 2020/6/2.
////
//#include <cassert>
//#include <iostream>
//#include "lock_server_table.h"
//
//#include "lock_server/manager_lock_server.h"
//#include "rpc_handler/lock_service_handler.h"
//
//#include "config.h"
//
//namespace dbx1000 {
//    ServerLockNode::ServerLockNode() {
//        this->write_ids = -1;
//    }
//
//    ServerLockTable::~ServerLockTable() {
//        for(auto &iter : lock_table_){
//            delete iter.second;
//        }
//    }
//
//    ServerLockTable::ServerLockTable(ManagerServer *manager_server) {
//        this->manager_server_ = manager_server;
//    }
//
//    void ServerLockTable::Init(uint64_t start_page, uint64_t end_page) {
//        for (uint64_t page_id = start_page; page_id < end_page; page_id++) {
//            ServerLockNode* lock_node = new ServerLockNode();
//            lock_table_.insert(std::pair<uint64_t, ServerLockNode*>(page_id, lock_node));
//        }
//    }
//
//    bool ServerLockTable::Lock(int instance_id, uint64_t page_id, LockMode req_mode, char *page_buf, size_t count) {
//        auto iter = lock_table_.find(page_id);
//        if (lock_table_.end() == iter) {
//            assert(false);
//            return false;
//        }
//        std::unique_lock <std::mutex> lck(iter->second->mtx);
//        if(req_mode == LockMode::X) {
//            if(count == 0) {    // 请求者拥有非 O 锁，申请锁
//                bool flag = true;
////                iter->second->mtx.lock();
//                assert(iter->second->write_ids == instance_id);
////                iter->second->mtx.unlock();
//                // 使拥有读锁的节点失效
////                for(auto item : iter->second->read_ids) {
////                    flag = manager_server_->instances()[item].buffer_manager_client->LockInvalid(page_id, nullptr, 0);
////                    assert(flag == true);
////                }
////                for(auto item : iter->second->read_ids) {
////                    // 删掉读节点集合
////                    iter->second->read_ids.erase(item);
////                }
//                return flag;
//            }
//            if(count == MY_PAGE_SIZE){
//                bool flag = true;
//                char page_buf[count];
//                if(iter->second->write_ids >= 0){
//                    flag = manager_server_->instances()[iter->second->write_ids].buffer_manager_client->LockInvalid( page_id,  page_buf, count);
//                } else {
//                    // TODO 从磁盘读
//                    memset(page_buf, 0, count);
//                }
//                assert(flag == true);
//                for(auto item : iter->second->read_ids) {
//                    flag = manager_server_->instances()[item].buffer_manager_client->LockInvalid( page_id,  nullptr, 0);
//                    assert(flag == true);
//                }
//                for(auto item : iter->second->read_ids) {
//                    // 删掉读节点集合
//                    iter->second->read_ids.erase(item);
//                }
//                iter->second->write_ids = instance_id;
//                return flag;
//            }
//            assert(false);
//        }
//        if(req_mode == LockMode::S) {
//            assert(iter->second->write_ids >= 0);
//            assert(count == MY_PAGE_SIZE);
//            bool flag = true;
//            char page_buf[count];
//            flag = manager_server_->instances()[iter->second->write_ids].buffer_manager_client->LockInvalid( page_id, page_buf, count);
//            assert(flag == true);
//            iter->second->write_ids = -1;
//            iter->second->read_ids.insert(instance_id);
//            return flag;
//        }
//    }
//
//
//    std::unordered_map<uint64_t, ServerLockNode*> ServerLockTable::lock_table() { return this->lock_table_; }
//}