////
//// Created by rrzhang on 2020/6/2.
////
//#include <cassert>
//#include <iostream>
//#include "server_lock_table.h"
//
//#include "buffer_manager/manager_server.h"
//#include "rpc_handler/buffer_manager_handler.h"
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
//    bool ServerLockTable::LockGet(int instance_id, uint64_t page_id, dbx1000::LockMode mode, char *&page_buf, size_t &count) {
//        auto iter = lock_table_.find(page_id);
//        if (lock_table_.end() == iter) {
//            assert(false);
//            return false;
//        }
//        std::unique_lock <std::mutex> lck(iter->second->mtx);
//        if(LockMode::S == mode) {
//            if (iter->second->write_ids < 0) {
//                iter->second->read_ids.insert(instance_id);
//                return true;
//            } else {
//                page_buf = new char[MY_PAGE_SIZE];
//                bool rc = manager_server_->instances()[iter->second->write_ids].buffer_manager_client->LockDowngrade(
//                        page_id, LockMode::X, LockMode::P, page_buf, MY_PAGE_SIZE);
//                assert(rc);
//                count = MY_PAGE_SIZE;
//                iter->second->read_ids.insert(instance_id);
//                return true;
//            }
//        }
//        if (LockMode::X == mode) {
//            if(iter->second->write_ids < 0 && iter->second->read_ids.size() == 0) {
//                return true;
//            }
//            if(iter->second->write_ids < 0 && iter->second->read_ids.size() > 0) {
//                for(auto temp_iter : iter->second->read_ids) {
//                bool rc = manager_server_->instances()[iter->second->write_ids].buffer_manager_client->LockDowngrade(
//                        page_id, LockMode::S, LockMode::O, page_buf, MY_PAGE_SIZE);
//                    assert(rc);
//                }
//                iter->second->read_ids.clear();
//                iter->second->write_ids = instance_id;
//                return true;
//            }
//            if(iter->second->write_ids > 0) {
//                page_buf = new char[MY_PAGE_SIZE];
//                bool rc = manager_server_->instances()[iter->second->write_ids].buffer_manager_client->LockDowngrade(
//                        page_id, LockMode::X, LockMode::O, page_buf, MY_PAGE_SIZE);
//                assert(rc);
//                count = MY_PAGE_SIZE;
//                iter->second->write_ids = instance_id;
//                return true;
//            }
//            assert(false);
//        }
//        assert(false);
//        return false;
//    }
//}