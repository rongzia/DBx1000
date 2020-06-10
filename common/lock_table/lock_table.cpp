//
// Created by rrzhang on 2020/6/2.
//
#include <cassert>
#include <iostream>
#include "lock_table.h"

#include "instance/manager_instance.h"
#include "rpc_handler/instance_handler.h"
#include "config.h"

namespace dbx1000 {
    LockNode::LockNode(int instanceid, bool val, LockMode mode, int count) {
        this->instance_id = instanceid;
//        this->valid = val;
        this->lock_mode = mode;
        this->count = count;
//        this->lock.clear();
    }
    LockTable::~LockTable() {
        for(auto &iter : lock_table_){
            delete iter.second;
        }
    }
    LockTable::LockTable() {}

    void LockTable::Init(uint64_t start_page, uint64_t end_page) {
        for (uint64_t page_id = start_page; page_id < end_page; page_id++) {
            LockNode* lock_node = new LockNode(0, true);
            lock_table_.insert(std::pair<uint64_t, LockNode*>(page_id, lock_node));
        }
    }

    bool LockTable::Lock(uint64_t page_id, LockMode mode) {
        auto iter = lock_table_.find(page_id);
        if (lock_table_.end() == iter) {
            assert(false);
            return false;
        }
        std::unique_lock <std::mutex> lck(iter->second->mtx);
        if(LockMode::X == mode) {
            if(LockMode::O == iter->second->lock_mode) {
                char page_buf[MY_PAGE_SIZE];
                assert(manager_instance_->instance_rpc_handler()->LockGet(manager_instance_->instance_id()
                        , page_id, LockMode::X, page_buf, MY_PAGE_SIZE));
                assert(iter->second->count == 0);
                iter->second->lock_mode = LockMode::X;
                iter->second->count++;
                return true;
            }
            if(LockMode::P == iter->second->lock_mode){
                assert(iter->second->count == 0);
                iter->second->lock_mode = LockMode::X;
                iter->second->count++;
                return true;
            }
            while(LockMode::S == iter->second->lock_mode){
                iter->second->cv.wait(lck);
            }
            assert(iter->second->count == 0);
            manager_instance_->instance_rpc_handler()->LockGet()
            iter->second->lock_mode = LockMode::X;
            iter->second->count ++;
            return true;
        }
        if(LockMode::S == mode) {
            if(LockMode::O == iter->second->lock_mode) {
                char page_buf[MY_PAGE_SIZE];
                assert(manager_instance_->instance_rpc_handler()->LockGet(manager_instance_->instance_id()
                        , page_id, LockMode::S, page_buf, MY_PAGE_SIZE));
                assert(iter->second->count == 0);
                iter->second->lock_mode = LockMode::S;
                iter->second->count++;
                return true;
            }
            if(LockMode::P == iter->second->lock_mode){
                assert(iter->second->count == 0);
                iter->second->lock_mode = LockMode::S;
                iter->second->count++;
                return true;
            }
            while (LockMode::X == iter->second->lock_mode) {
                iter->second->cv.wait(lck);
            }
            iter->second->lock_mode = LockMode::S;
            iter->second->count ++;
            return true;
        }
        assert(false);
        return false;
    }

    bool LockTable::UnLock(uint64_t page_id) {
        auto iter = lock_table_.find(page_id);
        if (lock_table_.end() == iter) {
            return false;
        }
        std::unique_lock <std::mutex> lck(iter->second->mtx);
        if(LockMode::X == iter->second->lock_mode) {
            assert(iter->second->count == 1);
            iter->second->count--;
            iter->second->lock_mode = LockMode::P;
            iter->second->cv.notify_one();
            return true;
        }
        if(LockMode::S == iter->second->lock_mode) {
            iter->second->count--;
            if(iter->second->count == 0) {
                iter->second->lock_mode = LockMode::P;
            }
            iter->second->cv.notify_one();
            return true;
        }
    }

    bool LockTable::LockDowngrade(uint64_t page_id, LockMode from, LockMode to) {
        
    }

    bool LockTable::LockGet(uint64_t page_id, uint16_t node_i, LockMode mode) {}

    bool LockTable::LockRelease(uint64_t page_id, uint16_t node_i, LockMode mode) {}

    bool LockTable::LockGetBatch(uint64_t start, uint64_t end, uint16_t node_i, LockMode mode) {}

    bool LockTable::LockReleaseBatch(uint64_t start, uint64_t end, uint16_t node_i, LockMode mode) {}

    std::unordered_map<uint64_t, LockNode*> LockTable::lock_table() {
        return this->lock_table_;
    }
}