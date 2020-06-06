//
// Created by rrzhang on 2020/6/2.
//
#include <cassert>
#include <iostream>
#include "lock_table.h"

#include "config.h"

namespace dbx1000 {
    LockNode::LockNode(int instanceid, bool val, LockMode mode, int count) {
        this->instance_id = instanceid;
        this->valid = val;
        this->lock_mode = mode;
        this->count = count;
        this->lock.clear();
    }

    void LockTable::Init(uint64_t start_page, uint64_t end_page) {
        for (uint64_t page_id = start_page; page_id < end_page; page_id++) {
            LockNode* lock_node = new LockNode(0, true);
            lock_table_.insert(std::pair<uint64_t, LockNode*>(page_id, lock_node));
        }
    }

    bool LockTable::Lock(uint64_t page_id, LockMode mode) {
        auto iter = lock_table_.find(page_id);
        if (lock_table_.end() == iter) {
            return false;
        }
        std::unique_lock <std::mutex> lck(iter->second->mtx);
        if(LockMode::X == mode) {
            while (LockMode::N != iter->second->lock_mode) {
//                std::cout << "LockTable::Lock wait." << std::endl;
                iter->second->cv.wait(lck);
            }
            iter->second->lock_mode = LockMode::X;
            assert(iter->second->count == 0);
            iter->second->count ++;
            return true;
        }
        if(LockMode::S == mode) {
            while (LockMode::X == iter->second->lock_mode) {
                iter->second->cv.wait(lck);
            }
            iter->second->lock_mode = LockMode::S;
            iter->second->count ++;
            return true;
        }

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
            iter->second->lock_mode = LockMode::N;
            iter->second->cv.notify_one();
            return true;
        }
        if(LockMode::S == iter->second->lock_mode) {
            iter->second->count--;
            if(iter->second->count == 0) {
                iter->second->lock_mode = LockMode::N;
            }
            iter->second->cv.notify_one();
            return true;
        }
    }

    bool LockTable::LockGet(uint64_t page_id, uint16_t node_i, LockMode mode) {}

    bool LockTable::LockRelease(uint64_t page_id, uint16_t node_i, LockMode mode) {}

    bool LockTable::LockGetBatch(uint64_t start, uint64_t end, uint16_t node_i, LockMode mode) {}

    bool LockTable::LockReleaseBatch(uint64_t start, uint64_t end, uint16_t node_i, LockMode mode) {}

    std::unordered_map<uint64_t, LockNode*> LockTable::lock_table() {
        return this->lock_table_;
    }
}