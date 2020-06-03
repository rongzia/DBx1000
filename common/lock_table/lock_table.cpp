//
// Created by rrzhang on 2020/6/2.
//

#include "lock_table.h"

namespace dbx1000 {

    LockNode::LockNode(uint16_t nodeid, bool val, LockMode mode) {
        node_id = nodeid;
        valid = val;
        lock_mode = mode;
        lock.clear();
    }
    bool LockTable::Lock(uint64_t page_id, LockMode mode) {
        auto iter = lock_table_.find(page_id);
        if (lock_table_.end() == iter) {
            return false;
        }
        while(iter->second->lock.test_and_set(std::memory_order_acquire)) {
            iter->second->lock_mode = mode;
            return true;
        }

        return false;
    }

    bool LockTable::UnLock(uint64_t page_id) {
        auto iter = lock_table_.find(page_id);
        if(lock_table_.end() != iter) {
            iter->second->lock_mode = LockMode::N;
            iter->second->lock.clear(std::memory_order_release);
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