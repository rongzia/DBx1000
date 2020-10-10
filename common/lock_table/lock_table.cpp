//
// Created by rrzhang on 2020/10/10.
//
#include "lock_table.h"


#include "common/buffer/buffer.h"
#include "instance/manager_instance.h"

namespace dbx1000 {


    LockNode::LockNode(int instanceid, LockMode mode) {
        instance_id = instanceid;
        lock_mode = mode;
    }

    LockTable::LockTable(TABLES table, int instance_id, uint64_t start_id, uint64_t end_id)
            : table_(table), instance_id_(instance_id) {
        for (uint64_t key = start_id; key < end_id; key++) {
            shared_ptr<LockNode> p;
//            lock_table_.insert(make_pair(key, make_pair(p, false)));
            lock_table_[key] =  make_pair(p, false);
            lock_node_local_[key] = false;
        }
    }

    RC LockTable::Lock(uint64_t item_id, LockMode mode) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter || iter->second.first.expired()) { assert(false); return RC::Abort; }

        shared_ptr<LockNode> lock_node = iter->second.first.lock();
        std::unique_lock<std::mutex> lck(lock_node->mtx);
        /// 事务执行中才会使用 Lock UnLock，本地应该存在该 item 的读写权限，即 lock_mode 为 非 O，
        /// 否则事务应该在执行前就回滚了
        if(!IsValid(item_id, lock_node.get())) { assert(false); return RC::Abort; }

        int temp_count;
        if(LockMode::X == mode) {
            lock_node->cv.wait(lck, [lock_node](){ return (LockMode::P == lock_node->lock_mode);});
            if(LockMode::O == lock_node->lock_mode) { return RC::Abort; }
            lock_node->lock_mode = LockMode::X;
            temp_count = lock_node->count.fetch_add(1);
            assert(0 == temp_count);
            return RC::RCOK;
        }
        if(LockMode::S == mode) {
            lock_node->cv.wait(lck, [lock_node](){ return (LockMode::X != lock_node->lock_mode); });
            if(LockMode::O == lock_node->lock_mode) { return RC::RCOK; }
            if(LockMode::P == lock_node->lock_mode) {
                temp_count = lock_node->count.fetch_add(1);
                assert(0 == temp_count);
                lock_node->lock_mode = LockMode::S;
            } else if(LockMode::S == lock_node->lock_mode) {
                temp_count = lock_node->count.fetch_add(1);
                assert(temp_count > 0);
            } else { assert(false); }
            return RC::RCOK;
        }
            // 只能读（S）或者写（X）
        else { assert(false); }

    }
    RC LockTable::UnLock(uint64_t item_id) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter || iter->second.first.expired()) { assert(false); return RC::Abort; }

        shared_ptr<LockNode> lock_node = iter->second.first.lock();
        std::unique_lock<std::mutex> lck(lock_node->mtx);
        /// 事务执行中才会使用 Lock UnLock，本地应该存在该 item 的读写权限，即 lock_mode 为 非 O，
        /// 否则事务应该在执行前就回滚了
        if(!IsValid(item_id, lock_node.get())) { assert(false); return RC::Abort; }

        int temp_count;
        if (LockMode::X == lock_node->lock_mode) {
            temp_count = lock_node->count.fetch_sub(1);
            assert(temp_count == 1);
            lock_node->lock_mode = LockMode::P;
            lock_node->cv.notify_all();
            return RC::RCOK;
        }
        /* */
        if (LockMode::S == lock_node->lock_mode) {
            // 仅非 LockMode::O 才需要更改锁表项状态
            temp_count = lock_node->count.fetch_sub(1);
            assert(temp_count > 0);
            if (0 == lock_node->count) { lock_node->lock_mode = LockMode::P; }
            lock_node->cv.notify_all();
            return RC::RCOK;
        }
        if (LockMode::O == lock_node->lock_mode || LockMode::P == lock_node->lock_mode) { return RC::RCOK; }
        assert(false);
    }
    bool LockTable::AddThread(uint64_t item_id, uint64_t thd_id) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter || iter->second.first.expired()) { assert(false); return false; }

        shared_ptr<LockNode> lock_node = iter->second.first.lock();
        std::unique_lock<std::mutex> lck(lock_node->mtx);
        auto thread_iter = lock_node->thread_set.find(thd_id);
        if(thread_iter == lock_node->thread_set.end()){
            lock_node->thread_set.insert(thd_id);
        }
        return true;
    }
    bool LockTable::RemoveThread(uint64_t item_id, uint64_t thd_id) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter || iter->second.first.expired()) { assert(false); return false; }

        shared_ptr<LockNode> lock_node = iter->second.first.lock();
        std::unique_lock<std::mutex> lck(lock_node->mtx);
        auto thread_iter = lock_node->thread_set.find(thd_id);
        assert(thread_iter != lock_node->thread_set.end());
        lock_node->thread_set.erase(thd_id);
        if(lock_node->thread_set.size() == 0) { lock_node->remote_locking_abort = false; }

        lock_node->cv.notify_all();
        return true;
    }
    RC LockTable::RemoteInvalid(uint64_t item_id, char *buf, size_t count) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter) { assert(false); return RC::Abort; }

        /// 本地 mode 为 O, 失效错误
        if(lock_node_local_[item_id] == false) { assert(false); return RC::Abort; }

        shared_ptr<LockNode> lock_node = GetLockTableItemSharedPtr(item_id);

        RC rc = RC::RCOK;
        lock_node->invalid_req = true;
        std::unique_lock<std::mutex> lck(lock_node->mtx);
        if(lock_node->cv.wait_for(lck, chrono::milliseconds(10), [lock_node](){ return (lock_node->thread_set.size() == 0); }))
        {
            assert(lock_node->thread_set.size() == 0);
            assert(lock_node->count == 0);
            Invalid(item_id, lock_node.get());
            manager_instance_->buffer()->BufferGet(item_id, buf, count);
            rc = RC::RCOK;
        } else {
            rc = RC::TIME_OUT;
        }
        lock_node->invalid_req = false;
        return rc;
    }

    shared_ptr<LockNode> LockTable::GetLockTableItemSharedPtr(uint64_t item_id) {
        auto iter = lock_table_.find(item_id);
        if (lock_table_.end() == iter) { assert(false); return nullptr; }

        shared_ptr<LockNode> lock_node;
        while(!ATOM_CAS(iter->second.second, false, true))
            PAUSE
                    lock_node = iter->second.first.lock();
        if(iter->second.first.expired()) {
            assert(lock_node == nullptr);
            assert(iter->second.first.use_count() == 0);
            assert(iter->second.first.expired());
            lock_node = std::make_shared<LockNode>(this->instance_id_, (lock_node_local_[item_id] == true ? LockMode::P : LockMode::O));
            iter->second.first = lock_node;
        } else {
            assert(!iter->second.first.expired());
            assert(lock_node != nullptr);
            assert(iter->second.first.use_count() > 0);
            assert(!iter->second.first.expired());
        }
        iter->second.second = true;

        return lock_node;
    }
}