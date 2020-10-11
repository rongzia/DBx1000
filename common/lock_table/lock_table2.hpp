//
// Created by rrzhang on 2020/10/9.
//

#ifndef DBX1000_LOCK_TABLE2_HPP
#define DBX1000_LOCK_TABLE2_HPP

#include <unordered_map>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <set>
#include <atomic>
#include <cassert>

#include "common/global.h"
#include "common/myhelper.h"
#include "instance/manager_instance.h"
#include "common/buffer/buffer.h"

namespace dbx1000 {
    enum class LockMode{
        O,  /// 失效
        P,  /// 独占
        S,  /// 读锁
        X,  /// 写锁
    };

    struct LockNode{
        LockNode(int instanceid, LockMode mode);
        int instance_id;
        LockMode lock_mode;
        std::atomic_int count;                      // 正在获取该 locknode 的调用数
        std::set<uint64_t> thread_set;              // 本地对该 locknode 有写意向的线程
        bool invalid_req;                           // 其他进程想要获取当前 page 的锁，在事务执行过程中只能置 invalid_req=true, 然后等待该机器的事务结束
        volatile bool lock_remoting;
        std::atomic<bool> remote_locking_abort;
        std::mutex mtx;
        std::condition_variable cv;
    };
    LockNode::LockNode(int instanceid, LockMode mode) {
        instance_id = instanceid;
        lock_mode = mode;
    }


    class LockTable {
    public:
        ~LockTable();
        LockTable(TABLES, int instance_id, uint64_t start_id, uint64_t end_id);
        RC Lock(uint64_t item_id, LockMode mode);
        RC UnLock(uint64_t page_id);
        bool AddThread(uint64_t item_id, uint64_t thd_id);
        bool RemoveThread(uint64_t item_id, uint64_t thd_id);
        RC RemoteInvalid(uint64_t item_id, char *buf, size_t count);

        void Valid(uint64_t item_id, LockNode *lockNode) { this->lock_node_local_[item_id] = true; lockNode->lock_mode = LockMode::P; }
        void Invalid(uint64_t item_id, LockNode *lockNode) { this->lock_node_local_[item_id] = false; lockNode->lock_mode = LockMode::O; }
        bool IsValid(uint64_t item_id, LockNode *lockNode) { return (lock_node_local_[item_id] && lockNode->lock_mode != LockMode::O); }
    private:
        shared_ptr<LockNode> GetLockTableItemSharedPtr(uint64_t item_id);

        TABLES table_;
        int instance_id_;
        std::unordered_map<uint64_t, std::pair<weak_ptr<LockNode>, bool>> lock_table_;
        std::unordered_map<uint64_t, bool> lock_node_local_;
        ManagerInstance* manager_instance_;
    };

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



#endif //DBX1000_LOCK_TABLE2_HPP
