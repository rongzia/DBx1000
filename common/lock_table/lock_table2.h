//
// Created by rrzhang on 2020/10/9.
//

#ifndef DBX1000_LOCK_TABLE2_H
#define DBX1000_LOCK_TABLE2_H

#include <unordered_map>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <set>
#include <atomic>

#include "common/global.h"
#include "common/myhelper.h"

namespace dbx1000 {
    enum class LockMode{
        O,  /// 失效
        P,  /// 独占
        S,  /// 读锁
        X,  /// 写锁
    };

    struct LockNode2{
        LockNode(int instanceid);
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

    class LockTable2 {
    public:
        ~LockTable2();
        LockTable2(TABLES, int instance_id, uint64_t start_id, uint64_t end_id);
        RC Lock(uint64_t item_id, LockMode mode);
        RC UnLock(uint64_t page_id);
        bool AddThread(uint64_t item_id, uint64_t thd_id);
        bool RemoveThread(uint64_t item_id, uint64_t thd_id);
        RC LockInvalid(uint64_t item_id, char *buf, size_t count);
        
        std::map<TABLES, std::unordered_map<uint64_t, std::pair<weak_ptr<LockNode2>, bool>> *> lock_table_;
    };

    LockTable2::LockTable2(TABLES table, int instance_id, uint64_t start_id, uint64_t end_id)
            : table_(table), instance_id_(instance_id) {
        for(uint64_t key = start_id; key < end_id; key++) {
            shared_ptr<LockNode2> p;
            lock_table_.insert(make_pair(key, make_pair(p, false)));
        }
    }

    RC LockTable2::Lock(uint64_t item_id, LockMode mode) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter || iter->second.first.expired()) { assert(false); return RC::Abort; }

        shared_ptr<LockNode2> lock_node = iter->second.first.lock();
        std::unique_lock<std::mutex> lck(lock_node->mtx);
        /// 事务执行中才会使用 Lock UnLock，本地应该存在该 item 的读写权限，即 lock_mode 为 非 O，
        /// 否则事务应该在执行前就回滚了
        if(LockMode::O == lock_node->lock_mode) { assert(false); return RC::Abort; }

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
            // if(LockMode::O == lock_node->lock_mode) { return RC::RCOK; }
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
    RC LockTable2::UnLock(uint64_t item_id) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter || iter->second.first.expired()) { assert(false); return RC::Abort; }

        shared_ptr<LockNode2> lock_node = iter->second.first.lock();
        std::unique_lock<std::mutex> lck(lock_node->mtx);
        /// 事务执行中才会使用 Lock UnLock，本地应该存在该 item 的读写权限，即 lock_mode 为 非 O，
        /// 否则事务应该在执行前就回滚了
        if(LockMode::O == lock_node->lock_mode) { assert(false); return RC::Abort; }

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

//    RC LockTable2::GetLockTableItemSharedPtr(uint64_t item_id) {
//        auto iter = lock_table_.find(item_id);
//        if (lock_table_.end() == iter) { assert(false); return RC::Abort; }
//
//        shared_ptr<LockNode2> lock_node;
//        while(!ATOM_CAS(iter->second.second, false, true))
//            PAUSE
//        lock_node = iter->second.first.lock();
//        if(iter->second.first.expired()) {
//            assert(lock_node == nullptr);
//            assert(iter->second.first.use_count() == 0);
//            assert(iter->second.first.expired());
//            lock_node = std::make_shared<LockNode2>(this->instance_id_);
//            iter->second.first = lock_node;
//        } else {
//            assert(!iter->second.first.expired());
//            assert(lock_node != nullptr);
//            assert(iter->second.first.use_count() > 0);
//            assert(!iter->second.first.expired());
//        }
//        iter->second.second = true;
//
//        return RC::RCOK;
//    }

}



#endif //DBX1000_LOCK_TABLE2_H
