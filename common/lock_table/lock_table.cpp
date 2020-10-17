//
// Created by rrzhang on 2020/10/10.
//
#include "lock_table.h"


#include "common/buffer/record_buffer.h"
#include "common/storage/row.h"
#include "instance/manager_instance.h"
#include "util/shared_ptr_helper.h"

namespace dbx1000 {
    void LockNode::Init(int instanceid, LockMode mode) {
        this->instance_id = instanceid;
        this->lock_mode = mode;
    }

    bool LockNode::AddThread(uint64_t thd_id) {
        std::unique_lock<std::mutex> lck(mtx);
        auto thread_iter = this->thread_set.find(thd_id);
        if(thread_iter == this->thread_set.end()){
            this->thread_set.insert(thd_id);
        }
        return true;
    }

    bool LockNode::RemoveThread(uint64_t thd_id) {
        std::unique_lock<std::mutex> lck(mtx);
        auto thread_iter = this->thread_set.find(thd_id);
        assert(thread_iter != this->thread_set.end());
        this->thread_set.erase(thd_id);
        if(this->thread_set.empty()) { this->remote_locking_abort = false; }

        this->cv.notify_all();
        return true;
    }
    RC LockNode::Lock(LockMode mode) {
        std::unique_lock<std::mutex> lck(mtx);

        int temp_count;
        if(LockMode::X == mode) {
            this->cv.wait(lck, [this](){ return (LockMode::P == this->lock_mode);});
            this->lock_mode = LockMode::X;
            temp_count = this->count.fetch_add(1);
            assert(0 == temp_count);
            return RC::RCOK;
        }
        if(LockMode::S == mode) {
            this->cv.wait(lck, [this](){ return (LockMode::X != this->lock_mode); });
            if(LockMode::P == this->lock_mode) {
                temp_count = this->count.fetch_add(1);
                assert(0 == temp_count);
                this->lock_mode = LockMode::S;
            } else if(LockMode::S == this->lock_mode) {
                temp_count = this->count.fetch_add(1);
                assert(temp_count > 0);
            } else { assert(false); }
            return RC::RCOK;
        }
            // 只能读（S）或者写（X）
        else { assert(false); }
    }

    RC LockNode::UnLock() {
        std::unique_lock<std::mutex> lck(this->mtx);

        int temp_count;
        if (LockMode::X == this->lock_mode) {
            temp_count = this->count.fetch_sub(1);
            assert(temp_count == 1);
            this->lock_mode = LockMode::P;
            this->cv.notify_all();
            return RC::RCOK;
        }
        /* */
        if (LockMode::S == this->lock_mode) {
            // 仅非 LockMode::O 才需要更改锁表项状态
            temp_count = this->count.fetch_sub(1);
            assert(temp_count > 0);
            if (0 == this->count) { this->lock_mode = LockMode::P; }
            this->cv.notify_all();
            return RC::RCOK;
        }
        if (LockMode::P == this->lock_mode) { return RC::RCOK; }
        assert(false);
    }

    LockTable::LockTable(TABLES table, int instance_id, uint64_t start_id, uint64_t end_id, ManagerInstance* manager_instance)
            : manager_instance_(manager_instance), table_(table), instance_id_(instance_id), size_(end_id - start_id) {
        this->lock_node_local_.resize(size_, false);
        shared_ptr<LockNode> p;
        for (uint64_t key = start_id; key < end_id; key++) {
            lock_table_[key] =  make_pair(p, false);
        }
    }


    RC LockTable::Lock(uint64_t item_id, LockMode mode) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter || iter->second.first.expired()) { assert(false); return RC::Abort; }

        /// 事务执行中才会使用 Lock，本地应该存在该 item 的读写权限，即 lock_mode 为 非 O，
        /// 否则事务应该在执行前就回滚了
        if(!IsValid(item_id)) { assert(false); return RC::Abort; }

        shared_ptr<LockNode> lock_node = iter->second.first.lock();
        return lock_node->Lock(mode);
    }

    RC LockTable::UnLock(uint64_t item_id) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter || iter->second.first.expired()) { assert(false); return RC::Abort; }

        /// 事务执行中才会使用 UnLock，本地应该存在该 item 的读写权限，即 lock_mode 为 非 O，
        /// 否则事务应该在执行前就回滚了
        if(!IsValid(item_id)) { assert(false); return RC::Abort; }

        shared_ptr<LockNode> lock_node = iter->second.first.lock();
        return lock_node->UnLock();
    }

    bool LockTable::AddThread(uint64_t item_id, uint64_t thd_id) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter || iter->second.first.expired()) { assert(false); return false; }

        shared_ptr<LockNode> lock_node = iter->second.first.lock();
        return lock_node->AddThread(thd_id);
    }
    bool LockTable::RemoveThread(uint64_t item_id, uint64_t thd_id) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter || iter->second.first.expired()) { assert(false); return false; }

        shared_ptr<LockNode> lock_node = iter->second.first.lock();
        return lock_node->RemoveThread(thd_id);
    }


    RC LockTable::RemoteInvalid(uint64_t item_id, char *buf, size_t count) {
        auto iter = lock_table_.find(item_id);
        if(lock_table_.end() == iter) { assert(false); return RC::Abort; }

        /// 本地 mode 为 O, 失效错误
//        if(!lock_node_local_.test(item_id)) { assert(false); return RC::Abort; }

        shared_ptr<LockNode> lock_node = GetOrCreateSharedPtr<LockNode>(lock_table_, item_id);

        RC rc;
        lock_node->invalid_req = true;
        std::unique_lock<std::mutex> lck(lock_node->mtx);
        if(lock_node->cv.wait_for(lck, chrono::milliseconds(10), [lock_node](){ return (lock_node->thread_set.empty()); }))
        {
            assert(lock_node->thread_set.empty());
            assert(lock_node->count == 0);
            Invalid(item_id);
            row_t* new_row;
//#if defined(B_M_L_R) || defined(B_M_L_P)
//            manager_instance_->record_buffer_->RecordBufferGet(table_, item_id, new_row);
//            delete new_row;
//#endif
//#if defined(B_P_L_P) || defined(B_P_L_R)
//            manager_instance_->record_buffer_->PageBufferGet(table_, item_id, new_row);
//            delete new_row;
//#endif
            manager_instance_->record_buffer_->RecordBufferGet(table_, item_id, new_row);
            assert(count == new_row->get_tuple_size());
            memcpy(buf, new_row->data, new_row->get_tuple_size());
            delete new_row;
            rc = RC::RCOK;
        } else {
            rc = RC::TIME_OUT;
        }
        lock_node->invalid_req = false;
        return rc;
    }
}