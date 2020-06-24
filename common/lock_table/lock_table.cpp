//
// Created by rrzhang on 2020/6/2.
//
#include <cassert>
#include <iostream>
#include "lock_table.h"

#include "common/buffer/buffer.h"
#include "instance/manager_instance.h"
#include "rpc_handler/instance_handler.h"
#include "config.h"

namespace dbx1000 {
    LockNode::LockNode(int instanceid) {
        this->instance_id = instanceid;
//        this->lock_mode = LockMode::P;
        this->lock_mode = LockMode::O;
        this->count = ATOMIC_VAR_INIT(0);
        this->thread_count = ATOMIC_VAR_INIT(0);
        this->invalid_req = false;
        this->lock_remoting = false;
    }
    LockTable::~LockTable() {
        for(auto &iter : lock_table_){
            delete iter.second;
        }
    }
    LockTable::LockTable() {}

    void LockTable::Init(uint64_t start_page, uint64_t end_page, int instance_id) {
        for (uint64_t page_id = start_page; page_id < end_page; page_id++) {
            LockNode* lock_node = new LockNode(instance_id);
            lock_table_.insert(std::pair<uint64_t, LockNode*>(page_id, lock_node));
        }
    }

/*
    bool LockTable::Lock(uint64_t page_id, LockMode mode) {
        auto iter = lock_table_.find(page_id);
        if (lock_table_.end() == iter) {
            assert(false);
            return false;
        }
        std::unique_lock <std::mutex> lck(iter->second->mtx);
        if(LockMode::X == mode){
            while(iter->second->lock_mode != LockMode::P){
                iter->second->cv.wait(lck);
            }
            assert(iter->second->count == 0);
            iter->second->lock_mode = LockMode::X;
            iter->second->count++;
            return true;
        }
        if(LockMode::S == mode) {
            while(iter->second->lock_mode == LockMode::X){
                iter->second->cv.wait(lck);
            }
            assert(iter->second->count >=  0);
            iter->second->lock_mode = LockMode::S;
            iter->second->count++;
            return true;
        }
        assert(false);
    }*/


    bool LockTable::Lock(uint64_t page_id, LockMode mode) {
        auto iter = lock_table_.find(page_id);
        if (lock_table_.end() == iter) { assert(false); }

        std::unique_lock<std::mutex> lck(iter->second->mtx);
        bool rc = false;
        if(LockMode::X == mode) {
            /*
            // Test_Lock_Table 中 30 个线程大概需要 1500 - 1600 微秒
//            if(iter->second->cv.wait_for(lck, chrono::microseconds (1500), [iter](){ return (LockMode::P == iter->second->lock_mode); }) {
            if(iter->second->cv.wait_for(lck, chrono::milliseconds(1000), [iter](){ return (LockMode::P == iter->second->lock_mode); })) {
                assert(iter->second->count == 0);
                iter->second->lock_mode = LockMode::X;
                iter->second->count++;
                rc = true;
            } else {
                assert(false);
                rc = false;
            }
            return rc; */
            iter->second->cv.wait(lck, [iter](){ return (LockMode::P == iter->second->lock_mode);});
            iter->second->lock_mode = LockMode::X;
            assert(0 == iter->second->count.fetch_add(1));
            rc = true;
//            cout << "  lock : " << LockModeToChar(mode) << ", page id : " << page_id << endl;
            return rc;
        }
        if(LockMode::S == mode) {
            iter->second->cv.wait(lck, [iter](){ return (LockMode::X != iter->second->lock_mode); });
            if(LockMode::O == iter->second->lock_mode) { }
            else if(LockMode::P == iter->second->lock_mode) {
                assert(0 == iter->second->count.fetch_add(1));
                iter->second->lock_mode = LockMode::S;
            } else if(LockMode::S == iter->second->lock_mode) {
                assert(iter->second->count.fetch_add(1) > 0);
                assert(iter->second->lock_mode == LockMode::S);
            } else { assert(false); }
//            cout << "  lock : " << LockModeToChar(mode) << ", page id : " << page_id << endl;
            rc = true;
            return true;
        }
        if(mode == LockMode::P || mode == LockMode::O) { assert(false); }
    }


    bool LockTable::UnLock(uint64_t page_id) {
        auto iter = lock_table_.find(page_id);
        if (lock_table_.end() == iter) { assert(false); }

        std::unique_lock<std::mutex> lck(iter->second->mtx);
        if (LockMode::X == iter->second->lock_mode) {
            assert(iter->second->count.fetch_sub(1) == 1);
            iter->second->lock_mode = LockMode::P;
//        cout << "lock : " << LockModeToChar(LockMode::X) << ", page id : " << page_id << endl;
            iter->second->cv.notify_all();
            return true;
        }
        /* */
        if (LockMode::S == iter->second->lock_mode) {
            // 仅非 LockMode::O 才需要更改锁表项状态
            assert(iter->second->count.fetch_sub(1) > 0);
            if (0 == iter->second->count) {
                iter->second->lock_mode = LockMode::P;
            }
//        cout << "unlock : " << LockModeToChar(LockMode::S) << ", page id : " << page_id << endl;
            iter->second->cv.notify_all();
            return true;
        }
        if (LockMode::O == iter->second->lock_mode || LockMode::P == iter->second->lock_mode) { return true; }
        assert(false);
    }

    bool LockTable::AddThread(uint64_t page_id, uint64_t thd_id){
        auto iter = lock_table_.find(page_id);
        if (lock_table_.end() == iter) { assert(false); }

        assert(lock_table_[page_id]->lock_mode != LockMode::O);
        lock_table_[page_id]->thread_count.fetch_add(1);
        return true;
    }
    bool LockTable::RemoveThread(uint64_t page_id, uint64_t thd_id){
        auto iter = lock_table_.find(page_id);
        if (lock_table_.end() == iter) { assert(false); }

        assert(iter->second->lock_mode != LockMode::O);
//        std::unique_lock<std::mutex> lck(iter->second->mtx);
        assert(iter->second->thread_count.fetch_sub(1) > 0);
//        std::notify_all_at_thread_exit(iter->second->cv, std::move(lck));
        iter->second->cv.notify_all();
        return true;
    }


    bool LockTable::LockInvalid(uint64_t page_id, char *page_buf, size_t count){
        auto iter = lock_table_.find(page_id);
        if (lock_table_.end() == iter) { assert(false); }
        iter->second->invalid_req = true;
        std::unique_lock<std::mutex> lck(iter->second->mtx);
        iter->second->cv.wait(lck, [iter](){ return (iter->second->thread_count == 0); });
        assert(iter->second->thread_count == 0);
        assert(iter->second->count == 0);
        iter->second->lock_mode = LockMode::O;
        manager_instance_->buffer()->BufferGet(page_id, page_buf, count);
        iter->second->invalid_req = false;
//        std::notify_all_at_thread_exit(iter->second->cv, std::move(lck));
//        iter->second->cv.notify_all();
        return true;
    }

    char LockTable::LockModeToChar(LockMode mode) {
        char a;
        if (mode == LockMode::X) { a = 'X'; }
        else if (mode == LockMode::S) { a = 'S'; }
        else if (mode == LockMode::P) { a = 'P'; }
        else if (mode == LockMode::O) { a = 'O'; }
        return a;
    }

    void LockTable::Print(){
        for(auto iter : this->lock_table_) {
            cout << "page_id : " << iter.first << ", lock mode : " << LockModeToChar(iter.second->lock_mode) << endl;
        }
    }
}