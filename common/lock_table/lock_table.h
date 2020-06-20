//
// Created by rrzhang on 2020/6/2.
//

#ifndef DBX1000_LOCK_TABLE_H
#define DBX1000_LOCK_TABLE_H

#include <unordered_map>
#include <atomic>
#include <mutex>
#include <condition_variable>

namespace dbx1000 {
    enum class LockMode{
        O,  /// 失效
        P,  /// 独占
        S,  /// 读锁
        X,  /// 写锁
    };

    struct LockNode {
        LockNode(int instanceid, bool val = false, LockMode mode = LockMode::O, int count = 0);
        int instance_id;
        int count;
//        bool valid;
        LockMode lock_mode;
//        std::atomic_flag lock;
        std::mutex mtx;
        std::condition_variable cv;
        bool invalid_req;
    };

    class ManagerInstance;
    class InstanceClient;
    class LockTable {
    public:
        ~LockTable();
        LockTable();
        void Init(uint64_t start_page, uint64_t end_page, int instance_id);
        bool Lock(uint64_t page_id, LockMode mode);
        bool UnLock(uint64_t page_id);

//        bool CanRead(std::unordered_map<uint64_t, LockNode*>::iterator iter);
//        bool CanWrite(std::unordered_map<uint64_t, LockNode*>::iterator iter);

        bool LockInvalid(uint64_t page_id);

        char LockModeToChar(LockMode mode);
        void Print();

        /// getter and setter
        std::unordered_map<uint64_t, LockNode*> lock_table() { return this->lock_table_; }
        ManagerInstance* manager_instance() { return this->manager_instance_; }

    private:
        std::unordered_map<uint64_t, LockNode*> lock_table_;
        ManagerInstance* manager_instance_;
    };
}


#endif //DBX1000_LOCK_TABLE_H
