//
// Created by rrzhang on 2020/6/2.
//

#ifndef DBX1000_LOCK_TABLE_H
#define DBX1000_LOCK_TABLE_H

#include <unordered_map>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <set>

namespace dbx1000 {
    enum class LockMode{
        O,  /// 失效
        P,  /// 独占
        S,  /// 读锁
        X,  /// 写锁
    };

    struct LockNode {
        LockNode(int instanceid);
        int instance_id;
        LockMode lock_mode;
        int count;                      // 正在获取该 locknode 的调用数
        std::atomic_int thread_count;   // 本地对该 locknode 有写意向的线程数
        bool invalid_req;
        bool lock_remoting;
        std::mutex mtx;
        std::condition_variable cv;
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
        bool AddThread(uint64_t page_id, uint64_t thd_id);
        bool RemoveThread(uint64_t page_id, uint64_t thd_id);

        bool LockInvalid(uint64_t page_id, char *buf, size_t count);

        char LockModeToChar(LockMode mode);
        void Print();




        /// getter and setter
        std::unordered_map<uint64_t, LockNode*> &lock_table()        { return this->lock_table_; }
        ManagerInstance* manager_instance()                         { return this->manager_instance_; }
        void set_manager_instance(ManagerInstance* managerInstance) {this->manager_instance_ = managerInstance; }

    private:
        std::unordered_map<uint64_t, LockNode*> lock_table_;
        ManagerInstance* manager_instance_;
    };
}


#endif //DBX1000_LOCK_TABLE_H
