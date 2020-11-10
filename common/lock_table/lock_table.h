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
#include <boost/dynamic_bitset.hpp>

#include "common/global.h"
#include "common/myhelper.h"

namespace dbx1000 {
    class ManagerInstance;

    enum class LockMode{
//        O,  /// 失效
        P,  /// 独占
        S,  /// 读锁
        X,  /// 写锁
    };

    class LockNode{
    public:
        LockNode() = default;
        void Init(int instanceid, LockMode mode);
        bool AddThread(uint64_t thd_id);
        bool RemoveThread(uint64_t thd_id);
        RC Lock(LockMode mode);
        RC UnLock();

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


    class LockTable {
    public:
        ~LockTable() = default;
        LockTable(TABLES, int instance_id, uint64_t start_id, uint64_t end_id, ManagerInstance* manager_instance);
        RC Lock(uint64_t item_id, LockMode mode);
        RC UnLock(uint64_t page_id);
        bool AddThread(uint64_t item_id, uint64_t thd_id);
        bool RemoveThread(uint64_t item_id, uint64_t thd_id);
        RC RemoteInvalid(uint64_t item_id, char *buf, size_t count);

        void Valid  (uint64_t item_id) { this->lock_node_local_.set(item_id); }
        void Invalid(uint64_t item_id) { this->lock_node_local_.reset(item_id); }
        bool IsValid(uint64_t item_id) { return lock_node_local_.test(item_id); }

        ManagerInstance* manager_instance_;
//    private:
        TABLES table_;
        int instance_id_;
        std::size_t size_;
        std::unordered_map<uint64_t, std::pair<weak_ptr<LockNode>, volatile bool>> lock_table_;
        boost::dynamic_bitset<> lock_node_local_;
    };
}



#endif //DBX1000_LOCK_TABLE2_HPP
