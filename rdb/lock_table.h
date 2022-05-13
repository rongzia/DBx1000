#ifndef LOCK_TABLE_H
#define LOCK_TABLE_H
#include <shared_mutex>
#include <mutex>
#include <tbb/concurrent_hash_map.h>
#include "global.h"
#include "helper.h"
#include "wl.h"

namespace rdb {
    using page_id_t = uint64_t;
    using key_id_t = uint64_t;
    using lock_table_key_t = std::pair<TABLES, uint64_t>;

    enum class lock_mode { P, S, X, O };
    enum class granularity { PAGE, RECORD };
    enum class remote_lock_flag { Undefined, Nothing, Processing, Succ, Abort };
    std::string remote_lock_flag_2_string(remote_lock_flag flag);
    class LockItemBase {
    public:
        LockItemBase() {
            latch_ = false;
            valid_latch_ = false;
            pin_ = false;
            remote_lock_flag_ = remote_lock_flag::Nothing;
        }
        virtual ~LockItemBase() {};

        remote_lock_flag AddTxn(txn_man* txn);
        void ClearTxn(txn_man* txn);

        // std::shared_mutex sh_mutex_;
        volatile bool latch_;
        granularity gran_;
        std::set<txn_man*> txn_set_;
        // 被 valid 线程设置和清空，
        std::atomic<bool> valid_latch_;
        remote_lock_flag remote_lock_flag_;

        volatile bool pin_;

    // protected:
        lock_mode lock_mode_;
    };
    class RecordLockItem : public LockItemBase {
    public:
        RecordLockItem(index_item* idx_item, LockItemBase* page) : LockItemBase::LockItemBase() {
            this->lock_mode_ = lock_mode::P;
            this->gran_ = granularity::RECORD;
            this->idx_item_ = idx_item;
            this->parent_page_ = page;
        }
        ~RecordLockItem() {};

        index_item* idx_item_;
        LockItemBase* parent_page_;
    };

    class PageLockItem : public LockItemBase {
    public:
        PageLockItem(page_id_t page_id) : LockItemBase::LockItemBase() {
            this->lock_mode_ = lock_mode::O;
            this->gran_ = granularity::PAGE;
            this->page_id_ = page_id;
        }
        ~PageLockItem();
        
        void AddRow(index_item* idx_item);

        page_id_t page_id_;
        tbb::concurrent_hash_map<key_id_t, RecordLockItem*> row_map_;

        using ConcurrentMap = tbb::concurrent_hash_map<key_id_t, RecordLockItem*>;
        using accessor = typename ConcurrentMap::accessor;
        using const_accessor = typename ConcurrentMap::const_accessor;
        using value_type = tbb::concurrent_hash_map<key_id_t, RecordLockItem*>::value_type;
        using iterator = tbb::concurrent_hash_map<key_id_t, RecordLockItem*>::iterator;
        using const_reference = tbb::concurrent_hash_map<key_id_t, RecordLockItem*>::const_reference;
    };

    class LockTable {
    public:
        using ConcurrentMap = tbb::concurrent_hash_map<lock_table_key_t, PageLockItem*>;
        using accessor = typename ConcurrentMap::accessor;
        using const_accessor = typename ConcurrentMap::const_accessor;
        using value_type = ConcurrentMap::value_type;
        using iterator = ConcurrentMap::iterator;
        using const_reference = ConcurrentMap::const_reference;

        ~LockTable();
        void init();
        void init_parallel(uint32_t thd_id);
        void check();

        ConcurrentMap lock_table_map_;
        workload* wl_;
    };
}


#endif // LOCK_TABLE_H