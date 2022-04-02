#ifndef LOCK_TABLE_H
#define LOCK_TABLE_H
#include <shared_mutex>
#include <mutex>
#include <tbb/concurrent_hash_map.h>
#include "global.h"
#include "helper.h"
#include "wl.h"

using page_id_t = uint64_t;
using key_id_t = uint64_t;

enum class lock_mode { P, R, W, O };

class LockItemBase {
public:

    LockItemBase() {
        this->lck_mode_ = lock_mode::P;
        latch_ = false;
    }
    virtual ~LockItemBase() {};

    volatile bool latch_;
    std::shared_mutex sh_mutex_;

protected:
    lock_mode lck_mode_;
};

class RecordLockItem : public LockItemBase {
public:
    RecordLockItem(index_item* idx_item) : LockItemBase::LockItemBase() {
        idx_item_ = idx_item;
    }
    ~RecordLockItem() {};

    index_item* idx_item_;
};

class PageLockItem : public LockItemBase {
public:
    PageLockItem(page_id_t page_id) : LockItemBase::LockItemBase() {
        page_id_ = page_id;
    }
    virtual ~PageLockItem();
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
    ~LockTable();
    void init();
    void init_parallel(uint32_t thd_id);
    void check();

    tbb::concurrent_hash_map<page_id_t, PageLockItem*> lock_table_map_;
    workload* wl_;

    using ConcurrentMap = tbb::concurrent_hash_map<page_id_t, PageLockItem*>;
    using accessor = typename ConcurrentMap::accessor;
    using const_accessor = typename ConcurrentMap::const_accessor;
    using value_type = tbb::concurrent_hash_map<page_id_t, PageLockItem*>::value_type;
    using iterator = tbb::concurrent_hash_map<page_id_t, PageLockItem*>::iterator;
    using const_reference = tbb::concurrent_hash_map<page_id_t, PageLockItem*>::const_reference;
};

#endif