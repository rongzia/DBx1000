
#include <algorithm>
#include <vector>
#include <thread>
#include "lock_table.h"
#include "catalog.h"
#include "table.h"
#include "row.h"
#include "ycsb.h"
#include "index_map_hash.h"

namespace rdb {
    PageLockItem::~PageLockItem() {
        for_each(row_map_.begin(), row_map_.end(), [this](const_reference iter) -> void {delete iter.second;});
    }

    LockTable::~LockTable() {
        for_each(lock_table_map_.begin(), lock_table_map_.end(), [this](const_reference iter) -> void {delete iter.second;});
    }
    
    void LockItemBase::ClearTxn(txn_man* txn) {
        while (!__sync_bool_compare_and_swap(&latch_, false, true)) { PAUSE }
        bool res = txn_set_.erase(txn);
        assert(res);
        
        if (0 == txn_set_.size()) {
            if (remote_lock_flag_ == remote_lock_flag::Processing) {
                // cout << __FILE__ << ":" << __LINE__ << ", " << page_id_ << endl;
            }
            assert(remote_lock_flag_ != remote_lock_flag::Processing);
            remote_lock_flag_ = remote_lock_flag::Nothing;
        }

        latch_ = false;
    }

    remote_lock_flag LockItemBase::AddTxn(txn_man* txn) {
        while (!valid_latch_) {
            while (!__sync_bool_compare_and_swap(&latch_, false, true)) { PAUSE }
            remote_lock_flag flag = remote_lock_flag_;
            if (valid_latch_) { latch_ = false; continue; }

            txn_set_.insert(txn);
            if (1 == txn_set_.size()) assert(remote_lock_flag_ == remote_lock_flag::Nothing);
            if (remote_lock_flag_ == remote_lock_flag::Nothing) {
                remote_lock_flag_ = remote_lock_flag::Processing;
                assert(1 == txn_set_.size());
            }

            latch_ = false;
            return flag;
        }
    }

#ifdef YCSB
    void LockTable::init() {

        std::vector<std::thread> v_thread;
        for (uint32_t thd = 0; thd < g_init_parallelism; thd++) {
            v_thread.emplace_back(&LockTable::init_parallel, this, thd);
        }

        for (uint32_t thd = 0; thd < g_init_parallelism; thd++) {
            v_thread[thd].join();
        }
        // check();
    }

    void LockTable::init_parallel(uint32_t thd_id) {

        // some check
        ycsb_wl* wl = (ycsb_wl*)wl_;
        wl->check();

        uint64_t page_id = thd_id * wl->pages_per_thd_;
        PageLockItem* page_lock_item = nullptr;
        index_item* idx_item = nullptr;

        wl->the_index->index_read(thd_id * wl->rows_per_thd_, idx_item);
        assert(page_id == idx_item->page_id_);
        page_lock_item = new PageLockItem(idx_item->page_id_);
        this->lock_table_map_.insert(value_type(std::make_pair(TABLES::MAIN_TABLE, idx_item->page_id_), page_lock_item));


        for (uint64_t i = 0; i < wl->rows_per_thd_; i++) {
            assert(idx_item);
            assert(idx_item->row_->get_primary_key() / 40 == idx_item->page_id_);

            if (page_id != idx_item->page_id_) {
                page_id++;
                page_lock_item = new PageLockItem(idx_item->page_id_);
                this->lock_table_map_.insert(value_type(std::make_pair(TABLES::MAIN_TABLE, idx_item->page_id_), page_lock_item));
            }

            page_lock_item->AddRow(idx_item);
            // /* rr::debug */ if (idx_item->row_->get_primary_key() <= 100) { cout << "page id: " << idx_item->page_id_ << ", " << "row id: " << idx_item->row_->get_primary_key() << endl; }

            idx_item = idx_item->next;
        }
    }

    void PageLockItem::AddRow(index_item* idx_item) {
        this->row_map_.insert(value_type(idx_item->row_->get_primary_key(), new RecordLockItem(idx_item, this)));
    }

    void LockTable::check() {
        // some check
        ycsb_wl* wl = (ycsb_wl*)wl_;
        wl->check();

        for (uint64_t page_id = 0; page_id < lock_table_map_.size(); page_id++) {
            const_accessor page_acc;
            bool find_page = lock_table_map_.find(page_acc, make_pair(TABLES::MAIN_TABLE, page_id));
            assert(find_page);
            for (uint64_t row_id = page_id * 40; row_id < (page_id + 1) * 40; row_id++) {
                PageLockItem::const_accessor row_acc;
                bool find_row = page_acc->second->row_map_.find(row_acc, row_id);
                assert(find_row);
            }
        }
    }
#endif // YCSB



    std::string remote_lock_flag_2_string(remote_lock_flag flag) {
        if (remote_lock_flag::Undefined == flag) return std::string("Undefined");
        if (remote_lock_flag::Nothing == flag) return std::string("Nothing");
        if (remote_lock_flag::Processing == flag) return std::string("Processing");
        if (remote_lock_flag::Succ == flag) return std::string("Succ");
        if (remote_lock_flag::Abort == flag) return std::string("Abort");
        else { assert(false); }
    }
} // rdb