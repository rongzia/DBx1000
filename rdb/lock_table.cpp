
#include <algorithm>
#include <vector>
#include <thread>
#include "lock_table.h"
#include "catalog.h"
#include "table.h"
#include "row.h"
#include "ycsb.h"
#include "index_map_hash.h"

PageLockItem::~PageLockItem() {
    for_each(row_map_.begin(), row_map_.end(), [this](const_reference iter) -> void {delete iter.second;});
}

LockTable::~LockTable() {
    for_each(lock_table_map_.begin(), lock_table_map_.end(), [this](const_reference iter) -> void {delete iter.second;});
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
    uint64_t rows_per_page = PAGE_SIZE / wl->the_table->get_schema()->get_tuple_size();
    uint64_t rows_per_thd = g_synth_table_size / g_init_parallelism;
    uint64_t pages_per_thd = rows_per_thd / rows_per_page;


    uint64_t page_id = thd_id * pages_per_thd;
    PageLockItem* page_lock_item = nullptr;
    index_item* idx_item = nullptr;

    wl->the_index->index_read(rows_per_thd * thd_id, idx_item);
    assert(page_id == idx_item->page_id_);
    page_lock_item = new PageLockItem(idx_item->page_id_);
    this->lock_table_map_.insert(value_type(idx_item->page_id_, page_lock_item));


    for (uint64_t i = 0; i < rows_per_thd; i++) {
        assert(idx_item);
        assert(idx_item->row_->get_primary_key() / 40 == idx_item->page_id_);

        if (page_id != idx_item->page_id_) {
            page_id++;
            page_lock_item = new PageLockItem(idx_item->page_id_);
            this->lock_table_map_.insert(value_type(idx_item->page_id_, page_lock_item));
        }

        page_lock_item->AddRow(idx_item);
        // /* rr::debug */ if (idx_item->row_->get_primary_key() <= 100) { cout << "page id: " << idx_item->page_id_ << ", " << "row id: " << idx_item->row_->get_primary_key() << endl; }

        idx_item = idx_item->next;
    }
}

void PageLockItem::AddRow(index_item* idx_item) {
    this->row_map_.insert(value_type(idx_item->row_->get_primary_key(), new RecordLockItem(idx_item)));
}

void LockTable::check() {
    // some check
    ycsb_wl* wl = (ycsb_wl*)wl_;
    wl->check();

    for (uint64_t page_id = 0; page_id < lock_table_map_.size(); page_id++) {
        const_accessor page_acc;
        bool find_page = lock_table_map_.find(page_acc, page_id);
        assert(find_page);
        for (uint64_t row_id = page_id * 40; row_id < (page_id + 1) * 40; row_id++) {
            PageLockItem::const_accessor row_acc;
            bool find_row = page_acc->second->row_map_.find(row_acc, row_id);
            assert(find_row);
        }
    }

}
#endif // YCSB