//
// Created by rrzhang on 2020/7/13.
//

#include "row_handler.h"

#include "common/buffer/record_buffer.h"
#include "common/index/index.h"
#include "common/lock_table/lock_table.h"
#include "common/storage/tablespace/page.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/catalog.h"
#include "common/storage/row.h"
#include "common/storage/table.h"
#include "common/workload/ycsb.h"
#include "common/myhelper.h"
#include "common/mystats.h"
#include "json/json.h"
#include "instance/benchmarks/ycsb_query.h"
#include "instance/concurrency_control/row_mvcc.h"
#include "instance/txn/ycsb_txn.h"
#include "instance/manager_instance.h"
#include "instance/thread.h"
#include "global_lock_service.h"
#include "shared_disk_service.h"
#include "config.h"

namespace dbx1000 {

    RowHandler::RowHandler(ManagerInstance *managerInstance) : manager_instance_(managerInstance){}

    /**
     * GetRow 和 ReturnRow 在原本 dbx1000 里是属于 row_t 的函数，但是采用刷盘机制，row_t 类被去掉了
     * @param key
     * @param type
     * @param txn
     * @param row
     * @return
     */
    RC RowHandler::GetRow(TABLES table, uint64_t key, access_t type, txn_man *txn, row_t *&row) {
        RC rc = RC::RCOK;
        uint64_t thd_id = txn->get_thd_id();
        TsType ts_type = (type == RD) ? R_REQ : P_REQ;
//        rc = this->mvcc_map_[key]->access(txn, ts_type, row);
        // rc = this->manager_instance_->mvcc_array()[key]->access(txn, ts_type, row);
        rc = txn->mvcc_maps_[table][key]->access(txn, ts_type, row);
        if (rc == RC::RCOK) {
            row = txn->cur_row;
        } else if (rc == RC::WAIT) {
            dbx1000::Profiler profiler;
            profiler.Start();
            while (!txn->ts_ready) { PAUSE }
            profiler.End();
            this->manager_instance_->stats().tmp_stats[thd_id]->time_wait += profiler.Nanos();
            txn->ts_ready = true;
            row = txn->cur_row;
        }
        return rc;
    }

    void RowHandler::ReturnRow(TABLES table, uint64_t key, access_t type, txn_man *txn, row_t *row) {
        RC rc = RC::RCOK;
#if CC_ALG == MVCC
        if (type == XP) {
//            this->manager_instance_->mvcc_map_[key]->access(txn, XP_REQ, row);
            // this->manager_instance_->mvcc_array()[key]->access(txn, XP_REQ, row);
            txn->mvcc_maps_[table][key]->access(txn, XP_REQ, row);
        } else if (type == WR) {
            assert(type == WR && row != nullptr);
//            this->manager_instance_->mvcc_map_[key]->access(txn, W_REQ, row);
            // this->manager_instance_->mvcc_array()[key]->access(txn, W_REQ, row);
            txn->mvcc_maps_[table][key]->access(txn, W_REQ, row);

            assert(rc == RC::RCOK);
        }
#endif
    }

/**
 * 读一行数据到 DB（这里 DB 是缓存+磁盘）
 * @param row
 * @return
 */
    RC RowHandler::ReadRow(TABLES table, uint64_t key, row_t * row, size_t size) {
        RC rc = RC::RCOK;
#if defined(B_P_L_R) || defined(B_P_L_P)
        assert(size == row->get_tuple_size());
        dbx1000::IndexItem indexItem;
        manager_instance_->m_workload_->indexes_[table]->IndexGet(key, &indexItem);
        dbx1000::Page* page = new dbx1000::Page(new char[MY_PAGE_SIZE]);
        rc = manager_instance_->m_workload_->buffers_[table]->BufferGet(indexItem.page_id_, page->page_buf(), MY_PAGE_SIZE);
        if(rc == RC::Abort) { assert(false); }
        page->Deserialize();
        if(page->page_id() != indexItem.page_id_) {
            //cout << page->page_id() << " + " << indexItem.page_id_ << endl;
            //page->Print();
            page->set_page_id(indexItem.page_id_);
            // indexItem.page_id_ = page->page_id();
            //cout << page->page_id() << " : " << indexItem.page_id_ << endl;
        }
        assert(page->page_id() == indexItem.page_id_);
        memcpy(row->data, &page->page_buf()[indexItem.page_location_], size);
        //cout<<"1"<<endl;
        delete page;
#elif defined(B_M_L_R) || defined(B_R_L_R)
        assert(size == row->get_tuple_size());
        rc = manager_instance_->m_workload_->buffers_[table]->BufferGet(key, row->data, size);
        assert(rc == RC::RCOK);
#else
        assert(false);
#endif
        return rc;
    }

/**
 * 写一行数据到 DB（这里 DB 是缓存+磁盘）
 * @param row
 * @return
 */
    RC RowHandler::WriteRow(TABLES table, uint64_t key, row_t * row, size_t size) {
        RC rc = RC::RCOK;
#if defined(B_P_L_R) || defined(B_P_L_P)
        assert(size == row->get_tuple_size());
        dbx1000::IndexItem indexItem;
        manager_instance_->m_workload_->indexes_[table]->IndexGet(key, &indexItem);
        assert(manager_instance_->lock_table_[table]->lock_table_[indexItem.page_id_]->lock_mode != LockMode::O);
        dbx1000::Page* page = new dbx1000::Page(new char[MY_PAGE_SIZE]);
        manager_instance_->m_workload_->buffers_[table]->BufferGet(indexItem.page_id_, page->page_buf(), MY_PAGE_SIZE);
        if(rc == RC::Abort) { assert(false); }
        page->Deserialize();
        if(page->page_id() != indexItem.page_id_)
        {
            page->set_page_id(indexItem.page_id_);
            // indexItem.page_id_ = page->page_id();
        }
        assert(page->page_id() == indexItem.page_id_);
        memcpy(&page->page_buf()[indexItem.page_location_], row->data, size);
        page->Serialize();
        rc = manager_instance_->m_workload_->buffers_[table]->BufferPut(page->page_id(), page->page_buf(), MY_PAGE_SIZE);
#ifdef DB2
        dbx1000::Profiler profiler; profiler.Start();
        rc = manager_instance_->global_lock_service_client_->Unlock(manager_instance_->instance_id_, table, page->page_id(), dbx1000::LockMode::O, page->page_buf(), MY_PAGE_SIZE);
        profiler.End();
        manager_instance_->stats_.total_time_Unlock_.fetch_add(profiler.Nanos());
        manager_instance_->stats_.total_count_Unlock_.fetch_add(1);
        if(RC::RCOK == rc) { manager_instance_->lock_table_[table]->lock_table_[page->page_id()]->lock_mode = dbx1000::LockMode::O; }
#endif // DB2
        delete page;
#elif defined(B_M_L_R) || defined(B_R_L_R)
        assert(size == row->get_tuple_size());
        rc = manager_instance_->m_workload_->buffers_[table]->BufferPut(key, row->data, size);
#else
        assert(false);
#endif
        return rc;
    }
}