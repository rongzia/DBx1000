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
        const BufferPool::PageKey pagekey = std::make_pair(table, indexItem.page_id_);
        const BufferPool::PageHandle* handle = manager_instance_->m_workload_->buffer_pool_.Get(pagekey);
        #ifdef CLREAR_BUF
        if(handle == NULL || handle == nullptr) {
            handle = manager_instance_->m_workload_->buffer_pool_.PutIfNotExist(pagekey);
        }
        #endif // CLREAR_BUF
        assert(handle);
        // handle->value.Deserialize();
        // assert(handle->value.page_id() == indexItem.page_id_);
        memcpy(row->data, &handle->value.page_buf_read()[indexItem.page_location_], size);
        manager_instance_->m_workload_->buffer_pool_.Release(handle);
#elif defined(B_M_L_R) || defined(B_R_L_R)
        assert(size == row->get_tuple_size());
        const RowBufferPool::RowKey rowkey = std::make_pair(table, key);
        const RowBufferPool::RowHandle* handle = manager_instance_->m_workload_->buffer_pool_.Get(rowkey);
        #ifdef CLREAR_BUF
        if(handle == NULL || handle == nullptr) {
                handle = manager_instance_->m_workload_->buffer_pool_.PutIfNotExist(rowkey, manager_instance_->m_workload_->tables_[table]);
        }
        #endif // CLREAR_BUF
        assert(handle);
        memcpy(row->data, handle->value.data, size);
        manager_instance_->m_workload_->buffer_pool_.Release(handle);
#else  // B_L
        assert(false);
#endif // B_L
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
        // 获取写锁
        // std::shared_ptr<dbx1000::LockNode> lockNode = manager_instance_->lock_table_[table]->lock_table_[indexItem.page_id_];
        // if(lockNode->lock_mode == LockMode::O) { cout << "page id: " << indexItem.page_id_ << endl; 
        //         lockNode->lock_mode = LockMode::P;
        // }
        // assert(lockNode->lock_mode != LockMode::O);
        // manager_instance_->lock_table_[table]->Lock(indexItem.page_id_, dbx1000::LockMode::X);

        auto pagekey = std::make_pair(table, indexItem.page_id_);
        const BufferPool::PageHandle* handle_read = manager_instance_->m_workload_->buffer_pool_.Get(pagekey);
        #ifdef CLREAR_BUF
        if(handle_read == NULL || handle_read == nullptr) {
            handle_read = manager_instance_->m_workload_->buffer_pool_.PutIfNotExist(pagekey);
        }
        #endif // CLREAR_BUF
        if(!handle_read) {cout << "page id: " << indexItem.page_id_ << endl;}
        assert(handle_read);
        // assert(indexItem.page_id_ == handle_read->value.page_id());

        const BufferPool::PageHandle* handle_write = manager_instance_->m_workload_->buffer_pool_.Put(pagekey, Page(handle_read->value.page_buf_read()));
        manager_instance_->m_workload_->buffer_pool_.Release(handle_read);

        // manager_instance_->lock_table_[table]->UnLock(indexItem.page_id_);
#ifdef DB2
        dbx1000::Profiler profiler; profiler.Start();
        rc = manager_instance_->global_lock_service_client_->Unlock(manager_instance_->instance_id_, table, handle_write->value->page_id(), dbx1000::LockMode::O, handle_write->value->page_buf(), MY_PAGE_SIZE);
        profiler.End();
        manager_instance_->stats_.total_time_Unlock_.fetch_add(profiler.Nanos());
        manager_instance_->stats_.total_count_Unlock_.fetch_add(1);
        if(RC::RCOK == rc) { manager_instance_->lock_table_[table]->lock_table_[handle_write->value->page_id()]->lock_mode = dbx1000::LockMode::O; }
        manager_instance_->m_workload_->buffer_pool_.Release(handle_write);
#endif // DB2
        // delete page;
#elif defined(B_M_L_R) || defined(B_R_L_R)
        assert(size == row->get_tuple_size());
        // 获取写锁
        std::shared_ptr<dbx1000::LockNode> lockNode = manager_instance_->lock_table_[table]->lock_table_[key];
        // if(lockNode->lock_mode == LockMode::O) { cout << "row id: " << key << endl; }
        // assert(lockNode->lock_mode != LockMode::O);
        // manager_instance_->lock_table_[table]->Lock(key, dbx1000::LockMode::X);

        auto rowkey = std::make_pair(table, key);
        const RowBufferPool::RowHandle* handle_read = manager_instance_->m_workload_->buffer_pool_.Get(rowkey);
        #ifdef CLREAR_BUF
        if(handle_read == NULL || handle_read == nullptr) {
            handle_read = manager_instance_->m_workload_->buffer_pool_.PutIfNotExist(rowkey, manager_instance_->m_workload_->tables_[table]);
        }
        #endif // CLREAR_BUF
        if(!handle_read) {cout << "row id: " << key << endl;}
        assert(handle_read);

        const RowBufferPool::RowHandle* handle_write = manager_instance_->m_workload_->buffer_pool_.Put(rowkey, row_t(*row));
        manager_instance_->m_workload_->buffer_pool_.Release(handle_read);
        manager_instance_->m_workload_->buffer_pool_.Release(handle_write);

        // manager_instance_->lock_table_[table]->UnLock(key);
#else  // B_L
        assert(false);
#endif // B_L
        return rc;
    }
}