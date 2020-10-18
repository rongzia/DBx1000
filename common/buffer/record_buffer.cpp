//
// Created by rrzhang on 2020/10/11.
//

#include <cstring>
#include <thread>
#include <iostream>
#include "record_buffer.h"

#include "common/workload/wl.h"
#include "common/storage/row.h"
#include "common/storage/catalog.h"
#include "common/storage/table.h"
#include "util/profiler.h"

namespace dbx1000 {
    void RecordBuffer::Init(workload* workload) {
        this->workload_ = workload;
#if WORKLOAD == YCSB
        this->tables_[TABLES::MAIN_TABLE] = workload_->tables["MAIN_TABLE"];
#elif WORKLOAD == TPCC
        this->tables_[TABLES::WAREHOUSE] = workload_->tables["WAREHOUSE"];
        this->tables_[TABLES::DISTRICT] = workload_->tables["DISTRICT"];
        this->tables_[TABLES::CUSTOMER] = workload_->tables["CUSTOMER"];
        this->tables_[TABLES::HISTORY] = workload_->tables["HISTORY"];
        this->tables_[TABLES::NEW_ORDER] = workload_->tables["NEW-ORDER"];
        this->tables_[TABLES::ORDER] = workload_->tables["ORDER"];
        this->tables_[TABLES::ORDER_LINE] = workload_->tables["ORDER-LINE"];
        this->tables_[TABLES::ITEM] = workload_->tables["ITEM"];
        this->tables_[TABLES::STOCK] = workload_->tables["STOCK"];
#endif
    }
    RC RecordBuffer::RecordBufferGet(TABLES table, uint64_t item_id, row_t *&row) {
        row = new row_t();
        row->init(tables_[table]);
        row->set_primary_key(item_id);

        tbb::concurrent_hash_map<uint64_t, row_t*>::const_accessor const_acc;
        bool res = buffers_[table].find(const_acc, item_id);
        if(res) {
            memcpy(row->data, const_acc->second->data, tables_[table]->get_schema()->get_tuple_size());
            row->set_value(0, &item_id);
        } else {
            row->set_value(0, &item_id);
        }
        return RC::RCOK;
    }

    RC RecordBuffer::RecordBufferPut(TABLES table, uint64_t item_id, row_t* row) {
        std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> start = std::chrono::system_clock::now();
        tbb::concurrent_hash_map<uint64_t, row_t*>::accessor accessor;
        bool res = buffers_[table].find(accessor, item_id);

        if(res) {
            memcpy(accessor->second->data, row->data, tables_[table]->get_schema()->get_tuple_size());
            accessor->second->set_primary_key(item_id);
            accessor->second->set_value(0, &item_id);
        } else {
            row_t* new_row = new row_t();
            new_row->init(tables_[table]);
            new_row->set_primary_key(item_id);
            row->set_value(0, &item_id);
            memcpy(new_row->data, row->data, tables_[table]->get_schema()->get_tuple_size());
            buffers_[table].insert(accessor, make_pair(item_id, new_row));
        }
        std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> end = std::chrono::system_clock::now();
        uint64_t dura = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
#if defined(B_M_L_P) || defined(B_M_L_R)
        while(true){ if(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - start).count() > 0.5 * dura) { break; } }
#elif defined(B_P_L_P) || defined(B_P_L_R)
        while(true){ if(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - start).count() > 5 * dura) { break; } }
#endif

        return RC::RCOK;
    }

    RC RecordBuffer::RecordBufferDel(TABLES table, uint64_t item_id) {
        buffers_[table].erase(item_id);
        return RC::RCOK;
    }
}