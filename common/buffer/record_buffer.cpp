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
//    void RecordBuffer::Init() {
//        this->workload_ = workload;
//#if WORKLOAD == YCSB
//        this->tables_[TABLES::MAIN_TABLE] = workload_->tables["MAIN_TABLE"];
//#elif WORKLOAD == TPCC
//        this->tables_[TABLES::WAREHOUSE] = workload_->tables["WAREHOUSE"];
//        this->tables_[TABLES::DISTRICT] = workload_->tables["DISTRICT"];
//        this->tables_[TABLES::CUSTOMER] = workload_->tables["CUSTOMER"];
//        this->tables_[TABLES::HISTORY] = workload_->tables["HISTORY"];
//        this->tables_[TABLES::NEW_ORDER] = workload_->tables["NEW-ORDER"];
//        this->tables_[TABLES::ORDER] = workload_->tables["ORDER"];
//        this->tables_[TABLES::ORDER_LINE] = workload_->tables["ORDER-LINE"];
//        this->tables_[TABLES::ITEM] = workload_->tables["ITEM"];
//        this->tables_[TABLES::STOCK] = workload_->tables["STOCK"];
//#endif
//    }
    RC RecordBuffer::BufferGet(uint64_t item_id, char *buf, std::size_t size) {
//        row = new row_t();
//        row->init(tables_[table]);
//        row->set_primary_key(item_id);

        tbb::concurrent_hash_map<uint64_t, char*>::const_accessor const_acc;
        bool res = buffer_.find(const_acc, item_id);
        if(res) {
            memcpy(buf, const_acc->second, size);
        } else {
            // TODO: buffer 不存在的情况
        }
        return RC::RCOK;
    }

    RC RecordBuffer::BufferPut(uint64_t item_id, const char *buf, std::size_t size) {
        std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> start = std::chrono::system_clock::now();
        tbb::concurrent_hash_map<uint64_t, char*>::accessor accessor;
        bool res = buffer_.find(accessor, item_id);

        if(res) {
            memcpy(accessor->second, buf, size);
        } else {
            char *data = new char[size];
            memcpy(data, buf, size);
            buffer_.insert(accessor, make_pair(item_id, data));
        }
        std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> end = std::chrono::system_clock::now();
        uint64_t dura = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
//#if defined(B_M_L_P) || defined(B_M_L_R)
//        while(true){ if(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - start).count() > 0.5 * dura) { break; } }
//#elif defined(B_P_L_P) || defined(B_P_L_R)
//        while(true){ if(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - start).count() > 5 * dura) { break; } }
//#endif

        return RC::RCOK;
    }

    RC RecordBuffer::BufferDel(uint64_t item_id) {
        buffer_.erase(item_id);
        return RC::RCOK;
    }
}