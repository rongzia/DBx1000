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
#include "common/storage/tablespace/page.h"
#include "util/profiler.h"

typedef tbb::concurrent_hash_map<uint64_t, char*> hashmap;
typedef typename hashmap ::const_accessor const_accessor;
typedef typename hashmap ::accessor accessor;
typedef typename hashmap ::iterator Iterator;

namespace dbx1000 {
    RecordBuffer::~RecordBuffer() {
        for(auto &iter : buffer_) {
            delete [] iter.second;
        }
    }

    RC RecordBuffer::BufferGet(uint64_t item_id, char *buf, std::size_t size) {
        // cout << "BufferGet : " << item_id << endl;
        RC rc = RC::RCOK;
        const_accessor const_acc;
        accessor  accessor;
        std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> start = std::chrono::system_clock::now();
        //tbb::concurrent_hash_map<uint64_t, char*>::const_accessor const_acc;
        //tbb::concurrent_hash_map<uint64_t, char*>::accessor accessor;
        bool res = buffer_.find(const_acc, item_id);
        if(res) {
            memcpy(buf, const_acc->second, size);
        } else {
            // TODO: buffer 不存在的情况
            //rc = RC::Abort;
            char *data = new char[size]; 
#if defined(B_P_L_R) || defined(B_P_L_P)
            assert(size == MY_PAGE_SIZE);
            uint64_t page_id = item_id;
            memcpy(&buf[0 * sizeof(uint64_t)], reinterpret_cast<void *>(&page_id), sizeof(uint64_t));
            memcpy(data, buf, sizeof(uint64_t));
#else 
            memcpy(buf, data, size);
#endif
            buffer_.insert(accessor, make_pair(item_id, data));

            // while(true){ if(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - start).count() > 50000) { break; } }
       }
        std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> end = std::chrono::system_clock::now();
        uint64_t dura = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
#if defined(B_M_L_R)
        while(true){ if(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - start).count() > 1 * dura) { break; } }
//#elif defined(B_P_L_P) || defined(B_P_L_R)
//        while(true){ if(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - start).count() > 5 * dura) { break; } }
#endif
        return rc;
    }

    RC RecordBuffer::BufferPut(uint64_t item_id, const char *buf, std::size_t size) {
        //tbb::concurrent_hash_map<uint64_t, char*>::accessor accessor;
        accessor accessor;
        Iterator iterator1;
        Iterator iterator2;
        hashmap hashmap1;
//        if(manager_instance_ != nullptr)
//         {
// #if defined(B_P_L_P)
//             if(manager_instance_->instance_id_ != 1)
//             {
//                 int size_ = buffer_.size();
//                 if(size_>=0.6*(SYNTH_TABLE_SIZE/204))
//                 {
//                     iterator1 = buffer_.begin();
//                     for(int i = 0; i<0.2*size_; i++)
//                     {
//                         iterator2 = (iterator1++);
//                         BufferDel(iterator1->first);
//                         iterator1 = iterator2;
//                     }
//                 }
//             }
// #endif
//         }

        std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> start = std::chrono::system_clock::now();

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
#if defined(B_M_L_R)
        while(true){ if(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - start).count() > 1 * dura) { break; } }
//#elif defined(B_P_L_P) || defined(B_P_L_R)
//        while(true){ if(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - start).count() > 5 * dura) { break; } }
#endif

        return RC::RCOK;
    }

    RC RecordBuffer::BufferDel(uint64_t item_id) {
        //tbb::concurrent_hash_map<uint64_t, char*>::accessor accessor;
        accessor accessor;
        bool res = buffer_.find(accessor, item_id);
        if(res) {
            delete [] accessor->second;
            buffer_.erase(accessor);
        }
        return RC::RCOK;
    }
}