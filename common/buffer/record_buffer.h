//
// Created by rrzhang on 2020/10/11.
//

#ifndef DBX1000_RECORD_BUFFER_H
#define DBX1000_RECORD_BUFFER_H

#include <map>
#include <tbb/concurrent_hash_map.h>
#include "common/myhelper.h"


class table_t;
class row_t;
class workload;
namespace dbx1000 {
    class RecordBuffer {
    public:
        RecordBuffer() = default;
//        void Init(workload* workload);
        RC BufferGet(uint64_t item_id, char *buf, std::size_t size);
        RC BufferPut(uint64_t item_id, const char *buf, std::size_t size);
        RC BufferDel(uint64_t item_id);
    private:

//        std::map<TABLES, tbb::concurrent_hash_map<uint64_t, row_t*>> buffers_;
        tbb::concurrent_hash_map<uint64_t, char*> buffer_;
//        std::map<TABLES, table_t*> tables_;
    };
}


#endif //DBX1000_RECORD_BUFFER_H
