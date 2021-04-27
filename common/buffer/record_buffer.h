//
// Created by rrzhang on 2020/10/11.
//

#ifndef DBX1000_RECORD_BUFFER_H
#define DBX1000_RECORD_BUFFER_H

#include <map>
#include <tbb/concurrent_hash_map.h>
#include "common/myhelper.h"
#if defined(RDB_BUFFER_WITH_SIZE) || defined(RDB_BUFFER_DIFF_SIZE)
#include "instance/manager_instance.h"
#endif


class table_t;
class row_t;
class workload;
#if defined(RDB_BUFFER_WITH_SIZE) || defined(RDB_BUFFER_DIFF_SIZE)
class ManagerInstance;
#endif
namespace dbx1000 {
    class RecordBuffer {
        using ConcurrentMap = tbb::concurrent_hash_map<uint64_t, char*>;

    public:
        RecordBuffer() = default;
#if defined(RDB_BUFFER_WITH_SIZE) || defined(RDB_BUFFER_DIFF_SIZE)
        RecordBuffer(ManagerInstance* instance_) {
            manager_instance_ = instance_;
        }
#endif
        ~RecordBuffer();

        RC BufferGet(uint64_t item_id, char *buf, std::size_t size);
        RC BufferPut(uint64_t item_id, const char *buf, std::size_t size);
        RC BufferDel(uint64_t item_id);
    private:

        ConcurrentMap buffer_;
#if defined(RDB_BUFFER_WITH_SIZE) || defined(RDB_BUFFER_DIFF_SIZE)
        ManagerInstance* manager_instance_;
#endif
    };
}


#endif //DBX1000_RECORD_BUFFER_H
