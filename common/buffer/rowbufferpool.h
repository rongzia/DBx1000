#ifndef DBX1000_BUFFER_POOL
#define DBX1000_BUFFER_POOL

#include <thread>
#include "LRUCache2.h"
#include "common/global.h"
#include "common/storage/row.h"
#include "common/storage/table.h"
#include "instance/manager_instance.h"


using namespace dbx1000;

class RowDeleter{
public:
	void operator() (row_t& value) const {
        
	}
};


class RowBufferPool{
public:
    using RowKey = std::pair<TABLES, uint64_t>;
    using RowHandle = LRUHandle<RowKey, row_t>;

private:
    LRUCache2<RowKey, row_t, RowDeleter> buffer_pool_;

public:

    const RowHandle* Get(const RowKey& key)             {
        const  RowHandle* handle = buffer_pool_.get(key);
        Counter(handle);
        return handle;
    }
    template<typename T>
    const RowHandle* Put(const RowKey& key, T&& value)   { return buffer_pool_.put(key, std::forward<T>(value)); }
    void Release(const RowHandle* handle)                { buffer_pool_.release(handle); }
    void Release(RowHandle* handle)                      { buffer_pool_.release(handle); }
    void Delete(const RowKey& key)                       { buffer_pool_.del(key); }
    void SetSize(size_t max_size)                        { this->buffer_pool_.set_max_size(max_size); }

    const RowHandle* PutIfNotExist(RowKey key, table_t* table) {
        // this_thread::sleep_for(chrono::nanoseconds(15000));
        row_t row;
        row.set_primary_key(key.second);
        row.init(table, 0, key.second);
        return Put(key, std::move(row));
    }


    LRUCache2<RowKey, row_t, RowDeleter>& buffer_pool()   { return buffer_pool_; }

    void Counter(const RowHandle* handle) {
        total_req_.fetch_add(1);
        if(handle) { hits_.fetch_add(1); }
    }

    atomic_uint64_t total_req_;
    atomic_uint64_t hits_;
	dbx1000::ManagerInstance* manager_instance_;
};

#endif // DBX1000_BUFFER_POOL