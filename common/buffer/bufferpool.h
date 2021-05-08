#ifndef DBX1000_BUFFER_POOL
#define DBX1000_BUFFER_POOL

#include <thread>
#include "LRUCache.h"
#include "common/global.h"
#include "common/storage/tablespace/page.h"
// #include "common/storage/row.h"
#include "instance/manager_instance.h"


using namespace dbx1000;

class PageDeleter{
public:
	void operator() (Page& value) const {
		// delete &value;
	}
};


class BufferPool{
public:
    using PageKey = std::pair<TABLES, uint64_t>;
    using PageHandle = LRUHandle<PageKey, Page>;

private:
    LRUCache<PageKey, Page, PageDeleter> buffer_pool_;

public:

    const PageHandle* Get(const PageKey& key)             {
        const  PageHandle* handle = buffer_pool_.get(key);
        Counter(handle);
        return handle;
    }

    // const PageHandle *Put(const PageKey& key, Page& value) { return buffer_pool_.put(key, std::forward<Page>(value)); }
    const PageHandle* Put(const PageKey& key, const Page& value) { return buffer_pool_.put(key, value); }
    const PageHandle* Put(const PageKey& key, Page&& value) { return buffer_pool_.put(key, std::forward<Page>(value)); }
    // const PageHandle* Put(const PageKey& key, T&& value) { return buffer_pool_.put(key, value); }
    void Release(const PageHandle* handle)                { buffer_pool_.release(handle); }
    void Release(PageHandle* handle)                      { buffer_pool_.release(handle); }
    void Delete(const PageKey& key)                       { buffer_pool_.del(key); }
    void SetSize(size_t max_size)                         { this->buffer_pool_.set_max_size(max_size); }

    const PageHandle* PutIfNotExist(PageKey pagekey) {
        this_thread::sleep_for(chrono::nanoseconds(10000));
        // if(ZIPF_THETA > 0.1) {
            // if(manager_instance_->threshold_ < 0.03) { this_thread::sleep_for(chrono::nanoseconds(int64_t(10000/WRITE_PERC))); }
            // if(0.03 <= manager_instance_->threshold_ && manager_instance_->threshold_ < 0.06) { this_thread::sleep_for(chrono::nanoseconds(int64_t(7000/WRITE_PERC))); }
            // if(0.06 <= manager_instance_->threshold_ && manager_instance_->threshold_ < 0.1) { this_thread::sleep_for(chrono::nanoseconds(int64_t(5000/WRITE_PERC))); }
            // if(0.1 <= manager_instance_->threshold_ && manager_instance_->threshold_ < 0.2) { this_thread::sleep_for(chrono::nanoseconds(int64_t(3000/WRITE_PERC))); }
            // if(0.2 <= manager_instance_->threshold_ && manager_instance_->threshold_ < 0.3) { this_thread::sleep_for(chrono::nanoseconds(int64_t(1000/WRITE_PERC))); }
            // if(0.4 < manager_instance_->threshold_) { this_thread::sleep_for(chrono::nanoseconds(int64_t(600/WRITE_PERC))); }
        // }
        Page page;
        page.Init();
        page.set_page_id(pagekey.second);
        page.Serialize();
        return Put(pagekey, std::move(page));
    }


    LRUCache<PageKey, Page, PageDeleter>& buffer_pool()   { return buffer_pool_; }

    void Counter(const PageHandle* handle) {
        total_req_.fetch_add(1);
        if(handle) { hits_.fetch_add(1); }
    }

    atomic_uint64_t total_req_;
    atomic_uint64_t hits_;
	dbx1000::ManagerInstance* manager_instance_;
};

#endif // DBX1000_BUFFER_POOL