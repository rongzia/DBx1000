#ifndef DBX1000_BUFFER_POOL
#define DBX1000_BUFFER_POOL


#include "LRUCache.h"
#include "common/global.h"
#include "common/storage/tablespace/page.h"
// #include "common/storage/row.h"


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

    const PageHandle* Get(const PageKey& key)             { return buffer_pool_.get(key); }

    // const PageHandle *Put(const PageKey& key, Page& value) { return buffer_pool_.put(key, std::forward<Page>(value)); }
    const PageHandle* Put(const PageKey& key, const Page& value) { return buffer_pool_.put(key, value); }
    const PageHandle* Put(const PageKey& key, Page&& value) { return buffer_pool_.put(key, std::forward<Page>(value)); }
    // const PageHandle* Put(const PageKey& key, T&& value) { return buffer_pool_.put(key, value); }
    void Release(const PageHandle* handle)                { buffer_pool_.release(handle); }
    void Release(PageHandle* handle)                      { buffer_pool_.release(handle); }
    void Delete(const PageKey& key)                       { buffer_pool_.del(key); }
    void SetSize(size_t max_size)                         { this->buffer_pool_.set_max_size(max_size); }

    const PageHandle* PutIfNotExist(PageKey pagekey) {
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
};

#endif // DBX1000_BUFFER_POOL