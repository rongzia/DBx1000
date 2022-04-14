#ifndef RR_CONCURRENT_LRU_CACHE
#define RR_CONCURRENT_LRU_CACHE

#include <mutex>
#include <thread>
#include <atomic>
#include <sys/mman.h>
#include <iostream>
#include <cassert>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>

#ifndef PAGE_SIZE
#define PAGE_SIZE (16 * 1024)
#endif // PAGE_SIZE

namespace rr {

    struct _List_node_base
    {
        _List_node_base* _M_next;
        _List_node_base* _M_prev;

        static void swap(_List_node_base& __x, _List_node_base& __y);
        void _M_transfer(_List_node_base* const __first, _List_node_base* const __last);
        void _M_reverse();
        void _M_hook(_List_node_base* const __position);
        void _M_unhook();
        void init() {
            this->_M_next = nullptr;
            this->_M_prev = nullptr;
        }
        ~_List_node_base() {
            init();
        }
    };

    template <typename _Tp>
    struct list_node : public _List_node_base
    {
        _Tp _M_data;
        _Tp* _M_valptr() { return std::__addressof(_M_data); }
        _Tp const* _M_valptr() const { return std::__addressof(_M_data); }


        list_node(const _Tp& t) : _M_data(t) {
            init();
            // std::cout << __FILE__ << ", " << __LINE__ << std::endl;
        }
        list_node(_Tp&& t) : _M_data(std::move(t)) {
            init();
            // std::cout << __FILE__ << ", " << __LINE__ << std::endl;
        }
        // list_node* operator=(_Tp&& t)
        // {
        //     this->_M_data = std::forward<_Tp>(t); init();
        //     std::cout << __FILE__ << ", " << __LINE__ << std::endl;
        // }

        list_node() { init(); }

        list_node(const list_node& _x) = delete;
        // {
        //     this->_M_data = _x->_M_data;
        //     if (_x->_M_next == _x)
        //         init();
        //     else
        //     {
        //         auto* const __node = this;
        //         __node->_M_next = _x->_M_next;
        //         __node->_M_prev = _x->_M_prev;
        //         __node->_M_next->_M_prev = __node->_M_prev->_M_next = __node;
        //         // _x.init();
        //     }
        // }

        list_node* operator=(const list_node& _x) = delete;

        list_node(list_node&& _x) = delete;
    };

    template <typename _Tp>
    struct _List_iterator
    {
        typedef _List_iterator<_Tp> _Self;
        typedef list_node<_Tp> _Node;

        typedef ptrdiff_t difference_type;
        typedef std::bidirectional_iterator_tag iterator_category;
        typedef _Tp value_type;
        typedef _Tp* pointer;
        typedef _Tp& reference;

        _List_iterator() : _M_node() {}

        explicit _List_iterator(_List_node_base* __x) : _M_node(__x) {}

        _Self _M_const_cast() const { return *this; }

        // Must downcast from _List_node_base to _List_node to get to value.
        reference  operator*() const { return *static_cast<_Node*>(_M_node)->_M_valptr(); }

        pointer operator->() const { return static_cast<_Node*>(_M_node)->_M_valptr(); }

        _Self& operator++() {
            _M_node = _M_node->_M_next;
            return *this;
        }

        _Self operator++(int) {
            _Self __tmp = *this;
            _M_node = _M_node->_M_next;
            return __tmp;
        }

        _Self& operator--() {
            _M_node = _M_node->_M_prev;
            return *this;
        }

        _Self operator--(int) {
            _Self __tmp = *this;
            _M_node = _M_node->_M_prev;
            return __tmp;
        }

        bool operator==(const _Self& __x) const { return _M_node == __x._M_node; }

        bool operator!=(const _Self& __x) const { return _M_node != __x._M_node; }

        // The only member points to the %list element.
        _List_node_base* _M_node;
    };

    /**
     *  @brief A list::const_iterator.
     *
     *  All the functions are op overloads.
     */
    template <typename _Tp>
    struct _List_const_iterator
    {
        typedef _List_const_iterator<_Tp> _Self;
        typedef const list_node<_Tp> _Node;
        typedef _List_iterator<_Tp> iterator;

        typedef ptrdiff_t difference_type;
        typedef std::bidirectional_iterator_tag iterator_category;
        typedef _Tp value_type;
        typedef const _Tp* pointer;
        typedef const _Tp& reference;

        _List_const_iterator() _GLIBCXX_NOEXCEPT
            : _M_node() {}

        explicit _List_const_iterator(const _List_node_base* __x) : _M_node(__x) {}

        _List_const_iterator(const iterator& __x) : _M_node(__x._M_node) {}

        iterator _M_const_cast() const {
            return iterator(const_cast<_List_node_base*>(_M_node));
        }

        // Must downcast from List_node_base to _List_node to get to value.
        reference operator*() const {
            return *static_cast<_Node*>(_M_node)->_M_valptr();
        }

        pointer operator->() const {
            return static_cast<_Node*>(_M_node)->_M_valptr();
        }

        _Self& operator++() {
            _M_node = _M_node->_M_next;
            return *this;
        }

        _Self operator++(int) {
            _Self __tmp = *this;
            _M_node = _M_node->_M_next;
            return __tmp;
        }

        _Self& operator--() {
            _M_node = _M_node->_M_prev;
            return *this;
        }

        _Self operator--(int) {
            _Self __tmp = *this;
            _M_node = _M_node->_M_prev;
            return __tmp;
        }

        bool operator==(const _Self& __x) const {
            return _M_node == __x._M_node;
        }

        bool operator!=(const _Self& __x) const {
            return _M_node != __x._M_node;
        }

        // The only member points to the %list element.
        const _List_node_base* _M_node;
    };


    namespace lru_cache {
        class LRUCache;
        class Page {
        public:
            void* page_ptr_;
            uint64_t page_id_;
            size_t size_;
            uint64_t used_size_;
            uint64_t version_;

            std::atomic<size_t> ref_;
            rr::list_node<Page*>* node_;
            LRUCache* cache_;

            std::atomic<uint64_t> internal_version_;

            size_t Ref() { return ref_.fetch_add(1); }
            size_t Unref();
            size_t RefSize() { return ref_.load(); }
            void RefClear() { ref_.store(0); }

            Page(LRUCache* cache) : page_ptr_(nullptr), page_id_(UINT64_MAX), size_(0)
                , used_size_(64), version_(0)
                , node_(nullptr), cache_(cache) {
                ref_.store(0);
                internal_version_ = 0;
            }

            Page(char* page_ptr, uint64_t page_id, size_t size, LRUCache* cache) : page_ptr_(page_ptr), page_id_(page_id), size_(size)
                , used_size_(64), version_(0)
                , node_(nullptr), cache_(cache) {
                ref_.store(0);
                internal_version_ = 0;
            }

            ~Page() {}

            void check() {
                if (page_ptr_ == nullptr || page_ptr_ == NULL) {
                    assert(size_ == 0 && page_id_ == UINT64_MAX);
                }
                else { assert(size_ == PAGE_SIZE); }
            }

            const Page& operator=(const Page& page) = delete;
            const Page& operator=(Page&& page) = delete;
            Page(const Page& page) = delete;
            Page(Page&& page) = delete;
            Page() = delete;

            void reset() {
                this->page_id_ = UINT64_MAX;
                this->node_ = nullptr;
                this->used_size_ = 64;
                this->version_ = 0;
            }

            void Print() {
                std::cout << "page_id: " << page_id_ << ", ptr: " << *(uint64_t*)(page_ptr_) << ", " << __FILE__ << ", " << __LINE__ << std::endl;
            }

            void Serialize() {
                assert(used_size_ <= size_);
                char* p = (char*)page_ptr_;
                memcpy(p, &page_id_, sizeof(uint64_t));
                memcpy(p + sizeof(uint64_t), &size_, sizeof(size_t));
                memcpy(p + sizeof(uint64_t) + sizeof(size_t), &used_size_, sizeof(uint64_t));
                memcpy(p + sizeof(uint64_t) + sizeof(size_t) + sizeof(uint64_t), &version_, sizeof(uint64_t));
            }

            void Deserialize() {
                char* p = (char*)page_ptr_;
                memcpy(&page_id_, p, sizeof(uint64_t));
                memcpy(&size_, p + sizeof(uint64_t), sizeof(size_t));
                memcpy(&used_size_, p + sizeof(uint64_t) + sizeof(size_t), sizeof(uint64_t));
                memcpy(&version_, p + sizeof(uint64_t) + sizeof(size_t) + sizeof(uint64_t), sizeof(uint64_t));
            }

            bool AddRow(void* row_date, size_t row_size) {
                if (used_size_ + row_size > PAGE_SIZE) {
                    return false;
                }
                else {
                    char* location = ((char*)page_ptr_) + used_size_;
                    memcpy(location, row_date, row_size);
                    used_size_ += row_size;
                    assert(used_size_ <= PAGE_SIZE);
                    memcpy(((char*)page_ptr_) + sizeof(uint64_t), &used_size_, sizeof(uint64_t));
                    return true;
                }
            }
        };



        void INIT_LIST_HEAD(rr::list_node<Page*>* node);

        void list_add_tail(rr::list_node<Page*>* temp, rr::list_node<Page*>* head);

        void list_del(rr::list_node<Page*>* temp);

        bool node_in_list(rr::list_node<Page*>* node);



        class LRUCache {
        public:
            // max_size_ in bytes, item_bytes_ in bytes, item_num_ = max_size_ / item_size_ 
            size_t max_bytes_;
            size_t item_bytes_;
            void* ptr_;

            tbb::concurrent_queue<Page*> free_list_;
            std::atomic<size_t> free_list_size_{ 0 };

            rr::list_node<Page*> in_use_list_;
            std::atomic<size_t> in_use_list_size_{ 0 };
            std::mutex in_use_list_mutex_;

            using ConcurrentMap = tbb::concurrent_hash_map<uint64_t, Page*>;
            using accessor = typename ConcurrentMap::accessor;
            using const_accessor = typename ConcurrentMap::const_accessor;
            ConcurrentMap map_;

        public:
            bool should_stop_;	// 父线程/进程退出，此时不再需要后台删除
            bool has_shutdown_;	// 表示后台线程成功退出，可以正常析构，不然要等后台线程退出才能析构

        public:
            /**
             * @brief 修改完 map_ 后，修改链表 in_use_list_ 异步操作，先把操作记录进 async_queue_，在慢慢合并至 in_use_list_。
             * async_queue_ 是个数组 async_queue_[CPU_num]，避免多线程访问同一个 ConcurrentQueue 竞争
             * async_queue_mutex_ 很少用
             */
            enum class OP { PUT, GET, UPDATE, DEL };
            struct Operator {
                Page* page_;
                OP op_;
                uint64_t internal_version_;
                Operator() {}
                Operator(Page* page, OP op, uint64_t internal_version) : page_(page), op_(op), internal_version_(internal_version) { }
            };
            tbb::concurrent_queue<Operator>** async_queue_;
            size_t thread_num_;
            std::mutex async_queue_mutex_;
            std::thread async_queue_thread_;

            rr::list_node<Page*> deleted_queue_;		// dummy_node_
            std::atomic<size_t> deleted_size_{ 0 };
            std::thread deleted_queue_thread_;




            LRUCache(size_t max_bytes, size_t item_bytes) :
                max_bytes_(max_bytes)
                , item_bytes_(item_bytes)
                // , free_list_(true)
                , should_stop_(false), has_shutdown_(false)
                , thread_num_(std::thread::hardware_concurrency())
            {
                // cout << "free_list_size_: " << free_list_size_ << ", max_bytes: " << max_bytes << ", item_bytes: " << item_bytes << endl;
                ptr_ = mmap(NULL, this->max_bytes_, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);


                free_list_size_.store(max_bytes / item_bytes);
                // std::unique_lock<std::mutex> lck(cmap_->in_use_list_mutex());
                char* cursor = (char*)ptr_;
                for (auto i = 0; i < free_list_size_; i++) {
                    free_list_.push((Page*)(new Page(cursor, UINT64_MAX, PAGE_SIZE, this)));
                    cursor += item_bytes_;
                }

                INIT_LIST_HEAD(&in_use_list_);
                in_use_list_size_.store(0);

                async_queue_ = new tbb::concurrent_queue<Operator>*[thread_num_];
                for (auto i = 0; i < thread_num_; i++) {
                    async_queue_[i] = new tbb::concurrent_queue<Operator>();
                    assert(async_queue_[i] != nullptr);
                }

                INIT_LIST_HEAD(&deleted_queue_);
                deleted_size_.store(0);

                async_queue_thread_ = std::thread(&LRUCache::AsyncRWListThread, this);
                async_queue_thread_.detach();
            }

            ~LRUCache() {
                should_stop_ = true;
                while (!has_shutdown_);


                // for (auto i = 0; i < thread_num_; i++) { async_queue_[i]->set_stop(); }
                for (auto i = 0; i < thread_num_; i++) { async_queue_[i]->clear(); delete async_queue_[i]; }
                munmap(ptr_, this->max_bytes_);
            }

            bool GetNewPage(Page*& page) {
                while (1) {
                    bool res = free_list_.try_pop(page);

                    if (res) {
                        if (page->RefSize() != 0) {
                            // 有个小 bug, 当 size 不够时，需要 evict, 且存在 delete 操作的话，会偶现 refsize == 0 的情况
                            std::cout << page->RefSize() << std::endl;
                            page->ref_.store(0);
                        }
                        assert(page);
                        assert(!page->node_);
                        assert(page->size_ == PAGE_SIZE);
                        page->reset();
                        page->internal_version_.fetch_add(1);
                        free_list_size_.fetch_sub(1);
                        return res;
                    }
                }
            }

            void afterTask(Page* page, OP op, int thread_id) {
                size_t before = page->Ref();
                // /* debug */ std::cout << __LINE__ << "\t, page/ref_size: " << page->page_id_ << "/" << before+1 << std::endl;
                assert(before >= 0);
                Operator operat(page, op, page->internal_version_);
                async_queue_[thread_id % thread_num_]->emplace(operat);
            }

            bool Write(const uint64_t& page_id, Page* page, Page*& prior, int thread_id = 0) {
                assert(page);
                assert(!prior);

                accessor acc;
                bool is_new = map_.insert(acc, page_id);
                if (is_new) {
                    assert(page->page_id_ == page_id);
                    assert(acc->second == nullptr);
                    acc->second = page;
                    size_t before = page->Ref();
                    // /* debug */ std::cout << __LINE__ << "\t, page/ref_size: " << page->page_id_ << "/" << before+1 << std::endl;
                    afterTask(page, OP::PUT, thread_id);
                }
                else {	// 仅读旧值，不写
                    assert(acc->second);
                    prior = acc->second;
                    prior->Ref();
                    page->reset();
                    free_list_.push(page);
                    free_list_size_.fetch_add(1);
                }

                return is_new;
            }

            bool Read(const uint64_t& page_id, Page*& page, int thread_id = 0) {
                assert(!page);

                const_accessor c_access;
                bool find = map_.find(c_access, page_id);
                if (find) {
                    // 需要在外部显式调用 Unref()
                    page = c_access->second;
                    page->Ref();
                    assert(page);
                    assert(page_id == page->page_id_);
                    assert(nullptr != page->page_ptr_);
                    afterTask(page, OP::GET, thread_id);
                }
                return find;
            }

            bool Delete(const uint64_t& page_id, int thread_id = 0) {
                accessor acc;
                bool find = map_.find(acc, page_id);
                if (find) {
                    Page* page = acc->second;
                    assert(page_id == page->page_id_);
                    page->page_id_ = UINT64_MAX;
                    bool deleted = map_.erase(acc);
                    assert(deleted);

                    afterTask(page, OP::DEL, thread_id);
                }
                return find;
            }

            void Print() {
                std::cout << "free_list_size_/in_use_list_size_/async_queue_[0]->size(): " << free_list_size_ << ", " << in_use_list_size_ << ", " << async_queue_[0]->unsafe_size() << std::endl;
            }

        private:

            void use(Page* page, OP op, uint64_t internal_version) {
                if(internal_version != page->internal_version_.load()) return;
                if (OP::DEL == op) {
                    assert(page->page_id_ == UINT64_MAX);
                    rr::list_node<Page*>* node = page->node_;
                    if (node != nullptr) {
                        assert(node_in_list(node));
                        list_del(node);
                        node->_M_prev = node->_M_next = nullptr;
                        delete node;
                        page->node_ = nullptr;
                        in_use_list_size_.fetch_sub(1);
                        size_t before = page->Unref();
                        // /* debug */ std::cout << __LINE__ << "\t, page/unref: " << page->page_id_ << "/" << before-1 << std::endl;
                    }
                }

                if (OP::PUT == op) {
                    if (page->page_id_ == UINT64_MAX) {

                    }
                    else if (page->node_ != nullptr) {
                        assert(false);
                    }
                    else if (page->node_ == nullptr) {
                        rr::list_node<Page*>* node = new rr::list_node<Page*>(page);
                        page->node_ = node;
                        size_t before = page->Ref();
                        list_add_tail(node, &in_use_list_);
                        in_use_list_size_.fetch_add(1);
                        // /* debug */ std::cout << __LINE__ << ", page/ref_size: " << page->page_id_ << "/" << before+1 << std::endl;
                    }
                    evict();
                }
                if (OP::GET == op) {
                    if (page->page_id_ == UINT64_MAX) {

                    }
                    else if (page->node_ == nullptr) {

                    }
                    else if (page->node_ != nullptr) {
                        rr::list_node<Page*>* node = page->node_;
                        assert(node_in_list(node));
                        list_del(node);
                        list_add_tail(node, &in_use_list_);
                    }
                }
                size_t before = page->Unref();
                // /* debug */ std::cout << __LINE__ << "\t, page/unref: " << page->page_id_ << "/" << before-1 << std::endl;
            }


            void evict() {
                rr::list_node<Page*>* first;
                while (free_list_size_.load() <= 0) {
                    first = (rr::list_node<Page*>*)this->in_use_list_._M_next;

                    if (first == &in_use_list_) { return; }
                    // else if (page->Deleted()) continue;
                    else {
                        Page* page = (Page*)(first->_M_data);
                        assert(page);
                        assert(node_in_list(first));
                        assert(first == page->node_);

                        accessor acc;
                        bool find = map_.find(acc, page->page_id_);
                        // assert(find);	// 和 Remove 接口冲突
                        if (find) {
                            assert(page == acc->second);
                            assert(UINT64_MAX != page->page_id_);
                            uint64_t old_page_id = page->page_id_;
                            page->page_id_ = UINT64_MAX;
                            map_.erase(acc);


                            list_del(first);
                            first->_M_prev = first->_M_next = nullptr;
                            delete first;
                            page->node_ = nullptr;
                            in_use_list_size_.fetch_sub(1);
                            size_t before = page->Unref();
                            // /* debug */ std::cout << __LINE__ << ", page/unref: " << old_page_id << "/" << before-1 << std::endl;
                        }
                        else {
                            // rr::list_node<Page*>* node = page->node_;
                            assert(UINT64_MAX == page->page_id_);
                            // if(first) {
                            list_del(first);
                            first->_M_prev = first->_M_next = nullptr;
                            delete first;
                            page->node_ = nullptr;
                            in_use_list_size_.fetch_sub(1);
                            size_t before = page->Unref();
                            // }
                        }
                    }
                }
            }

            void AsyncRWListThread() {
                int count = 0;

                while (!should_stop_) {
                    evict();
                    for (auto i = 0; i < thread_num_; i++) {
                        Operator front;
                        bool res = async_queue_[i]->try_pop(front);
                        if (res) {
                            std::unique_lock<std::mutex> lck(in_use_list_mutex_);
                            use(front.page_, front.op_, front.internal_version_);
                        }
                        else {
                            // std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        }
                    }
                    count++;
                }
                has_shutdown_ = true;
                return;
            }
        };
    }
    using namespace lru_cache;
}

#endif // RR_CONCURRENT_LRU_CACHE