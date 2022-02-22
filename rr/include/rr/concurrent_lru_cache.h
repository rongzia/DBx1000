#ifndef CONCURRENT_LRU_CACHE
#define CONCURRENT_LRU_CACHE

#include<sys/mman.h>
#include "concurrent_linked_hash_map.h"
using namespace std;

#define PAGE_SIZE (16 * 1024)
namespace rr {
	namespace lru_cache {
		class LRUCache;

		using CMap = rr::ConcurrentLinkedHashMap<uint64_t, void*, LRUCache>;
		using key_type = CMap::key_type;
		using value_type = CMap::value_type;
		using handle_type = rr::concurrent_linked_map::Handle<uint64_t, void*, LRUCache>;

		// template<typename KEY, typename VALUE, class Manager>
		// class Page final : public con_map::Handle<KEY, VALUE, Manager> {
		class Page final : public handle_type {
		public:
			size_t size_;
			std::atomic<size_t> page_ref_;
			LRUCache* cache_;

			virtual size_t PageRef() override { return page_ref_.fetch_add(1); }
			size_t PageUnref() { return page_ref_.fetch_sub(1); }
			virtual size_t PageRefSize() override { return page_ref_.load(); }

			Page() :handle_type::Handle(nullptr, UINT64_MAX, nullptr), size_(0), cache_(nullptr) { page_ref_.store(0); }
			Page(LRUCache* cache) : handle_type::Handle(nullptr, UINT64_MAX, nullptr)
				, size_(0), cache_(cache) {
				page_ref_.store(0);
			}
			Page(uint64_t key, LRUCache* cache) : handle_type::Handle(nullptr, key, nullptr)
				, size_(0), cache_(cache) {
				page_ref_.store(0);
			}
			Page(uint64_t key, void* ptr, LRUCache* cache);

			virtual void Delete();

			~Page() {}

			void check() {
				if (value() == nullptr || value() == NULL) {
					assert(size_ == 0 && key() == UINT64_MAX);
				}
				else {
					assert(size_ == PAGE_SIZE);
				}
			}

			const Page& operator=(const Page& page) = delete;
			const Page& operator=(Page&& page) = delete;
			Page(const Page& page) = delete;
			Page(Page&& page) = delete;

			void reset() {
				handle_type::key_ = UINT64_MAX;
				handle_type::value_ = nullptr;
				size_ = 0;
			}

			void set(void* ptr) {
				set_key(UINT64_MAX);
				handle_type::value() = ptr;
				size_ = PAGE_SIZE;
				memcpy(handle_type::value_, (void*)(&(handle_type::key_)), sizeof(uint64_t));
			}

			void set(const uint64_t& key, void* ptr) {
				set_key(key);
				handle_type::value_ = ptr;
				size_ = PAGE_SIZE;
				memcpy(handle_type::value_, (void*)(&(handle_type::key_)), sizeof(uint64_t));
			}

			void set_key(const uint64_t& key) {
				handle_type::key_ = key;
			}

			void set_id(const uint64_t& id) {
				assert(handle_type::key_ == id);
				memcpy(handle_type::value_, (void*)(&id), sizeof(uint64_t));
			}

			void Print() {
				cout << "key: " << handle_type::key_ << ", ptr: " << *(uint64_t*)(handle_type::value_) << ", " << __FILE__ << ", " << __LINE__ << endl;
			}
		};

		class LRUCache : rr::Manager_Hint {
		public:
			rr::ConcurrentQueue<handle_type*> free_list_;
			std::atomic<size_t> free_list_size_{ 0 };

			// max_size_ in bytes, item_bytes_ in bytes, item_num_ = max_size_ / item_size_ 
			size_t max_bytes_;
			size_t item_bytes_;
			void* ptr_;

			template<typename K, typename V, class M> friend class rr::ConcurrentLinkedHashMap;
			ConcurrentLinkedHashMap<uint64_t, void*, LRUCache>* cmap_;
			bool cmap_delete_done_;

		public:
			LRUCache(size_t max_bytes, size_t item_bytes) :
				// cmap_(max_bytes / item_bytes), 
				max_bytes_(max_bytes)
				, item_bytes_(item_bytes)
				, free_list_(true)
			{
				free_list_size_.store(max_bytes / item_bytes);
				// cout << "free_list_size_: " << free_list_size_ << ", max_bytes: " << max_bytes << ", item_bytes: " << item_bytes << endl;
				ptr_ = mmap(NULL, this->max_bytes_, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);

				cmap_ = new ConcurrentLinkedHashMap<uint64_t, void*, LRUCache>(max_bytes / item_bytes);
				cmap_->manager() = this;
				std::unique_lock<std::mutex> lck(cmap_->in_use_list_mutex());
				char* cursor = (char*)ptr_;
				for (auto i = 0; i < free_list_size_; i++) {
					free_list_.push((handle_type*)(new Page(UINT64_MAX, cursor, this)));
					cursor += item_bytes_;
				}
				cmap_delete_done_ = false;
			}

			~LRUCache() {
				delete cmap_;
				while (!cmap_delete_done_);

				munmap(ptr_, this->max_bytes_);
			}

			// bool Write(const key_type& key, const value_type& ptr, int thread_id = 0) {
			// 	value_type prior;
			// 	return cmap_->PutIfAbsent(key, ptr, prior, thread_id);
			// }

			bool Write(const key_type& key, Page* page, int thread_id = 0) {
				handle_type* prior;
				handle_type* handle = (handle_type*)page;
				page->set_key(key);
				bool res = cmap_->PutIfAbsent(key, handle, prior, thread_id);
				page->PageUnref();
				return res;
			}

			// bool Read(const key_type& key, value_type& ptr, int thread_id = 0) {
			// 	bool find = cmap_->Get(key, ptr, thread_id);
			// 	if (find) assert(ptr != nullptr);
			// 	return find;
			// }

			bool Read(const key_type& key, Page* page, int thread_id = 0) {
				handle_type* handle = (handle_type*)page;
				bool find = cmap_->Get(key, handle, thread_id);
				if (find) {
					assert(handle);
					assert(handle->value() != nullptr);
					assert(key == handle->key());
				}
				return find;
			}

			bool GetNewPage(Page*& page) {

				handle_type* handle = nullptr;
				while (1) {
					if (free_list_size_ <= 0) continue;
					bool res = free_list_.pop(handle);
					page = (Page*)handle;
					if (res) {
						free_list_size_.fetch_sub(1); assert(page); page->PageRef();
						return res;
					}
				}
			}
		private:
			virtual void Delete(void* handle) {
				handle_type* h = (handle_type*)handle;
				((Page*)h)->set_key(UINT64_MAX);
				free_list_.emplace_push(h);
				free_list_size_.fetch_add(1);
				h->init();
			}
			virtual void New(void* handle) {}


		public:
			void Print() {
				cmap_->wait_for_asyc_done();
				cout << "map       size: " << cmap_->map().size() << endl;
				cout << "free list size: " << free_list_size_.load() << endl << endl;

				size_t count_map = 0, count_list = 0;
				cout << "buffer         : " << endl;
				for (auto iter = cmap_->map().begin(); iter != cmap_->map().end(); iter++, count_map++) {
					cout << "key: " << iter->first << ", value: " << *(uint64_t*)((handle_type*)iter->second->value()) << endl;
				}

				cout << "free_list_     : " << endl;
				for (auto iter = free_list_.begin(); iter != free_list_.end(); iter++, count_list++) {
					cout << "key: " << (*iter)->key() << ", value: " << *(uint64_t*)((*iter)->value())
						<< ", addr: " << (*iter)->value() << endl;
				}
				cout << endl << "map       count: " << count_map << endl;
				cout << "free list count: " << count_list << endl;
			}
		};

		void Page::Delete() {
			handle_type* h = (handle_type*)this;
			this->set_key(UINT64_MAX);
			cache_->free_list_.emplace_push(h);
			cache_->free_list_size_.fetch_add(1);
			this->init();
		}

		Page::Page(uint64_t key, void* ptr, LRUCache* cache) : handle_type::Handle(cache->cmap_, key, ptr)
			, size_(PAGE_SIZE), cache_(cache) {
			page_ref_.store(0);
		}



		/**
		// template<typename Allocator>
		class LRUCache : public CMap {
		public:
			friend class Page;

		protected:
			rr::ConcurrentQueue<Page> free_list_;
			std::atomic<size_t> free_list_size_;

			// max_size_ in bytes, item_bytes_ in bytes, item_num_ = max_size_ / item_size_
			size_t max_bytes_;
			size_t item_bytes_;
			size_t item_num_;

			void* ptr_;

		public:
			LRUCache(size_t max_bytes, size_t item_bytes)
				// : CMap::ConcurrentLinkedHashMap(max_bytes / item_bytes)
				: ConcurrentLinkedHashMap(max_bytes / item_bytes)
				, max_bytes_(max_bytes)
				, item_bytes_(item_bytes)
				, item_num_(max_bytes / item_bytes)
				, free_list_(true)
			{
				ptr_ = mmap(NULL, this->max_bytes_, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);

				std::unique_lock<std::mutex> lck(this->in_use_list_mutex());
				char* cursor = (char*)ptr_;
				for (auto i = 0; i < item_num_; i++) {
					Page page(this, UINT64_MAX, cursor);
					// page.key_ = UINT64_MAX;
					// page.ptr_ = cursor;
					cursor += item_bytes_;
					free_list_.push(std::move(page));
				}
			}

			bool Write(const key_type& key, const value_type ptr, int thread_id = 0) {
				value_type prior;
				return CMap::PutIfAbsent(key, ptr, prior);
			}

			bool Read(const key_type& key, value_type ptr, int thread_id = 0) {
				return CMap::Get(key, ptr);
			}

			bool GetNewPage(Page& page) {
				while (free_list_size_ <= 0) {}
				// assert(free_list_.pop(page));
				// return true;

				bool res = free_list_.pop(page);
				if (res) { free_list_size_.fetch_sub(1); }
				return res;
			}

			void Print() {
				for (auto iter = CMap::map_.begin(); iter != CMap::map_.end(); iter++) {
					cout << "key: " << iter->first << ", value: " << string((char*)iter->second->value_, item_bytes_) << endl;
				}

				for (auto iter = free_list_.begin(); iter != free_list_.end(); iter++) {
					cout << "key: " << iter->key_ << ", value: " << string((char*)iter->value_, item_bytes_) << endl;
				}
			}

		};*/

	}
	using namespace lru_cache;
}



#endif // CONCURRENT_LRU_CACHE