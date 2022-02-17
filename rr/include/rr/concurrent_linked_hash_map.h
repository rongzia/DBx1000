#ifndef CONCURRENT_LINKED_HASH_MAP
#define CONCURRENT_LINKED_HASH_MAP

#include <tbb/concurrent_hash_map.h>
#include <iostream>
#include <cassert>
#include <mutex>
#include <forward_list>
#include <atomic>
#include <thread>
#include <map>
#include <queue>
#include "list.h"
#include "concurrent_queue.h"
using namespace std;

#define DEFAULT_MAX_SIZE SIZE_MAX



static bool node_in_list(::list_node* node) {
	// 应该从头找，但是这样太费时，直接判断了前后是否为自己
	bool in = !(node == node->next && node == node->prev);
	if (in) {
		assert(node != NULL);
		assert(node->next);
		// assert(node->prev);
	}
	return in;
}

namespace rr {
	class Hint {
	public:
		virtual void Delete(void* arg1) = 0;
		virtual void New(void* arg1) = 0;
	};

	template <typename KEY, typename VALUE, class Manager = Hint, typename Allocator = std::allocator<std::pair<KEY, VALUE>>>
	class ConcurrentLinkedHashMap;

	// concurrent_map
	namespace con_map {
		template<typename KEY, typename VALUE, class Manager>
		class HandleBase {
		public:
			template<typename K, typename V, class M, typename A> friend class ConcurrentLinkedHashMap;
			/* or: */
			// friend class ConcurrentLinkedHashMap<KEY, VALUE, Manager>;
			typedef ConcurrentLinkedHashMap<KEY, VALUE, Manager> Map;

			virtual ~HandleBase() {
				deleted_.store(true);
			}

			HandleBase(Map* map, const KEY& k, const VALUE& v) : mapParent_(map), key_(k), value_(v) {
				weight_.store(1);
				ref_.store(0);
				evicted_.store(false);
				deleted_.store(false);
			}
			HandleBase(Map* map, const KEY& k, VALUE&& v) : mapParent_(map), key_(k), value_(std::move(v)) {
				weight_.store(1);
				ref_.store(0);
				evicted_.store(false);
				deleted_.store(false);
			}

			const HandleBase& operator=(const HandleBase& handle) = delete;
			const HandleBase& operator=(HandleBase&& handle) = delete;
			HandleBase(const HandleBase& handle) = delete;
			HandleBase(HandleBase&& handle) = delete;
			HandleBase() = delete;

			bool IsAlive() { return (weight_.load() > 0); }
			bool IsDead() { return (weight_.load() <= 0); }

			virtual void MakeDead() {
				size_t before = weight_.fetch_sub(1);
				assert(1 == before);
			}

			virtual void MakeEvict() {
				assert(evicted_.load() == false);
				evicted_.store(true);
			}

		protected:
			Map* mapParent_;
			KEY key_;
			VALUE value_;
			std::atomic<size_t> weight_;
			// ref==0, 不代表 handle 就可以删除，ref 是 del handle 的必要条件（即ref==0，才能del；但是ref!=0，一定不能 del）
			std::atomic<size_t> ref_;
			std::atomic<bool> evicted_;	// evict 仅内部空间不够，被 lru 清除才调用
		public:
			std::atomic<bool> deleted_;	// 外部显示调用 Remove 才用到，后面异步删除掉 handle

			VALUE& value() { return value_; }
			KEY& key() { return key_; }

			bool Ref() { return ref_.fetch_add(1); }
			bool Unref() { return ref_.fetch_sub(1); }
			bool RefSize() { return ref_.load(); }
			bool Evicted() { return evicted_.load(); }
		};

		template<typename KEY, typename VALUE, class Manager>
		class Handle : public HandleBase<KEY, VALUE, Manager> {
		public:
			template<typename K, typename V, typename M, typename A> friend class ConcurrentLinkedHashMap;
			typedef ConcurrentLinkedHashMap<KEY, VALUE, Manager> Map;
			typedef typename ConcurrentLinkedHashMap<KEY, VALUE, Manager>::QueueNode QueueNode;
			using handle_base_type = HandleBase<KEY, VALUE, Manager>;

		private:
			::list_node node_; // dummy 节点
		protected:

		public:
			void copy(::list_node* node) {
				if (node->next == node) {
					assert(node->prev == node);
					INIT_LIST_HEAD(node);
				}
				else {
					::list_node* prev = node->prev;
					::list_node* next = node->next;
					this->node.prev = prev;
					prev->next = this->node;
					this->node.next = next;
					next->prev = this->node;
				}
			}

			virtual ~Handle() {
				node_.prev = nullptr;
				node_.next = nullptr;
				// no need free mapParent_ and queueNode_
			}

			Handle(Map* map, const KEY& k, const VALUE& v) : handle_base_type::HandleBase(map, k, v) {
				INIT_LIST_HEAD(&node_);
			}


			Handle(Map* map, const KEY& k, VALUE&& v) : handle_base_type::HandleBase(map, k, std::move(v)) {
				INIT_LIST_HEAD(&node_);
			}

			const Handle& operator=(const Handle& handle) = delete;
			// {
			// 	INIT_LIST_HEAD(&node);
			// 	this->mapParent_ = handle.mapParent_;
			// 	this->key = handle.key;
			// 	copy(&handle.node);
			// 	return *this;
			// }
			const Handle& operator=(Handle&& handle) = delete;
			Handle(const Handle& handle) = delete;
			// {
			// 	INIT_LIST_HEAD(&node);
			// 	this->mapParent_ = handle.mapParent_;
			// 	this->key = handle.key;
			// 	copy(&handle.node);
			// }
			Handle() = delete;

			virtual void MakeDead() {
				handle_base_type::MakeDead();
			}
		};
	} // namespace con_map concurrent_map

	template <typename KEY, typename VALUE, class Manager, typename Allocator>
	class ConcurrentLinkedHashMap {
	public:
		template<typename K, typename V, typename M> friend class HandleBase;

		typedef KEY key_type;
		typedef VALUE value_type;

		/* or: */
		// typedef Handle<KEY, VALUE> Handle_type;
		// typedef tbb::concurrent_hash_map<KEY, Handle*> ConcurrentMap;
		// typedef typename ConcurrentMap::accessor accessor;
		// typedef typename ConcurrentMap::const_accessor const_accessor;
		using handle_base_type = con_map::HandleBase<KEY, VALUE, Manager>;
		using handle_type = con_map::Handle<KEY, VALUE, Manager>;
		using ConcurrentMap = tbb::concurrent_hash_map<KEY, handle_base_type*>;
		using accessor = typename ConcurrentMap::accessor;
		using const_accessor = typename ConcurrentMap::const_accessor;


	public:
		struct QueueNode;

	private:
		Manager* manager_;
		bool should_stop_;	// 父线程/进程退出，此时不再需要后台删除
		bool has_shutdown_;	// 表示后台线程成功退出，可以正常析构，不然要等后台线程退出才能析构

		// 上层接口同步修改 map_，被删除的节点会放到异步驱逐队列 deleted_queue_, Handle* 被后台线程 delete；put 时 Handle* 被 new
		tbb::concurrent_hash_map<KEY, handle_base_type*> map_;
		size_t max_size_{ DEFAULT_MAX_SIZE };

		/**
		 * @brief 修改完 map_ 后，修改链表 in_use_list_ 异步操作，先把操作记录进 async_queue_，在慢慢合并至 in_use_list_。
		 * async_queue_ 是个数组 async_queue_[CPU_num]，避免多线程访问同一个 ConcurrentQueue 竞争
		 * async_queue_mutex_ 很少用
		 */
		enum class OP { PUT, GET, UPDATE, DEL };
		rr::ConcurrentQueue<QueueNode>** async_queue_;
		size_t thread_num_;
		std::mutex async_queue_mutex_;
		thread async_queue_thread_;

		/**
		 * @brief 先进先出列表，read 操作会把已经在里面的取到最后面，write 会直接追加至后面，从前面 pop 删除
		 * in_use_list_mutex_ 用于保护这几个成员
		 */
		::list_node in_use_list_;
		std::atomic<size_t> in_use_list_size_;
		std::mutex in_use_list_mutex_;

		rr::ConcurrentQueue<handle_base_type*> deleted_queue_;
		std::atomic<size_t> deleted_size_;
		thread deleted_queue_thread_;


		void AsyncRWListThread() {
			int count = 0;

			while (!should_stop_) {
				for (auto i = 0; i < thread_num_; i++) {
					QueueNode front;
					bool res = async_queue_[i]->pop(front);
					if (res) {
						std::unique_lock<std::mutex> lck(in_use_list_mutex_);
						use(front);
					}
					else {
						// sleep(1);
					}
				}
				count++;

				handle_base_type* handle;
				bool res = deleted_queue_.pop(handle);
				if (res) {
					if (handle->deleted_.load() == true || handle->Evicted()) {
						assert(!handle->Evicted());
						delete_handle((handle_type*)handle);
					}
				}
			}
			has_shutdown_ = true;
			return;
		}

	public:
		ConcurrentLinkedHashMap() { ConcurrentLinkedHashMap(SIZE_MAX); };
		ConcurrentLinkedHashMap(size_t max_size) : in_use_list_size_(0), thread_num_(std::thread::hardware_concurrency())
			, deleted_queue_(true)
			, should_stop_(false)
			, has_shutdown_(false)
			, manager_(nullptr) {
			INIT_LIST_HEAD(&in_use_list_);
			assert(in_use_list_size_ <= max_size_);
			max_size_ = max_size;

			async_queue_ = new rr::ConcurrentQueue<QueueNode>*[thread_num_];
			for (auto i = 0; i < thread_num_; i++) {
				async_queue_[i] = new rr::ConcurrentQueue<QueueNode>(true);
				assert(async_queue_[i] != nullptr);
			}
			// std::cout << thread_num_ << " cores" << std::endl;

			async_queue_thread_ = thread(&ConcurrentLinkedHashMap::AsyncRWListThread, this);
			async_queue_thread_.detach();
		}
		~ConcurrentLinkedHashMap() {
			should_stop_ = true;
			// deleted_queue_.clear();
			// 需要等后台线程成功退出
			while (!has_shutdown_);

			for (auto i = 0; i < thread_num_; i++) { async_queue_[i]->set_stop(); }
			for (auto i = 0; i < thread_num_; i++) { delete async_queue_[i]; }

			clear_map();
		}

		bool Put(const KEY& key, const VALUE& value, VALUE& prior, int thread_id = 0) { return         put(key, value, false, prior, thread_id); }
		bool Put(const KEY& key, VALUE&& value, VALUE& prior, int thread_id = 0) { return              put(key, std::move(value), false, prior, thread_id); }

		bool PutIfAbsent(const KEY& key, const VALUE& value, VALUE& prior, int thread_id = 0) { return put(key, value, true, prior, thread_id); }
		bool PutIfAbsent(const KEY& key, VALUE&& value, VALUE& prior, int thread_id = 0) { return      put(key, std::move(value), true, prior, thread_id); }

		/**
		 * @brief
		 *
		 * @param key
		 * @param prior
		 * @param thread_id
		 * @return true, has old value and delete successfully
		 * @return false
		 */
		bool Remove(const KEY& key, VALUE& prior, int thread_id = 0) {
			accessor acc;
			bool find = map_.find(acc, key);
			handle_base_type* handle = nullptr;
			if (find) {
				handle = acc->second;
				bool deleted = map_.erase(acc);

				prior = handle->value_;
				assert(handle->weight_ > 0);
				handle->MakeDead();
				afterTask(handle, OP::DEL, thread_id);
			}

			return find;
		}

		bool Get(const KEY& key, VALUE& result, int thread_id = 0) {
			const_accessor c_access;
			bool find = map_.find(c_access, key);
			if (find) {
				assert(c_access->second->IsAlive());
				afterTask(const_cast<handle_base_type*>(c_access->second), OP::GET, thread_id);
				result = c_access->second->value_;
			}
			return find;
		}

		bool GetQuietly(const KEY& key, VALUE& result) {
			const_accessor c_access;
			bool find = map_.find(c_access, key);
			if (find) {
				result = c_access->second->value_;
			}
			return find;
		}

		void Print() {
			// assert(map_.size() >= in_use_list_size_);
			for (auto iter = map_.begin(); iter != map_.end(); iter++) {}
		}

		int ThreadNum() { return this->thread_num_; }
		rr::ConcurrentQueue<QueueNode>**& AsyncQueue() { return async_queue_; }
		std::mutex& in_use_list_mutex() { return in_use_list_mutex_; }
		tbb::concurrent_hash_map<KEY, handle_base_type*>& map() { return map_; }
		Manager*& manager() { return manager_; }

	private:
		/**
		 * @brief
		 *
		 * @param key
		 * @param value
		 * @param onlyIfAbsent
		 * @param prior 旧值的引用，当返回值false, prior 才能访问，否则未定义
		 * @param thread_id 认为的线程ID，为了 async_queue_ 减少竞争
		 * @return true 说明没有旧值，是新加入的
		 * @return false
		 */
		template<typename _Arg>
		bool put(const KEY& key, _Arg&& value, bool onlyIfAbsent, VALUE& prior, int thread_id) {
			// std::cout << "put: " << "key: " << key << ", value: " << string((char*)value, 10) << std::endl;

			accessor acc;
			bool is_new = map_.insert(acc, key);
			if (is_new) {
				handle_base_type* handle = new handle_type(this, key, std::forward<_Arg>(value));
				size_of_newhandle_.fetch_add(1);
				assert(acc->second == nullptr);
				acc->second = handle;
				afterTask(acc->second, OP::PUT, thread_id);
				return is_new;
			}
			else if (onlyIfAbsent) {	// 仅读旧值，不写
				prior = acc->second->value_;
				return is_new;
			}
			// 需要更新 is_new==false, and onlyIfAbsent==false
			assert(acc->second->IsAlive());
			prior = acc->second->value_;
			acc->second->value_ = value;
			afterTask(acc->second, OP::GET, thread_id);
			return is_new;
		}

		// need lock in_use_list_mutex_ before call this
		// 头部表示最老的，尾部是最新的
		void use(QueueNode& node) {
			handle_type* handle = (handle_type*)(node.handle_);

			{	// some check
				/*debug for check*/ assert(handle->RefSize() > 0);
			}

			if (node.op_ == OP::DEL) {
				assert(handle->IsDead());
				if (node_in_list(&handle->node_)) {
					list_del(&handle->node_);
					in_use_list_size_.fetch_sub(1);
				}
				else assert(handle->node_.prev == &handle->node_ && handle->node_.next == &handle->node_);
				handle->Unref();
				// handle->MakeEvict();
				handle->deleted_.store(true);
			}
			else if (node.op_ == OP::PUT) {
				if (handle->IsAlive() && !handle->deleted_.load()) {
					assert(!node_in_list(&handle->node_));
					list_add_tail(&handle->node_, &in_use_list_);
					in_use_list_size_.fetch_add(1);
					// std::cout << handle->key_ << " add in list, list size: " << in_use_list_size_.load() << std::endl;
				}
				handle->Unref();
				evict();
			}
			else if (node.op_ == OP::GET) {
				if (handle->IsAlive() && !handle->deleted_.load()) {
					assert(!handle->Evicted());
					if (node_in_list(&handle->node_)) { list_move_tail(&handle->node_, &in_use_list_); }
				}
				handle->Unref();
			}

			if (handle->RefSize() <= 0 && handle->deleted_.load()) {
				deleted_queue_.push(std::move(node.handle_));
			}
		}

		// need lock in_use_list_mutex_ before call this
		void evict() {
			::list_node* first = &in_use_list_;
			while (in_use_list_size_.load() >= max_size_) {
				first = first->next;
				handle_type* handle = list_entry(first, handle_type, node_);

				if (first == &in_use_list_) { return; }
				if (handle->RefSize() > 0) { continue; }
				else {
					// std::cout << "ready evict key: " << handle->key_ << std::endl;
					size_t before = handle->Unref();
					if (before > 0) { handle->Ref(); continue;}

					accessor acc;
					bool find = map_.find(acc, handle->key_);
					if (find) {
						map_.erase(acc);
						assert(handle->weight_ > 0);
						handle->MakeDead();
					}

					assert(node_in_list(first));
					list_del(first);
					first->prev = first->next = first;
					in_use_list_size_.fetch_sub(1);


					handle->MakeEvict();
					assert(!handle->deleted_.load());
					// deleted_queue_.push(std::move(handle));	// 异步
					delete_handle(handle);					// 同步
				}
			}
		}

		void afterTask(handle_base_type* handle, OP op, int thread_id) {
			QueueNode node(handle, op);
			size_t before = handle->Ref();
			if(before < 0) return;		// 可能被 evict
			// async_queue_[syscall(SYS_gettid) % thread_num_]->push(std::move(node));
			async_queue_[thread_id % thread_num_]->push(std::move(node));
		}

		void delete_handle(handle_type* handle) {
			if (manager_) { manager_->Delete(handle); }
			delete handle; handle = nullptr;
			size_of_newhandle_.fetch_sub(1);
		}

		// for debug
		std::atomic<size_t> size_of_newhandle_{ 0 };

		void clear_map() {
			for(auto iter = map_.begin(); iter != map_.end(); iter++) {
				handle_type* handle = (handle_type*)iter->second;
				delete handle;
				size_of_newhandle_.fetch_sub(1);
				in_use_list_size_.fetch_sub(1);
			}
			INIT_LIST_HEAD(&in_use_list_);
			map_.clear();
		}

		static string OPToChar(OP op) {
			if (op == OP::PUT) { return string("OP::PUT"); }
			if (op == OP::GET) { return string("OP::GET"); }
			if (op == OP::DEL) { return string("OP::DEL"); }
			if (op == OP::UPDATE) { return string("OP::PUT"); }
			return string("NULL");
		}

		void wait_for_asyc_done() {
			while (1) {
				bool done = true;
				for (int i = 0; i < ThreadNum(); i++) {
					if (async_queue_[i]->size() > 0) {
						done = false;
						/*debug for print*/ cout << async_queue_[i]->size() << endl;
						/*debug for print*/ std::this_thread::sleep_for(chrono::seconds(1));
					}
				}
				if (deleted_queue_.size() > 0) { done = false; }
				if (done == true) return;
			}
		}

	public:
		struct QueueNode {
		public:
			QueueNode(handle_base_type* handle, OP op) : handle_(handle), op_(op) { };
			QueueNode& operator=(const QueueNode& node) {
				this->handle_ = node.handle_;
				this->op_ = node.op_;
				return *this;
			}
			QueueNode(QueueNode&& node) {
				this->handle_ = node.handle_;
				this->op_ = node.op_;
			}
			QueueNode(const QueueNode& node) {
				this->handle_ = node.handle_;
				this->op_ = node.op_;
			}
			QueueNode() {}
			~QueueNode() { /* no need free handle_*/ }

			handle_base_type* handle_;
			OP op_;
			/**
			 * @brief 多个线程指向同一个 handle_ 时，只要有一个线程 delete handle_，
			 * 其他线程不能用 if(handle_ == nullptr) 判断，只能通过 handle_exist_ 判断 handle_ 是否被释放
			 */
			 // bool handle_exist_;
		};

		void check() {
			wait_for_asyc_done();
			/*debug for print*/ std::cout << "map_size/handle_num/list_size: " << map_.size() << ", " << size_of_newhandle_ << ", " << in_use_list_size_.load() << std::endl;
			assert(map_.size() == in_use_list_size_.load());
			assert(map_.size() == size_of_newhandle_.load());

			for (::list_node* node = &in_use_list_; node->next != &in_use_list_; node = node->next) {
				/*debug for check*/assert(node->next->prev == node);
			}
		}
	};
}

#endif // CONCURRENT_LINKED_HASH_MAP