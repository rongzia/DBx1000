/*
  Copyright (c) 2019 Sogou, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  Authors: Wu Jiaxu (wujiaxu@sogou-inc.com)
*/

#ifndef _LRUCACHE_H_
#define _LRUCACHE_H_
#include <assert.h>
#include <mutex>
#include <tbb/concurrent_hash_map.h>
#include <atomic>

/**
 * @file   LRUCache.h
 * @brief  Template LRU Cache
 */

// RAII: NO. Release ref by LRUCache::release
// Thread safety: NO.
// DONOT change value by handler, use Cache::put instead
template<typename KEY, typename VALUE>
class LRUHandle
{
public:
	VALUE value;

private:
	LRUHandle():
		prev(NULL),
		next(NULL),
		ref(0)
	{}

	LRUHandle(const KEY& k, const VALUE& v):
		value(v),
		key(k),
		prev(NULL),
		next(NULL),
		ref(0),
		in_cache(false),
        in_use(false)
	{}

	LRUHandle(const KEY& k, VALUE&& v):
		value(std::move(v)),
		key(std::move(k)),
		prev(NULL),
		next(NULL),
		ref(0),
		in_cache(false),
        in_use(false)
	{}
	//ban copy constructor
	LRUHandle(const LRUHandle& copy);
	//ban copy operator
	LRUHandle& operator= (const LRUHandle& copy);
	//ban move constructor
	LRUHandle(LRUHandle&& move);
	//ban move operator
	LRUHandle& operator= (LRUHandle&& move);

	KEY key;
	LRUHandle *prev;
	LRUHandle *next;
	std::atomic_uint64_t ref;
	std::atomic_bool in_cache;
    std::atomic_bool in_use;

template<typename, typename, class> friend class LRUCache2;
};

// RAII: NO. Release ref by LRUCache::release
// Define ValueDeleter(VALUE& v) for value deleter
// Thread safety: YES
// Make sure KEY operator< usable
template<typename KEY, typename VALUE, class ValueDeleter>
class LRUCache2
{
protected:
//using Map = std::unordered_map<KEY, VALUE>;
//using Handle = LRUHandle<KEY, VALUE>;
//using Map = std::map<KEY, LRUHandle*>;
//using MapIterator = typename Map::iterator;
//using MapConstIterator = typename Map::const_iterator;
typedef LRUHandle<KEY, VALUE>			                Handle;
typedef tbb::concurrent_hash_map<KEY, Handle*>			ConcurrentMap;
typedef typename ConcurrentMap::const_accessor			ConstAccessor;
typedef typename ConcurrentMap::accessor	    		Accessor;


public:
typedef VALUE value_type;
	LRUCache2():
		max_size_(0),
		size_(0)
	{
		not_use_.next = &not_use_;
		not_use_.prev = &not_use_;
		in_use_.next = &in_use_;
		in_use_.prev = &in_use_;
	}

	~LRUCache2()
	{   // 非线程安全
		// Error if caller has an unreleased handle
		assert(in_use_.next == &in_use_);
		for (Handle *e = not_use_.next; e != &not_use_; )
		{
			Handle *next = e->next;

			assert(e->in_cache);
			e->in_cache = false;
			assert(e->ref == 1);// Invariant for not_use_ list.
			unref(e);
			e = next;
		}
	}

	// default max_size=0 means no-limit cache
	// max_size means max cache number of key-value pairs
	void set_max_size(size_t max_size)
	{
        // 非线程安全
		max_size_ = max_size;
	}

	size_t get_max_size() const { return max_size_; }
	size_t size() const { return size_; }

	// Remove all cache that are not actively in use.
	void prune()
	{
		while (not_use_.next != &not_use_)
		{
			Handle *e = not_use_.next;

			assert(e->ref == 1);
			cache_map_.erase(e->key);
			erase_node(e);
		}
	}

	// release handle by get/put
	void release(Handle *handle)
	{
		if (handle)
		{
			unref(handle);
		}
	}

	void release(const Handle *handle)
	{
		release(const_cast<Handle *>(handle));
	}

	// get handler
	// Need call release when handle no longer needed
	const Handle *get(const KEY& key)
	{
        // Accessor accessor;
        ConstAccessor accessor;

		if (cache_map_.find(accessor, key))
		{
			ref(accessor->second);
			return accessor->second;
		}

		return NULL;
	}

	// put copy
	// Need call release when handle no longer needed
	const Handle *put(const KEY& key, const VALUE& value)
	{
		Handle *e = new Handle(key, value);

		e->ref = 1;
		e->in_cache.store(true);
		e->ref.fetch_add(1);
		
		Accessor accessor;
		int res = cache_map_.insert(accessor, make_pair(key, e));
		if(res) { } else {
			assert(!accessor.empty());
			erase_node(accessor->second);
			accessor->second = e;
		}
        std::unique_lock<std::mutex> lock(mutex_);
		list_append(&in_use_, e);
        lock.unlock();

		accessor.release();
		size_.fetch_add(1);

		// if (max_size_ > 0)
		// {
		// 	while (size_ > max_size_ && not_use_.next != &not_use_)
		// 	{
		// 		Handle *old = not_use_.next;

		// 		assert(old->ref == 1);
		// 		cache_map_.erase(old->key);
		// 		erase_node(old);
		// 	}
		// }

		return e;
	}
	// put move
	// Need call release when handle no longer needed
	const Handle *put(const KEY& key, VALUE&& value)
	{
		Handle *e = new Handle(key, std::move(value));

		e->ref = 1;
		

		e->in_cache.store(true);
		e->ref.fetch_add(1);
		
		Accessor accessor;
		int res = cache_map_.insert(accessor, make_pair(key, e));
		if(res) {} else {
			assert(!accessor.empty());
			erase_node(accessor->second);
			accessor->second = e;
		}
        std::unique_lock<std::mutex> lock(mutex_);
		list_append(&in_use_, e);
        lock.unlock();

		accessor.release();
		size_.fetch_add(1);

		// if (max_size_ > 0)
		// {
		// 	while (size_ > max_size_ && not_use_.next != &not_use_)
		// 	{
		// 		Handle *old = not_use_.next;

		// 		assert(old->ref == 1);
		// 		cache_map_.erase(old->key);
		// 		erase_node(old);
		// 	}
		// }

		return e;
	}
	// 由于外层还要封装一下该 LRUCache，即 LRUCache 使用是会被特化，比如：`LRUCache<PageKey, Page, PageDeleter> buffer_pool_;`
	// 此时不能用第一种方法来转发，因为通用引用需要两个条件：
	// 1. 参数形只能为 T&&，`const T&&` 也不行。
	// 2. 需要用到类型推断，被特化后，下面第一种方法是行不通的，因为会被特化为 `universal_put1(PageKey key, Page&& value)`，这不是通用引用，而是特定的右值引用。
	// template<typename T1, typename T2>
	// const Handle *universal_put1(T1 key, T2&& value) {
	// 	return put(key, std::forward<T2>(value)); // 只能调用 copy 函数，不能调用移动函数
	// }

	// 方法2，使用Args...，这会调用到类型推断，所以是通用引用，在封装的cache里面，可以直接用 `buffer_pool_.universalPut(key, std::forward<T>(value));`
	template <class... Args>
	const Handle *universalPut(Args&&... args) {
		return put(std::forward<Args>(args)...);
	}

	// delete from cache, deleter delay called when all inuse-handle release.
	void del(const KEY& key)
	{
		Accessor accessor;
		if(cache_map_.find(accessor, key)) {
			Handle *node = accessor->second;
			cache_map_.erase(accessor);
			accessor.release();
			erase_node(node);
		}
	}

private:
	void list_remove(Handle *node)
	{
		node->next->prev = node->prev;
		node->prev->next = node->next;
	}

	void list_append(Handle *list, Handle *node)
	{
		node->next = list;
		node->prev = list->prev;
		node->prev->next = node;
		node->next->prev = node;
	}

	void ref(Handle *e)
	{
		if (e->in_cache && e->ref == 1)
		{
            // std::unique_lock<std::mutex> lock(mutex_);
			// list_remove(e);
			// list_append(&in_use_, e);
            // lock.unlock();
		}

		e->ref.fetch_add(1);
	}

	void unref(Handle *e)
	{
		// std::unique_lock<std::mutex> lock(mutex_);
		assert(e->ref > 0);

		uint64_t res = e->ref.fetch_sub(1);
		if(1 == res) {
			assert(!e->in_cache);
			value_deleter_(e->value);
			delete e;
		}
		// else if (e->in_cache.load() && 2 == res)
		// {
		// 	list_remove(e);
		// 	list_append(&not_use_, e);
		// }
		// lock.unlock();
	}

	void erase_node(Handle *e)
	{
        // std::unique_lock<std::mutex> lock(mutex_);
		assert(e->in_cache.load());
		e->in_cache.store(false);
		size_.fetch_sub(1);
		// list_remove(e);
        // lock.unlock();
		unref(e);
	}

	size_t max_size_;
	std::atomic<size_t> size_;

	Handle not_use_;
	Handle in_use_;
	std::mutex mutex_not_use_;
	std::mutex mutex_in_use_;
	std::mutex mutex_;
	ConcurrentMap cache_map_;

	ValueDeleter value_deleter_;
};

#endif  // SSS_LRUCACHE_H_

