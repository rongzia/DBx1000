//
// Created by rrzhang on 2020/4/9.
//

#ifndef DBX1000_BUFFER_H
#define DBX1000_BUFFER_H

#include <cstdlib>
#include <string>
#include <unordered_map>
#include <memory>
#include <iostream>
#include <mutex>
#include "config.h"

namespace dbx1000 {
    class LRU;
    class LruIndex;

    class Buffer {
    public:
        Buffer(uint64_t total_size, size_t page_size);
        ~Buffer();

        int BufferGet(uint64_t page_id, void* buf, size_t count);
        int BufferPut(uint64_t page_id, const void* buf, size_t count);

        void Print();
        void FlushALl();

    private:
        /// 将 page_list_ 尾部部分数据刷到磁盘，这些节点被放到 free_list_ 中
        void FlushPageList();
        /// 从 free_list_ 头部取出一个节点放到 page_list_ 中，同时设置节点的 key_ 和 page_
        void FreeListToPageList(uint64_t page_id, const void* page, size_t count);

        void *ptr_;                          /// 内存池，存放 page 数据
        uint64_t total_size_;               /// size of ptr, in bytes
        size_t page_size_;
        int page_num_;                      /// number of pages, == total_size_ / page_size_

        LRU* page_list_;     /// 被使用的链表
        LRU* free_list_;    /// 空闲链表
        LruIndex* lru_index_;   /// page_list_ 的索引，根据 key, 直接定位到相应的 RowNode

        std::mutex mutex_;
    };
}

#endif //DBX1000_BUFFER_H
