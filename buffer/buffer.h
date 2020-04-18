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
#include "leveldb/db.h"

namespace dbx1000 {

#ifdef USE_MEMORY_DB
    class MemoryDB {
    public:
        void Put(uint64_t key, const void* buf, size_t count){
            auto iter = db_.find(key);
            if(db_.end() != iter) {
                db_.erase(iter);
            }
            db_.insert(std::pair<uint64_t, std::string>(key, std::string((char*) buf, count)));
        }
        int Get(uint64_t key, void* buf, size_t count) {
            if(db_.end() == db_.find(key)) {
                return -1;
            }
            memcpy(buf, db_[key].data(), count);
            return 0;
        }

        void Print(){
            std::cout << "db : {" << std::endl;
            int count = 0;
            for (auto &iter : db_) {
                if (count % 10 == 0) { std::cout << "    "; }
                std::cout << ", {key:" << iter.first << ", row:" << iter.second << "}";
                if (count % 10 == 0) { std::cout << std::endl; }
                count++;
            }
            std::cout << std::endl << "}" << std::endl;
        }

        std::unordered_map<uint64_t, std::string> db_;
    };
#endif

    class RowNode;
    class LRU;
    class LruIndex;

    class Buffer {

    public:
        Buffer(uint64_t total_size, size_t row_size, std::string db_path);
        ~Buffer();

        int BufferGet(uint64_t key, void* buf, size_t count);
        int BufferPut(uint64_t key, const void* buf, size_t count);

        void Print();

//    private:
        /// 将 row_list_ 尾部部分数据刷到磁盘，这些节点被放到 free_list_ 中
        void FlushRowList();
        void FlushALl();
        /// 从 free_list_ 头部取出一个节点放到 row_list_ 中，同时设置节点的 key_ 和 row_
        void FreeListToRowList(uint64_t key, const void* row, size_t count);

        void *ptr;                          /// 内存池，存放 row 数据
        uint64_t total_size_;               /// size of ptr, in bytes
        size_t row_size_;
        int num_item_;                      /// number of items, == total_size_ / row_size_

        std::unique_ptr<LRU> row_list_;     /// 被使用的链表
        std::unique_ptr<LRU> free_list_;    /// 空闲链表
        std::unique_ptr<LruIndex> lru_index_;   /// row_list_ 的索引，根据 key, 直接定位到相应的 RowNode

#ifdef USE_MEMORY_DB
        std::unique_ptr<MemoryDB> db_;
#else
	    std::unique_ptr<leveldb::DB> db_;
	    std::string db_path_;
#endif

        std::mutex mutex_;
    };
}

#endif //DBX1000_BUFFER_H
