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

namespace dbx1000 {

    /// 测试使用，实际使用 leveldb
    class DB {
    public:
        void Put(uint64_t key, std::string value){
            auto iter = db_.find(key);
            if(db_.end() != iter) {
                db_.erase(iter);
            }
            db_.insert(std::pair<uint64_t, std::string>(key, value));
        }
        std::string Get(uint64_t key, std::string *value) {
            return (db_.end() != db_.find(key) ? db_[key] : nullptr);
        }

        void Print(){
            std::cout << "db : {" << std::endl;
            int count = 0;
            for (auto &iter : db_) {
                if (count % 10 == 0) { std::cout << "    "; }
                std::cout << ", {key:" << iter.first << ", row:" << iter.second << "}";
                count++;
                if (count % 10 == 0) { std::cout << std::endl; }
            }
            std::cout << std::endl << "}" << std::endl;
        }
        std::unordered_map<uint64_t, std::string> db_;
    };

    class RowNode;
    class LRU;
    class LruIndex;

    class Buffer {

    public:
        Buffer(uint64_t total_size, int row_size);
        ~Buffer();

        std::string BufferGet(uint64_t key);
        void BufferPut(uint64_t key, std::string value);

        void Print();

//    private:
        /// 将 row_list_ 尾部部分数据刷到磁盘，这些节点被放到 free_list_ 中
        void FlushRowList();
        void FlushALl();
        /// 从 free_list_ 头部取出一个节点放到 row_list_ 中，同时设置节点的 key_ 和 row_
        void FreeListToRowList(uint64_t key, const std::string &value);

        uint64_t total_size_;               /// in bytes
        int row_size_;
        int num_item_;                      /// numbers of item, == total_size_ / row_size_

        std::unique_ptr<LRU> row_list_;     /// 被使用的链表
        std::unique_ptr<LRU> free_list_;    /// 空闲链表

        std::unique_ptr<LruIndex> lru_index_;   /// row_list_ 的索引，根据 key, 直接定位到相应的 RowNode
        std::unique_ptr<DB> db_;

        void *ptr;                          /// 内存池
    };
}

#endif //DBX1000_BUFFER_H
