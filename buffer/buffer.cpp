//
// Created by rrzhang on 2020/4/9.
//

#include <cassert>
#include <cstring>
#include <string>
#include <sys/mman.h>
#include "buffer.h"

#include "lru_index.h"
#include "lru.h"
#include "../util/make_unique.h"

namespace dbx1000 {

    Buffer::Buffer(uint64_t total_size, int row_size)
            : total_size_(total_size)
              , row_size_(row_size)
              , num_item_(total_size / row_size) {
        row_list_ = make_unique<LRU>(row_size_);
        free_list_ = make_unique<LRU>(row_size_);
        db_ = make_unique<DB>();
        lru_index_ = make_unique<LruIndex>();
        ptr = mmap(NULL, total_size_, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);

        /// initital free list
        RowNode* row_node;
        for (int i = 0; i < num_item_; i++) {
            row_node = new RowNode((char *) ptr + i * row_size_);
            free_list_->Prepend(row_node);
        }
        row_node = nullptr;
        delete(row_node);
        assert(free_list_->Size() == num_item_);
        assert(row_list_->Size() == 0);
    }

    Buffer::~Buffer() {
        /// 使用智能指针创建 buffer 对象时，不需要显示 free
//        std::free(ptr);
        FlushALl();
    }

    void Buffer::FlushRowList() {
        assert(free_list_->Size() == 0);
        assert(row_list_->Size() == num_item_);
        RowNode *row_node;
        /// 只刷链表后 1/5 的数据
        for (int i = 0; i < num_item_ / 5; i++) {
            row_node = row_list_->Popback();
            db_->Put(row_node->key_, std::string(row_node->row_, row_size_));
            lru_index_->IndexInsert(row_node->key_, nullptr);
            row_node->key_ = UINT64_MAX;
            free_list_->Prepend(row_node);
        }
        row_node = nullptr;
        delete(row_node);
    }
    
    void Buffer::FlushALl() {
        RowNode* row_node;
        int row_list_size = row_list_->Size();
        for(int i = 0; i < row_list_size; i++) {
            row_node = row_list_->Popback();
            db_->Put(row_node->key_, std::string(row_node->row_, row_size_));
            lru_index_->IndexInsert(row_node->key_, nullptr);
            row_node->key_ = UINT64_MAX;
            free_list_->Prepend(row_node);
        }
    }

    void Buffer::FreeListToRowList(uint64_t key, const std::string &value) {
        assert(free_list_->Size() > 0);
        assert(row_list_->Size() < num_item_);

        RowNode *row_node = free_list_->Popback();
        row_node->key_ = key;
        std::memcpy(row_node->row_, value.data(), row_size_);
        row_list_->Prepend(row_node);
        lru_index_->IndexInsert(key, row_node);
        row_node = nullptr;
        delete(row_node);
    }

    std::string Buffer::BufferGet(uint64_t key) {
        assert(row_list_->Size() + free_list_->Size() == num_item_);
        IndexFlag flag;
        RowNode *row_node = lru_index_->IndexGet(key, &flag);

        /// no such row in db
        if(IndexFlag::NOT_EXIST == flag) {
            assert(nullptr == row_node);
            return nullptr;
        }
        /// row in buffer
        std::string value;
        if(IndexFlag::IN_BUFFER == flag) {
            assert(nullptr != row_node);
            row_list_->Get(row_node);
            value = std::string(row_node->row_, row_size_);
            return value;
        }
        /// row in disk
        if(IndexFlag::IN_DISK == flag) {
            assert(nullptr == row_node);
            /// leveldb->get(key)
            value = db_->Get(key, &value);
            if (free_list_->Size() <= 0) {
                FlushRowList();
            }
            FreeListToRowList(key, value);
            return value;
        }

        /// 不应该到达这一步
        assert(false);
    }

    void Buffer::BufferPut(uint64_t key, std::string value) {
        assert(row_list_->Size() + free_list_->Size() == num_item_);
        IndexFlag flag;
        RowNode *row_node = lru_index_->IndexGet(key, &flag);

        /// row in buffer
        if (IndexFlag::IN_BUFFER == flag) {
            assert(nullptr != row_node);
            row_list_->Get(row_node);
            std::memcpy(row_node->row_, value.data(), row_size_);
            return;
        }
        if(IndexFlag::NOT_EXIST == flag || IndexFlag::IN_DISK == flag) {
            assert(nullptr == row_node);
            if(free_list_->Size() <= 0) {
                FlushRowList();
            }
            FreeListToRowList(key, value);
            return;
        }

        /// 不应该到达这一步
        assert(false);
    }

    void Buffer::Print(){
        std::cout << "total_size:" << total_size_ << ", row_size:" << row_size_ << ", num_item_" << num_item_ << std::endl;
        db_->Print();
        lru_index_->Print();
        std::cout << "row_lsit_: " ;
        row_list_->Print();
        std::cout << "free_list_: " ;
        free_list_->Print();
    }
}