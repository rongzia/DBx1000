//
// Created by rrzhang on 2020/4/9.
//

#include <cassert>
#include <cstring>
#include <string>
#include <sys/mman.h>
#include "buffer.h"

#include "common/global.h"
#include "server/buffer/lru_index.h"
#include "server/buffer/lru.h"
#include "util/make_unique.h"
#include "util/numbercomparator.h"


namespace dbx1000 {

    Buffer::Buffer(uint64_t total_size, size_t row_size)
            : total_size_(total_size)
              , row_size_(row_size)
              , num_item_(total_size / row_size) {
        ptr_ = mmap(NULL, total_size_, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);
        row_list_ = new LRU(row_size_);
        free_list_ = new LRU(row_size_);
        lru_index_ = new LruIndex();

        /// initital free list
        for (int i = 0; i < num_item_; i++) {
            RowNode* row_node = new RowNode((char *) ptr_ + i * row_size_);
            free_list_->Prepend(row_node);
        }
        assert(free_list_->Size() == num_item_);
        assert(row_list_->Size() == 0);

#ifdef USE_MEMORY_DB
        db_ = new MemoryDB();
//        char buf[row_size_];
//        memset(buf, 0, row_size_);
        for(size_t i = 0; i < DB_NUM_ITEM; i++) {
            lru_index_->IndexInsert(i, nullptr);
//            db_->Put(i, buf, row_size_);
        }
#else
        leveldb::Options options;
        options.create_if_missing = true;
        options.comparator = new NumberComparatorImpl();
        leveldb::Status status = leveldb::DB::Open(options, g_db_path, &db_);
        assert(status.ok());

        /// db check and init lru_index_
        for(int i = 0; i < g_synth_table_size; i++) {
            lru_index_->IndexInsert(i, nullptr);
        }
//        size_t count = 0;
//        leveldb::Iterator *iter = db_->NewIterator(leveldb::ReadOptions());
//        iter->SeekToFirst();
//        while (iter->Valid()) {
//            assert(count == std::stoi(iter->key().ToString()));
//            assert(row_size == iter->value().size());
//            iter->Next();
//            count++;
//        }
//        std::cout << "db size : " << count << std::endl;
#endif
    }

    Buffer::~Buffer() {
        /// 使用智能指针创建 buffer 对象时，不需要显示 free
        FlushALl();
        delete free_list_;
        delete row_list_;
        delete lru_index_;
        delete db_;
//        delete comparator_;
        if(nullptr != ptr_) {
            munmap(ptr_, total_size_);
        }
    }

    void Buffer::FlushRowList() {
        assert(free_list_->Size() == 0);
        assert(row_list_->Size() == num_item_);

        /// 只刷链表后 1/5 的数据
        for (int i = 0; i < num_item_ / 5; i++) {
            RowNode* row_node = row_list_->Popback();
#ifdef USE_MEMORY_DB
            db_->Put(row_node->key_, row_node->row_, row_size_);
#else
            leveldb::Slice slice(row_node->row_, row_size_);
            leveldb::Status status = db_->Put(leveldb::WriteOptions(), std::to_string(row_node->key_), slice);
            assert(status.ok());
#endif
            lru_index_->IndexInsert(row_node->key_, nullptr);
            row_node->key_ = UINT64_MAX;
            free_list_->Prepend(row_node);
        }
    }
    
    void Buffer::FlushALl() {
        RowNode* row_node;
        int row_list_size = row_list_->Size();
        for(int i = 0; i < row_list_size; i++) {
            row_node = row_list_->Popback();
#ifdef USE_MEMORY_DB
            db_->Put(row_node->key_, row_node->row_, row_size_);
#else
            leveldb::Slice slice(row_node->row_, row_size_);
            leveldb::Status status = db_->Put(leveldb::WriteOptions(), std::to_string(row_node->key_), slice);
            assert(status.ok());
#endif
            lru_index_->IndexInsert(row_node->key_, nullptr);
            row_node->key_ = UINT64_MAX;
            free_list_->Prepend(row_node);
        }
    }

    void Buffer::FreeListToRowList(uint64_t key, const void* row, size_t count) {
        assert(free_list_->Size() > 0);
        assert(row_list_->Size() < num_item_);

        RowNode *row_node = free_list_->Popback();
        row_node->key_ = key;
        std::memcpy(row_node->row_, row, count);
        row_list_->Prepend(row_node);
        lru_index_->IndexInsert(key, row_node);
    }

    int Buffer::BufferGet(uint64_t key, void *buf, size_t count) {
        std::lock_guard<std::mutex> lock(mutex_);

        assert(row_list_->Size() + free_list_->Size() == num_item_);
        IndexFlag flag;
        RowNode *row_node = lru_index_->IndexGet(key, &flag);

        /// no such row in db
        if(IndexFlag::NOT_EXIST == flag) {
            assert(nullptr == row_node);
            return -1;
        }
        /// row in buffer
        if(IndexFlag::IN_BUFFER == flag) {
            assert(nullptr != row_node);
            row_list_->Get(row_node);
            memcpy(buf, row_node->row_, count);
            return 0;
        }
        /// row in disk
        if(IndexFlag::IN_DISK == flag) {
            assert(nullptr == row_node);
#ifdef USE_MEMORY_DB
            db_->Get(key, buf, count);
#else
            std::string temp;
            leveldb::Status status = db_->Get(leveldb::ReadOptions(), std::to_string(key), &temp);
            assert(status.ok());
            memcpy(buf, temp.data(), count);
#endif
            if (free_list_->Size() <= 0) {
                FlushRowList();
            }
            FreeListToRowList(key, buf, count);
            return 0;
        }

        assert(false);      /// 不应该到达这一步
    }

    int Buffer::BufferPut(uint64_t key, const void* buf, size_t count) {
        std::lock_guard<std::mutex> lock(mutex_);

        assert(row_list_->Size() + free_list_->Size() == num_item_);
        IndexFlag flag;
        RowNode *row_node = lru_index_->IndexGet(key, &flag);

        /// row in buffer
        if (IndexFlag::IN_BUFFER == flag) {
            assert(nullptr != row_node);
            row_list_->Get(row_node);
            std::memcpy(row_node->row_, buf, count);
            return 0;
        }
        if(IndexFlag::NOT_EXIST == flag || IndexFlag::IN_DISK == flag) {
            assert(nullptr == row_node);
            if(free_list_->Size() <= 0) {
                FlushRowList();
            }
            FreeListToRowList(key, buf, count);
            return 0;
        }

        assert(false);      /// 不应该到达这一步
    }

    void Buffer::Print(){
        std::cout << "total_size:" << total_size_ << ", row_size:" << row_size_ << ", num_item_" << num_item_ << std::endl;

#ifdef USE_MEMORY_DB
        db_->Print();
#else
        leveldb::Iterator *iter = db_->NewIterator(leveldb::ReadOptions());
        std::cout << "db : {" << std::endl;
        iter->SeekToFirst();
        int count = 0;
        iter->SeekToFirst();
        while (iter->Valid()) {
            if (count % 10 == 0) { std::cout << "    "; }
            std::cout << ", {key:" << iter->key().ToString() << ", row:" << iter->value().ToString() << "}";
            if (++count % 10 == 0) { std::cout << std::endl; }
            iter->Next();
        }
        std::cout << std::endl << "}" << std::endl;
#endif
        lru_index_->Print();
        std::cout << "row_lsit_: " ;
        row_list_->Print();
        std::cout << "free_list_: " ;
        free_list_->Print();
    }
}