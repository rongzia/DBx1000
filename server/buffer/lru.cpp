//
// Created by rrzhang on 2020/4/15.
//
#include <memory>
#include <cassert>
#include "lru.h"

namespace dbx1000 {
    RowNode::RowNode()
            : key_(UINT64_MAX)
            , row_(nullptr) {
        prev_ = next_ = nullptr;
    }

    RowNode::RowNode(char* row)
            : key_(UINT64_MAX)
            , row_(row){
        prev_ = next_ = nullptr;
    }

    LRU::LRU(int item_size)
            : size_(0)
            , item_size_(item_size){
        head_ = new RowNode();
        tail_ = head_;
    }

    LRU::~LRU() {
        std::cout << "~LRU" <<std::endl;
    }

    void LRU::Prepend(RowNode* row_node) {
        if(tail_ == head_) {
            tail_ = row_node;
            assert(nullptr == tail_->next_);
//            tail_->next_ = nullptr;
        }

        row_node->prev_ = head_;
        row_node->next_ = head_->next_;
        if(nullptr != row_node->next_) {
            row_node->next_->prev_ = row_node;
        }
        row_node->prev_->next_ = row_node;

        size_++;
    }

    void LRU::Get(RowNode* row_node) {
        if(tail_ == row_node) {
            tail_ = tail_->prev_;
            tail_->next_ = nullptr;
            row_node->prev_ = row_node->next_ = nullptr;
            Prepend(row_node);
            size_--;
        } else {
            assert(nullptr != row_node->next_);
            assert(nullptr != row_node->prev_);
            row_node->prev_->next_ = row_node->next_;
            row_node->next_->prev_ = row_node->prev_;
            row_node->prev_ = row_node->next_ = nullptr;
            Prepend(row_node);
            size_--;
        }
    }

    RowNode* LRU::Popback() {
        assert(tail_ != head_);
        RowNode* temp = tail_;
        tail_ = tail_->prev_;
        tail_->next_ = nullptr;
        temp->prev_ = temp->next_ = nullptr;
        size_--;
        return temp;
    }

    int LRU::Size() { return size_; };

    void LRU::DebugSize() {
        int count = 0;
        RowNode* rowNode = head_->next_;
        while(nullptr != rowNode) {
            count++;
            rowNode = rowNode->next_;
        }
        assert(size_ == count);
    }

    RowNode* LRU::Head() { return head_;};

    RowNode* LRU::Tail() { return tail_;};

    void LRU::Print() {
        std::cout << "list size:" << size_ << std::endl;
        RowNode* rowNode = head_->next_;
        std::cout << "    head --> ";
        while(nullptr != rowNode) {
            std::cout << rowNode->key_ << " : " << std::string(rowNode->row_, item_size_) << " : " << rowNode << " --> ";
            rowNode = rowNode->next_;
        }
        if(nullptr == rowNode){
            std::cout << "nullptr" << std::endl;
        }

        rowNode = tail_;
        std::cout << "    ";
        while (rowNode != head_) {
            std::cout << rowNode->key_ << " : " << std::string(rowNode->row_, item_size_) << " : " << rowNode << " --> ";
            rowNode = rowNode->prev_;
        }
        if(rowNode == head_) {
            std::cout << "head --> ";
            rowNode = rowNode->prev_;
        }
        if(nullptr == rowNode) {
            std::cout << "nullptr" << std::endl;
        }
    }
} // namespace dbx1000