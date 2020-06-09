//
// Created by rrzhang on 2020/4/15.
//
#include <memory>
#include <cassert>
#include <iostream>
#include "lru.h"

#include "common/storage/tablespace/page.h"

namespace dbx1000 {
    PageNode::PageNode() {
        prev_ = next_ = nullptr;
    }

    PageNode::PageNode(char* page_buf) {
        page_ = new Page(page_buf);
        prev_ = next_ = nullptr;
    }

    PageNode::~PageNode() {
        /// page_ 的空间是由缓存池提供的，不需要在这里释放，交给 buffer
//        delete page_;
    }

    LRU::LRU(int item_size)
            : size_(0) {
        head_ = new PageNode();
        tail_ = head_;
    }

    LRU::~LRU() {
        PageNode* page_node = head_->next_;
//        while(nullptr != page_node) {
//            delete page_node;
//            page_node = page_node->next_;
//        }
        delete head_->page_;
    }

    void LRU::Prepend(PageNode* page_node) {
        if(tail_ == head_) {
            tail_ = page_node;
        }

        page_node->prev_ = head_;
        page_node->next_ = head_->next_;
        if(nullptr != page_node->next_) {
            page_node->next_->prev_ = page_node;
        }
        page_node->prev_->next_ = page_node;

        size_++;
    }

    void LRU::Get(PageNode* page_node) {
        if(tail_ == page_node) {
            tail_ = tail_->prev_;
            tail_->next_ = nullptr;
            page_node->prev_ = page_node->next_ = nullptr;
            Prepend(page_node);
            size_--;
        } else {
//            assert(nullptr != page_node->next_);
//            assert(nullptr != page_node->prev_);
            page_node->prev_->next_ = page_node->next_;
            page_node->next_->prev_ = page_node->prev_;
            page_node->prev_ = page_node->next_ = nullptr;
            Prepend(page_node);
            size_--;
        }
    }

    PageNode* LRU::Popback() {
        assert(tail_ != head_);
        PageNode* temp = tail_;
        tail_ = tail_->prev_;
        tail_->next_ = nullptr;
        temp->prev_ = temp->next_ = nullptr;
        size_--;
        return temp;
    }

    int LRU::size() { return size_; };

    void LRU::DebugSize() {
        int count = 0;
        PageNode* page_node = head_->next_;
        while(nullptr != page_node) {
            count++;
            page_node = page_node->next_;
        }
        assert(size_ == count);
    }


    void LRU::Print() {
        std::cout << "list size:" << size_ << std::endl;
        PageNode* page_node = head_->next_;
        std::cout << "    head --> ";
        while(nullptr != page_node) {
            std::cout << "page_id:" << page_node->page_->page_id() << ", page_buf:" << std::string(page_node->page_->page_buf(), page_node->page_->page_size()) << ", addr of page_node:" << page_node << " --> ";
            page_node = page_node->next_;
        }
        if(nullptr == page_node){
            std::cout << "nullptr" << std::endl;
        }

        page_node = tail_;
        std::cout << "    ";
        while (page_node != head_) {
            std::cout << "page_id:" << page_node->page_->page_id() << ", page_buf:" << std::string(page_node->page_->page_buf(), page_node->page_->page_size()) << ", addr of page_node:" << page_node << " --> ";
            page_node = page_node->prev_;
        }
        if(page_node == head_) {
            std::cout << "head --> ";
            page_node = page_node->prev_;
        }
        if(nullptr == page_node) {
            std::cout << "nullptr" << std::endl;
        }
    }
} // namespace dbx1000