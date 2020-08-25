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
    }

    LRU::LRU() : size_(ATOMIC_VAR_INIT(0)) {
        head_ = nullptr;
        tail_ = nullptr;
    }

    LRU::~LRU() {
        PageNode* page_node = head_;
        while(nullptr != page_node) {
            delete page_node;
            page_node = page_node->next_;
        }
    }

    void LRU::Prepend(PageNode* page_node) {
#if defined(USE_CAS)
        for(;;){
            PageNode* old_head = head_;
            if(head_ == nullptr){
                if (head_.compare_exchange_weak(old_head, page_node)){
                    tail_ = page_node;
                    size_.fetch_add(1);
                    return;
                }
            } else {
                page_node->next_ = old_head;
                if(head_.compare_exchange_weak(old_head, page_node)){
                    old_head->prev_ = page_node;
                    size_.fetch_add(1);
                    return;
                }
            }
        }
#else
#if defined(USE_MUTEX)
        mtx_.lock();
#endif // USE_MUTEX
        assert(page_node->next_ == nullptr && page_node->prev_ == nullptr);
        if(head_ == nullptr) {
            head_ = page_node;
            tail_ = page_node;
        } else{
            page_node->next_ = head_;
            head_->prev_ = page_node;
            head_ = page_node;
        }
        size_.fetch_add(1);

#if defined(USE_MUTEX)
        mtx_.unlock();
#endif // USE_MUTEX
#endif // USE_CAS
    }

    void LRU::Get(PageNode* page_node) {
#if defined(USE_MUTEX)
        mtx_.lock();
#endif // USE_MUTEX
        assert(page_node->prev_ != nullptr || page_node->next_ != nullptr || page_node == head_);
        if(page_node == head_ && page_node == tail_) { }
        else if(page_node == head_) {}
        else if(page_node == tail_) {
            page_node->next_ = head_;
            head_->prev_ = page_node;
            head_ = head_->prev_;
            tail_ = tail_->prev_;
            tail_->next_ = nullptr;
            page_node->prev_ = nullptr;
        } else {
            page_node->prev_->next_ = page_node->next_;
            page_node->next_->prev_ = page_node->prev_;

            page_node->next_ = head_;
            page_node->prev_ = nullptr;
            head_->prev_ = page_node;
            head_ = page_node;
        }
#if defined(USE_MUTEX)
        mtx_.unlock();
#endif // USE_MUTEX
        return;
    }

    PageNode* LRU::Popback() {
#if defined(USE_CAS)
        PageNode* old_tail = tail_;
        for(;;){
            if(tail_.compare_exchange_weak(old_tail, old_tail->prev_)) {
                if(head_ == old_tail){ head_ = nullptr; }
                else{ tail_.load()->next_ = nullptr; }
                old_tail->prev_ = nullptr;
                size_.fetch_sub(1);
                return old_tail;
            }
        }
#else
#if defined(USE_MUTEX)
        mtx_.lock();
#endif // USE_MUTEX
        if(tail_ == nullptr) { return nullptr;}
        PageNode* pageNode = tail_;
        if(nullptr != pageNode->prev_){
            tail_ = pageNode->prev_;
            tail_->next_ = nullptr;
            pageNode->prev_ = nullptr;
        } else {
            assert(head_ == tail_);
            head_ = tail_ = nullptr;
        }
        size_.fetch_sub(1);
#if defined(USE_MUTEX)
        mtx_.unlock();
#endif // USE_MUTEX
        return pageNode;
#endif // USE_CAS
    }

    int LRU::size() { return size_; };

    void LRU::Check(){
 //        std::cout << "size : " << size_ << std::endl;
        if(size_ > 0) { assert(nullptr == head_->prev_ && nullptr == tail_->next_); }
        else { assert(nullptr == head_ && nullptr == tail_); }

        int count = 0;
        /// 从 head_ 开始验证
        PageNode* pageNode = head_;
        while(nullptr != pageNode){
 //        std::cout << pageNode->page_->page_id() << " " << *(uint64_t *)(pageNode->page_->page_buf()) << std::endl;
            count++;
            if(pageNode->next_ != nullptr){ assert(pageNode->next_->prev_ == pageNode); }
            else{ assert(pageNode == tail_ && count == size_); }
            pageNode = pageNode->next_;
        }
        assert(count == size_);

        /// 从 tail_ 开始验证
        count = 0;
        pageNode = tail_;
        while(nullptr != pageNode) {
            count++;
            if(pageNode->prev_ != nullptr) { assert(pageNode->prev_->next_ == pageNode); }
            else { assert(pageNode == head_ && count == size_); }
            pageNode = pageNode->prev_;
        }
        assert(count == size_);
    }
} // namespace dbx1000