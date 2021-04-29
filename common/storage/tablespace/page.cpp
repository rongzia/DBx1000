//
// Created by rrzhang on 2020/6/1.
//
#include <cassert>
#include <cstring>
#include <iostream>
#include "page.h"

namespace dbx1000 {
    // 禁止使用 Page(new char[])
    Page::Page(const char *buf, uint64_t size) { 
        // std::cout << "page(char *buf, uint64_t size)" << std::endl;
        Init();
        assert(size == MY_PAGE_SIZE);
        memcpy(this->page_buf_, buf, size);
        this->Deserialize();
    }

    Page::~Page() {
        // std::cout << "~page" << std::endl;
        if(this->page_size()!=0 && this->page_buf_!=nullptr) {
            delete [] page_buf_;
            page_buf_ = nullptr;
        }
    }

    Page::Page(const Page& page) {
        // std::cout << "page(&)" << std::endl;
        Init();
        assert(this->page_buf_ != nullptr && page.page_buf_ != nullptr);
        assert(this->page_size() == MY_PAGE_SIZE && page.page_size() == MY_PAGE_SIZE);
        // this->Print();
        memcpy(this->page_buf_, page.page_buf_, page.page_size());
        
        this->set_page_id(page.page_id_);
        this->set_used_size(page.used_size());
        this->set_version(page.version());
    }

    Page::Page(Page&& page) {
        // std::cout << "page(&&)" << std::endl;
        this->page_buf_ = page.page_buf_;
        this->set_page_id(page.page_id());
        this->set_page_size(page.page_size());
        this->set_used_size(page.used_size());
        this->set_version(page.version());

        page.page_buf_ = nullptr;
        page.set_page_size(0);
    }

    void Page::Init() {
        this->page_buf_ = new char[MY_PAGE_SIZE];
        this->set_page_size(MY_PAGE_SIZE);
        this->set_used_size(64);
    }

    int Page::PagePut(uint64_t page_id, const char *row_buf, size_t count) {
        assert(this->page_id_ == page_id);
        assert(used_size_ + count <= page_size_);
        memcpy(&page_buf_[this->used_size_], row_buf, count);
        this->used_size_ += count;

        return 0;
    }

    Page* Page::Serialize() {
        memcpy(&page_buf_[0 * sizeof(uint64_t)], reinterpret_cast<void *>(&page_id_), sizeof(uint64_t));
        memcpy(&page_buf_[1 * sizeof(uint64_t)], reinterpret_cast<void *>(&page_size_), sizeof(uint64_t));
        memcpy(&page_buf_[2 * sizeof(uint64_t)], reinterpret_cast<void *>(&used_size_), sizeof(uint64_t));
        memcpy(&page_buf_[3 * sizeof(uint64_t)], reinterpret_cast<void *>(&version_), sizeof(uint64_t));
        return this;
    }

    void Page::Deserialize() {
        page_id_ = *(uint64_t*)(&page_buf_[0 * sizeof(uint64_t*)]);
        page_size_ = *(uint64_t*)(&page_buf_[1 * sizeof(uint64_t*)]);
        used_size_ = *(uint64_t*)(&page_buf_[2 * sizeof(uint64_t*)]);
        version_ = *(uint64_t*)(&page_buf_[3 * sizeof(uint64_t*)]);
    }

    void Page::Print() const {
        std::cout << "page_id:" << page_id_ << ", page_size:" << page_size_ << ", used_size:" << used_size_
        << ", page_buf:" << std::string(&(page_buf_[64]), page_size_>74 ? 10:0)
        << ", version:" << version_ << std::endl;
    }


    void Page::set_page_id(uint64_t page_id) { this->page_id_ = page_id; }
    void Page::set_page_buf(const void *page_buf, size_t count) { memcpy(this->page_buf_, page_buf, count); }
    void Page::set_page_size(uint64_t page_size) { this->page_size_ = page_size; }
    void Page::set_used_size(uint64_t used_size) {this->used_size_ = used_size; }
    void Page::set_version(uint64_t version) { this->version_ = version; }
    uint64_t Page::page_id() const { return this->page_id_; }
    char *Page::page_buf() { return this->page_buf_; }
    const char *Page::page_buf_read() const { return this->page_buf_; }
    uint64_t Page::page_size() const { return this->page_size_; }
    uint64_t Page::used_size() const { return this->used_size_;}
    uint64_t Page::version() const { return this->version_; }
}