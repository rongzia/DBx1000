//
// Created by rrzhang on 2020/6/1.
//

#ifndef STORAGE_PAGE_H
#define STORAGE_PAGE_H

#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <iostream>
#include "config.h"
namespace dbx1000 {
    /// 前 64 字节用于存 page 信息
    class Page {
    public:
        Page() : page_size_(0), page_buf_(nullptr) { 
            // std::cout << "page()" << std::endl;
        }
        explicit Page(const char *buf, uint64_t size = MY_PAGE_SIZE);
        Page(const Page& page);
        Page(Page && page);
        Page& operator=(const Page &) = delete;
        Page& operator=(Page &&) = delete;
        ~Page();

        void Init();

        /// 调用之前，算好是否超过 page_size
        int PagePut(uint64_t page_id, const char *row_buf, size_t count);
        Page* Serialize();
        void Deserialize();

        void Print() const;

        /// getter and setter
        void set_page_id(uint64_t);
        void set_page_buf(const void *, size_t);
        void set_page_size(uint64_t);
        void set_used_size(uint64_t);
        void set_version(uint64_t);
        uint64_t page_id() const;
        char* page_buf();
        const char* page_buf_read() const;
        uint64_t page_size() const;
        uint64_t used_size() const;
        uint64_t version() const;

    private:
        uint64_t page_id_;
        char *page_buf_;
        uint64_t page_size_;
        uint64_t used_size_;
        uint64_t version_;
    };

}

#endif //STORAGE_PAGE_H
