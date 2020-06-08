//
// Created by rrzhang on 2020/4/21.
//
#include <cstring>
#include "row_item.h"

namespace dbx1000 {
    RowItem::RowItem(uint64_t key, size_t size)
            : key_(key)
              , size_(size) {
        if(0 != size_) {
            row_ = new char[size_];
        } else {
            row_ = nullptr;
        }
    }

    RowItem::~RowItem() {
        if(nullptr != row_) {
            delete row_;
        }
        size_ = 0;
        row_ = nullptr;
    }

//    void RowItem::init() {
//        row_ = new char[size_];
//    }
}