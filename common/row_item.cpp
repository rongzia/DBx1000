//
// Created by rrzhang on 2020/4/21.
//
#include <cstring>
#include "row_item.h"

namespace dbx1000 {

    /// ensure to->row_ != nullptr && from->row_ != nullptr
    /// && to->size_ == from->size_
//    void RowItem::CopyRowItem(RowItem *to, RowItem *from) {
//        assert(to->size_ == from->size_);
//        to->key_ = from->key_;
//        memcpy(to->row_, from->row_, from->size_);
//    }

    void RowItem::MergeFrom(const RowItem& from) {
        this->key_ = from.key_;
        this->size_ = from.size_;
        memcpy(this->row_, from.row_, from.size_);
    }

    void RowItem::MergeFrom(const Mess_RowItem& from) {
        this->key_ = from.key();
        this->size_ = from.size();
        memcpy(this->row_, from.row().data(), from.size());
    }

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
        row_ = nullptr;
    }

//    void RowItem::init() {
//        row_ = new char[size_];
//    }
}