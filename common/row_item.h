//
// Created by rrzhang on 2020/4/21.
//

#ifndef DBX1000_ROW_ITEM_H
#define DBX1000_ROW_ITEM_H

#include <cstdint>
#include <cstdlib>
#include "api/proto/api.pb.h"

namespace dbx1000 {
    class RowItem {
    public:

//        static void CopyRowItem(RowItem* to, RowItem* from);

        void MergeFrom(const RowItem& from);
        void MergeFrom(const Mess_RowItem& from);
//        RowItem(uint64_t key, size_t size, bool init_row = false);
        RowItem(uint64_t key, size_t size);
        RowItem() = delete;
        RowItem(const RowItem&) = delete;
        RowItem& operator=(const RowItem&) = delete;

        ~RowItem();

//        void init();

        uint64_t key_;
        size_t size_;
        char* row_;
    };
}

#endif //DBX1000_ROW_ITEM_H
