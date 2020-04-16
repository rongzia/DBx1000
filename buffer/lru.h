//
// Created by rrzhang on 2020/4/14.
//

#ifndef DBX1000_LRU_H
#define DBX1000_LRU_H

#include <list>
#include <iostream>
//#include "buffer.h"

namespace dbx1000 {

    class LruIndex;

    class RowNode{
    public:
        RowNode();
        RowNode(char *);

        uint64_t key_;
        char* row_;
        RowNode *prev_;
        RowNode *next_;
    };

    class LRU {
    public:
        explicit LRU(int item_size);

        void Prepend(RowNode* row_node);
        RowNode* Popback();
        void Get(RowNode*& row_node);

        int Size();
        void DebugSize();
        void Print();

        RowNode* Head();
        RowNode* Tail();
    private:

        RowNode* head_; /// head_ 指针不存数据
        RowNode* tail_;

        int size_;      /// length of this list
        int item_size_;
    };
} // namespace dbx1000

#endif //DBX1000_LRU_H
