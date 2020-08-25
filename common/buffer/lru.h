//
// Created by rrzhang on 2020/4/14.
//

#ifndef DBX1000_LRU_H
#define DBX1000_LRU_H


#include <atomic>
#include <mutex>
#include "buffer_def.h"

namespace dbx1000 {

    class LruIndex;
    class Page;

    class PageNode{
        /**
         * 双向链表
         */
    public:
        PageNode();
        /// 传入一段空间的地址值，初始化该 PageNode
        PageNode(char *);
        /// 构造函数的 char * 不在当前 ~PageNode 释放空间，因为并不是由当前 PageNode 申请的空间
        ~PageNode();

        Page* page_;
        PageNode *prev_;
        PageNode *next_;
    };

    class LRU {
    public:
        explicit LRU();
        ~LRU();

        void Prepend(PageNode* );   /// 加入链表头部，然后插入 lru_index
        PageNode* Popback();        /// 把尾部从链表中取出，返回 PageNode*
        void Get(PageNode* );       /// PageNode* 从 lru_index 获取，且必须已经在链表中

        void Check();

        /// getter and setter
        int size();

        PageNode* head() { return this->head_; }
        PageNode* tail() { return this->tail_; }

    private:
        PageNode* head_;
        PageNode* tail_;
        std::atomic_int size_;      /// length of this list
        std::mutex mtx_;
    };
} // namespace dbx1000

#endif //DBX1000_LRU_H
