//
// Created by rrzhang on 2020/4/14.
//

#ifndef DBX1000_LRU_H
#define DBX1000_LRU_H


namespace dbx1000 {

    class LruIndex;
    class Page;

    class PageNode{
    public:
        PageNode();
        PageNode(char *);
        ~PageNode();

        Page* page_;
        PageNode *prev_;
        PageNode *next_;
    };

    class LRU {
    public:
        explicit LRU(int item_size);
        ~LRU();

        void Prepend(PageNode* );
        PageNode* Popback();
        void Get(PageNode* );

        void DebugSize();
        void Print();

        /// getter and setter
        int size();

    private:
        PageNode* head_; /// head_ 指针不存数据
        PageNode* tail_;
        int size_;      /// length of this list
    };
} // namespace dbx1000

#endif //DBX1000_LRU_H
