//
// Created by rrzhang on 2020/4/14.
//

#ifndef DBX1000_LRU_INDEX_H
#define DBX1000_LRU_INDEX_H

#include <vector>
#include <unordered_map>

namespace dbx1000 {
    class PageNode;

    enum class LruIndexFlag{
        EXIST,
        NOT_EXIST,
    };

    class LruIndex {
    public:
        PageNode* IndexGet(uint64_t page_id, LruIndexFlag *flag);
        void IndexPut(uint64_t page_id, PageNode* page_node);
        void IndexDelete(uint64_t page_id);

        void Print();
    private:
        std::unordered_map<uint64_t, PageNode*> lru_map_;
    };
} // namespace dbx1000


#endif //DBX1000_LRU_INDEX_H
