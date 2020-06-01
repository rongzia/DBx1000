//
// Created by rrzhang on 2020/4/14.
//

#ifndef DBX1000_LRU_INDEX_H
#define DBX1000_LRU_INDEX_H

#include <vector>
#include <unordered_map>

namespace dbx1000 {
    class RowNode;

    enum class IndexFlag{
        NOT_EXIST,
        IN_BUFFER,
        IN_DISK,
    };

    class LruIndex {
    public:
        RowNode* IndexGet(uint64_t key, IndexFlag *flag);
        void IndexInsert(uint64_t key, RowNode* row_node);

        void Print();
    private:
        std::unordered_map<uint64_t, RowNode*> lru_map_;
    };
} // namespace dbx1000


#endif //DBX1000_LRU_INDEX_H
