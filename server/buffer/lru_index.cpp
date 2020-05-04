//
// Created by rrzhang on 2020/4/14.
//
#include <iostream>
#include <cassert>
#include "lru_index.h"

namespace dbx1000 {
    RowNode* LruIndex::IndexGet(uint64_t key, IndexFlag *flag) {
        auto iter = lru_map_.find(key);
        /// 不存在 key
        if(iter == lru_map_.end()) {
            *flag = IndexFlag::NOT_EXIST;
            return nullptr;
        }
        /// key 存在，但是内存没有, 只在 disk 中
        if(nullptr == iter->second){
            *flag = IndexFlag::IN_DISK;
            return nullptr;
        }
        *flag = IndexFlag::IN_BUFFER;
        assert(nullptr != iter->second);
        return iter->second;
    }

    void LruIndex::IndexInsert(uint64_t key, RowNode* row_node) {
        auto iter = lru_map_.find(key);
        if(lru_map_.end() != iter) {
            lru_map_.erase(iter);
        }
        lru_map_.insert(std::pair<uint64_t, RowNode*>(key, row_node));
    }

    void LruIndex::Print() {
        std::cout << "lru_index : {" << std::endl;
        int count = 0;
        for (auto &iter : lru_map_) {
            if (count % 10 == 0) { std::cout << "    "; }
            std::cout << ", {key:" << iter.first << ", row:" << iter.second << "}";
            count++;
            if (count % 10 == 0) { std::cout << std::endl; }
        }
        std::cout << std::endl << "}" << std::endl;
    }
}