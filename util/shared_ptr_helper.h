//
// Created by rrzhang on 2020/10/10.
//

#ifndef DBX1000_SHARED_PTR_HELPER_H
#define DBX1000_SHARED_PTR_HELPER_H

#include <memory>
#include <unordered_map>
#include <cassert>
#include <iostream>

template <typename P>
std::shared_ptr<P> GetOrCreateSharedPtr(std::unordered_map<uint64_t, std::pair<std::weak_ptr<P>, volatile bool>> &unorderedMap, uint64_t item_id) {
    auto iter = unorderedMap.find(item_id);
    if (unorderedMap.end() == iter) {
        std::cout << item_id << std::endl;
        assert(false); return nullptr; }

    std::shared_ptr <P> ptr;
    while (!__sync_bool_compare_and_swap(&iter->second.second, false, true))
            __asm__ ( "pause;" );
    ptr = iter->second.first.lock();
    if(iter->second.first.expired()) {
        assert(ptr == nullptr);
        assert(iter->second.first.use_count() == 0);
        assert(iter->second.first.expired());
        ptr = std::make_shared<P>();
        iter->second.first = ptr;
    } else {
        assert(!iter->second.first.expired());
        assert(ptr != nullptr);
        assert(iter->second.first.use_count() > 0);
        assert(!iter->second.first.expired());
    }
    iter->second.second = false;

    return ptr;
}
#endif //DBX1000_SHARED_PTR_HELPER_H