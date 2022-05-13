#include "concurrent_lru_cache.h"

namespace rr {
    namespace lru_cache {

        int64_t Page::Unref() {
            size_t before = ref_.fetch_sub(1);
            if (before == 1 && this->page_id_ == UINT64_MAX) {
                assert(this->node_ == nullptr);
                /* debug */ if (ref_.load() != 0) { std::cout << ref_.load() << std::endl; }
                assert(ref_.load() == 0);
                this->cache_->free_list_.push(this);
                this->cache_->free_list_size_.fetch_add(1);
                // /* debug */ std::cout << __LINE__ << "free_list_size_: " << cache_->free_list_size_ << std::endl;
            }
            return before;
        }

        void INIT_LIST_HEAD(rr::list_node<Page*>* node) {
            node->_M_prev = node;
            node->_M_next = node;
        }

        void list_add_tail(rr::list_node<Page*>* temp, rr::list_node<Page*>* head) {
            rr::list_node<Page*>* old_tail = (rr::list_node<Page*>*)head->_M_prev;
            head->_M_prev = temp;
            temp->_M_next = head;
            temp->_M_prev = old_tail;
            old_tail->_M_next = temp;
        }

        void list_del(rr::list_node<Page*>* temp) {
            temp->_M_prev->_M_next = temp->_M_next;
            temp->_M_next->_M_prev = temp->_M_prev;
            temp->_M_prev = nullptr;
            temp->_M_next = nullptr;
        }

        bool node_in_list(rr::list_node<Page*>* node) {
            // 应该从头找，但是这样太费时，直接判断了前后是否为自己
            bool in = !(node == node->_M_next && node == node->_M_prev && node->_M_next != nullptr && node->_M_prev != nullptr);
            if (in) {
                assert(node);
                assert(node->_M_next);
                assert(node->_M_prev);
            }
            return in;
        }
    }
}