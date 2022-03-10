#include "index_map_hash.h"



RC IndexMapHash::init(table_t * table) {
	this->table_ = table;
	return RCOK;
}

bool IndexMapHash::index_exist(idx_key_t key) {
	const_accessor con_acc;
	return index_map_.find(con_acc, key);
}

RC IndexMapHash::index_insert(idx_key_t key, index_item * item, index_item * prev) {
	accessor acc;
	bool is_new = index_map_.insert(acc, key);
	if(is_new) {
		acc->second = item;
		if(prev) {
			item->prev = prev;
			prev->next = item;
		}
		return RCOK;
	} else return Abort;
}

RC IndexMapHash::index_insert(idx_key_t key, index_item * item) {
	accessor acc;
	bool is_new = index_map_.insert(acc, key);
	if(is_new) {
		acc->second = item;
		return RCOK;
	} else return Abort;
}

RC IndexMapHash::index_read(idx_key_t key, index_item * &item) {
	const_accessor con_acc;
	bool find = index_map_.find(con_acc, key);
	if(find) {
		item = con_acc->second;
		return RCOK;
	} else return Abort;
}