#ifndef INDEX_MAP_HASH_H
#define INDEX_MAP_HASH_H
#include "global.h"
#include "helper.h"
#include "tbb/concurrent_hash_map.h"


class IndexMapHash
{
public:
	// RC 			init(uint64_t bucket_cnt, int part_cnt);
	// RC 			init(int part_cnt, 
	// 				table_t * table, 
	// 				uint64_t bucket_cnt);
	RC			init(table_t * table);
	bool 		index_exist(idx_key_t key); // check if the key exist.
	RC 			index_insert(idx_key_t key, index_item * item);
	// the following call returns a single item
	RC	 		index_read(idx_key_t key, index_item * &item);	
private:
    tbb::concurrent_hash_map<idx_key_t, index_item*> index_map_;
public:
	using ConcurrentMap = tbb::concurrent_hash_map<idx_key_t, index_item*>;
	using accessor = typename ConcurrentMap::accessor;
	using const_accessor = typename ConcurrentMap::const_accessor;
	table_t* table_;
};

#endif 