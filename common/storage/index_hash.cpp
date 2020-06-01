/*
#include "index_hash.h"

#include "make_unique.h"
#include "table.h"
#include "arena.h"

std::atomic_flag IndexHash::arena_lock_ = ATOMIC_FLAG_INIT;

RC IndexHash::init(uint64_t bucket_cnt, int part_cnt) {
	_bucket_cnt = bucket_cnt;
	_bucket_cnt_per_part = bucket_cnt / part_cnt;
	_buckets = new BucketHeader * [part_cnt];
	for (int i = 0; i < part_cnt; i++) {
		_buckets[i] = new (arena_->Allocate(sizeof(BucketHeader)*_bucket_cnt_per_part))BucketHeader[_bucket_cnt_per_part]();
		for (uint32_t n = 0; n < _bucket_cnt_per_part; n ++)
			_buckets[i][n].init(this);
	}
//	cout << "addr of arena : " << std::addressof(arena_) << endl;
	return RCOK;
}

RC
IndexHash::init(int part_cnt, table_t * table, uint64_t bucket_cnt) {
    arena_ = dbx1000::make_unique<dbx1000::Arena>();
//    arena_ = new dbx1000::Arena();

	init(bucket_cnt, part_cnt);
	this->table = table;
	return RCOK;
}

bool IndexHash::index_exist(idx_key_t key) {
	assert(false);
}

void
IndexHash::get_latch(BucketHeader * bucket) {
	while (!ATOM_CAS(bucket->locked, false, true)) {}
}

void
IndexHash::release_latch(BucketHeader * bucket) {
	bool ok = ATOM_CAS(bucket->locked, true, false);
	assert(ok);
}


//! 应该是每个 BucketHeader 存放多个 BucketNode，每添加一个 item 进 BucketHeader，就放到 BucketNode 链表的末端
//! 但是由于 BucketHeader 太大，每次进来的 key 只能映射到空的 BucketHeader里（即该 BucketHeader 里的 BucketNode 节点数为零）
RC IndexHash::index_insert(idx_key_t key, itemid_t * item, int part_id) {
	RC rc = RCOK;
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	// 1. get the ex latch
	get_latch(cur_bkt);

	// 2. update the latch list
	cur_bkt->insert_item(key, item, part_id);

	// 3. release the latch
	release_latch(cur_bkt);
	return rc;
}

RC IndexHash::index_read(idx_key_t key, itemid_t * &item, int part_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
//	get_latch(cur_bkt);
	cur_bkt->read_item(key, item, table->get_table_name());
	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;

}

RC IndexHash::index_read(idx_key_t key, itemid_t * &item,
						int part_id, int thd_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
//	get_latch(cur_bkt);
	cur_bkt->read_item(key, item, table->get_table_name());
	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;
}

/************** BucketHeader Operations ******************/

void BucketHeader::init(IndexHash *indexHash) {
//	cout << "addr of arena : " << std::addressof(arena_) << endl;
    indexHash_ = indexHash;
	node_cnt = 0;
	first_node = NULL;
	locked = false;
}

//! 当 key 不存在重复时，每次都放到链表的末端
//! key 重叠时，对 item_t->next 做处理
void BucketHeader::insert_item(idx_key_t key,
		itemid_t * item,
		int part_id)
{
	BucketNode * cur_node = first_node;
	BucketNode * prev_node = NULL;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		prev_node = cur_node;
		cur_node = cur_node->next;
	}
	if (cur_node == NULL) {
        //! index 是多线程并发访问的，需要对 arena_ 并发控制
		while (indexHash_->arena_lock_.test_and_set(std::memory_order_acquire));
		BucketNode * new_node = new (indexHash_->arena_->Allocate(sizeof(BucketNode)))BucketNode(key);
		indexHash_->arena_lock_.clear(std::memory_order_release);
		new_node->items = item;
		if (prev_node != NULL) {
			new_node->next = prev_node->next;
			prev_node->next = new_node;
		} else {
			new_node->next = first_node;
			first_node = new_node;
		}
	} else {
		item->next = cur_node->items;
		cur_node->items = item;
	}
}

void BucketHeader::read_item(idx_key_t key, itemid_t * &item, std::string tname)
{
	BucketNode * cur_node = first_node;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		cur_node = cur_node->next;
	}
	M_ASSERT(cur_node->key == key, "Key does not exist!");
	item = cur_node->items;
}
*/