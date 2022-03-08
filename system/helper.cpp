#include "global.h"
#include "helper.h"
#include "mem_alloc.h"
#include "time.h"

bool itemid_t::operator==(const itemid_t &other) const {
	return (type == other.type && location == other.location);
}

bool itemid_t::operator!=(const itemid_t &other) const {
	return !(*this == other);
}

void itemid_t::operator=(const itemid_t &other){
	this->valid = other.valid;
	this->type = other.type;
	this->location = other.location;
	assert(*this == other);
	assert(this->valid);
}

void itemid_t::init() {
	valid = false;
	location = 0;
	next = NULL;
}

int get_thdid_from_txnid(uint64_t txnid) {
	return txnid % g_thread_cnt;
}

uint64_t get_part_id(void * addr) {
	return ((uint64_t)addr / PAGE_SIZE) % g_part_cnt; 
}

uint64_t key_to_part(uint64_t key) {
	if (g_part_alloc)
		return key % g_part_cnt;
	else 
		return 0;
}

uint64_t merge_idx_key(UInt64 key_cnt, UInt64 * keys) {
	UInt64 len = 64 / key_cnt;
	UInt64 key = 0;
	for (UInt32 i = 0; i < len; i++) {
		assert(keys[i] < (1UL << len));
		key = (key << len) | keys[i];
	}
	return key;
}

uint64_t merge_idx_key(uint64_t key1, uint64_t key2) {
	assert(key1 < (1UL << 32) && key2 < (1UL << 32));
	return key1 << 32 | key2;
}

uint64_t merge_idx_key(uint64_t key1, uint64_t key2, uint64_t key3) {
	assert(key1 < (1 << 21) && key2 < (1 << 21) && key3 < (1 << 21));
	return key1 << 42 | key2 << 21 | key3;
}

/****************************************************/
// Global Clock!
/****************************************************/
/*
inline uint64_t get_server_clock() {
#if defined(__i386__)
    uint64_t ret;
    __asm__ __volatile__("rdtsc" : "=A" (ret));
#elif defined(__x86_64__)
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    uint64_t ret = ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
	ret = (uint64_t) ((double)ret / CPU_FREQ);
#else 
	timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
#endif
    return ret;
}

inline uint64_t get_sys_clock() {
#ifndef NOGRAPHITE
	static volatile uint64_t fake_clock = 0;
	if (warmup_finish)
		return CarbonGetTime();   // in ns
	else {
		return ATOM_ADD_FETCH(fake_clock, 100);
	}
#else
	if (TIME_ENABLE) 
		return get_server_clock();
	return 0;
#endif
}
*/
void myrand::init(uint64_t seed) {
	this->seed = seed;
}

uint64_t myrand::next() {
	seed = (seed * 1103515247UL + 12345UL) % (1UL<<63);
	return (seed / 65537) % RAND_MAX;
}

// uint8_t table_to_uint8(TABLES table) {
// 	uint8_t table_type = -1;
// 	if(table == TABLES::MAIN_TABLE) { table_type = 0; }
// 	else if(table == TABLES::WAREHOUSE) { table_type = 1; }
// 	else if(table == TABLES::DISTRICT) { table_type = 2; }
// 	else if(table == TABLES::CUSTOMER) { table_type = 3; }
// 	else if(table == TABLES::HISTORY) { table_type = 4; }
// 	else if(table == TABLES::NEW_ORDER) { table_type = 5; }
// 	else if(table == TABLES::ORDER) { table_type = 6; }
// 	else if(table == TABLES::ORDER_LINE) { table_type = 7; }
// 	else if(table == TABLES::ITEM) { table_type = 8; }
// 	else if(table == TABLES::STOCK) { table_type = 9; }
// 	return table_type;
// }

// TABLES uint8_to_table(uint8_t table_type) {
// 	TABLES table;
// 	if(table_type == 0) { table = TABLES::MAIN_TABLE; }
// 	else if(table_type == 1) { table = TABLES::WAREHOUSE; }
// 	else if(table_type == 2) { table = TABLES::DISTRICT; }
// 	else if(table_type == 3) { table = TABLES::CUSTOMER; }
// 	else if(table_type == 4) { table = TABLES::HISTORY; }
// 	else if(table_type == 5) { table = TABLES::NEW_ORDER; }
// 	else if(table_type == 6) { table = TABLES::ORDER; }
// 	else if(table_type == 7) { table = TABLES::ORDER_LINE; }
// 	else if(table_type == 8) { table = TABLES::ITEM; }
// 	else if(table_type == 9) { table = TABLES::STOCK; }
// 	return table;
// }

// void row_key_to_lru_key(TABLES table, uint64_t* row_key) {
// 	assert(sizeof(uint8_t) == 1);
// 	uint8_t table_type = table_to_uint8(table);
// 	memcpy(row_key, &table_type, sizeof(uint8_t));
// }

// TABLES lru_key_to_row_key(uint64_t* lru_key) {
// 	uint8_t table_type;
// 	memcpy(&table_type, lru_key, sizeof(uint8_t));
// 	TABLES table = uint8_to_table(table_type);
// 	memset(lru_key, 0, sizeof(uint8_t));
// 	return table;
// }