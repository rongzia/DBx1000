//! Arena 管理单个内存段，可以组织成链表的形式，但是这里面链表好像有点问题
//! mem_alloc 是由多线程管理的，有一个二维数组 _arenas
//! 每个线程拥有四个 Arena，分别管理 32、64、256、1024 的内存段，_arenas[i] 由线程 i 持有
//! 这里线程 i 是由 pid_arena 来和系统线程的 pid 联系起来的

#ifndef _MEM_ALLOC_H_
#define _MEM_ALLOC_H_

#include "global.h"
#include <map>

const int SizeNum = 4;
const UInt32 BlockSizes[] = {32, 64, 256, 1024};

typedef struct free_block {
    int size;
    struct free_block* next;
} FreeBlock;

class Arena {
public:
	void init(int arena_id, int size);
	void * alloc();
	void free(void * ptr);
private:
	char * 		_buffer;
	int 		_size_in_buffer;
	int 		_arena_id;
	int 		_block_size;
	FreeBlock * _head;
	char 		_pad[128 - sizeof(int)*3 - sizeof(void *)*2 - 8];
};

//! **_arenas represents (thread) * (different list), that every thread has four buffer list(size = 32, 64, 256, 1024...)

// _arenas 的 index（也即 arena id), 应该和 register_thread() 中的 thd_id 相等，且最大 index 应该等于 _bucket_cnt -1，
// 但是从初始化来看（init_thread_arena() 和 init()），二者应该是四倍左右的关系
class mem_alloc {
public:
    void init(uint64_t part_cnt, uint64_t bytes_per_part);
    void register_thread(int thd_id);   //！ 把自定义 thread 号和系统 pid 对应起来，称作 register
    void unregister();                  //! 清除掉所有的注册
    void * alloc(uint64_t size, uint64_t part_id);
    void free(void * block, uint64_t size);
	int get_arena_id();
private:
    void init_thread_arena();
	int get_size_id(UInt32 size);
	
	// each thread has several arenas for different block size
	Arena ** _arenas;
	int _bucket_cnt;    //
    std::pair<pthread_t, int>* pid_arena;// max_arena_id; 系统 pid 自定义线程 id 的对应
    pthread_mutex_t         map_lock; // only used for pid_to_arena update
};

#endif
