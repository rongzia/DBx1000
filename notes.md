## main.cpp:
```
	// 内存管理器初始化，g_part_cnt 为数据的分区数
	mem_allocator.init(g_part_cnt : 1, MEM_SIZE / g_part_cnt
	// 初始化 glob_manager
	glob_manager->init();
	// 初始化 workload
	m_wl->init();
	// 为 thread_t *m_thds 分配空间。
	// 初始化 query_queue
		query_queue->init(m_wl
```	
	
	
	
	
	
	
	
## Manager	
全局的时间戳管理、锁管理等。

## storage
column、catalog、row、table、、、之间的关系：  
column 是一个列的抽象包括在一行的第几个位置(id)，从一行的第多少字节开始(index),这一列占多少字节(size)，类型(type),名称(name)  
catalog 则是一个表的抽象，包括对所有列的描述(_columns),每行数据的长度(tuple_size),列数(field_cnt)  
table 包含了一个 catalog 指针，cur_tab_size表示该table内有所少行，还提供了 get_new_row(),来获取一个新的行。  
row_t 包含data，row id，分区 id，主键等。 

> TODO: row_t manager

## mem_alloc
内存区域的管理单元，系统使用一个全局的内存管理器 mem_alloc 为数据结构分配内存，避免频繁使用 malloc()。  
mem_alloc 含有一个 _arenas[n][4] 的数组(n表示使用该管理器的线程个数，4代表每个线程拥有 4 种 不同size的链表，分别为 32、64、256、1024)。  
每个 Arena 只是某个线程的某种size的链表。  
另外，mem_alloc 为每个线程创建了 <pthread_t, int> 这样的注册器，表示系统线程pid和index 的对应，index = 0...n-1, pid则由系统分配，  
这样线程调用 mem_alloc.alloc 时候就能通过调用者得 pid，来调用 _arenas[index]的分配器。  

struct free_block 链表的 node  
Arena 一个组织为链表结构的内存区域，使用时，Arena::alloc()先申请一块大内存，再按需分配给调用者。
mem_alloc，系统里的内存管理器，包含 Arena ** _arenas，以及 pid_arena 来记录线程pid和index的对应关系。

使用示例：  
ycsb_wl.cpp 里在加载数据的时候，调用 init_table_parallel() 根据 g_init_parallelism 去创建多个线程预热数据，单个区间(线程)的数据由 init_table_slice()加载，  
init_table_slice() 首先会调用 mem_allocator.register_thread(i) 去注册该线程，然后主线程 init_table_parallel() 在最后释放 unregister()。  
该内存分配器并不是为 row_t 分配的，而是分配给 itemid_t，itemid_t 有一个指针指向 row_t，row_t仍然是通过 malloc() 分配空间。  

#### 需要注意的是:
是否使用 Arena 来分配内存是由宏 THREAD_ALLOC 控制的，系统默认为 false，也就是说，默认还是用 malloc()，汗...  

#### 存在疑问:
既然是根据线程来并行，可是 mem_alloc 桶的数量 _bucket_cnt 设置成了 g_init_parallelism * 4 + 1，且和 init_thread_arena初始化的 _arenas 数量又不对应，这就很奇怪。

## query
ycsb_request 单个请求，记录查询的主键、类型、value(char)
ycsb_query : base_query	一个 query 包含多个请求：ycsb_request requests[16]。主要生成请求 gen_requests()，还有提供zipf分布的随机数。  
Query_thd	各线程，线程总数为全局 g_thread_cnt，包含一个查询数组: ycsb_query * queries;（该数组很大，约为100004）  
Query_queue	查询主线程，包含一个 Query_thd 队列：Query_thd ** all_queries，除了初始化和 get 下一个 query，没什么特别的。  


## wl, workload
该模块主要就是根据 txt里的表信息，来构建 schema（catalog），同时加载数据。  
基类两个成员：  
map<string, table_t *> tables;		用于存放表，string 为表名  
map<string, INDEX *> indexes;		存放索引，string 为表名  
tables 并不存数据，最后 row_t 只能通过 indexes 来获取。  



