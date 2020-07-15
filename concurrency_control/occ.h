#pragma once 

#include "row.h"

// TODO For simplicity, the txn hisotry for OCC is oganized as follows:
// 1. history is never deleted.
// 2. hisotry forms a single directional list. 
//		history head -> hist_1 -> hist_2 -> hist_3 -> ... -> hist_n
//    The head is always the latest and the tail the youngest. 
// 	  When history is traversed, always go from head -> tail order.

class txn_man;

class set_ent{
public:
	set_ent();
	UInt64 tn;
	txn_man * txn;
	UInt32 set_size;
	row_t ** rows;
	set_ent * next;
};

/** 分为本地读验证 per_row_validate 和 全局写验证 central_validate
 * 本地读验证在读取阶段，从 row_occ::access 里获取一个行 i ，保证当前 txn->start_ts > row_occ[i].wts
 * 验证时，验证所有的读写集合的 start_ts 仍然 > row_occ[i].wts
 * 提交时，把写集合的 end_ts 更新到 row_occ[i].wts
 *
 * 全局写验证：
 * OptCC 维护了两个结构： histroy 和 active，这两个结构是全局共享的并发栈结构
 * histroy 记录了所有已经完成检验、提交的事务写集合，active 则是当前正需要校验的写集合
 * 注意，histroy，active 都不记录只读事务
 *
 * 流程：
 * 1. line 95 - 98
 * 当一个事务进入验证阶段时，首先获取各自的读写集合：rset 和 wset
 * 2. line 103 - 117
 * 获取 OptCC::latch ，然后将当前事务的写集合 wset 加入到 active 栈中，同时，在释放 OptCC::latch 前，获取一个全局写集合快照，因为当前事务不需要和后来的事务验证，
 * 这个快照 finish_active 是 active 的一个子集，最后释放 OptCC::latch。active 提供了事务排序的功能
 * 假设获取 OptCC::latch 后 ，active : set_ent[5] -> set_ent[4] -> set_ent[3]，则快照 finish_active 为 set_ent[5] -> set_ent[4] -> set_ent[3]
 * 当前事务作为 set_ent[6] 加入到 active 中
 * 3. line 118 - 140
 * 验证，验证当前读集合和 finish_active[i] 有没有交集，若有交集，则意味着当前 txn 之前存在一个其他事务对同一行的写操作，
 * 验证当前写集合和 finish_active[i] 有没有交集。
 * 过验证通过，则 cleanup(RCOK)，写入
 * 4. line 142 - 166
 * 当前事务验证、写入通过，且事务 wset 非空（不是只读事务）,需要把相应的 active 内的 set_ent 加入到 histroy 内，需要涉及到 OptCC::latch。
 * 这里 active_len--, his_len++，且 tnc++，其实 tnc == his_len，都是从 1 自增。
 *
 *
 * 这里存在一个问题，即在 2 和 4 步骤之间，由于没有一把大锁，实际上 2 的 histroy 和 active 、4 的 histroy 和 active 是发生变化了的。
 * 即 3 步骤验证时，其实有可能 set_ent[3]，已经被其他线程加入到 histroy 中了，所以还需要和 histroy 中部分 set_ent 进行比较。
 * 代码考虑到了部分，118 - 127，但是我觉得没必要，若 set_ent[3] 验证通过，它在步骤 3 里，是存在于快照 finish_active 中的；若验证不通过 set_ent[3]，也没有什么影响
 */

class OptCC {
public:
	void init();
	RC validate(txn_man * txn);
	volatile bool lock_all;
	uint64_t lock_txn_id;
private:
	
	// per row validation similar to Hekaton.
	RC per_row_validate(txn_man * txn);

	// parallel validation in the original OCC paper.
	RC central_validate(txn_man * txn);
	bool test_valid(set_ent * set1, set_ent * set2);
	RC get_rw_set(txn_man * txni, set_ent * &rset, set_ent *& wset);
	
	// "history" stores write set of transactions with tn >= smallest running tn
	/** histroy ：所有已经
	 *
	 *
	 */
	set_ent * history;
	set_ent * active;
	uint64_t his_len;
	uint64_t active_len;
	volatile uint64_t tnc; // transaction number counter
	pthread_mutex_t latch;
};
