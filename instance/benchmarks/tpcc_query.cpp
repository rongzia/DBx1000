#include <mm_malloc.h>

#include "query.h"
#include "tpcc_query.h"
//#include "tpcc.h"
#include "common/workload/tpcc_helper.h"
//#include "mem_alloc.h"
//#include "wl.h"
#include "common/storage/table.h"
#include "instance/manager_instance.h"

void tpcc_query::init(uint64_t thd_id, Query_thd * query_thd) {
	queryThd_ = query_thd;
	double x = (double)(rand() % 100) / 100.0;
	part_to_access = new uint64_t [g_part_cnt]();
	if (x < g_perc_payment)
		gen_payment(thd_id);
	else
		gen_new_order(thd_id);
}

void tpcc_query::gen_payment(uint64_t thd_id) {
	type = TPCC_PAYMENT;
	if (FIRST_PART_LOCAL)
//		w_id = thd_id % g_num_wh + 1;
		w_id = thd_id % g_num_wh + queryThd_->queryQueue_->managerInstance_->wh_start_id;
	else
		w_id = URand(1, g_num_wh, thd_id % g_num_wh);
	d_w_id = w_id;
	uint64_t part_id = wh_to_part(w_id);
	part_to_access[0] = part_id;
	part_num = 1;

	d_id = URand(1, DIST_PER_WARE, w_id-1);
	h_amount = URand(1, 5000, w_id-1);
	int x = URand(1, 100, w_id-1);
	int y = URand(1, 100, w_id-1);


	/// 0.85 概率在本地仓库，0.15 概率在远程仓库
	if(x <= 85) {
		// home warehouse
		c_d_id = d_id;
		c_w_id = w_id;
	} else {
		// remote warehouse
		c_d_id = URand(1, DIST_PER_WARE, w_id-1);
		if(g_num_wh > 1) {
			while((c_w_id = URand(1, g_num_wh, w_id-1)) == w_id) {}
			if (wh_to_part(w_id) != wh_to_part(c_w_id)) {
				part_to_access[1] = wh_to_part(c_w_id);
				part_num = 2;
			}
		} else
			c_w_id = w_id;
		// conflict
		if(NUM_WH > 1) {
			while((c_w_id = URand(1, NUM_WH, w_id-1)) == w_id) {}
		}
	}
	/*
	if(y <= 60) {
		// by last name
		by_last_name = true;
		Lastname(NURand(255,0,999,w_id-1),c_last);
	} else */ {
		// by cust id
		by_last_name = false;
		c_id = NURand(1023, 1, g_cust_per_dist,w_id-1);
	}
}

void tpcc_query::gen_new_order(uint64_t thd_id) {
	type = TPCC_NEW_ORDER;
	if (FIRST_PART_LOCAL)
//		w_id = thd_id % g_num_wh + 1;
		w_id = thd_id % g_num_wh + queryThd_->queryQueue_->managerInstance_->wh_start_id;
	else
		w_id = URand(1, g_num_wh, thd_id % g_num_wh);
	d_id = URand(1, DIST_PER_WARE, w_id-1);
	c_id = NURand(1023, 1, g_cust_per_dist, w_id-1);
	rbk = URand(1, 100, w_id-1);
	ol_cnt = URand(5, 15, w_id-1);
	o_entry_d = 2013;
//	items = (Item_no *) _mm_malloc(sizeof(Item_no) * ol_cnt, 64);
	items = new Item_no[ol_cnt]();
	remote = false;
	part_to_access[0] = wh_to_part(w_id);
	part_num = 1;

	for (uint32_t oid = 0; oid < ol_cnt; oid ++) {
		items[oid].ol_i_id = NURand(8191, 1, g_max_items, w_id-1);
        uint32_t x = URand(1, 100, w_id-1);
		if (x > 1 || g_num_wh == 1) {
			items[oid].ol_supply_w_id = w_id;
		}
		else  {
//			while((items[oid].ol_supply_w_id = URand(1, g_num_wh, w_id-1)) == w_id) {}
            // no conflict
//            items[oid].ol_supply_w_id = w_id;
            // conflict
			while((items[oid].ol_supply_w_id = URand(1, NUM_WH, w_id-1)) == w_id) {}
			remote = true;
		}
		items[oid].ol_quantity = URand(1, 10, w_id-1);
	}
	// Remove duplicate items
	for (uint32_t i = 0; i < ol_cnt; i ++) {
		for (uint32_t j = 0; j < i; j++) {
			if (items[i].ol_i_id == items[j].ol_i_id) {
				for (uint32_t k = i; k < ol_cnt - 1; k++)
					items[k] = items[k + 1];
				ol_cnt --;
				i--;
			}
		}
	}
	for (uint32_t i = 0; i < ol_cnt; i ++)
		for (uint32_t j = 0; j < i; j++)
			assert(items[i].ol_i_id != items[j].ol_i_id);
	// update part_to_access
	for (uint32_t i = 0; i < ol_cnt; i ++) {
        uint32_t j;
		for (j = 0; j < part_num; j++ )
			if (part_to_access[j] == wh_to_part(items[i].ol_supply_w_id))
				break;
		if (j == part_num) // not found! add to it.
		part_to_access[part_num ++] = wh_to_part( items[i].ol_supply_w_id );
	}
}

void 
tpcc_query::gen_order_status(uint64_t thd_id) {
//	type = TPCC_ORDER_STATUS;
//	if (FIRST_PART_LOCAL)
//		w_id = thd_id % g_num_wh + 1;
//	else
//		w_id = URand(1, g_num_wh, thd_id % g_num_wh);
//	d_id = URand(1, DIST_PER_WARE, w_id-1);
//	c_w_id = w_id;
//	c_d_id = d_id;
//	int y = URand(1, 100, w_id-1);
//	if(y <= 60) {
//		// by last name
//		by_last_name = true;
//		Lastname(NURand(255,0,999,w_id-1),c_last);
//	} else {
//		// by cust id
//		by_last_name = false;
//		c_id = NURand(1023, 1, g_cust_per_dist, w_id-1);
//	}
}
