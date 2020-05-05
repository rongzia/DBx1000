//
// Created by rrzhang on 2020/4/23.
//
#ifndef WITH_RPC

#ifndef DBX1000_CS_API_H
#define DBX1000_CS_API_H

#include <cstdint>
#include <atomic>
#include <mutex>
#include "client/txn/txn.h"
#include "common/txn_row_man.h"

namespace dbx1000 {
    class API {
    public:
        /// for client
        static RC get_row(uint64_t key, access_t type, txn_man* txn, int accesses_index);
        static void return_row(uint64_t key, access_t type, txn_man* txn, int accesses_index);

        static void set_wl_sim_done();
        static bool get_wl_sim_done();


        static uint64_t get_next_ts(uint64_t thread_id);
        static void add_ts(uint64_t thread_id, uint64_t ts);

        /// for server
        static void SetTsReady(TxnRowMan* txnRowMan);

        static int count_;
        static std::mutex mtx_;
    };
}
#endif //DBX1000_CS_API_H

#endif // no define WITH_RPC