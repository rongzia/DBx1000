#ifndef RDB_HELPER_H
#define RDB_HELPER_H

#include <iostream>
#include <string>
#include "global.h"
#include "rdb/lock_service/lock_service.pb.h"
#include "lock_table.h"

namespace rdb {
    // for rdb_txn
    using record_tuple = std::tuple<TABLES, uint64_t, access_t>;
    using page_tuple = std::tuple<TABLES, uint64_t, access_t>;
    using normal_tuple = std::tuple<TABLES, uint64_t, access_t>;

    using map_key = std::pair<TABLES, uint64_t>;

    struct tuple_comp
    {
        bool operator() (const normal_tuple& a, const normal_tuple& b) {
            if (std::get<0>(a) < std::get<0>(b)
                || (std::get<0>(a) == std::get<0>(b) && std::get<1>(a) < std::get<1>(b))
                || (std::get<0>(a) == std::get<0>(b) && std::get<1>(a) == std::get<1>(b) && std::get<2>(a) < std::get<2>(b))
                ) {
                return true;
            }
            else return false;
        }
    };

    struct map_comp
    {
        bool operator() (const map_key& a, const map_key& b) {
            if (a.first < b.first
                || (a.first == b.first && a.second < b.second)
                ) {
                return true;
            }
            else return false;
        }
    };

    void print_tuple(const normal_tuple& tup);

    string TABLES_to_string(TABLES table);

    TABLES RpcTABLES_2_TABLES(rdb::RpcTABLES table);

    rdb::RpcTABLES TABLES_2_RpcTABLES(TABLES table);

    RC RpcRC_2_RC(rdb::RpcRC rc);

    rdb::RpcRC RC_2_RpcRC(RC rc);

    lock_mode RpcLM_2_LM(rdb::RpcLockMode lm);

    rdb::RpcLockMode LM_2_RpcLM(lock_mode lm);

    rdb::RpcLockMode access_t_2_RpcLM(access_t access);
}


#endif // RDB_HELPER_H