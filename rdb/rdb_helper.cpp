#include "rdb_helper.h"

namespace rdb {
    string TABLES_to_string(TABLES table) {
        string res("fuck");
        if (table == TABLES::MAIN_TABLE) { res = string("MAIN_TABLE"); }
        else if (table == TABLES::WAREHOUSE) { res = string("WAREHOUSE"); }
        else if (table == TABLES::DISTRICT) { res = string("DISTRICT"); }
        else if (table == TABLES::CUSTOMER) { res = string("CUSTOMER"); }
        else if (table == TABLES::HISTORY) { res = string("HISTORY"); }
        else if (table == TABLES::NEW_ORDER) { res = string("NEW_ORDER"); }
        else if (table == TABLES::ORDER) { res = string("ORDER"); }
        else if (table == TABLES::ORDER_LINE) { res = string("ORDER_LINE"); }
        else if (table == TABLES::ITEM) { res = string("ITEM"); }
        else if (table == TABLES::STOCK) { res = string("STOCK"); }
        return res;
    }


    void print_tuple(const normal_tuple& tup) {
        std::cout << "(" << TABLES_to_string(std::get<0>(tup)) << ", " << std::get<1>(tup) << ", " << std::get<2>(tup) << ")" << std::endl;
    }

    TABLES RpcTABLES_2_TABLES(rdb::RpcTABLES table) {
        if (rdb::RpcTABLES::MAIN_TABLE == table) return TABLES::MAIN_TABLE;
        if (rdb::RpcTABLES::WAREHOUSE == table) return TABLES::WAREHOUSE;
        if (rdb::RpcTABLES::DISTRICT == table) return TABLES::DISTRICT;
        if (rdb::RpcTABLES::CUSTOMER == table) return TABLES::CUSTOMER;
        if (rdb::RpcTABLES::NEW_ORDER == table) return TABLES::NEW_ORDER;
        if (rdb::RpcTABLES::ORDER == table) return TABLES::ORDER;
        if (rdb::RpcTABLES::ORDER_LINE == table) return TABLES::ORDER_LINE;
        if (rdb::RpcTABLES::ITEM == table) return TABLES::ITEM;
        if (rdb::RpcTABLES::STOCK == table) return TABLES::STOCK;
        else assert(false);
    }

    rdb::RpcTABLES TABLES_2_RpcTABLES(TABLES table) {
        if (TABLES::MAIN_TABLE == table) return RpcTABLES::MAIN_TABLE;
        if (TABLES::WAREHOUSE == table) return RpcTABLES::WAREHOUSE;
        if (TABLES::DISTRICT == table) return RpcTABLES::DISTRICT;
        if (TABLES::CUSTOMER == table) return RpcTABLES::CUSTOMER;
        if (TABLES::NEW_ORDER == table) return RpcTABLES::NEW_ORDER;
        if (TABLES::ORDER == table) return RpcTABLES::ORDER;
        if (TABLES::ORDER_LINE == table) return RpcTABLES::ORDER_LINE;
        if (TABLES::ITEM == table) return RpcTABLES::ITEM;
        if (TABLES::STOCK == table) return RpcTABLES::STOCK;
        else {  cout << TABLES_to_string(table) << __LINE__ << endl; assert(false);}
    }

    RC RpcRC_2_RC(rdb::RpcRC rc) {
        if (rdb::RpcRC::RCOK == rc) return RC::RCOK;
        if (rdb::RpcRC::Commit == rc) return RC::Commit;
        if (rdb::RpcRC::Abort == rc) return RC::Abort;
        if (rdb::RpcRC::WAIT == rc) return RC::WAIT;
        if (rdb::RpcRC::ERROR == rc) return RC::ERROR;
        if (rdb::RpcRC::FINISH == rc) return RC::FINISH;
        if (rdb::RpcRC::TIME_OUT == rc) return RC::Abort;
        else assert(false);
    }

    rdb::RpcRC RC_2_RpcRC(RC rc) {
        if (RC::RCOK == rc) return rdb::RpcRC::RCOK;
        if (RC::Commit == rc) return rdb::RpcRC::Commit;
        if (RC::Abort == rc) return rdb::RpcRC::Abort;
        if (RC::WAIT == rc) return rdb::RpcRC::WAIT;
        if (RC::ERROR == rc) return rdb::RpcRC::ERROR;
        if (RC::FINISH == rc) return rdb::RpcRC::FINISH;
        else assert(false);
    }

    lock_mode RpcLM_2_LM(rdb::RpcLockMode lm) {
        if (rdb::RpcLockMode::O == lm) return lock_mode::O;
        if (rdb::RpcLockMode::P == lm) return lock_mode::P;
        if (rdb::RpcLockMode::S == lm) return lock_mode::S;
        if (rdb::RpcLockMode::X == lm) return lock_mode::X;
        else assert(false);
    }

    rdb::RpcLockMode LM_2_RpcLM(lock_mode lm) {
        if (lock_mode::O == lm) return rdb::RpcLockMode::O;
        if (lock_mode::P == lm) return rdb::RpcLockMode::P;
        if (lock_mode::X == lm) return rdb::RpcLockMode::X;
        if (lock_mode::S == lm) return rdb::RpcLockMode::S;
        else assert(false);
    }

    rdb::RpcLockMode access_t_2_RpcLM(access_t access) {
        if (access_t::RD == access)  return rdb::RpcLockMode::S;
        if (access_t::WR == access)  return rdb::RpcLockMode::P;
        else assert(false);
    }
}