//
// Created by rrzhang on 2020/6/10.
//

#include "global_lock_service_helper.h"

namespace dbx1000 {


    namespace global_lock_service {
        RpcLockMode GlobalLockServiceHelper::SerializeLockMode(LockMode mode) {
            if (LockMode::O == mode) { return RpcLockMode::O; }
            if (LockMode::S == mode) { return RpcLockMode::S; }
            if (LockMode::P == mode) { return RpcLockMode::P; }
            if (LockMode::X == mode) { return RpcLockMode::X; }
            else { assert(false); }
        }

        RpcRC GlobalLockServiceHelper::SerializeRC(RC rc) {
            if (RC::RCOK == rc) { return RpcRC::RCOK; }
            if (RC::Commit == rc) { return RpcRC::Commit; }
            if (RC::Abort == rc) { return RpcRC::Abort; }
            if (RC::WAIT == rc) { return RpcRC::WAIT; }
            if (RC::ERROR == rc) { return RpcRC::ERROR; }
            if (RC::FINISH == rc) { return RpcRC::FINISH; }
            if (RC::TIME_OUT == rc) { return RpcRC::TIME_OUT; }
            else { assert(false); }
        }

        LockMode GlobalLockServiceHelper::DeSerializeLockMode(RpcLockMode mode) {
            if (RpcLockMode::O == mode) { return LockMode::O; }
            if (RpcLockMode::S == mode) { return LockMode::S; }
            if (RpcLockMode::P == mode) { return LockMode::P; }
            if (RpcLockMode::X == mode) { return LockMode::X; }
            else { assert(false); }
        }

        RC GlobalLockServiceHelper::DeSerializeRC(RpcRC rpcRc) {
            if (RpcRC::RCOK == rpcRc) { return RC::RCOK; }
            if (RpcRC::Commit == rpcRc) { return RC::Commit; }
            if (RpcRC::Abort == rpcRc) { return RC::Abort; }
            if (RpcRC::WAIT == rpcRc) { return RC::WAIT; }
            if (RpcRC::ERROR == rpcRc) { return RC::ERROR; }
            if (RpcRC::FINISH == rpcRc) { return RC::FINISH; }
            if (RpcRC::TIME_OUT == rpcRc) { return RC::TIME_OUT; }
            else { assert(false); }
        }


        RpcTABLES GlobalLockServiceHelper::SerializeTABLES(TABLES table) {
            if (TABLES::MAIN_TABLE == table) { return RpcTABLES::MAIN_TABLE; }
            if (TABLES::WAREHOUSE == table) { return RpcTABLES::WAREHOUSE; }
            if (TABLES::DISTRICT == table) { return RpcTABLES::DISTRICT; }
            if (TABLES::CUSTOMER == table) { return RpcTABLES::CUSTOMER; }
            if (TABLES::HISTORY == table) { return RpcTABLES::HISTORY; }
            if (TABLES::NEW_ORDER == table) { return RpcTABLES::NEW_ORDER; }
            if (TABLES::ORDER == table) { return RpcTABLES::ORDER; }
            if (TABLES::ORDER_LINE == table) { return RpcTABLES::ORDER_LINE; }
            if (TABLES::ITEM == table) { return RpcTABLES::ITEM; }
            if (TABLES::STOCK == table) { return RpcTABLES::STOCK; }
            else { assert(false); }
        }

        TABLES GlobalLockServiceHelper::DeSerializeTABLES(RpcTABLES table) {
            if(RpcTABLES::MAIN_TABLE == table) { return TABLES::MAIN_TABLE; }
            if(RpcTABLES::WAREHOUSE == table) { return TABLES::WAREHOUSE; }
            if(RpcTABLES::DISTRICT == table) { return TABLES::DISTRICT; }
            if(RpcTABLES::CUSTOMER == table) { return TABLES::CUSTOMER; }
            if(RpcTABLES::HISTORY == table) { return TABLES::HISTORY; }
            if(RpcTABLES::NEW_ORDER == table) { return TABLES::NEW_ORDER; }
            if(RpcTABLES::ORDER == table) { return TABLES::ORDER; }
            if(RpcTABLES::ORDER_LINE == table) { return TABLES::ORDER_LINE; }
            if(RpcTABLES::ITEM == table) { return TABLES::ITEM; }
            if(RpcTABLES::STOCK == table) { return TABLES::STOCK; }
            else { assert(false); }
        }

    }
}