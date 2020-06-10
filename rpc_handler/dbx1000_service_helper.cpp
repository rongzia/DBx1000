//
// Created by rrzhang on 2020/6/10.
//

#include "dbx1000_service_helper.h"

namespace dbx1000 {


    RpcLockMode DBx1000ServiceHelper::SerializeLockMode(LockMode mode) {
        if (LockMode::O == mode) { return RpcLockMode::O; }
        if (LockMode::S == mode) { return RpcLockMode::S; }
        if (LockMode::P == mode) { return RpcLockMode::P; }
        if (LockMode::X == mode) { return RpcLockMode::X; }
    }

    LockMode DBx1000ServiceHelper::DeSerializeLockMode(RpcLockMode mode) {
        if (RpcLockMode::O == mode) { return LockMode::O; }
        if (RpcLockMode::S == mode) { return LockMode::S; }
        if (RpcLockMode::P == mode) { return LockMode::P; }
        if (RpcLockMode::X == mode) { return LockMode::X; }
    }
}