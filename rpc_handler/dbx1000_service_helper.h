//
// Created by rrzhang on 2020/6/10.
//

#ifndef DBX1000_DBX1000_SERVICE_HELPER_H
#define DBX1000_DBX1000_SERVICE_HELPER_H

//#include "proto/dbx1000_service.grpc.pb.h"
#include "common/lock_table/lock_table.h"
#include "proto/dbx1000_service.pb.h"

namespace dbx1000 {
    class DBx1000ServiceHelper {
    public:
        static RpcLockMode SerializeLockMode(LockMode);
        static LockMode DeSerializeLockMode(RpcLockMode);
    };
}


#endif //DBX1000_DBX1000_SERVICE_HELPER_H
