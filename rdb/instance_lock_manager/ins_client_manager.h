#ifndef INS_CLIENT_MANAGER
#define INS_CLIENT_MANAGER

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/channel.h>
#include "rdb/lock_service/lock_service.pb.h"

namespace rdb {
    struct ClientHandler {
        brpc::Channel channel_;
        brpc::ChannelOptions options_;
        rdb::LockService_Stub* stub_;
    };

    class InsClientManager {
    public:
        InsClientManager();
        ~InsClientManager();

        void GetLock(brpc::Controller* cntl, rdb::GetLockRequest* request, rdb::GetLockReply* reply);
        void GetLocks(brpc::Controller* cntl, rdb::GetLocksRequest* request, rdb::GetLocksReply* reply);
        
        int ins_id_;
    private:
        ClientHandler handle_ls_;
    };
}

#endif // INS_CLIENT_MANAGER