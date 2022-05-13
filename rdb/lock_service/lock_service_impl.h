#ifndef LOCK_SERVICE_IMPL_H
#define LOCK_SERVICE_IMPL_H

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include "lock_service.pb.h"

// #ifndef LS_SERVER_ASYNC
// #define LS_SERVER_ASYNC
// #endif // LS_SERVER_ASYNC

namespace rdb {
    class LockServiceImpl : public rdb::LockService {
    private:
        /* data */
    public:
        LockServiceImpl() = default;
        ~LockServiceImpl() = default;

        void Start(const std::string& host);

        virtual void GetLock(::google::protobuf::RpcController* controller,
            const ::rdb::GetLockRequest* request,
            ::rdb::GetLockReply* response,
            ::google::protobuf::Closure* done);

        virtual void GetLocks(::google::protobuf::RpcController* controller,
            const ::rdb::GetLocksRequest* request,
            ::rdb::GetLocksReply* response,
            ::google::protobuf::Closure* done);
        // common function for async and sync
        static void ProcessGetLock(const ::rdb::GetLockRequest* request, ::rdb::GetLockReply* response);
        static void ProcessGetLocks(const ::rdb::GetLocksRequest* request, ::rdb::GetLocksReply* response);

    private:
        brpc::Server server_;
    };

#ifdef LS_SERVER_ASYNC
    struct AsyncGetLock {
        brpc::Controller* cntl;
        const rdb::GetLockRequest* request;
        rdb::GetLockReply* response;
        google::protobuf::Closure* done;

        void run();
        void run_and_delete();
    };

    struct AsyncGetLocks {
        brpc::Controller* cntl;
        const rdb::GetLocksRequest* request;
        rdb::GetLocksReply* response;
        google::protobuf::Closure* done;

        void run();
        void run_and_delete();
    };
#endif // LS_SERVER_ASYNC
}
#endif // LOCK_SERVICE_IMPL_H


