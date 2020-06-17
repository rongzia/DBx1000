//
// Created by rrzhang on 2020/6/10.
//

#ifndef DBX1000_LOCK_SERVICE_HANDLER_H
#define DBX1000_LOCK_SERVICE_HANDLER_H

#include <grpcpp/grpcpp.h>
#include "proto/dbx1000_service.grpc.pb.h"
#include "common/lock_table/lock_table.h"
#include "config.h"

namespace dbx1000 {
    class ManagerServer;

    class BufferManagerServer : DBx1000Service::Service {
    public:
        virtual ::grpc::Status LockRemote(::grpc::ServerContext* context, const ::dbx1000::LockRemoteRequest* request, ::dbx1000::LockRemoteReply* response);
        virtual ::grpc::Status InstanceInitDone(::grpc::ServerContext* context, const ::dbx1000::InstanceInitDoneRequest* request, ::dbx1000::InstanceInitDoneReply* response);
        virtual ::grpc::Status BufferManagerInitDone(::grpc::ServerContext* context, const ::dbx1000::BufferManagerInitDoneRequest* request, ::dbx1000::BufferManagerInitDonReplye* response);
        virtual ::grpc::Status GetTestNum(::grpc::ServerContext* context, const ::dbx1000::GetTestNumRequest* request, ::dbx1000::GetTestNumReply* response);
        void Start(const std::string& host);

        ManagerServer* manager_server_;
    };

    class ManagerServer;
    class BufferManagerClient {
    public:
        BufferManagerClient(const std::string&);
        BufferManagerClient() = delete;
        BufferManagerClient(const BufferManagerClient&) = delete;
        BufferManagerClient &operator=(const BufferManagerClient&) = delete;
        ManagerServer* manager_server();

        bool LockInvalid(uint64_t page_id, char *page_buf, size_t count);

     private:
        std::unique_ptr<dbx1000::DBx1000Service::Stub> stub_;
        ManagerServer* manager_server_;
    };
}


#endif //DBX1000_LOCK_SERVICE_HANDLER_H
