//
// Created by rrzhang on 2020/6/10.
//

#ifndef DBX1000_BUFFER_MANAGER_HANDLER_H
#define DBX1000_BUFFER_MANAGER_HANDLER_H

#include <grpcpp/grpcpp.h>
#include "proto/dbx1000_service.grpc.pb.h"
#include "common/lock_table/lock_table.h"
#include "config.h"

namespace dbx1000 {
    class ManagerServer;

    class BufferManagerServer : DBx1000Service::Service {
    public:
        virtual ::grpc::Status LockGet(::grpc::ServerContext* context, const ::dbx1000::LockGetRequest* request, ::dbx1000::LockGetReply* response);
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

        bool LockDowngrade(uint64_t page_id, LockMode from, LockMode to, char *page_buf, size_t count = MY_PAGE_SIZE);
        bool LockInvalid(uint64_t page_id);

     private:
        std::unique_ptr<dbx1000::DBx1000Service::Stub> stub_;
        ManagerServer* manager_server_;
    };
}


#endif //DBX1000_BUFFER_MANAGER_HANDLER_H
