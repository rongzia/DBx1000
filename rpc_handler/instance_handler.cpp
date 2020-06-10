//
// Created by rrzhang on 2020/6/10.
//
#include <cstring>
#include "instance_handler.h"

#include "dbx1000_service_helper.h"

namespace dbx1000 {
    ::grpc::Status InstanceServer::LockDowngrade(::grpc::ServerContext* context, const ::dbx1000::LockDowngradeRequest* request, ::dbx1000::LockDowngradeReply* response) {

    }
    ::grpc::Status InstanceServer::LockInvalid(::grpc::ServerContext* context, const ::dbx1000::LockInvalidRequest* request, ::dbx1000::LockInvalidReply* response) {

    }




    InstanceClient::InstanceClient(const std::string &addr) : stub_(dbx1000::DBx1000Service::NewStub(
            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())
    )) {}

    bool
    InstanceClient::LockGet(int instance_id, uint64_t page_id, dbx1000::LockMode mode, char *page_buf, size_t count) {
        dbx1000::LockGetRequest request;
        grpc::ClientContext context;
        dbx1000::LockGetReply reply;

        request.set_instance_id(instance_id);
        request.set_page_id(page_id);
        request.set_mode(DBx1000ServiceHelper::SerializeLockMode(mode));


        ::grpc::Status status = stub_->LockGet(&context, request, &reply);
        assert(status.ok());

        if (reply.page_buf().size() == count) {
            memcpy(page_buf, reply.page_buf().data(), count);
        }
        return reply.rc();
    }


    ManagerInstance *InstanceClient::manager_instance() { return this->manager_instance_; }
}