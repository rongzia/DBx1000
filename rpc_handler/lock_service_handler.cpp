//
// Created by rrzhang on 2020/6/10.
//
#include <iostream>
#include "lock_service_handler.h"
#include "dbx1000_service_helper.h"

#include "lock_server/manager_lock_server.h"
#include "lock_server/lock_server_table/lock_server_table.h"
#include "config.h"

namespace dbx1000 {

    ::grpc::Status BufferManagerServer::LockRemote(::grpc::ServerContext* context, const ::dbx1000::LockRemoteRequest* request, ::dbx1000::LockRemoteReply* response) {
        char a;
        if (DBx1000ServiceHelper::DeSerializeLockMode(request->request_mode()) == LockMode::X) { a = 'X'; }
        else if (DBx1000ServiceHelper::DeSerializeLockMode(request->request_mode()) == LockMode::S) { a = 'S'; }
        else if (DBx1000ServiceHelper::DeSerializeLockMode(request->request_mode()) == LockMode::P) { a = 'P'; }
        else if (DBx1000ServiceHelper::DeSerializeLockMode(request->request_mode()) == LockMode::O) { a = 'O'; }
        std::cout << "BufferManagerServer::LockRemote, instance_id:" << request->instance_id() << ", page_id:" << request->page_id()
        << ", mode:" << a << ", count:" << request->count() << std::endl;
        size_t size = request->count();

        if(size > 0) {
            assert(MY_PAGE_SIZE == size);
            char page_buf[size];
            manager_server_->lock_table()->Lock(request->instance_id(), request->page_id(), DBx1000ServiceHelper::DeSerializeLockMode(request->request_mode()), page_buf, size);
            response->set_count(size);
            response->set_page_buf(page_buf, size);
        std::cout << "BufferManagerServer::LockRemote, instance_id:" << request->instance_id() << " get " << a << " lock on page:" << request->page_id() << " succ"", page_id:" << std::endl;
            return ::grpc::Status::OK;
        }
        if(size == 0) {
            manager_server_->lock_table()->Lock(request->instance_id(), request->page_id(), DBx1000ServiceHelper::DeSerializeLockMode(request->request_mode()), nullptr, 0);
            response->set_count(0);
            return ::grpc::Status::OK;
        }

        assert(false);
    }

    ::grpc::Status BufferManagerServer::InstanceInitDone(::grpc::ServerContext* context, const ::dbx1000::InstanceInitDoneRequest* request
            , ::dbx1000::InstanceInitDoneReply* response) {
        manager_server_->set_instance_i(request->instance_id());
        return ::grpc::Status::OK;
    }

    ::grpc::Status BufferManagerServer::BufferManagerInitDone(::grpc::ServerContext* context, const ::dbx1000::BufferManagerInitDoneRequest* request, ::dbx1000::BufferManagerInitDonReplye* response) {
        response->set_init_done(manager_server_->init_done());
        return ::grpc::Status::OK;
    }

    ::grpc::Status BufferManagerServer::GetTestNum(::grpc::ServerContext* context, const ::dbx1000::GetTestNumRequest* request, ::dbx1000::GetTestNumReply* response) {
        response->set_num(manager_server_->test_num);
        return ::grpc::Status::OK;
    }

    void BufferManagerServer::Start(const std::string& host) {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(host, grpc::InsecureServerCredentials());
        builder.RegisterService(this);
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        std::cout << "BufferManagerServer listening on : " << host << std::endl;
        server->Wait();
    }





    BufferManagerClient::BufferManagerClient(const std::string &addr) : stub_(dbx1000::DBx1000Service::NewStub(
            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())
    )) {}

    bool BufferManagerClient::LockInvalid(uint64_t page_id, char *page_buf, size_t count) {
        std::cout << "BufferManagerClient::LockInvalid, page_id:" << page_id << ", count:" << count << std::endl;
        dbx1000::LockInvalidRequest request;
        ::grpc::ClientContext context;
        dbx1000::LockInvalidReply reply;

//        request.set_instance_id(instance_id);
//        request.set_request_mode(DBx1000ServiceHelper::SerializeLockMode(req_mode));
        request.set_page_id(page_id);
        request.set_count(count);

        ::grpc::Status status = stub_->LockInvalid(&context, request, &reply);
        assert(status.ok());
        assert(reply.rc() == true);
        if(count > 0){
            assert(count == MY_PAGE_SIZE);
            memcpy(page_buf, reply.page_buf().data(), count);
        }
        return reply.rc();
    }
}