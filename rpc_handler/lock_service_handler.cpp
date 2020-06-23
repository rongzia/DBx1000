//
// Created by rrzhang on 2020/6/10.
//
#include <iostream>
#include "lock_service_handler.h"
#include "dbx1000_service_helper.h"

#include "common/buffer/buffer.h"
#include "common/index/index.h"
#include "lock_server/manager_lock_server.h"
#include "lock_server/lock_server_table/lock_server_table.h"
#include "common/storage/tablespace/page.h"
#include "config.h"

namespace dbx1000 {

    ::grpc::Status BufferManagerServer::LockRemote(::grpc::ServerContext* context, const ::dbx1000::LockRemoteRequest* request, ::dbx1000::LockRemoteReply* response) {

        std::cout << "BufferManagerServer::LockRemote, instance_id : " << request->instance_id() << ", page_id : " << request->page_id() << ", count : " << request->count() << std::endl;
        RC rc;
        char page_buf[MY_PAGE_SIZE];
        size_t count = request->count();
        if(count > 0) { assert(MY_PAGE_SIZE == count); }
        else {assert(0 == count);}

        rc = manager_server_->LockRemote(request->instance_id(), request->page_id(), page_buf, count);

        if(RC::TIME_OUT  == rc) {
            response->set_rc(RpcRC::TIME_OUT);
        }
        assert(RC::RCOK == rc);
        response->set_rc(RpcRC::RCOK);
        if(count > 0) {
            response->set_page_buf(page_buf, count);
        }

        return ::grpc::Status::OK;
    }

    /*
    ::grpc::Status BufferManagerServer::UnLockRemote(::grpc::ServerContext* context, const ::dbx1000::UnLockRemoteRequest* request, ::dbx1000::UnLockRemoteReply* response) {

        if(DBx1000ServiceHelper::DeSerializeLockMode(request->req_mode()) == LockMode::S){
            assert(manager_server_->lock_table()->UnLock(request->page_id()));
            return ::grpc::Status::OK;
        }
        if(DBx1000ServiceHelper::DeSerializeLockMode(request->req_mode()) == LockMode::X) {
            dbx1000::Page* page = new dbx1000::Page(new char[MY_PAGE_SIZE]);
            dbx1000::IndexItem indexItem;
            manager_server_->index()->IndexGet(request->key(), &indexItem);
            assert(request->count() == MY_PAGE_SIZE);
            assert(0 == manager_server_->buffer()->BufferPut(indexItem.page_id_, request->page_buf().data(), request->count()));

            assert(manager_server_->lock_table()->UnLock(request->page_id()));
            return ::grpc::Status::OK;
        }
    }*/

    ::grpc::Status BufferManagerServer::InstanceInitDone(::grpc::ServerContext* context, const ::dbx1000::InstanceInitDoneRequest* request
            , ::dbx1000::InstanceInitDoneReply* response) {
        manager_server_->set_instance_i(request->instance_id());
        return ::grpc::Status::OK;
    }

    ::grpc::Status BufferManagerServer::BufferManagerInitDone(::grpc::ServerContext* context, const ::dbx1000::BufferManagerInitDoneRequest* request, ::dbx1000::BufferManagerInitDonReply* response) {
        response->set_init_done(manager_server_->init_done());
        return ::grpc::Status::OK;
    }

    ::grpc::Status BufferManagerServer::GetNextTs(::grpc::ServerContext* context, const ::dbx1000::GetNextTsRequest* request, ::dbx1000::GetNextTsReply* response) {
        response->set_ts(manager_server_->GetNextTs(-1));
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

    RC BufferManagerClient::Invalid(uint64_t page_id, char *page_buf, size_t count){
        InvalidRequest request;
        ::grpc::ClientContext context;
        InvalidReply reply;

        request.set_page_id(page_id);
        request.set_count(count);

        ::grpc::Status status = stub_->Invalid(&context, request, &reply);
        assert(status.ok());

        if(RC::TIME_OUT == DBx1000ServiceHelper::DeSerializeRC(reply.rc())) {
            return RC::TIME_OUT;
        }
        assert(RC::RCOK == DBx1000ServiceHelper::DeSerializeRC(reply.rc()));
        if(count > 0) {
            assert(MY_PAGE_SIZE == count);
            assert(reply.count() == count);
            memcpy(page_buf, reply.page_buf().data(), count);
        }
        return RC::RCOK ;
    }
}