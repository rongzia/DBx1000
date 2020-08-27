//
// Created by rrzhang on 2020/8/19.
//

#include <brpc/channel.h>
#include <butil/time.h>
#include <brpc/log.h>
#include <cassert>
#include "global_lock_service.h"

#include "global_lock_service_helper.h"
#include "global_lock/global_lock.h"
#include "common/lock_table/lock_table.h"
#include "instance/manager_instance.h"

namespace dbx1000 {
    namespace global_lock_service {
        GlobalLockServiceClient::GlobalLockServiceClient(const std::string &addr) {
            auto channel = ::grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
            stub_ = dbx1000::GlobalLockService::NewStub(channel);

            ::grpc::ClientContext context;
            dbx1000::TestRequest request;
            dbx1000::TestReply reply;

            ::grpc::Status status = stub_->Test(&context, request, &reply);
            if (!status.ok()) {
                std::cerr << "try connet to " << addr << ", request failed: " << status.error_message() << std::endl;;
            }
            assert(status.ok());
        }

        RC GlobalLockServiceClient::LockRemote(int instance_id, uint64_t page_id, LockMode req_mode, char *page_buf
                                               , size_t count) {
//        cout << "InstanceClient::LockRemote page_id : " << page_id << ", count : " << count << endl;
            dbx1000::LockRemoteRequest request;
            ::grpc::ClientContext context;
            dbx1000::LockRemoteReply reply;

            request.set_instance_id(instance_id);
            request.set_page_id(page_id);
            request.set_req_mode(GlobalLockServiceHelper::SerializeLockMode(req_mode));
            request.set_count(count);

            ::grpc::Status status = stub_->LockRemote(&context, request, &reply);
            assert(status.ok());

            if (RC::TIME_OUT == GlobalLockServiceHelper::DeSerializeRC(reply.rc())) {
                return RC::TIME_OUT;
            }
            if (RC::Abort == GlobalLockServiceHelper::DeSerializeRC(reply.rc())) {
                return RC::Abort;
            }
            assert(RC::RCOK == GlobalLockServiceHelper::DeSerializeRC(reply.rc()));
            // 要是 server 版本比当前新，返回最新的版本
            if (count > 0) {
                assert(MY_PAGE_SIZE == count);
                assert(reply.count() == count);
                memcpy(page_buf, reply.page_buf().data(), count);
            }
            return RC::RCOK;
        }

        void GlobalLockServiceClient::InstanceInitDone(int instance_id) {
            dbx1000::InstanceInitDoneRequest request;
            ::grpc::ClientContext context;
            dbx1000::InstanceInitDoneReply reply;

            request.set_instance_id(instance_id);

            ::grpc::Status status = stub_->InstanceInitDone(&context, request, &reply);
            assert(status.ok());
        }

        bool GlobalLockServiceClient::GlobalLockInitDone() {
            dbx1000::GlobalLockInitDoneRequest request;
            ::grpc::ClientContext context;
            dbx1000::GlobalLockInitDoneReply reply;

            ::grpc::Status status = stub_->GlobalLockInitDone(&context, request, &reply);
            assert(status.ok());
            return reply.init_done();
        }

        uint64_t GlobalLockServiceClient::GetNextTs() {
            dbx1000::GetNextTsRequest request;
            ::grpc::ClientContext context;
            dbx1000::GetNextTsReply reply;

            ::grpc::Status status = stub_->GetNextTs(&context, request, &reply);
            assert(status.ok());
            return reply.ts();
        }

        RC GlobalLockServiceClient::Invalid(uint64_t page_id, char *page_buf, size_t count) {
            InvalidRequest request;
            ::grpc::ClientContext context;
            InvalidReply reply;

            request.set_page_id(page_id);
            request.set_count(count);

            ::grpc::Status status = stub_->Invalid(&context, request, &reply);
            if (!status.ok()) {
                std::cerr << "Invalid page : " << page_id << status.error_message() << std::endl;;
                return RC::Abort;
            }
//        assert(status.ok());

            if (RC::TIME_OUT == GlobalLockServiceHelper::DeSerializeRC(reply.rc())) {
                return RC::TIME_OUT;
            }
            assert(RC::RCOK == GlobalLockServiceHelper::DeSerializeRC(reply.rc()));
            if (count > 0) {
                assert(MY_PAGE_SIZE == count);
                assert(reply.count() == count);
                memcpy(page_buf, reply.page_buf().data(), count);
            }
            return RC::RCOK;
        }









        void GlobalLockServiceImpl::Start(const std::string &host) {
            grpc::ServerBuilder builder;
            builder.AddListeningPort(host, grpc::InsecureServerCredentials());
            builder.RegisterService(this);
            std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
            std::cout << "LockServiceServer listening on : " << host << std::endl;
            server->Wait();
        }

        ::grpc::Status
        GlobalLockServiceImpl::Invalid(::grpc::ServerContext *context, const ::dbx1000::InvalidRequest *request
                                       , ::dbx1000::InvalidReply *response) {
//        cout << "other want to invalid page " << request->page_id() << ", count : " << request->count() << endl;
            size_t count = request->count();
            if (count > 0) { assert(request->count() == MY_PAGE_SIZE); }
            else { assert(0 == count); }
            char page_buf[MY_PAGE_SIZE];
            assert(RC::RCOK == manager_instance_->lock_table()->LockInvalid(request->page_id(), page_buf, count));
            response->set_page_buf(page_buf, count);
            response->set_count(count);
            response->set_rc(RpcRC::RCOK);
            return ::grpc::Status::OK;
        }

        ::grpc::Status
        GlobalLockServiceImpl::LockRemote(::grpc::ServerContext *context, const ::dbx1000::LockRemoteRequest *request
                                          , ::dbx1000::LockRemoteReply *response) {

//        std::cout << "LockServiceServer::LockRemote, instance_id : " << request->instance_id() << ", page_id : " << request->page_id() << ", count : " << request->count() << std::endl;
            RC rc;
            char *page_buf = nullptr;
            size_t count = request->count();
            if (count > 0) {
                assert(MY_PAGE_SIZE == count);
                page_buf = new char[MY_PAGE_SIZE];
            } else { assert(0 == count); }

            rc = global_lock_->LockRemote(request->instance_id(), request->page_id(), page_buf, count);

            if (RC::TIME_OUT == rc) {
                response->set_rc(RpcRC::TIME_OUT);
            }
            if (RC::Abort == rc) {
                response->set_rc(RpcRC::Abort);
                return ::grpc::Status::OK;
            }
            assert(RC::RCOK == rc);
            response->set_rc(RpcRC::RCOK);
            if (count > 0) {
                response->set_page_buf(page_buf, count);
            }
            response->set_count(count);

            return ::grpc::Status::OK;
        }

        /*
        ::grpc::Status GlobalLockServiceImpl::UnLockRemote(::grpc::ServerContext* context, const ::dbx1000::UnLockRemoteRequest* request, ::dbx1000::UnLockRemoteReply* response) {

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

        ::grpc::Status GlobalLockServiceImpl::InstanceInitDone(::grpc::ServerContext *context
                                                               , const ::dbx1000::InstanceInitDoneRequest *request
                                                               , ::dbx1000::InstanceInitDoneReply *response) {
            global_lock_->set_instance_i(request->instance_id());
            return ::grpc::Status::OK;
        }

        ::grpc::Status GlobalLockServiceImpl::GlobalLockInitDone(::grpc::ServerContext *context
                                                                 , const ::dbx1000::GlobalLockInitDoneRequest *request
                                                                 , ::dbx1000::GlobalLockInitDoneReply *response) {
            response->set_init_done(global_lock_->init_done());
            return ::grpc::Status::OK;
        }

        ::grpc::Status
        GlobalLockServiceImpl::GetNextTs(::grpc::ServerContext *context, const ::dbx1000::GetNextTsRequest *request
                                         , ::dbx1000::GetNextTsReply *response) {
            response->set_ts(global_lock_->GetNextTs(-1));
            return ::grpc::Status::OK;
        }

        ::grpc::Status GlobalLockServiceImpl::Test(::grpc::ServerContext* context, const ::dbx1000::TestRequest* request, ::dbx1000::TestReply* response){
            return ::grpc::Status::OK;
        }
    }
}