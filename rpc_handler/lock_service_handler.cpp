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

//        std::cout << "BufferManagerServer::LockRemote, instance_id:" << request->instance_id() << ", page_id:" << request->page_id()
//        << ", mode:" << a << ", count:" << request->count() << std::endl;
        size_t size = request->count();


        dbx1000::Page* page = new dbx1000::Page(new char[MY_PAGE_SIZE]);
        dbx1000::IndexItem indexItem;
        manager_server_->index()->IndexGet(request->key(), &indexItem);
        bool rc = manager_server_->lock_table()->Lock(indexItem.page_id_, DBx1000ServiceHelper::DeSerializeLockMode(request->req_mode()));
        assert(true == rc);
        manager_server_->buffer()->BufferGet(indexItem.page_id_, page->page_buf(), MY_PAGE_SIZE);
        page->Deserialize();
        // TODO 校验row长度
//        assert(row->size_ == ((ycsb_wl *) (this->m_workload_))->the_table->get_schema()->get_tuple_size());
        uint64_t key_version;
        // TODO 72 要修改
        memcpy(&key_version, &page->page_buf()[indexItem.page_location_ + 72], sizeof(uint64_t));

        if(page->version() > request->page_version() && key_version > request->key_version()) {
            assert(request->count() == MY_PAGE_SIZE);
            response->set_page_buf(page->page_buf(), request->count());
            response->set_count(MY_PAGE_SIZE);
        } else {
            response->set_count(0);
        }
        if(DBx1000ServiceHelper::DeSerializeLockMode(request->req_mode()) == LockMode::X) {
            manager_server_->test_num++;
        }

        return ::grpc::Status::OK;
    }

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
    }

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
}