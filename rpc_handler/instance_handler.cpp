//
// Created by rrzhang on 2020/6/10.
//
#include <cstring>
#include "instance_handler.h"
#include "dbx1000_service_helper.h"

#include "instance/manager_instance.h"
#include "config.h"

namespace dbx1000 {
    ::grpc::Status InstanceServer::LockInvalid(::grpc::ServerContext* context, const ::dbx1000::LockInvalidRequest* request, ::dbx1000::LockInvalidReply* response) {
        std::cout << "InstanceServer::LockInvalid, ins_id:" << manager_instance_->instance_id() << ", page_id:" << request->page_id() << ", count:" << request->count() << std::endl;
//        while (!ATOM_CAS(manager_instance_->lock_table()->lock_table()[request->page_id()]->invalid_req, false, true))
//			PAUSE
manager_instance_->lock_table()->lock_table()[request->page_id()]->invalid_req = true;
        manager_instance_->lock_table()->LockInvalid(request->page_id());


        int count = request->count();
        if(count > 0){
            assert(count == MY_PAGE_SIZE);
            // TODO : 设置 response->page_buf
            char buf[count];
            memset(buf, 0 ,count);
            response->set_page_buf(buf, count);
        }
        manager_instance_->lock_table()->lock_table()[request->page_id()]->invalid_req = false;
        response->set_rc(true);
        response->set_count(count);
        return ::grpc::Status::OK;
    }

    void InstanceServer::Start(const std::string& host){
        grpc::ServerBuilder builder;
        builder.AddListeningPort(host, grpc::InsecureServerCredentials());
        builder.RegisterService(this);
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        std::cout << "InstanceServer listening on : " << host << std::endl;
        server->Wait();
    }




    InstanceClient::InstanceClient(const std::string &addr) : stub_(dbx1000::DBx1000Service::NewStub(
            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())
    )) {}

    bool InstanceClient::LockRemote(int instance_id, uint64_t page_id, dbx1000::LockMode mode, char *page_buf, size_t count) {
        cout << "InstanceClient::LockRemote, instance_id:" << instance_id << ", page_id:" << page_id
        << ", mode:" << manager_instance_->lock_table()->LockModeToChar(mode) << ", count:" << count << endl;
        dbx1000::LockRemoteRequest request;
        grpc::ClientContext context;
        dbx1000::LockRemoteReply reply;

        request.set_instance_id(instance_id);
        request.set_page_id(page_id);
        request.set_request_mode(DBx1000ServiceHelper::SerializeLockMode(mode));
        if(count > 0 && page_buf != nullptr) {
//            request.set_page_buf(page_buf, count);
            assert(count == MY_PAGE_SIZE);
            request.set_count(count);
        }

        ::grpc::Status status = stub_->LockRemote(&context, request, &reply);
        assert(status.ok());

        if(count > 0){
            assert(reply.count() == count);
            assert(reply.page_buf().size() == count);
            memcpy(page_buf, reply.page_buf().data(), count);
        }
        return reply.rc();
    }

    void InstanceClient::InstanceInitDone(int instance_id) {
        dbx1000::InstanceInitDoneRequest request;
        ::grpc::ClientContext context;
        dbx1000::InstanceInitDoneReply reply;

        request.set_instance_id(instance_id);

        ::grpc::Status status = stub_->InstanceInitDone(&context, request, &reply);
        assert(status.ok());
    }

    bool InstanceClient::BufferManagerInitDone() {
        dbx1000::BufferManagerInitDoneRequest request;
        ::grpc::ClientContext context;
        dbx1000::BufferManagerInitDonReplye reply;

        ::grpc::Status status = stub_->BufferManagerInitDone(&context, request, &reply);
        assert(status.ok());
        return reply.init_done();
    }

    int InstanceClient::GetTestNum(){
        dbx1000::GetTestNumRequest request;
        ::grpc::ClientContext context;
        dbx1000::GetTestNumReply reply;

        ::grpc::Status status = stub_->GetTestNum(&context, request, &reply);
        assert(status.ok());
        return reply.num();
    }

    ManagerInstance *InstanceClient::manager_instance() { return this->manager_instance_; }
}