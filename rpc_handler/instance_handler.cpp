////
//// Created by rrzhang on 2020/6/10.
////
//#include <cstring>
//#include "instance_handler.h"
//
//#include "dbx1000_service_helper.h"
//
//namespace dbx1000 {
//    ::grpc::Status InstanceServer::LockInvalid(::grpc::ServerContext* context, const ::dbx1000::LockInvalidRequest* request, ::dbx1000::LockInvalidReply* response) {
//        if(request->request_mode() == DBx1000ServiceHelper::SerializeLockMode(LockMode::X)){
//            if(request.)
//        }
//        if(request->request_mode() == DBx1000ServiceHelper::SerializeLockMode(LockMode::S)){
//
//        }
//    }
//
//
//
//
//    InstanceClient::InstanceClient(const std::string &addr) : stub_(dbx1000::DBx1000Service::NewStub(
//            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())
//    )) {}
//
//    bool InstanceClient::LockRemote(int instance_id, uint64_t page_id, dbx1000::LockMode mode, char *page_buf, size_t count) {
//        dbx1000::LockRemoteRequest request;
//        grpc::ClientContext context;
//        dbx1000::LockRemoteReply reply;
//
//        request.set_instance_id(instance_id);
//        request.set_page_id(page_id);
//        request.set_request_mode(DBx1000ServiceHelper::SerializeLockMode(mode));
//        if(count > 0 && page_buf != nullptr) {
//            request.set_page_buf(page_buf, count);
//            request.set_count(count);
//        }
//
//        ::grpc::Status status = stub_->LockRemote(&context, request, &reply);
//        assert(status.ok());
//
//        if(count > 0){
//            assert(reply.count() == count);
//            assert(reply.page_buf().size() == count);
//            memcpy(page_buf, reply.page_buf().data(), count);
//        }
//        return reply.rc();
//    }
//
//
//    ManagerInstance *InstanceClient::manager_instance() { return this->manager_instance_; }
//}