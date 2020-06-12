////
//// Created by rrzhang on 2020/6/10.
////
//
//#include "buffer_manager_handler.h"
//#include "dbx1000_service_helper.h"
//
//#include "buffer_manager/manager_server.h"
//#include "buffer_manager/server_lock_table/server_lock_table.h"
//#include "config.h"
//
//namespace dbx1000 {
//    ::grpc::Status BufferManagerServer::LockGet(::grpc::ServerContext *context, const ::dbx1000::LockGetRequest *request
//                                                , ::dbx1000::LockGetReply *response) {
//        char *page_buf = nullptr;
//        size_t count = 0;
//        bool rc = manager_server_->lock_table()->LockGet(request->instance_id(), request->page_id()
//                , DBx1000ServiceHelper::DeSerializeLockMode(request->mode()), page_buf, count);
//        if(nullptr != page_buf) {
//            assert(count == MY_PAGE_SIZE);
//            response->set_page_buf(page_buf, count);
//        }
//        response->set_rc(rc);
//
//        if(nullptr != page_buf) {
//            delete page_buf;
//        }
//
//        return ::grpc::Status::OK;
//    }
//
//
//
//
//
//    BufferManagerClient::BufferManagerClient(const std::string &addr) : stub_(dbx1000::DBx1000Service::NewStub(
//            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())
//    )) {}
//
//    bool
//    BufferManagerClient::LockDowngrade(uint64_t page_id, LockMode from, LockMode to, char *page_buf, size_t count) {
//        dbx1000::LockDowngradeRequest request;
//        ::grpc::ClientContext context;
//        dbx1000::LockDowngradeReply reply;
//
//        request.set_page_id(page_id);
//        request.set_from(DBx1000ServiceHelper::SerializeLockMode(from));
//        request.set_to(DBx1000ServiceHelper::SerializeLockMode(to));
//
//        ::grpc::Status status = stub_->LockDowngrade(&context, request, &reply);
//        assert(status.ok());
//
//        if (reply.page_buf().size() == count) {
//            memcpy(page_buf, reply.page_buf().data(), count);
//        }
//        return reply.rc();
//    }
//
//    bool BufferManagerClient::LockInvalid(uint64_t page_id) {
//        dbx1000::LockInvalidRequest request;
//        ::grpc::ClientContext context;
//        dbx1000::LockInvalidReply reply;
//
//        request.set_page_id(page_id);
//
//        ::grpc::Status status = stub_->LockInvalid(&context, request, &reply);
//        assert(status.ok());
//        return reply.rc();
//    }
//}