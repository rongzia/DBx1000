////
//// Created by rrzhang on 2020/6/10.
////
//
//#ifndef DBX1000_INSTANCE_HANDLER_H
//#define DBX1000_INSTANCE_HANDLER_H
//
//#include <grpcpp/grpcpp.h>
//#include "proto/dbx1000_service.grpc.pb.h"
//#include "common/lock_table/lock_table.h"
//
//namespace dbx1000 {
//    class ManagerInstance;
//
//    class InstanceServer : DBx1000Service::Service {
//    public:
//        virtual ::grpc::Status LockInvalid(::grpc::ServerContext *context, const ::dbx1000::LockInvalidRequest *request , ::dbx1000::LockInvalidReply *response);
//
//        ManagerInstance *manager_instance_;
//    };
//
//
//    class InstanceClient{
//    public:
//        InstanceClient(const std::string&);
//        InstanceClient() = delete;
//        InstanceClient(const InstanceClient&) = delete;
//        InstanceClient &operator=(const InstanceClient&) = delete;
//
//        bool LockRemote(int instance_id, uint64_t page_id, dbx1000::LockMode mode, char *page_buf, size_t count);
//
//        ManagerInstance* manager_instance();
//
//     private:
//        std::unique_ptr<dbx1000::DBx1000Service::Stub> stub_;
//        ManagerInstance* manager_instance_;
//    };
//}
//
//
//#endif //DBX1000_INSTANCE_HANDLER_H
