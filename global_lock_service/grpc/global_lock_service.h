//
// Created by rrzhang on 2020/8/19.
//

#ifndef DBX1000_GLOBAL_LOCK_SERVICE_H
#define DBX1000_GLOBAL_LOCK_SERVICE_H


#include <grpcpp/grpcpp.h>
#include "global_lock_service.grpc.pb.h"
#include "common/lock_table/lock_table.h"
#include "common/global.h"
#include "config.h"

namespace dbx1000 {
    namespace global_lock{
        class GlobalLock;
    }
    class ManagerInstance;

    namespace global_lock_service {
        class GlobalLockServiceClient {
        public:
            GlobalLockServiceClient(const std::string&);
            GlobalLockServiceClient() = delete;
            GlobalLockServiceClient(const GlobalLockServiceClient&) = delete;
            GlobalLockServiceClient &operator=(const GlobalLockServiceClient&) = delete;

        public: /// for instance

            RC LockRemote(int instance_id, uint64_t page_id, LockMode req_mode, char *page_buf, size_t count);
            void InstanceInitDone(int instance_id);
            bool GlobalLockInitDone();
            uint64_t GetNextTs();
            ManagerInstance* manager_instance() { return this->manager_instance_; }


        public: /// for global lock
            RC Invalid(uint64_t page_id, char *page_buf, size_t count);
            // getter and setter
            global_lock::GlobalLock* global_lock() { return this->global_lock_; };

        public: /// common
            int Test();

        private:
            ManagerInstance* manager_instance_;
            global_lock::GlobalLock* global_lock_;
            std::unique_ptr<dbx1000::GlobalLockService::Stub> stub_;
        };

        class GlobalLockServiceImpl : dbx1000::GlobalLockService::Service {
        public:
            void Start(const std::string &host);

        public: /// for instance
            virtual ::grpc::Status Invalid(::grpc::ServerContext* context, const ::dbx1000::InvalidRequest* request, ::dbx1000::InvalidReply* response);
            ManagerInstance *manager_instance_;

        public: /// for global lock
            virtual ::grpc::Status LockRemote(::grpc::ServerContext* context, const ::dbx1000::LockRemoteRequest* request, ::dbx1000::LockRemoteReply* response);
            virtual ::grpc::Status InstanceInitDone(::grpc::ServerContext* context, const ::dbx1000::InstanceInitDoneRequest* request, ::dbx1000::InstanceInitDoneReply* response);
            virtual ::grpc::Status GlobalLockInitDone(::grpc::ServerContext* context, const ::dbx1000::GlobalLockInitDoneRequest* request, ::dbx1000::GlobalLockInitDoneReply* response);
            virtual ::grpc::Status GetNextTs(::grpc::ServerContext* context, const ::dbx1000::GetNextTsRequest* request, ::dbx1000::GetNextTsReply* response);

            public: /// for common
            virtual ::grpc::Status Test(::grpc::ServerContext* context, const ::dbx1000::TestRequest* request, ::dbx1000::TestReply* response);

            global_lock::GlobalLock* global_lock_;
        };
    }
}


#endif //DBX1000_GLOBAL_LOCK_SERVICE_H
