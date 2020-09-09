//
// Created by rrzhang on 2020/8/19.
//

#ifndef DBX1000_GLOBAL_LOCK_SERVICE_H
#define DBX1000_GLOBAL_LOCK_SERVICE_H

#include <brpc/channel.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <thread>
#include "common/lock_table/lock_table.h"
#include "common/global.h"
#include "global_lock_service.pb.h"

namespace dbx1000 {
    namespace global_lock{
        class GlobalLock;
    }
    class ManagerInstance;
    class Stats;

    namespace global_lock_service {

        class OnLockRemoteDone;
        class OnInstanceInitDone;
        class OnInvalidDone;
        class OnAllInstanceReadyDone;

        class GlobalLockServiceClient {
        public:
            GlobalLockServiceClient(const std::string&);
            GlobalLockServiceClient() = delete;
            GlobalLockServiceClient(const GlobalLockServiceClient&) = delete;
            GlobalLockServiceClient &operator=(const GlobalLockServiceClient&) = delete;

        public: /// for instance

            RC LockRemote(int instance_id, uint64_t page_id, LockMode req_mode, char *page_buf, size_t count);
            void AsyncLockRemote(int instance_id, uint64_t page_id, LockMode req_mode, char *page_buf, size_t count, OnLockRemoteDone* done);
            void InstanceInitDone(int instance_id);
            void AsyncInstanceInitDone(int instance_id, OnInstanceInitDone* done);
            bool GlobalLockInitDone();
            uint64_t GetNextTs();
            void ReportResult(const Stats &stats, int instance_id);
            ManagerInstance* manager_instance() { return this->manager_instance_; }


        public: /// for global lock
            RC Invalid(uint64_t page_id, char *page_buf, size_t count);
            void AsyncInvalid(uint64_t page_id, char *page_buf, size_t count, OnInvalidDone* done);
            void AllInstanceReady();
            void AsyncAllInstanceReady(OnAllInstanceReadyDone* done);
            // getter and setter
            global_lock::GlobalLock* global_lock() { return this->global_lock_; };

        public: /// common
            int Test();

         private:
            ManagerInstance* manager_instance_;
            global_lock::GlobalLock* global_lock_;
            ::brpc::Channel channel_;
            std::unique_ptr<dbx1000::GlobalLockService_Stub> stub_;
        };

        class GlobalLockServiceImpl : dbx1000::GlobalLockService {
        public:
            GlobalLockServiceImpl() {}
            ~GlobalLockServiceImpl() {}
            void Start( const std::string &host);

        public: /// for instance
            virtual void Invalid(::google::protobuf::RpcController* controller,
                               const ::dbx1000::InvalidRequest* request,
                               ::dbx1000::InvalidReply* response,
                               ::google::protobuf::Closure* done);
            virtual void AsyncInvalid(::google::protobuf::RpcController* controller,
                                      const ::dbx1000::InvalidRequest* request,
                                      ::dbx1000::InvalidReply* response,
                                      ::google::protobuf::Closure* done);
            virtual void AllInstanceReady(::google::protobuf::RpcController* controller,
                    const ::dbx1000::AllInstanceReadyRequest* request,
                    ::dbx1000::AllInstanceReadyReply* response,
                    ::google::protobuf::Closure* done);
            ManagerInstance *manager_instance_;

        public: /// for global lock
            virtual void LockRemote(::google::protobuf::RpcController* controller,
                               const ::dbx1000::LockRemoteRequest* request,
                               ::dbx1000::LockRemoteReply* response,
                               ::google::protobuf::Closure* done);
            virtual void AsyncLockRemote(::google::protobuf::RpcController* controller,
                               const ::dbx1000::LockRemoteRequest* request,
                               ::dbx1000::LockRemoteReply* response,
                               ::google::protobuf::Closure* done);
            virtual void InstanceInitDone(::google::protobuf::RpcController* controller,
                               const ::dbx1000::InstanceInitDoneRequest* request,
                               ::dbx1000::InstanceInitDoneReply* response,
                               ::google::protobuf::Closure* done);
            virtual void GlobalLockInitDone(::google::protobuf::RpcController* controller,
                               const ::dbx1000::GlobalLockInitDoneRequest* request,
                               ::dbx1000::GlobalLockInitDoneReply* response,
                               ::google::protobuf::Closure* done);
            virtual void GetNextTs(::google::protobuf::RpcController* controller,
                               const ::dbx1000::GetNextTsRequest* request,
                               ::dbx1000::GetNextTsReply* response,
                               ::google::protobuf::Closure* done);
            virtual void ReportResult(::google::protobuf::RpcController* controller,
                    const ::dbx1000::ReportResultRequest* request,
                    ::dbx1000::ReportResultReply* response,
                    ::google::protobuf::Closure* done);

            public: /// for common
            virtual void Test(::google::protobuf::RpcController* controller,
                       const ::dbx1000::TestRequest* request,
                       ::dbx1000::TestReply* response,
                       ::google::protobuf::Closure* done);

            global_lock::GlobalLock* global_lock_;
            brpc::Server server;
        };


        /// 异步回调
        class OnLockRemoteDone: public google::protobuf::Closure {
        public:
            void Run();

            LockRemoteReply reply;
            brpc::Controller cntl;
            size_t count;
            char *page_buf;
        };

        class OnInstanceInitDone: public google::protobuf::Closure {
        public:
            void Run();

            InstanceInitDoneReply reply;
            brpc::Controller cntl;
        };

        class OnInvalidDone: public google::protobuf::Closure {
        public:
            void Run();

            InvalidReply reply;
            brpc::Controller cntl;
            uint64_t page_id;
            size_t count;
            char *page_buf;
        };

        class OnAllInstanceReadyDone: public google::protobuf::Closure {
        public:
            void Run();

            AllInstanceReadyReply reply;
            brpc::Controller cntl;
        };

        /// 服务端异步
        struct AsyncLockRemoteJob {
            brpc::Controller* cntl;
            const LockRemoteRequest* request;
            LockRemoteReply* reply;
            google::protobuf::Closure* done;
            global_lock::GlobalLock* global_lock_;

            void run();

            void run_and_delete() {
                run();
                delete this;
            }
        };

        struct AsyncInvalidJob {
            brpc::Controller* cntl;
            const InvalidRequest* request;
            InvalidReply* reply;
            google::protobuf::Closure* done;
            ManagerInstance* manager_instance_;

            void run();

            void run_and_delete() {
                run();
                delete this;
            }
        };

    }   // global_lock_service
} // dbx1000


#endif //DBX1000_GLOBAL_LOCK_SERVICE_H
