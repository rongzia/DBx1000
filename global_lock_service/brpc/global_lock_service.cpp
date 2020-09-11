//
// Created by rrzhang on 2020/8/19.
//

#include <brpc/channel.h>
#include <butil/time.h>
#include <brpc/log.h>
#include <cassert>
#include "global_lock_service.h"

#include "common/mystats.h"
#include "global_lock_service_helper.h"
#include "global_lock/global_lock.h"
#include "instance/manager_instance.h"

namespace dbx1000 {
    namespace global_lock_service {
        GlobalLockServiceClient::GlobalLockServiceClient(const std::string &addr) {
            brpc::ChannelOptions options;
            options.use_rdma = false;
            options.connect_timeout_ms = 6000; // 1 分钟
            options.timeout_ms = 6000;         // 1 分钟
            if (channel_.Init(addr.data(), &options) != 0) {
                LOG(FATAL) << "Fail to initialize channel";
                assert(false);
            }
            stub_.reset(new dbx1000::GlobalLockService_Stub(&channel_));
            Test();
        }

        RC GlobalLockServiceClient::LockRemote(int instance_id, uint64_t page_id, LockMode req_mode, char *page_buf, size_t count) {
//            cout << "GlobalLockServiceClient::LockRemote instance_id: " << instance_id << ", page_id: " << page_id << ", count: " << count << endl;
            dbx1000::LockRemoteRequest request;
            dbx1000::LockRemoteReply reply;
            ::brpc::Controller cntl;

            request.set_instance_id(instance_id);
            request.set_page_id(page_id);
            request.set_req_mode(GlobalLockServiceHelper::SerializeLockMode(req_mode));
            request.set_count(count);

//            stub_->LockRemote(&cntl, &request, &reply, NULL);
            stub_->AsyncLockRemote(&cntl, &request, &reply, NULL);
            if (!cntl.Failed()) {
                RC rc = GlobalLockServiceHelper::DeSerializeRC(reply.rc());
                if(RC::TIME_OUT == rc) {
                    return RC::TIME_OUT;
                }
                if(RC::Abort == rc) {
                    return RC::Abort;
                }
                assert(RC::RCOK == rc);
                // 要是 server 数据版本比当前新，返回最新的版本
                if(count > 0) {
                    assert(MY_PAGE_SIZE == count);
                    assert(reply.count() == count);
                    memcpy(page_buf, reply.page_buf().data(), count);
                }
                return RC::RCOK ;
            } else {
                if (cntl.ErrorCode() == 1008){return RC::Abort;}
                LOG(FATAL) << cntl.ErrorText();
                assert(false);
                return RC::Abort;
            }
        }

        void GlobalLockServiceClient::AsyncLockRemote(int instance_id, uint64_t page_id, LockMode req_mode, char *page_buf, size_t count, OnLockRemoteDone* done) {
//            cout << "GlobalLockServiceClient::LockRemote instance_id: " << instance_id << ", page_id: " << page_id << ", count: " << count << endl;
            dbx1000::LockRemoteRequest request;
            request.set_instance_id(instance_id);
            request.set_page_id(page_id);
            request.set_req_mode(GlobalLockServiceHelper::SerializeLockMode(req_mode));
            request.set_count(count);

            done->page_buf = page_buf;
            done->count = count;

            stub_->AsyncLockRemote(&done->cntl, &request, &done->reply, done);
//            stub_->LockRemote(&done->cntl, &request, &done->reply, done);
        }

        void GlobalLockServiceClient::InstanceInitDone(int instance_id) {
            dbx1000::InstanceInitDoneRequest request;
            dbx1000::InstanceInitDoneReply reply;
            ::brpc::Controller cntl;

            request.set_instance_id(instance_id);

            stub_->InstanceInitDone(&cntl, &request, &reply, nullptr);
            if (!cntl.Failed()) { } else {
                LOG(FATAL) << cntl.ErrorText();
                assert(false);
            }
        }

        void GlobalLockServiceClient::AsyncInstanceInitDone(int instance_id, OnInstanceInitDone* done) {
            dbx1000::InstanceInitDoneRequest request;

            request.set_instance_id(instance_id);

            stub_->InstanceInitDone(&done->cntl, &request, &done->reply, done);
        }

        bool GlobalLockServiceClient::GlobalLockInitDone() {
            dbx1000::GlobalLockInitDoneRequest request;
            dbx1000::GlobalLockInitDoneReply reply;
            ::brpc::Controller cntl;

//            stub_->GlobalLockInitDone(&cntl, &request, &reply, nullptr);
            stub_->AsyncGlobalLockInitDone(&cntl, &request, &reply, nullptr);
            if (!cntl.Failed()) {
                return reply.init_done();
            } else {
//                LOG(FATAL) << cntl.ErrorText();
//                assert(false);
                return false;
            }
        }

        uint64_t GlobalLockServiceClient::GetNextTs() {
            dbx1000::GetNextTsRequest request;
            dbx1000::GetNextTsReply reply;
            ::brpc::Controller cntl;

            stub_->GetNextTs(&cntl, &request, &reply, nullptr);
            if (!cntl.Failed()) {
                return reply.ts();
            } else {
                LOG(FATAL) << cntl.ErrorText();
                assert(false);
            }
        }

        void GlobalLockServiceClient::ReportResult(const Stats &stats, int instance_id) {
            dbx1000::ReportResultRequest request;
            dbx1000::ReportResultReply reply;
            ::brpc::Controller cntl;

            request.set_total_run_time(stats.total_run_time_);
            request.set_total_txn_cnt(stats.total_txn_cnt_);
            request.set_total_latency(stats.total_latency_);
            request.set_total_time_remote_lock(stats.total_time_remote_lock_);
            request.set_instance_run_time(stats.instance_run_time_);
            request.set_instance_id(instance_id);

            stub_->ReportResult(&cntl, &request, &reply, nullptr);
            if (!cntl.Failed()) { } else {
                LOG(FATAL) << cntl.ErrorText();
                assert(false);
            }
        }

        RC GlobalLockServiceClient::Invalid(uint64_t page_id, char *page_buf, size_t count){
            dbx1000::InvalidRequest request;
            dbx1000::InvalidReply reply;
            ::brpc::Controller cntl;

            request.set_page_id(page_id);
            request.set_count(count);

//            cntl.set_timeout_ms(20);
//            stub_->Invalid(&cntl, &request, &reply, nullptr);
            stub_->AsyncInvalid(&cntl, &request, &reply, nullptr);

            if (!cntl.Failed()) {
//                cout << "remote Invalid page:" << page_id << " success." << endl;
                RC rc = GlobalLockServiceHelper::DeSerializeRC(reply.rc());
                if(RC::TIME_OUT == rc) {
                    return RC::TIME_OUT;
                } else if(RC::Abort == rc) {
                    return RC::Abort;
                }
                assert(RC::RCOK == rc);
                if(count > 0) {
                    assert(MY_PAGE_SIZE == count);
                    assert(reply.count() == count);
                    memcpy(page_buf, reply.page_buf().data(), count);
                }
                return RC::RCOK ;
            } else {
                if(cntl.ErrorCode() == 1008){
                    return RC::TIME_OUT;
                }
                LOG(FATAL) << cntl.ErrorText();
                assert(false);
                return RC::Abort;
            }
        }

        void GlobalLockServiceClient::AsyncInvalid(uint64_t page_id, char *page_buf, size_t count, OnInvalidDone* done){
            dbx1000::InvalidRequest request;

            request.set_page_id(page_id);
            request.set_count(count);

            done->count = count;
            done->page_buf = page_buf;
            done->page_id = page_id;

            stub_->Invalid(&done->cntl, &request, &done->reply, done);
        }

        void GlobalLockServiceClient::AllInstanceReady(){
            dbx1000::AllInstanceReadyRequest request;
            dbx1000::AllInstanceReadyReply reply;
            brpc::Controller cntl;

            stub_->AllInstanceReady(&cntl, &request, &reply, nullptr);
        }

        void GlobalLockServiceClient::AsyncAllInstanceReady(OnAllInstanceReadyDone* done){
            dbx1000::AllInstanceReadyRequest request;
            stub_->AllInstanceReady(&done->cntl, &request, &done->reply, done);
        }

        int GlobalLockServiceClient::Test() {
            dbx1000::TestRequest request;
            dbx1000::TestReply reply;
            ::brpc::Controller cntl;

            stub_->Test(&cntl, &request, &reply, nullptr);
            if (!cntl.Failed()) {
                return 0;
            } else {
                LOG(FATAL) << cntl.ErrorText();
                return -1;
            }
        }









        void GlobalLockServiceImpl::Start( const std::string &host) {
//            brpc::Server server;

            if (this->server.AddService(this, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(FATAL) << "Fail to add service";
                assert(false);
            }

            brpc::ServerOptions options;
            if (this->server.Start(std::stoi(host.substr(host.find(':')+1)), &options) != 0) {
                LOG(FATAL) << "Fail to start GlobalLockServiceImpl";
                assert(false);
            }
//            this->server.RunUntilAskedToQuit();
        }

        void GlobalLockServiceImpl::Invalid(::google::protobuf::RpcController* controller,
                               const ::dbx1000::InvalidRequest* request,
                               ::dbx1000::InvalidReply* response,
                               ::google::protobuf::Closure* done) {
//            cout << "other want to invalid page " << request->page_id() << ", count : " << request->count() << endl;
            ::brpc::ClosureGuard done_guard(done);
            ::brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

            size_t count = request->count();
            if(count > 0) { assert(request->count() == MY_PAGE_SIZE); }
            else { assert(0 == count); }
            char page_buf[MY_PAGE_SIZE];
//            cout << request->page_id() << " GlobalLockServiceImpl::Invalid in" << endl;
            RC rc = manager_instance_->lock_table()->LockInvalid(request->page_id(), page_buf, count);
            assert(RC::RCOK == rc || RC::TIME_OUT == rc);
            if(RC::TIME_OUT == rc){
                response->set_rc(RpcRC::TIME_OUT);
            } else if(RC::RCOK == rc){
                response->set_page_buf(page_buf, count);
                response->set_count(count);
                response->set_rc(RpcRC::RCOK);
            } else {
                response->set_rc(RpcRC::Abort);
            }
//            cout << request->page_id() << " GlobalLockServiceImpl::Invalid out" << endl;
        }

        void GlobalLockServiceImpl::AsyncInvalid(::google::protobuf::RpcController* controller,
                               const ::dbx1000::InvalidRequest* request,
                               ::dbx1000::InvalidReply* response,
                               ::google::protobuf::Closure* done) {
            ::brpc::ClosureGuard done_guard(done);

            AsyncInvalidJob *job = new AsyncInvalidJob();
            job->cntl = static_cast<brpc::Controller *>(controller);
            job->request = request;
            job->reply = response;
            job->done = done;
            job->manager_instance_ = this->manager_instance_;

            auto process_thread = [](void* arg) -> void* {
                AsyncInvalidJob* job = static_cast<AsyncInvalidJob*>(arg);
                job->run_and_delete();
                return nullptr;
            };

            bthread_t th;
            CHECK_EQ(0, bthread_start_background(&th, NULL, process_thread, job));
            done_guard.release();
        }

        void GlobalLockServiceImpl::AllInstanceReady(::google::protobuf::RpcController *controller
                                                     , const ::dbx1000::AllInstanceReadyRequest *request
                                                     , ::dbx1000::AllInstanceReadyReply *response, ::google::protobuf::Closure *done) {
            ::brpc::ClosureGuard done_guard(done);
            ::brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
            manager_instance_->all_instances_ready_ = true;
        }

        void GlobalLockServiceImpl::LockRemote(::google::protobuf::RpcController* controller,
                               const ::dbx1000::LockRemoteRequest* request,
                               ::dbx1000::LockRemoteReply* response,
                               ::google::protobuf::Closure* done) {
//            std::cout << "GlobalLockServiceImpl::LockRemote, instance_id : " << request->instance_id() << ", page_id : " << request->page_id() << ", count : " << request->count() << std::endl;
            ::brpc::ClosureGuard done_guard(done);
            ::brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

            RC rc;
            char *page_buf = nullptr;
            size_t count = request->count();
            if(count > 0) {
                assert(MY_PAGE_SIZE == count);
                page_buf = new char [MY_PAGE_SIZE];
            }
            else {assert(0 == count);}

            rc = global_lock_->LockRemote(request->instance_id(), request->page_id(), page_buf, count);

            if(RC::TIME_OUT  == rc) {
                response->set_rc(RpcRC::TIME_OUT);
                return;
            }
            if(RC::Abort  == rc) {
                response->set_rc(RpcRC::Abort);
                return;
            }
            assert(RC::RCOK == rc);
            response->set_rc(RpcRC::RCOK);
            if(count > 0) {
                response->set_page_buf(page_buf, count);
            }
            response->set_count(count);
        }

        void GlobalLockServiceImpl::AsyncLockRemote(::google::protobuf::RpcController* controller,
                               const ::dbx1000::LockRemoteRequest* request,
                               ::dbx1000::LockRemoteReply* response,
                               ::google::protobuf::Closure* done) {
            ::brpc::ClosureGuard done_guard(done);

            AsyncLockRemoteJob *job = new AsyncLockRemoteJob();
            job->cntl = static_cast<brpc::Controller *>(controller);
            job->request = request;
            job->reply = response;
            job->done = done;
            job->global_lock_ = this->global_lock_;

            auto process_thread = [](void* arg) -> void* {
                AsyncLockRemoteJob* job = static_cast<AsyncLockRemoteJob*>(arg);
                job->run_and_delete();
                return nullptr;
            };

            bthread_t th;
            CHECK_EQ(0, bthread_start_background(&th, NULL, process_thread, job));
            done_guard.release();
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

        void GlobalLockServiceImpl::InstanceInitDone(::google::protobuf::RpcController* controller,
                               const ::dbx1000::InstanceInitDoneRequest* request,
                               ::dbx1000::InstanceInitDoneReply* response,
                               ::google::protobuf::Closure* done) {
            ::brpc::ClosureGuard done_guard(done);
            ::brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

            global_lock_->set_instance_i(request->instance_id());
        }

        void GlobalLockServiceImpl::GlobalLockInitDone(::google::protobuf::RpcController* controller,
                               const ::dbx1000::GlobalLockInitDoneRequest* request,
                               ::dbx1000::GlobalLockInitDoneReply* response,
                               ::google::protobuf::Closure* done) {
            ::brpc::ClosureGuard done_guard(done);
            ::brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
            response->set_init_done(global_lock_->init_done());
        }

        void GlobalLockServiceImpl::AsyncGlobalLockInitDone(::google::protobuf::RpcController* controller,
                               const ::dbx1000::GlobalLockInitDoneRequest* request,
                               ::dbx1000::GlobalLockInitDoneReply* response,
                               ::google::protobuf::Closure* done) {
            ::brpc::ClosureGuard done_guard(done);

            AsyncGlobalLockInitDoneJob *job = new AsyncGlobalLockInitDoneJob();
            job->cntl = static_cast<brpc::Controller *>(controller);
            job->request = request;
            job->reply = response;
            job->done = done;
            job->global_lock_ = this->global_lock_;

            auto process_thread = [](void* arg) -> void* {
                AsyncGlobalLockInitDoneJob* job = static_cast<AsyncGlobalLockInitDoneJob*>(arg);
                job->run_and_delete();
                return nullptr;
            };

            bthread_t th;
            CHECK_EQ(0, bthread_start_background(&th, NULL, process_thread, job));
            done_guard.release();
        }

        void GlobalLockServiceImpl::GetNextTs(::google::protobuf::RpcController* controller,
                               const ::dbx1000::GetNextTsRequest* request,
                               ::dbx1000::GetNextTsReply* response,
                               ::google::protobuf::Closure* done) {
            ::brpc::ClosureGuard done_guard(done);
            ::brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
            response->set_ts(global_lock_->GetNextTs(-1));
        }

        void GlobalLockServiceImpl::ReportResult(::google::protobuf::RpcController* controller,
                                                 const ::dbx1000::ReportResultRequest* request,
                                                 ::dbx1000::ReportResultReply* response,
                                                 ::google::protobuf::Closure* done) {
            ::brpc::ClosureGuard done_guard(done);
            ::brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

            Stats *stats = &(global_lock_->instances()[request->instance_id()].stats);
            stats->total_run_time_ = request->total_run_time();
            stats->total_latency_  = request->total_latency();
            stats->total_txn_cnt_  = request->total_txn_cnt();
            stats->total_time_remote_lock_ = request->total_time_remote_lock();
            stats->instance_run_time_ = request->instance_run_time();

            global_lock_->instances()[request->instance_id()].instance_run_done = true;
            bool all_instance_run_done = true;
            for (int i = 0; i < PROCESS_CNT; i++ )
                if (!global_lock_->instances()[i].instance_run_done) {
                    all_instance_run_done = false;
                    break;
                }



            if(all_instance_run_done) {
                uint64_t total_run_time = 0;
                uint64_t total_latency = 0;
                uint64_t total_txn_cnt = 0;
                uint64_t total_instance_run_time = 0;
                uint64_t average_instance_run_time = 0;
                for (int i = 0; i < PROCESS_CNT; i++ ) {
                    total_run_time += global_lock_->instances()[i].stats.total_run_time_;
                    total_latency  += global_lock_->instances()[i].stats.total_latency_;
                    total_txn_cnt  += global_lock_->instances()[i].stats.total_txn_cnt_;
                    total_instance_run_time += global_lock_->instances()[i].stats.instance_run_time_;
                }
                average_instance_run_time = total_instance_run_time / PROCESS_CNT;
                cout << "total_run_time: "<< total_run_time << endl;
                cout << "total_latency: "<< total_latency << endl;
                cout << "total_txn_cnt: "<< total_txn_cnt << endl;
                cout << "total_instance_run_time: "<< total_instance_run_time << endl;
                cout << "average_instance_run_time: "<< average_instance_run_time << endl;

                cout << "total txn cnt:   " << THREAD_CNT * PROCESS_CNT * MAX_TXN_PER_PART << endl;
                cout << "average latency: " << total_latency / 1000UL / (THREAD_CNT * PROCESS_CNT * MAX_TXN_PER_PART) << " us." << endl;
                cout << "instance throughtput: " << (THREAD_CNT * PROCESS_CNT * MAX_TXN_PER_PART) * 1000000000L / average_instance_run_time << " tps." << endl;
            }
        }

        void GlobalLockServiceImpl::Test(::google::protobuf::RpcController* controller,
                       const ::dbx1000::TestRequest* request,
                       ::dbx1000::TestReply* response,
                       ::google::protobuf::Closure* done){
            ::brpc::ClosureGuard done_guard(done);
            ::brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
        }









        /// 回调
        void OnLockRemoteDone::Run() {
//            std::unique_ptr<OnLockRemoteDone> self_guard(this);
            if (!cntl.Failed()) {
                if (GlobalLockServiceHelper::DeSerializeRC(reply.rc()) == RC::RCOK
                    && count > 0) {
                    assert(MY_PAGE_SIZE == count);
                    assert(reply.count() == count);
                    memcpy(page_buf, reply.page_buf().data(), count);
                }
            } else {
                LOG(FATAL) << cntl.ErrorText();
                assert(false);
            }
        }

        void OnInstanceInitDone::Run() {
            if (!cntl.Failed()) { } else {
                LOG(FATAL) << cntl.ErrorText();
                assert(false);
            }
        }

        void OnInvalidDone::Run() {
            if (!cntl.Failed()) {
//                cout << "remote Invalid page:" << page_id << " success." << endl;
                if(GlobalLockServiceHelper::DeSerializeRC(reply.rc()) == RC::RCOK && count > 0) {
                    assert(MY_PAGE_SIZE == count);
                    assert(reply.count() == count);
                    memcpy(page_buf, reply.page_buf().data(), count);
                }
            } else {
                LOG(FATAL) << cntl.ErrorText();
                assert(false);
            }
        }

        void OnAllInstanceReadyDone::Run() {
            if (!cntl.Failed()) { } else {
                LOG(FATAL) << cntl.ErrorText();
                assert(false);
            }
        }


        void AsyncLockRemoteJob::run() {
            brpc::ClosureGuard done_guard(done);

            RC rc;
            char *page_buf = nullptr;
            size_t count = request->count();
            if(count > 0) {
                assert(MY_PAGE_SIZE == count);
                page_buf = new char [MY_PAGE_SIZE];
            }
            else {assert(0 == count);}

            rc = global_lock_->LockRemote(request->instance_id(), request->page_id(), page_buf, count);

            if(RC::TIME_OUT  == rc) {
                reply->set_rc(RpcRC::TIME_OUT);
                return;
            }
            if(RC::Abort  == rc) {
                reply->set_rc(RpcRC::Abort);
                return;
            }
            assert(RC::RCOK == rc);
            reply->set_rc(RpcRC::RCOK);
            if(count > 0) {
                reply->set_page_buf(page_buf, count);
            }
            reply->set_count(count);
        }


        void AsyncGlobalLockInitDoneJob::run() {
            brpc::ClosureGuard done_guard(done);
            reply->set_init_done(global_lock_->init_done());
        }

        void AsyncInvalidJob::run() {
            brpc::ClosureGuard done_guard(done);

            size_t count = request->count();
            if(count > 0) { assert(request->count() == MY_PAGE_SIZE); }
            else { assert(0 == count); }
            char page_buf[MY_PAGE_SIZE];
//            cout << request->page_id() << " GlobalLockServiceImpl::Invalid in" << endl;
            RC rc = manager_instance_->lock_table()->LockInvalid(request->page_id(), page_buf, count);
            assert(RC::RCOK == rc || RC::TIME_OUT == rc);
            if(RC::TIME_OUT == rc){
                reply->set_rc(RpcRC::TIME_OUT);
            } else if(RC::RCOK == rc){
                reply->set_page_buf(page_buf, count);
                reply->set_count(count);
                reply->set_rc(RpcRC::RCOK);
            } else {
                reply->set_rc(RpcRC::Abort);
            }
//            cout << request->page_id() << " GlobalLockServiceImpl::Invalid out" << endl;
        }
    }
}