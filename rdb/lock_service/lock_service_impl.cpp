#include <thread>

// #include "global.h"
#include "lock_service_impl.h"

namespace rdb {
    // LockServiceImpl::~LockServiceImpl() {};
    // LockServiceImpl::LockServiceImpl() {};
    /**
     * @brief
     *
     * @param host 0.0.0.0:30051
     */
    void LockServiceImpl::Start(const std::string& host) {
        if (this->server_.AddService(this, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
            assert(false);
        }

        brpc::ServerOptions options;
        // if (this->server_.Start(std::stoi(host.substr(host.find(':') + 1)), &options) != 0) {
        if (this->server_.Start(host.c_str(), &options) != 0) {
            LOG(FATAL) << "Fail to start LockServiceImpl";
            assert(false);
        }
        this->server_.RunUntilAskedToQuit();
    }


    void LockServiceImpl::ProcessGetLock(const ::rdb::GetLockRequest* request, ::rdb::GetLockReply* response) {
        int32_t ins_id = request->instance_id();
        ::rdb::GetLockItem item = request->req_item();

        /**
         * @brief TODO
         *
         */

        ::rdb::ReturnLockItem ret_item;
        ret_item.set_rc(rdb::RCOK);
        ret_item.set_count(item.count());
        ret_item.set_buf(new char[item.count()]);
        response->set_allocated_ret_item(&ret_item);
    }
    void LockServiceImpl::ProcessGetLocks(const ::rdb::GetLocksRequest* request, ::rdb::GetLocksReply* response) {
        int32_t ins_id = request->instance_id();
        int req_size = request->req_items_size();

        /**
         * @brief TODO
         *
         */

        for (int i = 0; i < req_size; i++) {
            ::rdb::ReturnLockItem* ret_item = response->add_ret_items();
            ret_item->set_rc(rdb::RCOK);
            ret_item->set_table(request->req_items(i).table());
            ret_item->set_item_id(request->req_items(i).item_id());
            ret_item->set_count(request->req_items(i).count());
            ret_item->set_buf(new char[request->req_items(i).count()]);
        }
    }

#ifndef LS_SERVER_ASYNC  // 同步
    void LockServiceImpl::GetLock(::google::protobuf::RpcController* controller,
        const ::rdb::GetLockRequest* request,
        ::rdb::GetLockReply* response,
        ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

        LockServiceImpl::ProcessGetLock(request, response);
    }

    void LockServiceImpl::GetLocks(::google::protobuf::RpcController* controller,
        const ::rdb::GetLocksRequest* request,
        ::rdb::GetLocksReply* response,
        ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

        LockServiceImpl::ProcessGetLocks(request, response);
        // std::cout << __LINE__ << std::endl;
    }

#elif defined(LS_SERVER_ASYNC)  // LS_SERVER_ASYNC 异步

    static void* get_lock_thread(void* args) {
        AsyncGetLock* job = static_cast<AsyncGetLock*>(args);
        job->run_and_delete();
        return NULL;
    }
    static void* get_locks_thread(void* args) {
        AsyncGetLocks* job = static_cast<AsyncGetLocks*>(args);
        job->run_and_delete();
        return NULL;
    }

    void AsyncGetLock::run() {
        brpc::ClosureGuard done_guard(done);
        LockServiceImpl::ProcessGetLock(request, response);
    }
    void AsyncGetLock::run_and_delete() {
        run();
        delete this;
    }
    void AsyncGetLocks::run() {
        brpc::ClosureGuard done_guard(done);
        LockServiceImpl::ProcessGetLocks(request, response);
    }
    void AsyncGetLocks::run_and_delete() {
        run();
        delete this;
    }

    void LockServiceImpl::GetLock(::google::protobuf::RpcController* controller,
        const ::rdb::GetLockRequest* request,
        ::rdb::GetLockReply* response,
        ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

        AsyncGetLock* job = new AsyncGetLock();
        job->cntl = cntl;
        job->request = request;
        job->response = response;
        job->done = done;

        std::thread(get_lock_thread, job);
        done_guard.release();
    }

    void LockServiceImpl::GetLocks(::google::protobuf::RpcController* controller,
        const ::rdb::GetLocksRequest* request,
        ::rdb::GetLocksReply* response,
        ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

        AsyncGetLocks* job = new AsyncGetLocks();
        job->cntl = cntl;
        job->request = request;
        job->response = response;
        job->done = done;

        std::thread(get_lock_thread, job);
        done_guard.release();
    }
#endif                          // LS_SERVER_ASYNC
}