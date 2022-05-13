

#include "ins_client_manager.h"

namespace rdb {
    DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
    DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
    DEFINE_string(server, "127.0.0.1:30051", "IP Address of server");
    DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
    DEFINE_bool(dont_fail, false, "Print fatal when some call failed");


    InsClientManager::~InsClientManager() {
        delete handle_ls_.stub_;
    }

    InsClientManager::InsClientManager() {
        handle_ls_.options_.protocol = FLAGS_protocol;
        handle_ls_.options_.timeout_ms = FLAGS_timeout_ms/*milliseconds*/;
        if (handle_ls_.channel_.Init(FLAGS_server.c_str(), &handle_ls_.options_) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
            assert(false);
        }

        handle_ls_.stub_ = new rdb::LockService_Stub(&handle_ls_.channel_);
    }

    void InsClientManager::GetLock(brpc::Controller* cntl, rdb::GetLockRequest* request, rdb::GetLockReply* reply) {
        handle_ls_.stub_->GetLock(cntl, request, reply, NULL);
    }

    void InsClientManager::GetLocks(brpc::Controller* cntl, rdb::GetLocksRequest* request, rdb::GetLocksReply* reply) {
        handle_ls_.stub_->GetLocks(cntl, request, reply, NULL);
    }
}