//
// Created by rrzhang on 2020/5/3.
//
#ifdef WITH_RPC

#ifndef DBX1000_API_TXN_H
#define DBX1000_API_TXN_H

#include <memory>
#include <grpcpp/grpcpp.h>
#include "api.grpc.pb.h"
#include "common/global.h"


class txn_man;

namespace dbx1000 {

    class ApiTxnServer : public dbx1000::DBx1000Service::Service {
    public:
        virtual ::grpc::Status SetTsReady(::grpc::ServerContext* context, const ::dbx1000::SetTsReadyRequest* request, ::dbx1000::SetTsReadyReply* response);
        virtual ::grpc::Status Test(::grpc::ServerContext* context, const ::dbx1000::TestRequest* request, ::dbx1000::TestReply* response);
    };

    class ApiTxnClient {
    public:
        ApiTxnClient(std::string addr);
        ApiTxnClient(const ApiTxnClient &) = delete;
        ApiTxnClient &operator=(const ApiTxnClient &) = delete;

        /// 告知 cc 服务，该事务已经准备好，即该事务的服务 rpc 进程已经启动
        void TxnReady(uint64_t thread_id, const string& thread_host);
        /// 查看 cc 服务的 init_wl 是否完成
        bool InitWlDone();
        /// 从 cc 服务 获取行大小
        uint64_t GetRowSize();

        /// for txn[.h|.cpp]
        RC GetRow(uint64_t key, access_t type, txn_man* txn, int accesses_index);
        void ReturnRow(uint64_t key, access_t type, txn_man* txn, int accesses_index);

        /// for thread[.h|.cpp]
        void SetWlSimDone();
        bool GetWlSimDone();
        uint64_t get_next_ts(uint64_t thread_id);
        void add_ts(uint64_t thread_id, uint64_t ts);
        uint64_t GetAndAddTs(uint64_t thread_id);

        /// 告知 cc 服务进程，该事务线程执行完毕，cc 收到所有的事务进程完成消息，则退出
        void ThreadDone(uint64_t thread_id);

    private:
        std::unique_ptr<dbx1000::DBx1000Service::Stub> stub_;
    };
}


#endif // DBX1000_API_TXN_H


#endif // WITH_RPC