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
//        ApiTxnClient(std::shared_ptr<grpc::Channel> channel);
        ApiTxnClient(std::string addr);
        ApiTxnClient(const ApiTxnClient &) = delete;
        ApiTxnClient &operator=(const ApiTxnClient &) = delete;

        void TxnReady(uint64_t thread_id);
        bool InitWlDone();

        RC GetRow(uint64_t key, access_t type, txn_man* txn, int accesses_index);
        void ReturnRow(uint64_t key, access_t type, txn_man* txn, int accesses_index);

        void SetWlSimDone();
        bool GetWlSimDone();


        uint64_t get_next_ts(uint64_t thread_id);
        void add_ts(uint64_t thread_id, uint64_t ts);

        void ThreadDone(uint64_t thread_id);

    private:
        std::unique_ptr<dbx1000::DBx1000Service::Stub> stub_;
    };
}


#endif // DBX1000_API_TXN_H


#endif // WITH_RPC