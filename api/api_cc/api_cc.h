//
// Created by rrzhang on 2020/5/3.
//
#ifdef WITH_RPC

#ifndef DBX1000_API_CC_CLIENT_H
#define DBX1000_API_CC_CLIENT_H

#include <memory>

#include <grpcpp/grpcpp.h>
#include "api.grpc.pb.h"

namespace dbx1000 {


    class ApiConCtlServer : public dbx1000::DBx1000Service::Service {
    public:
        virtual ::grpc::Status TxnReady(::grpc::ServerContext* context, const ::dbx1000::TxnReadyRequest* request, ::dbx1000::TxnReadyReply* response);
        virtual ::grpc::Status InitWlDone(::grpc::ServerContext* context, const ::dbx1000::InitWlDoneRequest* request, ::dbx1000::InitWlDoneReply* response);
        virtual ::grpc::Status GetRowSize(::grpc::ServerContext* context, const ::dbx1000::GetRowSizeRequest* request, ::dbx1000::GetRowSizeReply* response);
        virtual ::grpc::Status GetRow(::grpc::ServerContext *context, const ::dbx1000::GetRowRequest *request, ::dbx1000::GetRowReply *response);
        virtual ::grpc::Status ReturnRow(::grpc::ServerContext* context, const ::dbx1000::ReturnRowRequest* request, ::dbx1000::ReturnRowReply* response);
        virtual ::grpc::Status SetWlSimDone(::grpc::ServerContext* context, const ::dbx1000::SetWlSimDoneRequest* request, ::dbx1000::SetWlSimDoneReply* response);
        virtual ::grpc::Status GetWlSimDone(::grpc::ServerContext* context, const ::dbx1000::GetWlSimDoneRequest* request, ::dbx1000::GetWlSimDoneReply* response);
        virtual ::grpc::Status GetNextTs(::grpc::ServerContext* context, const ::dbx1000::GetNextTsRequest* request, ::dbx1000::GetNextTsReply* response);
        virtual ::grpc::Status AddTs(::grpc::ServerContext* context, const ::dbx1000::AddTsRequest* request, ::dbx1000::AddTsReply* response);
        virtual ::grpc::Status ThreadDone(::grpc::ServerContext* context, const ::dbx1000::ThreadDoneRequest* request, ::dbx1000::ThreadDoneReply* response);
    };

    class TxnRowMan;
    class ApiConCtlClient {
    public:
        ApiConCtlClient(std::string addr);
        ApiConCtlClient(const ApiConCtlClient &) = delete;
        ApiConCtlClient &operator=(const ApiConCtlClient &) = delete;

        void SetTsReady(TxnRowMan* global_server_txn);
        void Test();

    private:
        std::unique_ptr<dbx1000::DBx1000Service::Stub> stub_;
    };
}

#endif //DBX1000_API_CC_CLIENT_H

#endif // WITH_RPC