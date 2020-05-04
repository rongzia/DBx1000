//
// Created by rrzhang on 2020/5/3.
//

#ifndef DBX1000_API_CC_CLIENT_H
#define DBX1000_API_CC_CLIENT_H

#include <grpcpp/grpcpp.h>
#include <memory>
#include "global.h"

#include "api.grpc.pb.h"
#include "api.pb.h"

namespace dbx1000 {


    class ApiConCtlServer : public dbx1000::DBx1000Service::Service {
    public:
//        virtual ::grpc::Status InitWlDone(::grpc::ServerContext* context, const ::dbx1000::InitWlDoneRequest* request, ::dbx1000::InitWlDoneReply* response);
        virtual ::grpc::Status GetRow(::grpc::ServerContext *context, const ::dbx1000::GetRowRequest *request, ::dbx1000::GetRowReply *response);
        virtual ::grpc::Status ReturnRow(::grpc::ServerContext* context, const ::dbx1000::ReturnRowRequest* request, ::dbx1000::ReturnRowReply* response);
        virtual ::grpc::Status SetWlSimDone(::grpc::ServerContext* context, const ::dbx1000::SetWlSimDoneRequest* request, ::dbx1000::SetWlSimDoneReply* response);
        virtual ::grpc::Status GetWlSimDone(::grpc::ServerContext* context, const ::dbx1000::GetWlSimDoneRequest* request, ::dbx1000::GetWlSimDoneReply* response);
        virtual ::grpc::Status GetNextTs(::grpc::ServerContext* context, const ::dbx1000::GetNextTsRequest* request, ::dbx1000::GetNextTsReply* response);
        virtual ::grpc::Status AddTs(::grpc::ServerContext* context, const ::dbx1000::AddTsRequest* request, ::dbx1000::AddTsReply* response);
    };

    class TxnRowMan;
    class ApiConCtlClient {
    public:
        ApiConCtlClient();
        ApiConCtlClient(const ApiConCtlClient &) = delete;
        ApiConCtlClient &operator=(const ApiConCtlClient &) = delete;

        void SetTsReady(TxnRowMan* global_server_txn);

    private:
        std::unique_ptr<dbx1000::DBx1000Service::Stub> stub_;
    };
}

#endif //DBX1000_API_CC_CLIENT_H
