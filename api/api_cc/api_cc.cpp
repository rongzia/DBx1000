//
// Created by rrzhang on 2020/5/3.
//
#ifdef WITH_RPC

#include <common/txn_row_man.h>
#include "api_cc.h"

#include "api/proto/dbx1000_service_util.h"
#include "common/row_item.h"
#include "common/myhelper.h"
#include "server/manager_server.h"
#include "server/concurrency_control/row_mvcc.h"
#include "server/workload/ycsb_wl.h"
#include "server/storage/table.h"
#include "server/storage/catalog.h"

namespace dbx1000 {
    ::grpc::Status ApiConCtlServer::TxnReady(::grpc::ServerContext* context, const ::dbx1000::TxnReadyRequest* request, ::dbx1000::TxnReadyReply* response) {
        glob_manager_server->txn_ready_[request->thread_id()] = true;
        return ::grpc::Status::OK;
    }

    ::grpc::Status ApiConCtlServer::InitWlDone(::grpc::ServerContext* context, const ::dbx1000::InitWlDoneRequest* request, ::dbx1000::InitWlDoneReply* response) {
        response->set_is_done(glob_manager_server->init_wl_done_);
        return ::grpc::Status::OK;
    }

    ::grpc::Status ApiConCtlServer::GetRowSize(::grpc::ServerContext* context, const ::dbx1000::GetRowSizeRequest* request, ::dbx1000::GetRowSizeReply* response) {
        response->set_row_size(glob_manager_server->wl_->the_table->get_schema()->get_tuple_size());
        return ::grpc::Status::OK;
    }

    ::grpc::Status ApiConCtlServer::GetRow(::grpc::ServerContext *context, const ::dbx1000::GetRowRequest *request
                                        , ::dbx1000::GetRowReply *response) {
//        cout << "ApiConCtlServer::GetRow" << endl;
        TsType ts_type = MyHelper::IntToTsType(request->ts_type());
        uint64_t thread_id = request->txnman().thread_id();

//    stats.tmp_stats[thread_id]->profiler->Clear();
//    stats.tmp_stats[thread_id]->profiler->Start();
        glob_manager_server->SetTxn(request->txnman());
        RC rc = glob_manager_server->row_mvccs_[request->key()]->access(&glob_manager_server->all_txns_[thread_id], ts_type);
        if(RCOK == rc) {
            response->set_rc(MyHelper::RCToInt(rc));
            response->set_row(glob_manager_server->all_txns_[thread_id].cur_row_->row_, glob_manager_server->all_txns_[thread_id].cur_row_->size_);
//    stats.tmp_stats[thread_id]->profiler->End();
//    response->set_time(stats.tmp_stats[thread_id]->profiler->Nanos());
            return ::grpc::Status::OK;
        } if(Commit == rc || Abort == rc || WAIT == rc || ERROR == rc || FINISH == rc) {
            response->set_rc(MyHelper::RCToInt(rc));
//    stats.tmp_stats[thread_id]->profiler->End();
//    response->set_time(stats.tmp_stats[thread_id]->profiler->Nanos());
            return ::grpc::Status::OK;
        }

//    stats.tmp_stats[thread_id]->profiler->End();
        assert(false);
    }

    ::grpc::Status ApiConCtlServer::ReturnRow(::grpc::ServerContext *context, const ::dbx1000::ReturnRowRequest *request
                                           , ::dbx1000::ReturnRowReply *response) {
//        cout << "ApiConCtlServer::ReturnRow" << endl;
        TsType ts_type = MyHelper::IntToTsType(request->ts_type());
        uint64_t thread_id = request->txnman().thread_id();

        glob_manager_server->SetTxn(request->txnman());
        glob_manager_server->row_mvccs_[request->key()]->access(&glob_manager_server->all_txns_[thread_id], ts_type);
        return ::grpc::Status::OK;
    }

    ::grpc::Status ApiConCtlServer::SetWlSimDone(::grpc::ServerContext* context, const ::dbx1000::SetWlSimDoneRequest* request, ::dbx1000::SetWlSimDoneReply* response) {
        glob_manager_server->wl_->sim_done_.exchange(true, std::memory_order_consume);
        return ::grpc::Status::OK;
    }

    ::grpc::Status ApiConCtlServer::GetWlSimDone(::grpc::ServerContext* context, const ::dbx1000::GetWlSimDoneRequest* request, ::dbx1000::GetWlSimDoneReply* response) {
        response->set_sim_done(glob_manager_server->wl_->sim_done_.load(std::memory_order_consume));
        return ::grpc::Status::OK;
    }

    ::grpc::Status ApiConCtlServer::GetNextTs(::grpc::ServerContext* context, const ::dbx1000::GetNextTsRequest* request, ::dbx1000::GetNextTsReply* response) {
        response->set_timestamp(glob_manager_server->get_next_ts(request->thread_id()));
        return ::grpc::Status::OK;
    }

    ::grpc::Status ApiConCtlServer::AddTs(::grpc::ServerContext* context, const ::dbx1000::AddTsRequest* request, ::dbx1000::AddTsReply* response) {
        glob_manager_server->add_ts(request->thread_id(), request->timestamp());
        return ::grpc::Status::OK;
    }

    ::grpc::Status ApiConCtlServer::ThreadDone(::grpc::ServerContext* context, const ::dbx1000::ThreadDoneRequest* request, ::dbx1000::ThreadDoneReply* response) {
        glob_manager_server->thread_done_[request->thread_id()] = true;
        return ::grpc::Status::OK;
    }


    ApiConCtlClient::ApiConCtlClient(std::string addr) : stub_(dbx1000::DBx1000Service::NewStub(
            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())
            )) {
        cout << "ApiConCtlClient connect success." << endl;
    }

    void ApiConCtlClient::SetTsReady(TxnRowMan* global_server_txn) {
        /// gen request
        assert(true == global_server_txn->ts_ready_);
        dbx1000::SetTsReadyRequest request;
        request.set_thread_id(global_server_txn->thread_id_);
        Mess_RowItem* messRowItem = new Mess_RowItem();
        DBx1000ServiceUtil::Set_Mess_RowItem(global_server_txn->cur_row_->key_, global_server_txn->cur_row_->row_
                                             , global_server_txn->cur_row_->size_, messRowItem);
        request.set_allocated_cur_row(messRowItem);

        grpc::ClientContext context;
        dbx1000::SetTsReadyReply reply;
        ::grpc::Status status = stub_->SetTsReady(&context, request, &reply);

        assert(status.ok());
    }

    void ApiConCtlClient::Test() {
        dbx1000::TestRequest request;
        ::grpc::Status status = stub_->Test(new ::grpc::ClientContext(), request, new dbx1000::TestReply);
        assert(status.ok());
    }
} // namespace dbx1000

#endif // WITH_RPC