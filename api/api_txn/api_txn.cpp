//
// Created by rrzhang on 2020/5/3.
//

#include "api_txn.h"

#include "dbx1000_service_util.h"
#include "global.h"
#include "myhelper.h"
#include "txn.h"
#include "manager_client.h"
#include "manager_server.h"
#include "row_mvcc.h"
#include "row_item.h"
#include "profiler.h"

namespace dbx1000 {

    ::grpc::Status ApiTxnServer::SetTsReady(::grpc::ClientContext* context
            , const ::dbx1000::SetTsReadyRequest& request, ::dbx1000::SetTsReadyReply* response) {
        uint64_t thread_id = request.thread_id();
        int row_cnt = glob_manager_client->all_txns_[thread_id]->row_cnt;

        glob_manager_client->all_txns_[thread_id]->ts_ready = true;
        glob_manager_client->all_txns_[thread_id]->accesses[row_cnt]->orig_row->MergeFrom(request.cur_row());

        return ::grpc::Status::OK;
    }







    ApiTxnClient::ApiTxnClient() : stub_(dbx1000::DBx1000Service::NewStub(
            grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials())
            )) {
        cout << "ApiTxnClient connect success." << endl;
    }

//    bool ApiTxnClient::InitWlDone() {
//        /// gen request
//        dbx1000::InitWlDoneRequest request;
//        grpc::ClientContext context;
//        dbx1000::InitWlDoneReply reply;
//        ::grpc::Status status = stub_->InitWlDone(&context, request, &reply);
//        assert(status.ok());
//
//        return reply.is_done();
//    }

    RC ApiTxnClient::GetRow(uint64_t key, access_t type, txn_man* txn, int accesses_index) {
        TsType ts_type = (type == RD || type == SCAN) ? R_REQ : P_REQ;
        uint64_t thread_id = txn->get_thd_id();

        /// gen request
        dbx1000::GetRowRequest request;
        Mess_RowItem messRowItem;
        Mess_TxnRowMan messTxnRowMan;
        request.set_key(key);
        request.set_ts_type(MyHelper::TsTypeToInt(ts_type));
        DBx1000ServiceUtil::Set_Mess_RowItem(key, nullptr, 0, &messRowItem);
        DBx1000ServiceUtil::Set_Mess_TxnRowMan(txn, messRowItem, &messTxnRowMan);
        request.set_allocated_txnman(&messTxnRowMan);
//        request.set_accesses_index(accesses_index);

        grpc::ClientContext context;
        dbx1000::GetRowReply reply;
        grpc::Status status = stub_->GetRow(&context, request, &reply);


//        mtx_.lock();
//        count_++;
//        mtx_.unlock();

        if(!status.ok()) {
            /// debug
            cout << status.error_message() << endl;
            cout << status.error_details() << endl;
            /// cout something
        }
        assert(status.ok());
        RC rc = MyHelper::IntToRC(reply.rc());
        if (RCOK == rc) {
            assert(reply.row().size() == txn->accesses[accesses_index]->orig_row->size_);
            memcpy(txn->accesses[accesses_index]->orig_row->row_, reply.row().data(), reply.row().size());
        }
        else if (WAIT == rc) {
            std::unique_ptr<dbx1000::Profiler> profiler(new dbx1000::Profiler());
            profiler->Start();
            while (!txn->ts_ready) { PAUSE }
            profiler->End();
            INC_TMP_STATS(txn->get_thd_id(), time_wait, profiler->Nanos());

            txn->ts_ready = true;
            memcpy(txn->accesses[accesses_index]->orig_row->row_, reply.row().data(), reply.row().size());
        }
        return rc;
    }

    void ApiTxnClient::ReturnRow(uint64_t key, access_t type, txn_man *txn, int accesses_index) {
        /// gen request
        dbx1000::ReturnRowRequest request;
        Mess_RowItem messRowItem;
        Mess_TxnRowMan messTxnRowMan;
        request.set_key(key);

        TxnRowMan txnRowMan(txn->get_thd_id(), txn->get_txn_id(), txn->ts_ready, nullptr, txn->get_ts());
        if (type == XP) {
            request.set_ts_type(MyHelper::TsTypeToInt(XP_REQ));
            DBx1000ServiceUtil::Set_Mess_RowItem(key, txn->accesses[accesses_index]->orig_row->row_, txn->accesses[accesses_index]->orig_row->size_, &messRowItem);
            DBx1000ServiceUtil::Set_Mess_TxnRowMan(txn, messRowItem, &messTxnRowMan);
        } else if (type == WR) {
            request.set_ts_type(MyHelper::TsTypeToInt(W_REQ));
            DBx1000ServiceUtil::Set_Mess_RowItem(key, txn->accesses[accesses_index]->data->row_, txn->accesses[accesses_index]->data->size_, &messRowItem);
            DBx1000ServiceUtil::Set_Mess_TxnRowMan(txn, messRowItem, &messTxnRowMan);
        } else if (type == RD) { return;}
        else if (type == SCAN) { cout << "SCAN." << endl; return;}
        else { assert(false); }

        grpc::ClientContext context;
        dbx1000::ReturnRowReply reply;
        grpc::Status status = stub_->ReturnRow(&context, request, &reply);
        assert(status.ok());
    }

    void ApiTxnClient::SetWlSimDone() {
        /// gen request
        dbx1000::SetWlSimDoneRequest request;

        grpc::ClientContext context;
        dbx1000::SetWlSimDoneReply reply;
        ::grpc::Status status = stub_->SetWlSimDone(&context, request, &reply);
        assert(status.ok());
    }

    bool ApiTxnClient::GetWlSimDone() {
        /// gen request
        dbx1000::GetWlSimDoneRequest request;

        grpc::ClientContext context;
        dbx1000::GetWlSimDoneReply reply;
        ::grpc::Status status = stub_->GetWlSimDone(&context, request, &reply);
        assert(status.ok());

        return reply.sim_done();
    }


    uint64_t ApiTxnClient::get_next_ts(uint64_t thread_id) {
        /// gen request
        dbx1000::GetNextTsRequest request;
        request.set_thread_id(thread_id);

        grpc::ClientContext context;
        dbx1000::GetNextTsReply reply;
        ::grpc::Status status = stub_->GetNextTs(&context, request, &reply);
        assert(status.ok());

        return reply.timestamp();
    }

    void ApiTxnClient::add_ts(uint64_t thread_id, uint64_t ts) {
        /// gen request
        dbx1000::AddTsRequest request;
        request.set_thread_id(thread_id);
        request.set_timestamp(ts);

        grpc::ClientContext context;
        dbx1000::AddTsReply reply;
        ::grpc::Status status = stub_->AddTs(&context, request, &reply);
        assert(status.ok());
    }
}