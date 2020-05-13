//
// Created by rrzhang on 2020/5/3.
//
#ifdef WITH_RPC

#include <common/mystats.h>
#include "api_txn.h"

#include "api/proto/dbx1000_service_util.h"
#include "client/txn/txn.h"
#include "client/manager_client.h"
#include "common/row_item.h"
#include "common/myhelper.h"
#include "server/concurrency_control/row_mvcc.h"

namespace dbx1000 {

    ::grpc::Status ApiTxnServer::SetTsReady(::grpc::ServerContext* context, const ::dbx1000::SetTsReadyRequest* request, ::dbx1000::SetTsReadyReply* response) {
//        cout << "ApiTxnServer::SetTsReady" << endl;
        uint64_t thread_id = request->thread_id();
        int row_cnt = glob_manager_client->all_txns_[thread_id]->row_cnt;

        glob_manager_client->all_txns_[thread_id]->ts_ready = true;
        assert(glob_manager_client->all_txns_[thread_id]->accesses[row_cnt]->orig_row->size_ == request->cur_row().size()
            && request->cur_row().size() == request->cur_row().row().size());
        memcpy(glob_manager_client->all_txns_[thread_id]->accesses[row_cnt]->orig_row->row_
            , request->cur_row().row().data(), request->cur_row().row().size());

        return ::grpc::Status::OK;
    }

    ::grpc::Status ApiTxnServer::Test(::grpc::ServerContext* context, const ::dbx1000::TestRequest* request, ::dbx1000::TestReply* response) {
        cout << " ApiTxnServer::Test" << endl;
        return ::grpc::Status::OK;
    }







ApiTxnClient::ApiTxnClient(std::string addr) : stub_(dbx1000::DBx1000Service::NewStub(
            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())
            )) {
        cout << "ApiTxnClient::ApiTxnClient connect to " << addr << " success." << endl;
    }

    void ApiTxnClient::TxnReady(uint64_t thread_id) {
        /// gen request
        dbx1000::TxnReadyRequest request;
        request.set_thread_id(thread_id);
        ::grpc::ClientContext context;
        dbx1000::TxnReadyReply reply;
        ::grpc::Status status = stub_->TxnReady(&context, request, &reply);
        assert(status.ok());
    }

    bool ApiTxnClient::InitWlDone() {
        /// gen request
        dbx1000::InitWlDoneRequest request;
        grpc::ClientContext context;
        dbx1000::InitWlDoneReply reply;
        ::grpc::Status status = stub_->InitWlDone(&context, request, &reply);
        assert(status.ok());

        return reply.is_done();
    }

    RC ApiTxnClient::GetRow(uint64_t key, access_t type, txn_man* txn, int accesses_index) {
//        cout << "ApiTxnClient::GetRow" << endl;
        TsType ts_type = (type == RD || type == SCAN) ? R_REQ : P_REQ;
        uint64_t thread_id = txn->get_thd_id();

        /// gen request
        dbx1000::GetRowRequest request;
        Mess_RowItem* messRowItem = new Mess_RowItem();
        Mess_TxnRowMan* messTxnRowMan = new Mess_TxnRowMan();
        request.set_key(key);
        request.set_ts_type(MyHelper::TsTypeToInt(ts_type));
        DBx1000ServiceUtil::Set_Mess_RowItem(key, nullptr, 0, messRowItem);
        DBx1000ServiceUtil::Set_Mess_TxnRowMan(txn, messRowItem, messTxnRowMan);
        request.set_allocated_txnman(messTxnRowMan);
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
//            cout << "ApiTxnClient::GetRow, type == WAIT" << endl;
//            uint64_t thread_id = txn->get_thd_id();
            dbx1000::Profiler profiler;
            profiler.Start();
            while (!txn->ts_ready) { PAUSE }
            profiler.End();
            stats.tmp_stats[thread_id]->time_wait += profiler.Nanos();
//            INC_TMP_STATS(txn->get_thd_id(), time_wait, profiler->Nanos());
            txn->ts_ready = true;
            memcpy(txn->accesses[accesses_index]->orig_row->row_, reply.row().data(), reply.row().size());
        }
        return rc;
    }

    void ApiTxnClient::ReturnRow(uint64_t key, access_t type, txn_man *txn, int accesses_index) {
//        cout << "ApiTxnClient::ReturnRow" << endl;

        if(RD == type) { return;}
        else if(SCAN == type) { cout << "SCAN." << endl; return;}

        /// gen request
        dbx1000::ReturnRowRequest request;
        Mess_RowItem* messRowItem = new Mess_RowItem();
        Mess_TxnRowMan* messTxnRowMan = new Mess_TxnRowMan();
        request.set_key(key);

        TxnRowMan txnRowMan(txn->get_thd_id(), txn->get_txn_id(), txn->ts_ready, nullptr, txn->get_ts());
        if (type == XP) {
            request.set_ts_type(MyHelper::TsTypeToInt(XP_REQ));
            assert(txn->accesses[accesses_index]->orig_row->size_ > 0);
            assert(txn->accesses[accesses_index]->orig_row->row_ != nullptr);
            DBx1000ServiceUtil::Set_Mess_RowItem(key, txn->accesses[accesses_index]->orig_row->row_, txn->accesses[accesses_index]->orig_row->size_, messRowItem);
            DBx1000ServiceUtil::Set_Mess_TxnRowMan(txn, messRowItem, messTxnRowMan);
            request.set_allocated_txnman(messTxnRowMan);
        } else if (type == WR) {
            request.set_ts_type(MyHelper::TsTypeToInt(W_REQ));
            assert(txn->accesses[accesses_index]->data->size_ > 0);
            assert(txn->accesses[accesses_index]->data->row_ != nullptr);
            DBx1000ServiceUtil::Set_Mess_RowItem(key, txn->accesses[accesses_index]->data->row_, txn->accesses[accesses_index]->data->size_, messRowItem);
            DBx1000ServiceUtil::Set_Mess_TxnRowMan(txn, messRowItem, messTxnRowMan);
            request.set_allocated_txnman(messTxnRowMan);
        }
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
        dbx1000::Profiler profiler;
        profiler.Start();
        /// gen request
        dbx1000::GetNextTsRequest request;
        request.set_thread_id(thread_id);

        grpc::ClientContext context;
        dbx1000::GetNextTsReply reply;
        ::grpc::Status status = stub_->GetNextTs(&context, request, &reply);
        assert(status.ok());

        profiler.End();
        stats._stats[thread_id]->time_ts_alloc += profiler.Nanos();

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

    void ApiTxnClient::ThreadDone(uint64_t thread_id) {
        dbx1000::ThreadDoneRequest request;
        request.set_thread_id(thread_id);
        ::grpc::ClientContext context;
        dbx1000::ThreadDoneReply reply;
        ::grpc::Status status = stub_->ThreadDone(&context, request, &reply);
        assert(status.ok());
    }
}

#endif // WITH_RPC