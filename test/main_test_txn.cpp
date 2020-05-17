//
// Created by rrzhang on 2020/5/4.
//
#ifdef WITH_RPC

#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include "leveldb/db.h"


//#include "ycsb_query.h"
#include "server/workload/ycsb_wl.h"
#include "server/storage/table.h"
#include "server/storage/catalog.h"

#include "util/numbercomparator.h"
//#include "cs_api.h"
#include "common/global.h"
#include "server/manager_server.h"
#include "client/manager_client.h"
#include "client/benchmarks/query.h"
#include "client/benchmarks/ycsb_query.h"
#include "client/thread.h"
#include "common/myhelper.h"

#include "api/api_cc/api_cc.h"
#include "api/api_txn/api_txn.h"

using namespace std;

void RunTxnServer() {
    std::string txn_server_address("0.0.0.0:50040");
  	dbx1000::ApiTxnServer service;

  	grpc::ServerBuilder builder;
  	builder.AddListeningPort(txn_server_address, grpc::InsecureServerCredentials());
  	builder.RegisterService(&service);
  	std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  	cout << "Server listening on:" << txn_server_address << endl;
  	server->Wait();
}
void f(thread_t* thread) {
    thread->run();
}

void parser(int argc, char * argv[]);

int main(int argc, char* argv[]) {
    cout << "mian test txn thread" << endl;
    api_txn_client = new dbx1000::ApiTxnClient("127.0.0.1:50051");
    std::thread txn_server(RunTxnServer);
    txn_server.detach();

    for(size_t i = 0; i < g_thread_cnt; i++) {
        api_txn_client->TxnReady(i);
    }



    while(!api_txn_client->InitWlDone()) {PAUSE}

	parser(argc, argv);
    stats.init();

    glob_manager_client = new dbx1000::ManagerClient();
    glob_manager_client->init();
    glob_manager_client->row_size_ = api_txn_client->GetRowSize();

    query_queue = new Query_queue();
    query_queue->init();

	warmup_finish = true;

    thread_t *thread_t_s = new thread_t[g_thread_cnt]();
    std::vector<std::thread> v_thread;
    for(int i = 0; i < g_thread_cnt; i++) {
        thread_t_s[i].init(i);
        v_thread.emplace_back(f, &thread_t_s[i]);
    }

    for(int i = 0; i < g_thread_cnt; i++) {
        v_thread[i].join();
    }

    for(size_t i = 0; i < g_thread_cnt; i++) {
        api_txn_client->ThreadDone(i);
    }

    stats.print2();

//    while(1) {}
    cout << "exit main." << endl;
    return 0;
};

#endif // WITH_RPC