//
// Created by rrzhang on 2020/4/2.
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

void RunConCtlServer() {
    std::string txn_server_address("0.0.0.0:50051");
  	dbx1000::ApiConCtlServer service;

  	grpc::ServerBuilder builder;
  	builder.AddListeningPort(txn_server_address, grpc::InsecureServerCredentials());
  	builder.RegisterService(&service);
  	std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  	cout << "Server listening on:" << txn_server_address << endl;
  	server->Wait();
}

void parser(int argc, char * argv[]);

void Test_wl() {
    glob_manager_server = new dbx1000::ManagerServer();
    std::unique_ptr<workload> m_wl(new ycsb_wl());
    m_wl->init();
    glob_manager_server->InitWl((ycsb_wl*)m_wl.get());
}


int main(int argc, char* argv[]) {
    cout << "mian test concurrency control" << endl;

	parser(argc, argv);
    stats.init();

    for(int i = 0; i < g_thread_cnt; i++) {
        stats.init(i);
    }
//    Test_wl();
    glob_manager_server = new dbx1000::ManagerServer();
    std::thread cc_server(RunConCtlServer);
    cc_server.detach();
    std::unique_ptr<workload> m_wl(new ycsb_wl());
    m_wl->init();
    glob_manager_server->InitWl((ycsb_wl*)m_wl.get());



    while (!glob_manager_server->AllTxnReady()) { PAUSE }
    api_con_ctl_client = new dbx1000::ApiConCtlClient("127.0.0.1:50040");
    api_con_ctl_client->Test();


    while(!glob_manager_server->AllThreadDone()) {}
    stats.print();

    cout << "exit main." << endl;
    return 0;
}

#endif // WITH_RPC