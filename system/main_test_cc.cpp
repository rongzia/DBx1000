//
// Created by rrzhang on 2020/4/2.
//

#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include "leveldb/db.h"

//#include "ycsb_query.h"
#include "ycsb_wl.h"
#include "table.h"
#include "catalog.h"

#include "numbercomparator.h"
//#include "cs_api.h"
#include "global.h"
#include "manager_server.h"
#include "manager_client.h"
#include "query.h"
#include "ycsb_query.h"
#include "thread.h"

#include "api_cc.h"
#include "api_txn.h"

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
    std::unique_ptr<workload> m_wl(new ycsb_wl());
glob_manager_server = new dbx1000::ManagerServer();
    glob_manager_server->init(m_wl.get());
    m_wl->init();
}


int main(int argc, char* argv[]) {
    cout << "mian test concurrency control" << endl;

	parser(argc, argv);
    stats.init();
//    Test_wl();
    glob_manager_server = new dbx1000::ManagerServer();
    std::unique_ptr<workload> m_wl(new ycsb_wl());
    m_wl->init();
    glob_manager_server->init(m_wl.get());


    std::thread cc_server(RunConCtlServer);
    cc_server.detach();
    std::this_thread::sleep_for(std::chrono::seconds(10));
    api_con_ctl_client = new dbx1000::ApiConCtlClient();



    while(1) {}
    cout << "exit main." << endl;
    return 0;
}
