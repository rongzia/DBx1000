//
// Created by rrzhang on 2020/5/4.
//
#ifdef WITH_RPC

#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include "util/numbercomparator.h"
//#include "cs_api.h"
#include "common/global.h"
#include "server/manager_server.bak.h"
#include "client/manager_client.h"
#include "client/benchmarks/query.h"
#include "client/benchmarks/ycsb_query.h"
#include "client/thread.h"
#include "common/myhelper.h"

#include "api/api_cc/api_cc.h"
#include "api/api_txn/api_txn.h"

using namespace std;

void RunTxnServer(const std::string& port) {
    std::string txn_server_address("0.0.0.0:" + port);
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
	parser(argc, argv);

    std::thread txn_server(RunTxnServer, txn_thread_host.substr(txn_thread_host.find(":")+1));
    txn_server.detach();

    glob_manager_client = new dbx1000::ManagerClient();
    glob_manager_client->init();
    glob_manager_client->api_txn_client()->TxnReady(txn_thread_id, txn_thread_host);



    while(!glob_manager_client->api_txn_client()->InitWlDone()) {PAUSE}
    glob_manager_client->SetRowSize(glob_manager_client->api_txn_client()->GetRowSize());

    query_queue = new Query_queue();
    query_queue->init();

	warmup_finish = true;
    stats.init();

    thread_t *thread_t_s = new thread_t[1]();
    std::vector<std::thread> v_thread;
    for(int i = 0; i < 1; i++) {
        thread_t_s[i].init(txn_thread_id);
        v_thread.emplace_back(f, &thread_t_s[i]);
    }
    for(int i = 0; i < 1; i++) {
        v_thread[i].join();
    }

    glob_manager_client->api_txn_client()->ThreadDone(txn_thread_id);
    stats.print_rpc();

    cout << "exit main." << endl;
    return 0;
};

#endif // WITH_RPC