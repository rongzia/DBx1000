//
// Created by rrzhang on 2020/5/4.
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
#include "txn.h"

#include "api_cc.h"
#include "api_txn.h"

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
    std::thread txn_server(RunTxnServer);
    txn_server.detach();
    api_txn_client = new dbx1000::ApiTxnClient();

//    while(a)

	parser(argc, argv);
    stats.init();

    glob_manager_client = new dbx1000::ManagerClient();
    glob_manager_client->init();

    query_queue = new Query_queue();
    query_queue->init();
//
//
//
//
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


    stats.print();

    for(int i = 0; i <  g_thread_cnt; i++) {
        cout << thread_t_s[i].count1 << endl;
        cout << thread_t_s[i].count2 << endl;
        cout << "txn[" << i << "].count:" << glob_manager_client->all_txns_[i]->txn_count << endl;

    }

    while(1) {}
    cout << "exit main." << endl;
    return 0;
};
