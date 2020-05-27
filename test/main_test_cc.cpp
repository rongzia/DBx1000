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
#include "server/buffer/buffer.h"
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

int main(int argc, char* argv[]) {
    cout << "mian test concurrency control" << endl;

	parser(argc, argv);
    stats.init();
    for(int i = 0; i < g_thread_cnt; i++) {
        stats.init(i);
    }

    glob_manager_server = new dbx1000::ManagerServer();
    std::thread cc_server(RunConCtlServer);
    cc_server.detach();

/// workload
    cout << endl << "init wl:" << endl;
    workload* m_wl;
    dbx1000::Buffer* buffer;

    m_wl = new ycsb_wl();
#ifdef USE_MEMORY_DB
    dbx1000::MemoryDB* db = new dbx1000::MemoryDB();
#else
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.comparator = new dbx1000::NumberComparatorImpl();
    leveldb::Status status = leveldb::DB::Open(options, g_db_path, &db);
    assert(status.ok());
#endif
    m_wl->init();
    size_t tuple_size = ((ycsb_wl*)m_wl)->the_table->get_schema()->get_tuple_size();
    /// init buffer here, because 'the_table' can be use util schema be inititaled
    buffer = new dbx1000::Buffer(g_synth_table_size / 8 * tuple_size, tuple_size, db);
    m_wl->buffer_ = buffer;
    ((ycsb_wl*)m_wl)->init_table();
    cout << endl;
/// workload

    glob_manager_server->InitWl((ycsb_wl*)m_wl);
    while (!glob_manager_server->AllTxnReady()) { PAUSE }


    while(!glob_manager_server->AllThreadDone()) { PAUSE }
    stats.print_rpc();

    cout << endl << "delete buffer" << endl;
    delete buffer;
    cout << endl << "delete db" << endl;
    delete db;
    cout << endl << "delete m_wl" << endl;
    delete m_wl;
//    delete api_con_ctl_client;
    delete glob_manager_server;

    cout << "exit main." << endl;
    return 0;
}

#endif // WITH_RPC