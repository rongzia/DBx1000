//
// Created by rrzhang on 2020/6/16.
//
#include <iostream>
#include "lock_server/lock_server_table/lock_server_table.h"
#include "lock_server/manager_lock_server.h"
#include "rpc_handler/lock_service_handler.h"
#include "config.h"
using namespace std;

extern int parser_host(int argc, char *argv[], std::map<int, std::string> &hosts_map);

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

int main(int argc, char* argv[]){
    dbx1000::ManagerServer* managerServer = new dbx1000::ManagerServer();
    managerServer->set_buffer_manager_id(parser_host(argc, argv, managerServer->hosts_map()));

    dbx1000::BufferManagerServer * bufferManagerServer = new dbx1000::BufferManagerServer();
    bufferManagerServer->manager_server_ = managerServer;
    bufferManagerServer->Start(managerServer->hosts_map()[-1]);

    return 0;
}