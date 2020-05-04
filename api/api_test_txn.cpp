//
// Created by rrzhang on 2020/5/4.
//

#include <iostream>
#include <thread>
#include "api_txn.h"
#include "global.h"

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

int main(){

    std::thread txn_server(RunTxnServer);
    txn_server.detach();
    api_txn_client = new dbx1000::ApiTxnClient();


    while(1) {}
}