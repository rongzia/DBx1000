//
// Created by rrzhang on 2020/5/4.
//

#include <iostream>
#include <thread>
#include "api_cc.h"
#include "global.h"


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

int main() {
    std::thread cc_server(RunConCtlServer);
    cc_server.detach();
    std::this_thread::sleep_for(std::chrono::seconds(10));


    api_con_ctl_client = new dbx1000::ApiConCtlClient();
//    api_con_ctl_client.


    while(1) {}
}