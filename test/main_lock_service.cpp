//
// Created by rrzhang on 2020/6/16.
//
#include <iostream>
#include <thread>
#include "lock_service/manager_lock_service.h"
#include "rpc_handler/lock_service_handler.h"
#include "config.h"
using namespace std;

extern int parser_host(int argc, char *argv[], std::map<int, std::string> &hosts_map);

void RunBufferManagerServer(dbx1000::BufferManagerServer * bufferManagerServer, dbx1000::ManagerService* managerService) {
    bufferManagerServer->Start(managerService->hosts_map()[-1]);
}

int main(int argc, char* argv[]){
    dbx1000::ManagerService* managerService = new dbx1000::ManagerService();
    managerService->set_buffer_manager_id(parser_host(argc, argv, managerService->hosts_map()));

    dbx1000::BufferManagerServer * bufferManagerServer = new dbx1000::BufferManagerServer();
    bufferManagerServer->manager_service_ = managerService;

    thread lock_service_server(RunBufferManagerServer, bufferManagerServer, managerService);
//    lock_service_server.detach();


    while(1){};
    return 0;
}