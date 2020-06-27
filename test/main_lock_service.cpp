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

void RunLockServiceServer(dbx1000::LockServiceServer * bufferManagerServer, dbx1000::ManagerLockService* managerLockService) {
    bufferManagerServer->Start(managerLockService->hosts_map()[-1]);
}

int main(int argc, char* argv[]){
    dbx1000::ManagerLockService* managerLockService = new dbx1000::ManagerLockService();
    managerLockService->set_buffer_manager_id(parser_host(argc, argv, managerLockService->hosts_map()));

    dbx1000::LockServiceServer * lockServiceServer = new dbx1000::LockServiceServer();
    lockServiceServer->manager_lock_service_ = managerLockService;

    thread lock_service_server(RunLockServiceServer, lockServiceServer, managerLockService);
//    lock_service_server.detach();


    while(1){};
    return 0;
}