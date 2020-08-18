//
// Created by rrzhang on 2020/6/16.
//
#include <iostream>
#include <thread>
#include "lock_service/manager_lock_service.h"
#include "lock_service_handler.h"
#include "config.h"
using namespace std;

extern int parser_host(int argc, char *argv[], std::map<int, std::string> &hosts_map);

void RunLockServiceServer(dbx1000::LockServiceServer * lockServiceServer, dbx1000::ManagerLockService* managerLockService) {
    std::string port = managerLockService->hosts_map()[managerLockService->lock_service_id()].substr(managerLockService->hosts_map()[-1].find(':'));
    lockServiceServer->Start("0.0.0.0" + port);
}

int main(int argc, char* argv[]){
    dbx1000::ManagerLockService* managerLockService = new dbx1000::ManagerLockService();
    parser_host(argc, argv, managerLockService->hosts_map());
    managerLockService->set_lock_service_id(-1);

    dbx1000::LockServiceServer * lockServiceServer = new dbx1000::LockServiceServer();
    lockServiceServer->manager_lock_service_ = managerLockService;

    thread lock_service_server(RunLockServiceServer, lockServiceServer, managerLockService);
//    lock_service_server.detach();


    while(1){};
    return 0;
}