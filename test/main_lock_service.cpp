//
// Created by rrzhang on 2020/6/16.
//
#include <iostream>
#include <thread>
#include "lock_server/lock_server_table/lock_server_table.h"
#include "lock_server/manager_lock_server.h"
#include "rpc_handler/lock_service_handler.h"
#include "config.h"
using namespace std;

extern int parser_host(int argc, char *argv[], std::map<int, std::string> &hosts_map);

void RunBufferManagerServer(dbx1000::BufferManagerServer * bufferManagerServer, dbx1000::ManagerServer* managerServer) {
    bufferManagerServer->Start(managerServer->hosts_map()[-1]);
}

int main(int argc, char* argv[]){
    dbx1000::ManagerServer* managerServer = new dbx1000::ManagerServer();
    managerServer->set_buffer_manager_id(parser_host(argc, argv, managerServer->hosts_map()));
    managerServer->lock_table()->lock_table().clear();

    managerServer->lock_table()->Init(0, 10);

    dbx1000::BufferManagerServer * bufferManagerServer = new dbx1000::BufferManagerServer();
    bufferManagerServer->manager_server_ = managerServer;

    thread lock_service_server(RunBufferManagerServer, bufferManagerServer, managerServer);
    lock_service_server.detach();




    while(1){};
    return 0;
}