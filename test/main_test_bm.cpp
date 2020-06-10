//
// Created by rrzhang on 2020/4/2.
//
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "buffer_manager/server_lock_table/server_lock_table.h"
#include "buffer_manager/manager_server.h"

using namespace std;

void parser(int argc, char * argv[]);

int main(int argc, char* argv[]) {
    cout << "mian test concurrency control" << endl;

    dbx1000::ManagerServer* managerServer = new dbx1000::ManagerServer();


    cout << "exit main." << endl;
    return 0;
}