//
// Created by rrzhang on 2020/6/12.
//

#include <iostream>
#include "shared_disk/shared_disk_service.h"
using namespace std;

int main() {

    dbx1000::SharedDiskServer* server = new dbx1000::SharedDiskServer();
    server->Start("0.0.0.0:5000");

    return 0;
}