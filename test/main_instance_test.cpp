//
// Created by rrzhang on 2020/6/16.
//

#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "common/buffer/buffer.h"
#include "common/index/index.h"
#include "common/lock_table/lock_table.h"
#include "common/storage/disk/file_io.h"
#include "common/storage/tablespace/page.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/catalog.h"
#include "common/storage/table.h"
#include "common/workload/ycsb_wl.h"
#include "common/workload/wl.h"
#include "common/global.h"
#include "common/myhelper.h"
#include "common/mystats.h"
#include "json/json.h"
#include "instance/benchmarks/ycsb_query.h"
#include "instance/benchmarks/query.h"
#include "instance/concurrency_control/row_mvcc.h"
#include "instance/txn/ycsb_txn.h"
#include "instance/txn/txn.h"
#include "instance/manager_instance.h"
#include "instance/thread.h"
#include "rpc_handler/instance_handler.h"
#include "config.h"

using namespace std;

extern int parser_host(int argc, char *argv[], std::map<int, std::string> &hosts_map);

void RunInstanceServer(dbx1000::InstanceServer *instanceServer, dbx1000::ManagerInstance *managerInstance) {
    instanceServer->Start(managerInstance->host_map()[managerInstance->instance_id()]);
}

int main(int argc, char *argv[]) {
    assert(argc >= 2);

    dbx1000::ManagerInstance *managerInstance = new dbx1000::ManagerInstance();
//    managerInstance->Init(SHARED_DISK_HOST);

    int ins_id = parser_host(argc, argv, managerInstance->host_map());
    managerInstance->set_instance_id(ins_id);
    managerInstance->lock_table() = new dbx1000::LockTable();
    managerInstance->lock_table()->Init(0, 1, ins_id);
    managerInstance->lock_table()->manager_instance_ = managerInstance;
    cout << "managerInstance->host_map()[-1] : " << managerInstance->host_map()[-1] << endl;
    managerInstance->set_instance_rpc_handler(new dbx1000::InstanceClient(managerInstance->host_map()[-1]));

    dbx1000::InstanceServer *instanceServer = new dbx1000::InstanceServer();
    instanceServer->manager_instance_ = managerInstance;

    thread instance_service_server(RunInstanceServer, instanceServer, managerInstance);
    instance_service_server.detach();

    managerInstance->set_init_done(true);
    managerInstance->instance_rpc_handler()->InstanceInitDone(managerInstance->instance_id());

    int a = managerInstance->instance_rpc_handler()->GetTestNum();
    vector<thread> write_threads;
    for(int i = 0; i< g_thread_cnt; i++) {
        write_threads.emplace_back(thread([&](){
            managerInstance->lock_table()->Lock(0, dbx1000::LockMode::X);
            a++;
            if(managerInstance->lock_table()->lock_table()[0]->invalid_req == true){
            managerInstance->lock_table()->UnLock(0);
                return;
            }
            managerInstance->lock_table()->UnLock(0);
        }));
    }
//    for(int i = 0; i< g_thread_cnt; i++) {
//        write_threads.emplace_back(thread([&](){
//            managerInstance->lock_table()->Lock(0, dbx1000::LockMode::S);
////            a++;
//
//            managerInstance->lock_table()->UnLock(0);
//        }));
//    }
    for(int i = 0; i< g_thread_cnt; i++) {
        write_threads[i].join();
    }
    cout << a << endl;

//    while(1) {};


    return 0;
}
