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
void f(thread_t* thread) {
    thread->run();
}


int main(int argc, char *argv[]) {
    assert(argc >= 2);

    dbx1000::ManagerInstance *managerInstance = new dbx1000::ManagerInstance();
    int ins_id = parser_host(argc, argv, managerInstance->host_map());
    managerInstance->set_instance_id(ins_id);
    cout <<__LINE__ << endl;
    managerInstance->Init(SHARED_DISK_HOST);
    cout <<__LINE__ << endl;
    // 连接集中锁管理器
    managerInstance->set_instance_rpc_handler(new dbx1000::InstanceClient(managerInstance->host_map()[-1]));
    cout <<__LINE__ << endl;
    {   // instanc 服务端
//        dbx1000::InstanceServer *instanceServer = new dbx1000::InstanceServer();
//        instanceServer->manager_instance_ = managerInstance;
//        thread instance_service_server(RunInstanceServer, instanceServer, managerInstance);
//        instance_service_server.detach();
    }

    cout <<__LINE__ << endl;
    managerInstance->set_init_done(true);
    managerInstance->instance_rpc_handler()->InstanceInitDone(managerInstance->instance_id());

    cout <<__LINE__ << endl;

    while(!managerInstance->instance_rpc_handler()->BufferManagerInitDone()) PAUSE
//    while(!managerInstance->instance_rpc_handler()->BufferManagerInitDone()) { std::this_thread::yield();}

    cout << "instance start." <<endl;

	warmup_finish = true;

    thread_t *thread_t_s = new thread_t[g_thread_cnt]();
    std::vector<std::thread> v_thread;
    for(int i = 0; i < g_thread_cnt; i++) {
        thread_t_s[i].init(i, managerInstance->m_workload());
        thread_t_s[i].manager_client_ = managerInstance;
        v_thread.emplace_back(f, &thread_t_s[i]);
    }
    for(int i = 0; i < g_thread_cnt; i++) {
        v_thread[i].join();
    }

    managerInstance->stats().print();

    delete managerInstance;

//    while(1) {};


    return 0;
}
