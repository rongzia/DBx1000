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
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/catalog.h"
#include "common/storage/table.h"
#include "common/workload/ycsb.h"
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
#include "global_lock_service.h"
#include "util/parse_result.h"
#include "config.h"

using namespace std;
using namespace dbx1000::global_lock_service;

extern void parser_host(int argc, char *argv[], int &ins_id, std::map<int, std::string> &hosts_map);

void RunInstanceServer(GlobalLockServiceImpl *instanceServer, dbx1000::ManagerInstance *managerInstance) {
    instanceServer->Start(managerInstance->host_map_[managerInstance->instance_id()]);
}
void f(thread_t* thread) {
    thread->run();
}


int main(int argc, char *argv[]) {
    assert(argc >= 2);

    dbx1000::ManagerInstance *managerInstance = new dbx1000::ManagerInstance();
    parser_host(argc, argv, managerInstance->instance_id_, managerInstance->host_map_);
    cout << "this instane id : " << managerInstance->instance_id_ << ", host : " << managerInstance->host_map_[managerInstance->instance_id_] << endl
         << "server id : " << managerInstance->host_map_[-1] << endl;
    managerInstance->Init(std::string(SHARED_DISK_HOST));

/**/
    {   // instance 服务端
        GlobalLockServiceImpl *globalLockService = new GlobalLockServiceImpl();
        globalLockService->manager_instance_ = managerInstance;
        thread instance_service_server(RunInstanceServer, globalLockService, managerInstance);
        instance_service_server.detach();
    }
    std::this_thread::sleep_for(chrono::seconds(2));
#ifndef SINGLE_NODE
    // 连接集中锁管理器
    managerInstance->global_lock_service_client_ = new GlobalLockServiceClient(managerInstance->host_map_[-1]);

    managerInstance->set_init_done(true);
    managerInstance->global_lock_service_client_->InstanceInitDone(managerInstance->instance_id());

    /// 等待所有 instance 初始化完成
    while(!managerInstance->global_lock_service_client_->GlobalLockInitDone()) { std::this_thread::sleep_for(chrono::milliseconds(5));}
    cout << "instance " << managerInstance->instance_id() << " start." <<endl;
#endif // SINGLE_NODE



    thread_t *thread_t_s;
    std::vector<std::thread> v_thread;
    dbx1000::Profiler profiler;
#ifdef WITH_WARM_UP
    /// 预热
	warmup_finish = false;
    thread_t_s = new thread_t[g_thread_cnt]();
    for(uint32_t i = 0; i < g_thread_cnt; i++) {
        thread_t_s[i].init(i, managerInstance->m_workload());
        thread_t_s[i].manager_client_ = managerInstance;
        v_thread.emplace_back(f, &thread_t_s[i]);
    }
    for(uint32_t i = 0; i < g_thread_cnt; i++) {
        v_thread[i].join();
    }
    v_thread.clear();
    delete [] thread_t_s;

    this_thread::sleep_for(chrono::seconds(5));
    managerInstance->ReRun();
    managerInstance->global_lock_service_client_->WarmupDone(managerInstance->instance_id_);
    while(!managerInstance->global_lock_service_client_->WaitWarmupDone()) { std::this_thread::sleep_for(chrono::milliseconds(5));}
#endif // WITH_WARM_UP

    /// run
    warmup_finish = true;
    thread_t_s = new thread_t[g_thread_cnt]();
    v_thread.resize(g_thread_cnt);
    profiler.Start();
    for(uint32_t i = 0; i < g_thread_cnt; i++) {
        thread_t_s[i].init(i, managerInstance->m_workload());
        thread_t_s[i].manager_client_ = managerInstance;
        v_thread[i] = std::thread(f, &thread_t_s[i]);
    }
    for(uint32_t i = 0; i < g_thread_cnt; i++) {
        v_thread[i].join();
    }
    profiler.End();

    managerInstance->stats().instance_run_time_ += profiler.Nanos();
    managerInstance->stats().print(managerInstance->instance_id());
#ifndef SINGLE_NODE
    managerInstance->global_lock_service_client_->ReportResult(managerInstance->stats(), managerInstance->instance_id());
#endif


    while(1) { this_thread::yield(); }
    delete managerInstance;

    return 0;
}
