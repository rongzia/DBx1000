//
// Created by rrzhang on 2020/5/4.
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
#include "instance/benchmarks/ycsb_query.h"
#include "instance/benchmarks/query.h"
#include "instance/concurrency_control/row_mvcc.h"
#include "instance/txn/ycsb_txn.h"
#include "instance/txn/txn.h"
#include "instance/manager_instance.h"
#include "instance/thread.h"
#include "json/json.h"
#include "config.h"

using namespace std;

void f(thread_t* thread) {
    thread->run();
}

extern void parser(int argc, char * argv[]);
extern int parser_host(int argc, char *argv[], std::map<int, std::string> &hosts_map);
extern void Gen_DB_single_thread();
extern void Check_DB();

int main(int argc, char* argv[]) {

    system("rm -rf /home/zhangrongrong/CLionProjects/DBx1000/db/*");
    Gen_DB_single_thread();
    Check_DB();
    dbx1000::FileIO::Close();





    parser(argc, argv);
    cout << "mian test txn thread" << endl;

    dbx1000::ManagerInstance* managerInstance = new dbx1000::ManagerInstance("127.0.0.1:5000");
    managerInstance->set_instance_id(parser_host(argc, argv, managerInstance->host_map()));
    managerInstance->set_init_done(true);

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
//    glob_manager_client->api_txn_client()->ThreadDone(txn_thread_id);
//    stats.print_rpc();

    delete managerInstance;
    dbx1000::FileIO::Close();

    cout << "exit main." << endl;
    return 0;
};