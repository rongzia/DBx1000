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

int main(int argc, char* argv[]) {
    assert(argc >= 2);

    dbx1000::ManagerInstance* managerInstance = new dbx1000::ManagerInstance(SHARED_DISK_HOST);
    managerInstance->set_instance_id(parser_host(argc, argv, managerInstance->host_map()));
    managerInstance->set_instance_rpc_handler(new dbx1000::InstanceClient(managerInstance->host_map()[-1]));

    dbx1000::InstanceServer* instanceServer = new dbx1000::InstanceServer();
    instanceServer->manager_instance_ = managerInstance;
    instanceServer->Start(managerInstance->host_map()[managerInstance->instance_id()]);
    managerInstance->set_init_done(true);

    return 0;
}
