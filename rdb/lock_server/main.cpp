#include <iostream>
#include <string>
#include "tbb/concurrent_unordered_map.h"

#include "global.h"
#include "mem_alloc.h"
#include "lock_server.h"
#include "rdb/lock_service/lock_service_impl.h"

using namespace std;

void parser(int argc, char * argv[]);

rdb::LockServer* lock_server;
rdb::LockServiceImpl* lock_service_impl;


int main(int argc, char* argv[]) {
	parser(argc, argv);
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
    
    lock_server = new rdb::LockServer();
    lock_server->Init();

    lock_service_impl = new rdb::LockServiceImpl();
    lock_service_impl->Start("0.0.0.0:30051");

    cout << lock_server->directories_[TABLES::MAIN_TABLE].size();
    return 0;
}