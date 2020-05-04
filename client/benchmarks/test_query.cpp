//
// Created by rrzhang on 2020/4/25.
//

#include <iostream>
#include <memory>
#include "query.h"
#include "ycsb_query.h"


int main() {
    std::unique_ptr<Query_queue> query_queue(new Query_queue());
    std::cout << "debug1" << std::endl;
    query_queue->init();


    return 0;
}