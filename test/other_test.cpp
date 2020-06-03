//
// Created by rrzhang on 2020/6/3.
//

//
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <unistd.h>
using namespace std;

void Test_access() {
    cout <<( (access("/home/zhangrongrong/CLionProjects/DBx1000/db/", F_OK ) >=0) ? "exist" : "not exist") << endl;
    cout <<( (access("/home/zhangrongrong/CLionProjects/DBx1000/db/MAIN_TABLE", F_OK ) >=0) ? "exist" : "not exist") << endl;
}

int main() {
Test_access();

    return 0;
}