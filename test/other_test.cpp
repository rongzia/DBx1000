//
// Created by rrzhang on 2020/6/3.
//

//
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <map>
#include <unistd.h>

using namespace std;

void Test_access() {
    cout << ((access("/home/zhangrongrong/CLionProjects/DBx1000/db/", F_OK) >= 0) ? "exist" : "not exist") << endl;
    cout << ((access("/home/zhangrongrong/CLionProjects/DBx1000/db/MAIN_TABLE", F_OK) >= 0) ? "exist" : "not exist")
         << endl;
}

map<uint64_t, char *> GetMap() {
    map<uint64_t, char *> str_map;
    return str_map;
}

void Test_map() {
    map<uint64_t, char *> map_2 = GetMap();
    map_2.insert(std::pair<uint64_t , char*>(1, "sdf"));
}

int main() {
//    Test_access();
    Test_map();
    return 0;
}