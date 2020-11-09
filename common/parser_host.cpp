//
// Created by rrzhang on 2020/6/6.
//
#include <cassert>
#include <map>
#include <fstream>
#include <iostream>

using namespace std;

#include "json/json.h"

void parser_host(int argc, char *argv[], int &ins_id, std::map<int, std::string> &hosts_map) {
    ins_id = -1;
    for (int i = 1; i < argc; i++) {
        assert(argv[i][0] == '-');
        if (std::string(&argv[i][1], 11) == "instance_id") {
            ins_id = std::stoi(&argv[i][13]);
        }
    }

    std::ifstream in("../config.json", std::ios::out | std::ios::binary);
    assert(in.is_open());
    Json::Value root;
    in >> root;
    Json::Value::Members members = root.getMemberNames();
    for (auto iter = members.begin(); iter != members.end(); iter++) {
        hosts_map.insert(std::pair<int, std::string>(
                root[*iter]["id"].asInt(), root[*iter]["ip"].asString() + ":" + root[*iter]["port"].asString()));
    }
    in.close();
}