//
// Created by rrzhang on 2020/5/21.
//
#include <cassert>
#include <fstream>
#include <iostream>
#include "include/json/json.h"
using namespace std;


bool GetJsonRoot(Json::Value &root){
    Json::Reader reader;
    ifstream in("../config.json", ios::binary);
    return reader.parse(in, root);
}

int main(){

    Json::Value root;
    assert(true == GetJsonRoot(root));

    cout << root["server_host"].asString() << endl;
    cout << root["client1_host"].asString() << endl;
    cout << root["client2_host"].asString() << endl;
}