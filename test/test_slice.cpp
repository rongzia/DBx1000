//
// Created by rrzhang on 2020/5/13.
//

#include <iostream>
#include <cstring>
#include "leveldb/slice.h"
using namespace std;

int main() {
    char *p = new char[10];
    memset(p, 'a', 10);
    leveldb::Slice *slice = new leveldb::Slice(p, 10);

    delete slice;

    cout << p << endl;


    return 0;
}