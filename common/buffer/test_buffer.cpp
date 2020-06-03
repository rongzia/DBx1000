//
// Created by rrzhang on 2020/6/2.
//


#include <iostream>
#include <cstring>
#include "config.h"
#include "buffer/buffer.h"
#include "util/profiler.h"
using namespace std;

void Test_Buffer_Simple() {
    dbx1000::Buffer* buffer = new dbx1000::Buffer(FILE_SIZE, PAGE_SIZE);
    dbx1000::Profiler profiler;
    /// write
    profiler.Start();
    char buf[PAGE_SIZE];
    memset(buf, 'a', PAGE_SIZE);
    for(int i = 0; i <  ITEM_NUM_PER_FILE * 10; ++i) {
        buffer->BufferPut(i, buf, PAGE_SIZE);
    }
    profiler.End();
    cout << "buffer write time    : " << profiler.Nanos() << endl;


    /// read
    profiler.Clear();
    profiler.Start();
    for(int i = 0; i <  ITEM_NUM_PER_FILE * 10; ++i) {
        buffer->BufferGet(i, buf, PAGE_SIZE);
    }
    profiler.End();
    cout << "buffer read time     : " << profiler.Nanos() << endl;

    delete buffer;
}

//int main() {
//    Test_Simple();
//    return 0;
//}