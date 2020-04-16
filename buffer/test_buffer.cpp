//
// Created by rrzhang on 2020/4/15.
//

#include <iostream>
#include <cassert>
#include <cstring>
#include "buffer.h"
#include "lru_index.h"
#include "lru.h"
#include "../util/string_util.h"
#include "../util/profiler.h"

using namespace dbx1000;
using namespace std;

int pool_size = 6553600000/10*9;
int row_size = 65536;
int pool_item = pool_size/row_size;
int total_item = pool_item * 10;

void SimpleTest(Buffer *buffer){
    /// Warmup, initital lruindex and db
    cout << "start WarmUp" << endl;
    for(int i = 0; i < total_item; i++){
        std::string str("aaaaaaaaaa");
        std::string num = std::to_string(i);
        buffer->db_->Put(i, str.replace(0, num.size(), num));
        buffer->lru_index_->IndexInsert(i, nullptr);
    }
    buffer->Print();
    cout << "WarmUp finish" << endl;

    /// simple test
    for(int i = 0; i< 11; i++) {
        string row(10, 'b');
        buffer->BufferPut(i, row);
//        buffer->Print();
    }buffer->Print();
    for(int i = 0; i < 11; i++) {
        string row = buffer->BufferGet(i);
        cout << row << endl;
//        buffer->Print();
    }
    buffer->Print();
    buffer->FlushALl();
    buffer->Print();
}

int main(){
    std::unique_ptr<Profiler> profiler(new Profiler());
    std::unique_ptr<Buffer> buffer(new Buffer(pool_size, row_size));
    cout << "buffer inited" << endl;

//    SimpleTest(buffer.get());

    /// 一共 total_item 个数据，row_map 用来对比后期读出来的数据一致否
    cout << endl << endl;
    unordered_map<int , string> row_map;
    RandNum_generator rng('a', 'z');
    profiler->Start();
    for(int i = 0; i < total_item; i++) {
        if(0 == i % pool_item ) { cout << i << endl; }
        row_map.insert(pair<int, string>(i, StringUtil::Random_string(rng, row_size)));
    }
    profiler->End();
    cout << "init row_map time : " << profiler->Seconds() << endl;

    /// 写入
    cout << "start put" << endl;
    profiler->Clear();
    profiler->Start();
    int count = 0;
    for(auto &iter : row_map) {
        if(0 == count % pool_item ) { cout << count++ << endl; }
        buffer->BufferPut(iter.first, iter.second);
    }
    // buffer->Print();
    cout << "put finish" << endl;
    profiler->End();
    cout << "put time : " << profiler->Seconds() << endl;

    /// 按写入的顺序顺序读
    count = 0;
    profiler->Clear();
    profiler->Start();
    for(auto &iter : row_map) {
        if(0 == count % pool_item ) { cout << count++ << endl; }
        string row = buffer->BufferGet(iter.first);
        assert(0 == strcmp(row.data(), iter.second.data()));
    }
    cout << "sequential read finish" << endl;
    profiler->End();
    cout << "sequential read time : " << profiler->Seconds() << endl;

    /// 随机读总数量的十倍次
    count = 0;
    profiler->Clear();
    profiler->Start();
    srand(10);
    for(int i = 0; i< total_item * 10; i++) {
        if(0 == i % pool_item ) { cout << i << endl; }
        int a = rand() % total_item;
//        cout << a << endl;
        string row = buffer->BufferGet(a);
        assert(0 == strcmp(row.data(), row_map[a].data()));
    }
    cout << "read finish" << endl;
    profiler->End();
    cout << "read time : " << profiler->Seconds() << endl;

    return 0;
}