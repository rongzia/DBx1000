//
// Created by rrzhang on 2020/4/16.
//
#include <iostream>
#include <memory>
#include "lru.h"

using namespace dbx1000;
using namespace std;

void Test_LRU() {
    std::unique_ptr <LRU> lru(new LRU(3));

    char *buf1 = "aaa";
    char *buf2 = "bbb";
    char *buf3 = "ccc";
    char *buf4 = "ddd";

    RowNode *rowNode1 = new RowNode(buf1);
    RowNode *rowNode2 = new RowNode(buf2);
    RowNode *rowNode3 = new RowNode(buf3);
    RowNode *rowNode4 = new RowNode(buf4);


    lru->Prepend(rowNode1);
    cout << "add (aaa)" << endl;
    lru->DebugSize();
    lru->Print();

    lru->Get(rowNode1);
    cout << "Get (aaa)" << endl;
    lru->DebugSize();
    lru->Print();

    lru->Prepend(rowNode2);
    cout << "add (bbb)" << endl;
    lru->DebugSize();
    lru->Print();

    lru->Get(rowNode1);
    cout << "Get (aaa)" << endl;
    lru->DebugSize();
    lru->Print();

    lru->Get(rowNode1);
    cout << "Get (aaa)" << endl;
    lru->DebugSize();
    lru->Print();

    lru->Get(rowNode2);
    cout << "Get (bbb)" << endl;
    lru->DebugSize();
    lru->Print();

    lru->Prepend(rowNode3);
    cout << "add (ccc)" << endl;
    lru->DebugSize();
    lru->Print();

    lru->Get(rowNode1);
    cout << "Get (aaa)" << endl;
    lru->DebugSize();
    lru->Print();

    RowNode *temp = lru->Popback();
    cout << "Pop (tail)" << endl;
    lru->DebugSize();
    lru->Print();
}

void TestSomething(int**& p2, int a){
    int* p1 = &a;
    p2 = &p1;
    cout << (uint64_t* )(&p1) << ", " << (uint64_t* )(p2) << endl;
    cout << **p2 << endl;
}

int main() {
//    Test_LRU();
    uint64_t a = 100;
    uint64_t* p = &a;
    cout << "addr of a:" << p << ", addressof(a)" << std::addressof(a) << endl;
    cout << "(uint64_t*)(&p):" << (uint64_t*)(&p) << ", addr of p:" << &p << ", addressof(p)" << std::addressof(p) << endl;


    char* buf = new char[3]; buf[0] = 'a'; buf[1] = 'b'; buf[2] = 'c';
    cout << "(uint64_t* )(&buf[0]) : " << (uint64_t* )(&buf[0]) << endl;
    cout << "(uint64_t* )(&buf[1]) : " << (uint64_t* )(&buf[1]) << endl;
    cout << "(uint64_t* )(&buf[2]) : " << (uint64_t* )(&buf[2]) << endl;

    cout << "(uint64_t* )(&buf) : " << (uint64_t* )(&buf) << endl;
    cout << "std::addressof(buf) : " << std::addressof(buf) << endl;

    std::string str(buf, 3);
    cout << "(uint64_t* )(str[0]) : " << (uint64_t* )(&(str.data()[0])) << endl;
    cout << "(uint64_t* )(str[1]) : " << (uint64_t* )(&(str.data()[1])) << endl;

    int** p2;
    TestSomething(p2, a);
    cout << (uint64_t* )(p2) << endl;
    cout << **p2 << endl;
    return 0;
}