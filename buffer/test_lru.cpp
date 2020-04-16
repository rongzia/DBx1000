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

int main() {
    Test_LRU();
    return 0;
}