//
// Created by rrzhang on 2020/6/3.
//

#include <iostream>
#include <vector>
#include <thread>
#include "lock_table.h"

using namespace std;

/// 定义一个全为 0 的数组，几个线程并发对所有的位置都加 1，最后验证是否所有的值都为线程数
void Print_Array(int num_array[100]) {
    for (int i = 0; i < 100; i++) {
        cout << num_array[i] << " ";
        if (i % 10 == 9) {
            cout << endl;
        }
    }
    cout << endl;
}

void Test_Lock_Table() {
    int num_array[100];
    for (int &i : num_array) {
        i = 0;
    }
    Print_Array(num_array);

    dbx1000::LockTable *lockTable = new dbx1000::LockTable();
    for (int i = 0; i < 100; i++) {
        lockTable->lock_table().insert(std::pair<uint64_t, dbx1000::LockNode *>(i, new dbx1000::LockNode(0)));
    }

    std::vector<thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back(thread([&num_array, &lockTable]() {
            for (int i = 0; i < 100; i++) {
                lockTable->Lock(i, dbx1000::LockMode::X);
                num_array[i]++;
                lockTable->UnLock(i);
            }
        }));
    }
    for (int i = 0; i < 10; i++) {
        threads[i].join();
    }

    Print_Array(num_array);
}