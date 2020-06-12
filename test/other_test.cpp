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
#include <mutex>
#include <condition_variable>
#include <cassert>
#include <util/profiler.h>

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
    map_2.insert(std::pair<uint64_t, char *>(1, "sdf"));
}

enum class LockMode {
    O,  /// 失效
    S,  /// 读锁
    X,  /// 写锁
};
struct LockNode {
    LockMode lock;
    mutex mtx;
    std::condition_variable cv;
    int count;
};

bool Lock(LockNode *lockNode, LockMode mode, int thead_id) {
    if (mode == LockMode::X) {
        std::unique_lock<std::mutex> lck(lockNode->mtx);
        while (lockNode->lock != LockMode::O) {
            cout << "thread " << thead_id << " lock X waiting..." << endl;
            lockNode->cv.wait(lck);
        }
        assert(lockNode->count == 0);
        lockNode->lock = LockMode::X;
        lockNode->count++;
        return true;
    }
    if (mode == LockMode::S) {
        std::unique_lock<std::mutex> lck(lockNode->mtx);
        while (lockNode->lock == LockMode::X) {
            cout << "thread " << thead_id << " lock S waiting..." << endl;
            lockNode->cv.wait(lck);
        }
        assert(lockNode->count >= 0);
        lockNode->lock = LockMode::S;
        lockNode->count++;
        return true;
    }
    assert(false);
}

bool UnLock(LockNode *lockNode) {
    std::unique_lock<std::mutex> lck(lockNode->mtx);
    if (LockMode::X == lockNode->lock) {
        assert(lockNode->count == 1);
        lockNode->count--;
        lockNode->lock = LockMode::O;
//        lockNode->cv.notify_one();
        lockNode->cv.notify_all();
        return true;
    }
    if (LockMode::S == lockNode->lock) {
        lockNode->count--;
        assert(lockNode->count >= 0);
        lockNode->lock = LockMode::S;
        if (lockNode->count == 0) {
            lockNode->lock = LockMode::O;
        }
//        lockNode->cv.notify_one();
        lockNode->cv.notify_all();
        return true;
    }
//    assert(false);
}

void Write(LockNode *lockNode, int thread_index) {
    Lock(lockNode, LockMode::X, thread_index);
    cout << "thread " << thread_index << " lock X success" << endl;
    this_thread::sleep_for(std::chrono::seconds(1));
    UnLock(lockNode);
    cout << "thread " << thread_index << " unlock success" << endl;
};

void Read(LockNode *lockNode, int thread_index) {
    Lock(lockNode, LockMode::S, thread_index);
    cout << "thread " << thread_index << " lock S success" << endl;
    this_thread::sleep_for(std::chrono::seconds(1));
    UnLock(lockNode);
    cout << "thread " << thread_index << " unlock success" << endl;
};

void TestLock() {
    dbx1000::Profiler profiler;
    profiler.Start();
    LockNode *lockNode = new LockNode();
    lockNode->count = 0;
    lockNode->lock = LockMode::O;
    Lock(lockNode, LockMode::X, 0);

    vector<thread> lock_threads;
    for (int i = 0; i < 10; i++) {
        lock_threads.emplace_back(thread(Read, lockNode, i + 1));
    }
    for (int i = 10; i < 20; i++) {
        lock_threads.emplace_back(thread(Write, lockNode, i + 1));
    }
    UnLock(lockNode);
    for (int i = 0; i < 20; i++) {
        lock_threads[i].join();
    }

    profiler.End();
    cout << "exe time : " << profiler.Seconds() << endl;
}


int main() {
//    Test_access();
//    Test_map();
//    TestLock();

    int a = INT32_MAX;
    unsigned int ui = 2;
    off_t offset = UINT64_MAX;
//    size_type
    long l = 4;
    long long ll = 3;
    long long int lli = 3;
    unsigned long ul = 4;
    unsigned long long ull = 3;
    unsigned long long int ulli = 3;
    int64_t i64 = 7;
    size_t s = UINT64_MAX;
    cout << typeid(a).name() << endl;
    cout << typeid(ui).name() << endl;
    cout << typeid(offset).name() << endl;
    cout << typeid(l).name() << endl;
    cout << typeid(ll).name() << endl;
    cout << typeid(lli).name() << endl;
    cout << typeid(ull).name() << endl;
    cout << typeid(ulli).name() << endl;
    cout << typeid(ulli).name() << endl;
    cout << typeid(i64).name() << endl;
    cout << typeid(s).name() << endl;
    cout << s << endl;
    cout << offset << endl;
    cout << a << endl;
    return 0;
}