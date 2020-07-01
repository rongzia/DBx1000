//
// Created by rrzhang on 2020/7/1.
//
#include <cassert>
#include <fstream>
#include <iostream>
#include <sstream>
#include <mutex>
using namespace std;

#include "parse_result.h"

std::mutex mtx;

void AppendRunTime(uint64_t run_time) {
    std::lock_guard<std::mutex> lck(mtx);
    ofstream out("runtime", ios::app | ios::out);
    assert(out.is_open());
    out << run_time << endl;
}

void AppendLatency(uint64_t latency) {
    std::lock_guard<std::mutex> lck(mtx);
    ofstream out("latency", ios::app | ios::out);
    assert(out.is_open());
    out << latency << endl;
}
void AppendThroughtput(uint64_t throughtput) {
    std::lock_guard<std::mutex> lck(mtx);
    ofstream out("throughtput", ios::app | ios::out);
    assert(out.is_open());
    out << throughtput << endl;
}
void AppendRemoteLockTime(uint64_t remoteLockTime) {
    std::lock_guard<std::mutex> lck(mtx);
    ofstream out("remote_lock_time", ios::app | ios::out);
    assert(out.is_open());
    out << remoteLockTime << endl;
}


void ParseRunTime(){
    ifstream in("runtime", ios::app | ios::out);
    assert(in.is_open());
    std::stringstream ss;
    ss << in.rdbuf();

    uint64_t temp;
    int count = 0;
    uint64_t run_time = 0;

    while(ss >> temp){
        count ++;
//        cout << temp << endl;
        run_time += temp;
    }
    in.close();
    cout << "run_time : " << (double)run_time / count << ", count : " << count << endl;
}
void ParseLatency(){
    ifstream in("latency", ios::app | ios::out);
    assert(in.is_open());
    std::stringstream ss;
    ss << in.rdbuf();

    uint64_t temp;
    int count = 0;
    uint64_t total_latency = 0;

    while(ss >> temp){
        count ++;
//        cout << temp << endl;
        total_latency += temp;
    }
    in.close();
    cout << "total_latency : " << total_latency << ", count : " << count << endl;
}

void ParseThroughtput(){
    ifstream in("throughtput", ios::app | ios::out);
    assert(in.is_open());
    std::stringstream ss;
    ss << in.rdbuf();

    uint64_t temp;
    int count = 0;
    uint64_t throughtput = 0;

    while(ss >> temp){
        count ++;
//        cout << temp << endl;
        throughtput += temp;
    }
    in.close();
    cout << "total throughtput : " << throughtput << ", count : " << count << endl;
}
void ParseRemoteLockTime(){
    ifstream in("remote_lock_time", ios::app | ios::out);
    assert(in.is_open());
    std::stringstream ss;
    ss << in.rdbuf();

    uint64_t temp;
    int count = 0;
    uint64_t remoteLockTime = 0;

    while(ss >> temp){
        count ++;
//        cout << temp << endl;
        remoteLockTime += temp;
    }
    in.close();
    cout << "total remoteLockTime : " << remoteLockTime << ", count : " << count << endl;
}