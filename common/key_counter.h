#ifndef KEY_COUNTER_H
#define KEY_COUNTER_H

#include "config.h"

#ifdef KEY_COUNT

#include <tbb/concurrent_hash_map.h>
#include <map>
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cassert>
#include <set>
#include <mutex>
#include "common/global.h"

class KeyCounter{
public:
    KeyCounter() : total_count_(ATOMIC_VAR_INIT(0)) {};
    ~KeyCounter() { Clear(); }

    RC Add(TABLES table, uint64_t key) {
        total_count_.fetch_add(1);
        std::unique_lock<std::mutex> lck = std::unique_lock<std::mutex>(mtxes_[table], std::defer_lock);
        lck.lock();
        counter_[table].insert(key);
    }

    void Serialize(TABLES table) {
        std::ostringstream oss;
        for(const auto& iter : counter_[table]) {
            oss << TABLESToString(table) << " " << iter << std::endl;
        }
        std::ofstream out(TABLESToString(table), std::ios::out | std::ios::binary);
        out << oss.str();
        out.close();
    }

    
    void Serialize() {
        for(int t = (int)TABLES::MAIN_TABLE; t <= (int)TABLES::STOCK; t++){
            std::ostringstream oss;
            for(const auto& iter : counter_[(TABLES)t]) {
                oss << TABLESToString((TABLES)t) << " " << iter << std::endl;
            }
            std::ofstream out(TABLESToString((TABLES)t), std::ios::out | std::ios::binary);
            out << oss.str();
            out.close();
        }
    }

    void Print(TABLES table) {
        for(const auto& iter : counter_[table]) {
            std::cout << TABLESToString(table) << ", " << iter << std::endl;
        }
    }

    void Print() {
        for(int t = (int)TABLES::MAIN_TABLE; t <= (int)TABLES::STOCK; t++){
            for(const auto& iter : counter_[(TABLES)t]) {
                std::cout << TABLESToString((TABLES)t) << ", " << iter << std::endl;
            }
        }
    }

    
    void Clear() {
        total_count_.store(0);
        for(int t = (int)TABLES::MAIN_TABLE; t <= (int)TABLES::STOCK; t++){
            counter_[(TABLES)t].clear();
        }
    }

    std::string TABLESToString(TABLES table) {
        if(table == TABLES::MAIN_TABLE) { return std::string     ("MAIN_TABLE"); }
        else if(table == TABLES::WAREHOUSE) { return std::string ("WAREHOUSE "); }
        else if(table == TABLES::DISTRICT) { return std::string  ("DISTRICT  "); }
        else if(table == TABLES::CUSTOMER) { return std::string  ("CUSTOMER  "); }
        else if(table == TABLES::HISTORY) { return std::string   ("HISTORY   "); }
        else if(table == TABLES::NEW_ORDER) { return std::string ("NEW_ORDER "); }
        else if(table == TABLES::ORDER) { return std::string     ("ORDER     "); }
        else if(table == TABLES::ORDER_LINE) { return std::string("ORDER_LINE"); }
        else if(table == TABLES::ITEM) { return std::string      ("ITEM      "); }
        else if(table == TABLES::STOCK) { return std::string     ("STOCK     "); }
    }

private:

    std::map<TABLES, std::multiset<uint64_t>> counter_;
    std::map<TABLES, std::mutex> mtxes_;
public:
    atomic_uint64_t total_count_;


    // tbb::concurrent_hash_map<std::pair<TABLES, uint64_t>, uint64_t> counter_;
    // RC Add(TABLES table, uint64_t key) {
    //     bool res;
    //     tbb::concurrent_hash_map<std::pair<TABLES, uint64_t>, uint64_t>::accessor accessor;
    //     res = counter_.insert(accessor, tbb::concurrent_hash_map<std::pair<TABLES, uint64_t>, uint64_t>::value_type(std::make_pair(table, key), 0));
    //     accessor->second += 1;
    // }

    // void Serialize() {
    //     std::ostringstream oss;
    //     tbb::concurrent_hash_map<std::pair<TABLES, uint64_t>, uint64_t>::iterator iter;
    //     for(iter = counter_.begin(); iter != counter_.end(); iter++) {
    //         oss << TABLESToString(iter->first.first) << " " << iter->first.second << " " << iter->second << std::endl;
    //     }
    //     std::ofstream out("keycout", std::ios::out | std::ios::binary);
    //     out << oss.str();
    //     out.close();
    // }

    // void Print() {
    //     tbb::concurrent_hash_map<std::pair<TABLES, uint64_t>, uint64_t>::iterator iter;
    //     for(iter = counter_.begin(); iter != counter_.end(); iter++) {
    //         std::cout << TABLESToString(iter->first.first) << ", " << iter->first.second << ",\t " << iter->second << std::endl;
    //     }
    // }
    // void Clean() {
    //     tbb::concurrent_hash_map<std::pair<TABLES, uint64_t>, uint64_t>::iterator iter;
    //     for(iter = counter_.begin(); iter != counter_.end();) {
    //         tbb::concurrent_hash_map<std::pair<TABLES, uint64_t>, uint64_t>::iterator curr = iter;
    //         iter++;
    //         bool res = counter_.erase(curr->first);
    //         assert(res);
    //     }
    // }
};

#endif // KEY_COUNT
#endif // KEY_COUNTER_H 