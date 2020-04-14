//
// Created by rrzhang on 2020/4/2.
//

#include <iostream>
#include <memory>
#include "leveldb/db.h"
//#include "ycsb.h"
#include "ycsb_query.h"
#include "workload/ycsb_wl.h"
#include "table.h"
#include "catalog.h"
#include "util/numbercomparator.h"

void Test_wl() {
    leveldb::DB *db;
//    if (0) {
    if (1) {
        std::unique_ptr<workload> m_wl(new ycsb_wl);
        m_wl->init();
        std::unique_ptr<table_t> table(m_wl->tables["MAIN_TABLE"]);
        table->get_schema()->print_schema();
        db = m_wl->db_.get();
    } else {
        leveldb::Options options;
        options.create_if_missing = true;
        options.comparator = dbx1000::NumberComparator();
        leveldb::Status status = leveldb::DB::Open(options, "/home/zhangrongrong/leveldb", &db);
        assert(status.ok());
    }

    leveldb::Iterator *iter = db->NewIterator(leveldb::ReadOptions());
    leveldb::Iterator *end = db->NewIterator(leveldb::ReadOptions());
    end->SeekToLast();
    iter->SeekToFirst();
    cout << "first : key(" << iter->key().size() << "):" << iter->key().ToString() << ", value(" << iter->value().size() << "):"
         << iter->value().ToString() << endl;
    cout << "end : key(" << end->key().size() << "):" << end->key().ToString() << ", value(" << end->value().size() << "):"
         << end->value().ToString() << endl;

    int i = 0;
    while (iter->Valid()) {
        assert(i == std::stoi(iter->key().ToString()));
        assert(100 == iter->value().size());

        if (i % (1024 * 1024) == 0 || iter->key() == end->key()) {
            cout << "key(" << iter->key().size() << "):" << iter->key().ToString() << ", value(" << iter->value().size() << "):"
                 << iter->value().ToString() << endl;
        }
        iter->Next();
        i++;
    }
}

int main() {

    Test_wl();
    cout << "exit main." << endl;
    return 0;
}