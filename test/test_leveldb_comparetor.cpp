//
// Created by rrzhang on 2020/5/14.
//

#include <iostream>
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/comparator.h"
#include "util/numbercomparator.h"

using namespace std;

int main()
{
    std::string cmd = "rm -rf /tmp/leveldb_for_dbx1000";
    int rc = system(cmd.data());
    cout << "rmdir rc : " << rc << endl;

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    dbx1000::NumberComparatorImpl comparator;
    options.comparator = &comparator;
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/leveldb_for_dbx1000", &db);
    assert(status.ok());
    for(int i = 0; i < 100000; i++) {
        db->Put(leveldb::WriteOptions(), to_string(i), "aaaaaaaaaa");
    }

    leveldb::Iterator *iter = db->NewIterator(leveldb::ReadOptions());
    iter->SeekToFirst();
    while (iter->Valid()) {
        iter->Next();
    }
    iter->SeekToLast();
    cout << iter->value().ToString() << endl;


    delete db;
    return 0;
}