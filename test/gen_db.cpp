//
// Created by rrzhang on 2020/6/8.
//
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "common/buffer/buffer.h"
#include "common/index/index.h"
#include "common/lock_table/lock_table.h"
#include "common/storage/disk/file_io.h"
#include "common/storage/tablespace/page.h"
#include "common/storage/tablespace/row_item.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/catalog.h"
#include "common/storage/table.h"
#include "common/workload/ycsb_wl.h"
#include "common/workload/wl.h"
#include "common/global.h"
#include "common/myhelper.h"
#include "common/mystats.h"
#include "instance/benchmarks/ycsb_query.h"
#include "instance/benchmarks/query.h"
#include "instance/concurrency_control/row_mvcc.h"
#include "instance/txn/ycsb_txn.h"
#include "instance/txn/txn.h"
#include "instance/thread.h"
#include "json/json.h"
#include "config.h"

#define row_size (80)

void Gen_DB_single_thread() {
    dbx1000::TableSpace *tableSpace = new dbx1000::TableSpace("MAIN_TABLE");
    dbx1000::Index *index = new dbx1000::Index("MAIN_TABLE_INDEX");


    vector<thread> threads;                 /// for multi threads
    dbx1000::Profiler profiler;
    profiler.Start();
    dbx1000::Page *page = new dbx1000::Page(new char[MY_PAGE_SIZE]);
    page->set_page_id(tableSpace->GetNextPageId());
    char row[row_size];
    uint64_t version = 1;
    memset(row, 0, row_size);
    for (uint64_t key = 0; key < SYNTH_TABLE_SIZE; key++) {
        if (row_size > (MY_PAGE_SIZE - page->used_size())) {
            page->Serialize();
            dbx1000::FileIO::WritePage(page->page_id(), page->page_buf());
            assert((((MY_PAGE_SIZE - 64) / row_size * row_size) + 64) ==
                   page->used_size());  /// 检查 use_size
            page->set_page_id(tableSpace->GetNextPageId());
            page->set_used_size(64);
        }
        memcpy(&row[0], &key, sizeof(uint64_t));    /// 头 8 字节为 key
        memcpy(&row[row_size - 8], &version, sizeof(uint64_t));   /// 尾 8 字节为 version
        page->PagePut(page->page_id(), row, row_size);
        dbx1000::IndexItem indexItem(page->page_id(), page->used_size() - row_size);
        index->IndexPut(key, &indexItem);
    }
    if (page->used_size() > 64) {
        page->Serialize();
        dbx1000::FileIO::WritePage(page->page_id(), page->page_buf());
    }
    delete page;
    profiler.End();
    cout << "Gen_DB time : " << profiler.Micros() << " micros" << endl;
    index->Serialize();
    tableSpace->Serialize();
    delete index;
    delete tableSpace;
}

void Gen_DB() {
    dbx1000::TableSpace *tableSpace = new dbx1000::TableSpace("MAIN_TABLE");
    dbx1000::Index *index = new dbx1000::Index("MAIN_TABLE_INDEX");


    vector<thread> threads;                 /// for multi threads
    dbx1000::Profiler profiler;
    profiler.Start();
    for (int thd = 0; thd < 10; thd++) {      /// for multi threads
        threads.emplace_back(thread(          /// for multi threads
                [&, thd]() {                  /// for multi threads
                    dbx1000::Page *page = new dbx1000::Page(new char[MY_PAGE_SIZE]);
                    page->set_page_id(tableSpace->GetNextPageId());
                    char row[row_size];
                    uint64_t version = 1;
                    memset(row, 0, row_size);
                    for (uint64_t key = (SYNTH_TABLE_SIZE / 10) * thd;          /// for multi threads
                         key < (SYNTH_TABLE_SIZE / 10) * (thd + 1); key++) {    /// for multi threads
//                    for (uint64_t key = 0; key < num_item; key++) {
                        if (row_size > (MY_PAGE_SIZE - page->used_size())) {
                            page->Serialize();
                            dbx1000::FileIO::WritePage(page->page_id(), page->page_buf());
                            assert((((MY_PAGE_SIZE - 64) / row_size * row_size) + 64) ==
                                   page->used_size());  /// 检查 use_size
                            page->set_page_id(tableSpace->GetNextPageId());
                            page->set_used_size(64);
                        }
                        memcpy(&row[0], &key, sizeof(uint64_t));    /// 头 8 字节为 key
                        memcpy(&row[row_size - 8], &version, sizeof(uint64_t));   /// 尾 8 字节为 version
                        page->PagePut(page->page_id(), row, row_size);
                        dbx1000::IndexItem indexItem(page->page_id(), page->used_size() - row_size);
                        index->IndexPut(key, &indexItem);
                    }
                    if (page->used_size() > 64) {
                        page->Serialize();
                        dbx1000::FileIO::WritePage(page->page_id(), page->page_buf());
                    }
                    delete page;
                }                         /// for multi threads
        ));                               /// for multi threads
    }                                     /// for multi threads
    for (int thd = 0; thd < 10; thd++) {  /// for multi threads
        threads[thd].join();              /// for multi threads
    }                                     /// for multi threads
    profiler.End();
    cout << "Gen_DB time : " << profiler.Micros() << " micros" << endl;
    index->Serialize();
    tableSpace->Serialize();
    delete index;
    delete tableSpace;
}


void Check_DB() {
    dbx1000::TableSpace *tableSpace2 = new dbx1000::TableSpace("MAIN_TABLE");
    dbx1000::Index *index2 = new dbx1000::Index("MAIN_TABLE_INDEX");
    tableSpace2->DeSerialize();
    index2->DeSerialize();

    vector<thread> threads;                 /// for multi threads
    dbx1000::Profiler profiler;
    profiler.Start();
    for (int thd = 0; thd < 10; thd++) {      /// for multi threads
        threads.emplace_back(thread(          /// for multi threads
                [&, thd]() {                  /// for multi threads
                    dbx1000::Page *page2 = new dbx1000::Page(new char[MY_PAGE_SIZE]);
                    char row[row_size];
                    for (uint64_t key = (SYNTH_TABLE_SIZE / 10) * thd;          /// for multi threads
                         key < (SYNTH_TABLE_SIZE / 10) * (thd + 1); key++) {    /// for multi threads
//                    for (uint64_t key = 0; key < num_item; key++) {
                        dbx1000::IndexItem indexItem;
                        index2->IndexGet(key, &indexItem);
                        dbx1000::FileIO::ReadPage(indexItem.page_id_, page2->page_buf());
                        page2->Deserialize();
//                        assert((((MY_PAGE_SIZE - 64) / row_size * row_size) + 64) ==
//                               page2->used_size());  /// 检查 use_size
                        memcpy(row, &page2->page_buf()[indexItem.page_location_], row_size);
                        uint64_t temp_key;
                        uint64_t version;
                        memcpy(&temp_key, &row[0], sizeof(uint64_t));
                        memcpy(&version, &row[row_size - 8], sizeof(uint64_t));
                        if(key == 6293) { cout << "temp_key : " <<temp_key  << endl;}
                        assert(key == temp_key);
                        assert(version == 1);
                        for (int i = 8; i < 72; i++) { assert(0 == row[i]); }
                    }
                    delete page2;
                }                         /// for multi threads
        ));                               /// for multi threads
    }                                     /// for multi threads
    for (int thd = 0; thd < 10; thd++) {  /// for multi threads
        threads[thd].join();              /// for multi threads
    }                                     /// for multi threads
    profiler.End();
    cout << "Check_DB time : " << profiler.Micros() << " micros" << endl;
    index2->Serialize();
    tableSpace2->Serialize();
    delete index2;
    delete tableSpace2;
}

int main() {
    system("rm -rf /home/zhangrongrong/CLionProjects/DBx1000/db/*");

    Gen_DB_single_thread();
    Check_DB();

    dbx1000::FileIO::Close();
    system("scp -r /home/zhangrongrong/CLionProjects/DBx1000/db/ zhangrongrong@10.11.6.121:/home/zhangrongrong/CLionProjects/DBx1000/");
    return 0;
}