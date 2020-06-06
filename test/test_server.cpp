//
// Created by rrzhang on 2020/6/5.
//

#include <iostream>
#include <cassert>
#include <cstring>
#include <thread>
#include <vector>
#include "buffer_server/manager_server.h"

#include "common/global.h"
//#include "common/buffer/buffer.h"
#include "common/index/index.h"
#include "common/storage/disk/file_io.h"
#include "common/storage/tablespace/tablespace.h"
#include "common/storage/tablespace/page.h"
using namespace std;

#define row_size (80)

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
                    for (uint64_t key = (g_synth_table_size / 10) * thd;          /// for multi threads
                         key < (g_synth_table_size / 10) * (thd + 1); key++) {    /// for multi threads
//                    for (uint64_t key = 0; key < num_item; key++) {
                        dbx1000::IndexItem indexItem;
                        index2->IndexGet(key, &indexItem);
                        dbx1000::FileIO::ReadPage(indexItem.page_id_, page2->page_buf());
                        page2->Deserialize();
                        memcpy(row, &page2->page_buf()[indexItem.page_location_], row_size);
                        uint64_t temp_key;
                        uint64_t version;
                        memcpy(&temp_key, &row[0], sizeof(uint64_t));
                        memcpy(&version, &row[row_size - 8], sizeof(uint64_t));
                        if(key!= temp_key) {
                            cout << "key:" << key << ", temp_key:" << temp_key << endl;
                        }
                        assert(key == temp_key);
                        assert(version == 0);
                        for (int i = 8; i < 72; i++) { assert('a' == row[i]); }
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
    delete index2;
    delete tableSpace2;
}


int main() {

    dbx1000::ManagerServer* managerServer = new dbx1000::ManagerServer();
    delete managerServer;

    Check_DB();
    return 0;
}