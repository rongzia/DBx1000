#include "lock_server.h"
#include "config.h"
#include "ycsb.h"
#include "tpcc.h"
#include "rr/include/rr/profiler.h"

namespace rdb {
    void LockServer::Init() {
        switch (WORKLOAD) {
        case YCSB:
            m_wl = new ycsb_wl; break;
        case TPCC:
            m_wl = new tpcc_wl; break;
        default:
            assert(false);
        }
        rr::Profiler profiler;
        profiler.Start();
        m_wl->init();
        profiler.End();
        printf("workload initialized!\n");
        cout << "workload init time: " << profiler.Millis() << " ms." << endl;
#ifdef YCSB
        uint64_t total_page = g_synth_table_size / PAGE_SIZE;
        for(uint64_t i = 0; i < total_page; i++) {
            directories_[TABLES::MAIN_TABLE][i] = new DirectoryItem();
        }
#endif // YCSB
    }

    LockServer::~LockServer() {
        // delete item in directories_
        using map_1 = std::map<TABLES, tbb::concurrent_unordered_map<uint64_t, DirectoryItem*>>;
        using map_2 = tbb::concurrent_unordered_map<uint64_t, DirectoryItem*>;
        map_1::iterator it_1;
        map_2::iterator it_2;
        for(it_1 = directories_.begin(); it_1 != directories_.end(); it_1++) {
            map_2& temp = (it_1->second);
            for(it_2 = temp.begin(); it_2 != temp.end(); it_2++) {
                delete it_2->second;
            }
        }
    }

    DirectoryItem::DirectoryItem() {
        item_owner = -1;
    }
}