//
// Created by rrzhang on 2020/5/5.
//

#ifndef DBX1000_MYSTATS_H
#define DBX1000_MYSTATS_H

#include <memory>
#include "util/profiler.h"

namespace dbx1000 {
    class Stats_thd {
    public:
        void init(uint64_t thd_id);
        void clear();

        uint64_t thread_id_;
        uint64_t txn_cnt;
        uint64_t abort_cnt;
        uint64_t run_time;
        uint64_t time_man;
        uint64_t time_index;
        uint64_t time_wait;
        uint64_t time_abort;
        uint64_t time_cleanup;
        uint64_t time_ts_alloc;     //! 获取时间戳的耗时
        uint64_t time_query;          //! 为每个 txn 生成 m_query 的时间
        uint64_t wait_cnt;
        uint64_t debug1;
        uint64_t debug2;
        uint64_t debug3;
        uint64_t debug4;
        uint64_t debug5;

        uint64_t latency;
        uint64_t *all_debug1;
        uint64_t *all_debug2;

        std::unique_ptr<dbx1000::Profiler> profiler;
    };

    class Stats_tmp {
    public:
        void init();
        void clear();

        uint64_t time_man;
        uint64_t time_index;
        uint64_t time_wait;

        std::unique_ptr<dbx1000::Profiler> profiler;
    };

    class Stats {
    public:
        // PER THREAD statistics
        Stats_thd **_stats;    //! 数组大小等于线程数
        // stats are first written to tmp_stats, if the txn successfully commits,
        // copy the values in tmp_stats to _stats
        Stats_tmp **tmp_stats;

        // GLOBAL statistics
        uint64_t dl_detect_time;
        uint64_t dl_wait_time;
        uint64_t cycle_detect;
        uint64_t deadlock;

        void init();
        void init(uint64_t thread_id);
        void clear(uint64_t tid);
        void add_debug(uint64_t thd_id, uint64_t value, uint32_t select);
        void commit(uint64_t thd_id);
        void abort(uint64_t thd_id);
        void print();
        void print_lat_distr();
    };
}


#endif //DBX1000_MYSTATS_H
