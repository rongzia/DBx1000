//
// Created by rrzhang on 2020/6/3.
//

#ifndef DBX1000_MANAGER_SERVER_H
#define DBX1000_MANAGER_SERVER_H

#include <cstdint>
#include <string>

class workload;

namespace dbx1000 {

    class Buffer;
    class Index;
    class LockTable;
    class TableSpace;
    class Stats;
    class InstanceClient;


    class ManagerServer {
    public:
        ManagerServer();
        ~ManagerServer();
        ManagerServer(const ManagerServer&) = delete;
        ManagerServer &operator=(const ManagerServer&) = delete;

        struct InstanceInfo {
            uint16_t instance_id;
            uint16_t thread_num;
            std::string host;
            bool init_done;
            InstanceClient* instance_rpc_handler;
        };

        bool LockGet();

        /// getter and setter
        Buffer* buffer();
        Index *index();
        LockTable *lock_table();
        TableSpace *table_space();
        workload *m_workload();
        Stats* stats();
        bool init_done() const;
        const std::string& host() const;
        InstanceInfo* instances();
        void set_buffer(Buffer*);
        void set_index(Index*);
        void set_lock_table(LockTable *);
        void set_table_space(TableSpace *);
        void set_m_workload(workload *);
        void set_stats(Stats*);
        void set_init_done(bool);
        void set_host(const std::string&);
        void set_instances_i(InstanceInfo*);


    private:
        bool CheckDB();

        Buffer* buffer_;
        Index* index_;
        LockTable* lock_table_;
        TableSpace* table_space_;
        workload* m_workload_;
        Stats* stats_;

        bool init_done_;
        std::string host_;
        InstanceInfo* instances_;
    };
}


#endif //DBX1000_MANAGER_SERVER_H
