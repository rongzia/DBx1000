#ifndef RDB_TXN_MANAGER
#define RDB_TXN_MANAGER

#include "../rdb/rdb_helper.h"

class txn_man;
class base_query;

namespace rdb {
    class RDB_Txn_Manager {
    public:

        void Init();
        void AddTxn(txn_man* txn, uint64_t thd_id);
        void ParseQuery(base_query* m_query, uint64_t thd_id);
        void ClearQuery(uint64_t thd_id);
        RC GetLock(uint64_t thd_id, uint64_t item_id, access_t access, size_t count);
        RC GetLocks(uint64_t thd_id);
        void CheckLocks(uint64_t thd_id);

        txn_man** txn_mans_;
        std::mutex txn_mutex_;
        LockTable* lock_table_;

        std::map<rdb::map_key, access_t, rdb::map_comp>* record_map_;
        std::map<rdb::map_key, access_t, rdb::map_comp>* page_map_;
    };
} // rdb

#endif // RDB_TXN_MANAGER