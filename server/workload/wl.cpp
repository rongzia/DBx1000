#include "wl.h"

#include "global.h"
#include "helper.h"
//#include "row.h"
//#include "index_hash.h"
//#include "index_btree.h"
#include "catalog.h"
#include "table.h"
#include "buffer.h"

#include "leveldb/db.h"
#include "arena.h"
#include "numbercomparator.h"
#include "make_unique.h"

workload::workload(){
    cout << "workload::workload()" << endl;
}
workload::~workload(){
    cout << "workload::~workload()" << endl;
}

RC workload::init() {
    for (int i = 0; i < g_init_parallelism; i++) {
        arenas_.emplace_back(dbx1000::make_unique<dbx1000::Arena>(i));
    }

    sim_done_ = false;
    return RCOK;
}

RC workload::init_schema(string schema_file) {
    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);
    string line;
    ifstream fin(schema_file);
    Catalog *schema = new Catalog();
    while (getline(fin, line)) {
        if (line.compare(0, 6, "TABLE=") == 0) {
			std::string tname = std::string(&line[6], line.size() - 6);
            getline(fin, line);
            int col_count = 0;
            // Read all fields for this table.
            vector<string> lines;
            while (line.length() > 1) {
                lines.push_back(line);
                getline(fin, line);
            }
            schema->init(tname, lines.size());
            for (uint32_t i = 0; i < lines.size(); i++) {
                string line = lines[i];
                size_t pos = 0;
                string token;
                int elem_num = 0;
                int size = 0;
                string type;
                string name;
                while (line.length() != 0) {
                    pos = line.find(",");
                    if (pos == string::npos)
                        pos = line.length();
                    token = line.substr(0, pos);
                    line.erase(0, pos + 1);
                    switch (elem_num) {
                        case 0:
                            size = atoi(token.c_str());
                            break;
                        case 1:
                            type = token;
                            break;
                        case 2:
                            name = token;
                            break;
                        default:
                            assert(false);
                    }
                    elem_num++;
                }
                assert(elem_num == 3);
                schema->add_col(name, size, type);
                col_count++;
            }
            table_t *cur_tab = new table_t();
            cur_tab->init(schema);
            tables[tname] = cur_tab;
        }
//        else if (!line.compare(0, 6, "INDEX=")) {
//            string iname;
//            iname = &line[6];
//            getline(fin, line);
//
//            vector<string> items;
//            string token;
//            size_t pos;
//            while (line.length() != 0) {
//                pos = line.find(",");
//                if (pos == string::npos)
//                    pos = line.length();
//                token = line.substr(0, pos);
//                items.push_back(token);
//                line.erase(0, pos + 1);
//            }
//
//            string tname(items[0]);
//            INDEX *index = new INDEX();
//            int part_cnt = (CENTRAL_INDEX) ? 1 : g_part_cnt;
//            if (tname == "ITEM")
//                part_cnt = 1;
//#if INDEX_STRUCT == IDX_HASH
//#if WORKLOAD == YCSB
//            index->init(part_cnt, tables[tname], g_synth_table_size * 2);
//#elif WORKLOAD == TPCC
//            assert(tables[tname] != NULL);
//            index->init(part_cnt, tables[tname], stoi( items[1] ) * part_cnt);
//#endif
//#else
//            index->init(part_cnt, tables[tname]);
//#endif
//            indexes[iname] = index;
//        }
    }
    fin.close();
    return RCOK;
}


//void workload::index_insert(string index_name, uint64_t key, row_t *row) {
//    assert(false);
//    INDEX *index = (INDEX *) indexes[index_name];
//    index_insert(index, key, row);
//}
//
//void workload::index_insert(INDEX *index, uint64_t key, row_t *row, int64_t part_id) {
//    uint64_t pid = part_id;
//    if (part_id == -1)
//        pid = get_part_id(row);
////    itemid_t *m_item =
////            (itemid_t *) mem_allocator.alloc(sizeof(itemid_t), pid);
//    itemid_t *m_item = new (arenas_[key / (g_synth_table_size / g_init_parallelism)]->Allocate(sizeof(itemid_t))) itemid_t();
//    m_item->init();
//    m_item->type = DT_row;
//    m_item->location = row;
//    m_item->valid = true;
//
//#ifdef CREATE_LEVELDB
//    leveldb::Status status = workload::db_->Put(leveldb::WriteOptions(), std::to_string(key), std::string(row->data, row->get_tuple_size()));
//    assert(status.ok());
//#endif // CREATE_LEVELDB
//
//    assert(index->index_insert(key, m_item, pid) == RCOK);
//}


