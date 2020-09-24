// #include <iostream>
// #include <thread>
// #include <mutex>
// #include <random>

// #include "common/buffer/buffer.h"
// #include "common/index/index.h"
// #include "common/lock_table/lock_table.h"
// #include "common/storage/disk/file_io.h"
// #include "common/storage/tablespace/page.h"
// #include "common/storage/tablespace/row_item.h"
// #include "common/storage/tablespace/tablespace.h"
// #include "common/storage/catalog.h"
// #include "common/storage/table.h"
// #include "common/workload/ycsb_wl.h"
// #include "common/workload/wl.h"
// #include "common/global.h"
// #include "common/myhelper.h"
// #include "common/mystats.h"
// #include "json/json.h"
// #include "instance/benchmarks/ycsb_query.h"
// #include "instance/benchmarks/query.h"
// #include "instance/concurrency_control/row_mvcc.h"
// #include "instance/txn/ycsb_txn.h"
// #include "instance/txn/txn.h"
// #include "instance/manager_instance.h"
// #include "instance/thread.h"
// #include "global_lock_service.h"
// #include "util/parse_result.h"
// #include "config.h"
// /// 该文件主要是测试 mvcc 数组结构的垃圾回收，原始的做法是，每个行都有一个对应的 mvcc 结构，这在程序开始就初始化了的
// /// ，但是随行数增加，这个结构也会增加，切没有自动回收垃圾的功能，C++11 引入了 shared_ptr 这个类，能够在离开作用域时
// /// ，自动调用原始指针的析构函数，达到自动回收垃圾的目的。



// /// 原始的方法，指针数组，且指向的内容一直存在，除非退出进程，或者显示调用析构函数，但是显示调用析构函数的实际不明确
// /// 终端 top 显示内存占用, 轻轻松松几个 G

// size_t tuple_sizes = 80;
// uint64_t array_size = 10*1024*1024UL;

// void Test_mvcc() {
//     std::cout << "ManagerInstance::InitMvccs" << std::endl;
//     std::vector<Row_mvcc*> mvcc_vector_;
//     mvcc_vector_.resize(array_size);

//     /// 初始化
//     dbx1000::Profiler profiler;
//     profiler.Start();
//     for (uint64_t key = 0; key < array_size; key++) {
//         mvcc_vector_[key] = new Row_mvcc();
//         mvcc_vector_[key]->init(key, tuple_sizes, nullptr);
//     }

//     profiler.End();
//     std::cout << "ManagerInstance::InitMvccs done. time : " << profiler.Millis() << " millis." << std::endl;
//     while(1) { this_thread::yield(); }
// }

// /// 带垃圾回收功能，仅占用百兆左右，虽然 vector 里多一个 bool 变量来控制并发。
// /// 主要思路：
// /// 程序启动仅仅声明 vector 数组，里面的 weak_ptr<Row_mvcc> 指针没有初始化，不指向任何值
// /// 使用时，判断 weak_ptr<Row_mvcc> 管理的内容是否被析构，
// /// 1）若被析构，通过 make_shared 初始化一个 mvcc 对象，weak_ptr 指向该 shared_ptr
// /// 2）若没有析构，直接 shared_ptr = weak_ptr.lock ,weak_ptr 引用计数++
// /// 这里 shared_ptr 声明周期只在调用者内部，出了调用，则 weak_ptr 引用为 0，自动析构 mvcc
// ///
// /// 为什么不用std::vector<std::pair<shared_ptr<Row_mvcc>, bool> >
// /// 而是      std::vector<std::pair<weak_ptr<Row_mvcc>, bool> >
// /// 初始化在哪里？1）调用外部，那么和普通指针有何区别 2）调用内部，那么多个线程之间的 shared_ptr 引用指向谁？weak_ptr 可以起到 "监视的作用"，shared_ptr 不行
// ///  
// /// 为什么不用std::vector<std::pair<Row_mvcc*, bool> >
// /// 线程内部是 shared_ptr 使用的，不能同时指向同一个原始指针
// void Test_mvcc_with_gc() {
//     std::cout << "ManagerInstance::InitMvccs" << std::endl;
//     std::vector<std::pair<weak_ptr<Row_mvcc>, bool> > mvcc_vector_;
//     mvcc_vector_.resize(array_size);

//     /// 初始化
//     dbx1000::Profiler profiler;
//     profiler.Start();
//     for (uint64_t key = 0; key < array_size; key++) {
//         mvcc_vector_[key].second = false;
//         assert(mvcc_vector_[key].first.expired());
//         assert(mvcc_vector_[key].first.use_count() == 0);
//         assert(!mvcc_vector_[key].second);
//     }
    
//     /// 多个线程随机访问数组，不明确 mvcc 结构的析构函数调用时机
//     std::vector<std::thread> threads;
//     for(int i = 0; i < 10; i++) {
//         threads.emplace_back(thread(
//             [&mvcc_vector_](){
//             for (uint64_t j = 0; j < array_size; j++) {
//                 std::random_device rd;
//                 uint64_t key = rd()%g_synth_table_size;


//                 // bool flag = false;
//                 while(!ATOM_CAS(mvcc_vector_[key].second, false, true))
//                     PAUSE
//                 shared_ptr<Row_mvcc> shared_p = mvcc_vector_[key].first.lock();
//                 if(mvcc_vector_[key].first.expired()) {
//                     assert(shared_p == nullptr);
//                     assert(mvcc_vector_[key].first.use_count() == 0);
//                     assert(mvcc_vector_[key].first.expired());
//                     shared_p = make_shared<Row_mvcc>();
//                     mvcc_vector_[key].first = shared_p;
//                     shared_p->init(key, tuple_sizes, nullptr);
//                 } else {
//                     assert(!mvcc_vector_[key].first.expired());
//                     // shared_p = mvcc_vector_[key].first.lock();
//                     assert(shared_p != nullptr);
//                     assert(mvcc_vector_[key].first.use_count() > 0 || mvcc_vector_[key].first.use_count() <=10);
//                     assert(!mvcc_vector_[key].first.expired());
//                 }
//                 mvcc_vector_[key].second = false;

//                 /// 使用 shared_p
//                 assert(mvcc_vector_[key].first.use_count() > 0 || mvcc_vector_[key].first.use_count() <=10);
//                 assert(!mvcc_vector_[key].first.expired());
//                 assert(shared_p != nullptr);
//                 /// 离开作用域，mvcc_vector_[key].first 的引用计数会 --
//             }
//         }
//         ));
//     }
//     for(auto &thread : threads){
//         thread.join();
//     }
    
//     for(uint64_t key = 0; key < g_synth_table_size; key++) {
//         assert(mvcc_vector_[key].first.expired());
//         assert(mvcc_vector_[key].first.use_count() == 0);
//         assert(!mvcc_vector_[key].second);
//     }

//     profiler.End();
//     std::cout << "ManagerInstance::InitMvccs done. time : " << profiler.Millis() << " millis." << std::endl;
//     while(1) { this_thread::yield(); }
// }

int main() {
//     // Test_mvcc();
//     Test_mvcc_with_gc();
    return 0;
}
