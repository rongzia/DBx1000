cmake_minimum_required(VERSION 2.8)
project(Dbx1000)

SET (CMAKE_C_COMPILER "gcc")
SET (CMAKE_CXX_COMPILER "g++")
SET (CMAKE_CXX_FLAGS "-std=c++11 -Wno-deprecated-declarations -fno-rtti" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)

add_definitions(-DNOGRAPHITE=1)

# include header files
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/util)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/benchmarks/ ${PROJECT_SOURCE_DIR}/concurrency_control/ ${PROJECT_SOURCE_DIR}/storage/ ${PROJECT_SOURCE_DIR}/system/)
# lib files
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/libs)
#file(GLOB_RECURSE SRC_FILES benchmarks/*.cpp concurrency_control/*.cpp storage/*.cpp system/*.cpp config.cpp)
set(SRC_FILES
        benchmarks/query.cpp
        benchmarks/ycsb_query.cpp
        concurrency_control/row_mvcc.cpp
        storage/catalog.cpp
        storage/index_hash.cpp
        storage/row.cpp
        storage/table.cpp
        system/global.cpp
        system/helper.cpp
        system/manager.cpp
        system/parser.cpp
        system/stats.cpp
        system/thread.cpp
        txn/txn.cpp
        txn/ycsb_txn.cpp
        util/arena.cpp
        util/numbercomparator.cpp
        workload/wl.cpp
        workload/ycsb_wl.cpp
        config.cpp
        )
add_executable(rundb system/main.cpp ${SRC_FILES})
target_link_libraries(rundb  libleveldb.a libsnappy.a libjemalloc.so libpthread.so)



add_definitions(-DCACHING_FACILITY)
#add_definitions(-DCREATE_LEVELDB)
set(WL_FILES
        benchmarks/query.cpp
        benchmarks/ycsb_query.cpp
        concurrency_control/row_mvcc.cpp
        storage/catalog.cpp
        storage/index_hash.cpp
        storage/row.cpp
        storage/table.cpp
        system/global.cpp
        system/helper.cpp
        system/manager.cpp
        system/parser.cpp
        system/stats.cpp
        system/thread.cpp
        txn/txn.cpp
        txn/ycsb_txn.cpp
        util/arena.cpp
        util/numbercomparator.cpp
        workload/wl.cpp
        workload/ycsb_wl.cpp
        config.cpp
        )
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/libs)
add_executable(test_wl test/test_wl.cpp ${WL_FILES})
target_link_libraries(test_wl libleveldb.a libsnappy.a libjemalloc.so libpthread.so)