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
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/benchmarks/ ${PROJECT_SOURCE_DIR}/concurrency_control/ ${PROJECT_SOURCE_DIR}/storage/ ${PROJECT_SOURCE_DIR}/system/)
# lib files
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/libs)
#file(GLOB_RECURSE SRC_FILES benchmarks/*.cpp concurrency_control/*.cpp storage/*.cpp system/*.cpp config.cpp)
set(SRC_FILES
        ${PROJECT_SOURCE_DIR}/benchmarks/numbercomparator.cpp
        ${PROJECT_SOURCE_DIR}/benchmarks/ycsb_wl.cpp
        ${PROJECT_SOURCE_DIR}/benchmarks/ycsb_query.cpp
        ${PROJECT_SOURCE_DIR}/benchmarks/ycsb_txn.cpp
        ${PROJECT_SOURCE_DIR}/concurrency_control/row_mvcc.cpp
        ${PROJECT_SOURCE_DIR}/storage/catalog.cpp
        ${PROJECT_SOURCE_DIR}/storage/index_hash.cpp
        ${PROJECT_SOURCE_DIR}/storage/row.cpp
        ${PROJECT_SOURCE_DIR}/storage/table.cpp
        ${PROJECT_SOURCE_DIR}/system/arena.cpp
        ${PROJECT_SOURCE_DIR}/system/global.cpp
        ${PROJECT_SOURCE_DIR}/system/helper.cpp
        ${PROJECT_SOURCE_DIR}/system/manager.cpp
        ${PROJECT_SOURCE_DIR}/system/parser.cpp
        ${PROJECT_SOURCE_DIR}/system/query.cpp
        ${PROJECT_SOURCE_DIR}/system/stats.cpp
        ${PROJECT_SOURCE_DIR}/system/thread.cpp
        ${PROJECT_SOURCE_DIR}/system/txn.cpp
        ${PROJECT_SOURCE_DIR}/system/wl.cpp
        ${PROJECT_SOURCE_DIR}/config.cpp
        ${PROJECT_SOURCE_DIR}/system/mem_alloc.cpp
        ${PROJECT_SOURCE_DIR}/system/main.cpp
        )
add_executable(rundb ${SRC_FILES})
target_link_libraries(rundb  libleveldb.a libsnappy.a libjemalloc.so libpthread.so)



add_definitions(-DCACHING_FACILITY)
#add_definitions(-DCREATE_LEVELDB)
set(WL_FILES
        ${PROJECT_SOURCE_DIR}/benchmarks/numbercomparator.cpp
        ${PROJECT_SOURCE_DIR}/benchmarks/ycsb_txn.cpp
        ${PROJECT_SOURCE_DIR}/benchmarks/ycsb_query.cpp
        ${PROJECT_SOURCE_DIR}/benchmarks/ycsb_wl.cpp
        ${PROJECT_SOURCE_DIR}/concurrency_control/row_mvcc.cpp
        ${PROJECT_SOURCE_DIR}/storage/catalog.cpp
        ${PROJECT_SOURCE_DIR}/storage/index_hash.cpp
        ${PROJECT_SOURCE_DIR}/storage/row.cpp
        ${PROJECT_SOURCE_DIR}/storage/table.cpp
        ${PROJECT_SOURCE_DIR}/system/arena.cpp
        ${PROJECT_SOURCE_DIR}/system/global.cpp
        ${PROJECT_SOURCE_DIR}/system/helper.cpp
        ${PROJECT_SOURCE_DIR}/system/manager.cpp
        ${PROJECT_SOURCE_DIR}/system/parser.cpp
        ${PROJECT_SOURCE_DIR}/system/query.cpp
        ${PROJECT_SOURCE_DIR}/system/stats.cpp
        ${PROJECT_SOURCE_DIR}/system/thread.cpp
        ${PROJECT_SOURCE_DIR}/system/txn.cpp
        ${PROJECT_SOURCE_DIR}/system/wl.cpp
        ${PROJECT_SOURCE_DIR}/config.cpp
        ${PROJECT_SOURCE_DIR}/system/mem_alloc.cpp
        )
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/libs)
add_executable(test_wl test/test_wl.cpp ${WL_FILES})
target_link_libraries(test_wl libleveldb.a libsnappy.a libjemalloc.so libpthread.so)