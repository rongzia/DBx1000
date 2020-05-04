cmake_minimum_required(VERSION 2.8)
project(Dbx1000)

SET (CMAKE_C_COMPILER "gcc")
SET (CMAKE_CXX_COMPILER "g++")
#SET (CMAKE_CXX_FLAGS "-std=c++11 -Wno-deprecated-declarations -fno-rtti" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS "-std=c++11 -Wno-deprecated-declarations" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)

add_definitions(-DNOGRAPHITE=1)

# include header files
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/api)
include_directories(${PROJECT_SOURCE_DIR}/api/api_single_machine)
include_directories(${PROJECT_SOURCE_DIR}/client)
include_directories(${PROJECT_SOURCE_DIR}/client/benchmarks)
include_directories(${PROJECT_SOURCE_DIR}/client/txn)
include_directories(${PROJECT_SOURCE_DIR}/common)
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/server)
include_directories(${PROJECT_SOURCE_DIR}/server/buffer)
include_directories(${PROJECT_SOURCE_DIR}/server/concurrency_control)
include_directories(${PROJECT_SOURCE_DIR}/server/storage)
include_directories(${PROJECT_SOURCE_DIR}/server/workload)
include_directories(${PROJECT_SOURCE_DIR}/system)
include_directories(${PROJECT_SOURCE_DIR}/util)


# lib files
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/libs)


set(SRC_FILES
        api/api_cc/api_cc.cpp
#        api/api_single_machine/cs_api.cpp
        api/api_txn/api_txn.cpp
        api/proto/api.grpc.pb.cc
        api/proto/api.pb.cc
        api/proto/dbx1000_service_util.cpp
        client/benchmarks/ycsb_query.cpp
        client/benchmarks/query.cpp
        client/txn/ycsb_txn.cpp
        client/txn/txn.cpp
        client/manager_client.cpp
        client/thread.cpp
        common/global.cpp
        common/myhelper.cpp
        common/manager.cpp
        common/row_item.cpp
        common/txn_row_man.cpp
        server/buffer/lru_index.cpp
        server/buffer/lru.cpp
        server/buffer/buffer.cpp
        server/concurrency_control/row_mvcc.cpp
        server/storage/catalog.cpp
        server/storage/table.cpp
        server/workload/ycsb_wl.cpp
        server/workload/wl.cpp
        server/manager_server.cpp
        system/stats.cpp
        system/parser.cpp
        util/numbercomparator.cpp
        util/arena.cpp
        config.cpp
        )
set(SRC_FILES_TXN
        api/api_cc/api_cc.cpp
#        api/api_single_machine/cs_api.cpp
        api/api_txn/api_txn.cpp
        api/proto/api.grpc.pb.cc
        api/proto/api.pb.cc
        api/proto/dbx1000_service_util.cpp
        client/benchmarks/ycsb_query.cpp
        client/benchmarks/query.cpp
        client/txn/ycsb_txn.cpp
        client/txn/txn.cpp
        client/manager_client.cpp
        client/thread.cpp
        common/global.cpp
        common/myhelper.cpp
        common/manager.cpp
        common/row_item.cpp
        common/txn_row_man.cpp
        system/stats.cpp
        system/parser.cpp
        util/numbercomparator.cpp
        util/arena.cpp
        config.cpp
        )
set(SRC_FILES_CC
        api/api_cc/api_cc.cpp
#        api/api_single_machine/cs_api.cpp
        api/api_txn/api_txn.cpp
        api/proto/api.grpc.pb.cc
        api/proto/api.pb.cc
        api/proto/dbx1000_service_util.cpp
        common/global.cpp
        common/myhelper.cpp
        common/manager.cpp
        common/row_item.cpp
        common/txn_row_man.cpp
        server/buffer/lru_index.cpp
        server/buffer/lru.cpp
        server/buffer/buffer.cpp
        server/concurrency_control/row_mvcc.cpp
        server/storage/catalog.cpp
        server/storage/table.cpp
        server/workload/ycsb_wl.cpp
        server/workload/wl.cpp
        server/manager_server.cpp
        system/stats.cpp
        system/parser.cpp
        util/numbercomparator.cpp
        util/arena.cpp
        config.cpp
        )
#add_executable(rundb system/main.cpp ${SRC_FILES})
#target_link_libraries(rundb libleveldb.a libsnappy.a libjemalloc.so libpthread.so)

# for buffer, buffer can use leveldb or memorydb
add_definitions(-DUSE_MEMORY_DB)
#add_definitions(-Ddb_num_item_=1000000)
#add_definitions(-DWARMUP_LEVELDB)

# grpc:
set(PROTOBUF_ROOT $ENV{HOME}/.local/protobuf-3.6.1)
include(${CMAKE_CURRENT_SOURCE_DIR}/api/cmake/protobuf.cmake)
set(GRPC_ROOT $ENV{HOME}/.local/grpc-v1.23.x)
include(${CMAKE_CURRENT_SOURCE_DIR}/api/cmake/grpc.cmake)
find_library(ZLIB_LIBRARY libz.so /usr/lib/x86_64-linux-gnu/)

execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
        -I ${CMAKE_CURRENT_SOURCE_DIR}/api/proto
        --cpp_out=${CMAKE_CURRENT_SOURCE_DIR}/api/proto
        ${CMAKE_CURRENT_SOURCE_DIR}/api/proto/api.proto)
execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
        -I ${CMAKE_CURRENT_SOURCE_DIR}/api/proto
        --grpc_out=${CMAKE_CURRENT_SOURCE_DIR}/api/proto
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/api/proto/api.proto)

include_directories(${PROJECT_SOURCE_DIR}/api/api_cc)
include_directories(${PROJECT_SOURCE_DIR}/api/api_txn)
include_directories(${PROJECT_SOURCE_DIR}/api/proto)
include_directories(${Protobuf_INCLUDE_DIR})
include_directories(${gRPC_INCLUDE_DIR})

add_executable(main_test_cc system/main_test_cc.cpp ${SRC_FILES})
add_executable(main_test_txn system/main_test_txn.cpp ${SRC_FILES})
target_link_libraries(main_test_cc libleveldb.a libsnappy.a
        ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY} pthread)
target_link_libraries(main_test_txn libleveldb.a libsnappy.a
        ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY} pthread)

# test query
add_executable(test_query client/benchmarks/test_query.cpp
        client/benchmarks/query.cpp
        client/benchmarks/ycsb_query.cpp
        common/global.cpp
        util/arena.cpp)
target_link_libraries(test_query pthread)


add_executable(test_1 test_1.cpp)

add_executable(api_test_cc api/api_test_cc.cpp ${SRC_FILES})
add_executable(api_test_txn api/api_test_txn.cpp ${SRC_FILES})
target_link_libraries(api_test_cc ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY} pthread)
target_link_libraries(api_test_txn ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY} pthread)