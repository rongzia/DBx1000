cmake_minimum_required(VERSION 2.8)
project(Dbx1000)

SET (CMAKE_C_COMPILER "gcc")
SET (CMAKE_CXX_COMPILER "g++")
#SET (CMAKE_CXX_FLAGS "-std=c++11 -Wno-deprecated-declarations -fno-rtti" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS "-std=c++11 -Wno-deprecated-declarations" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)

add_definitions(-DNOGRAPHITE=1)


### ******************************* grpc *******************************:
add_definitions(-DWITH_RPC)
option(WITH_RPC "if use rpc then on, else off?" ON)
if (WITH_RPC)
    message("WITH_RPC : " ${WITH_RPC})
endif ()

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
### ******************************* grpc *******************************


# include header files
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)


# lib files
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/libs)


set(SRC_FILES
        client/benchmarks/ycsb_query.cpp
        client/benchmarks/query.cpp
        client/txn/ycsb_txn.cpp
        client/txn/txn.cpp
        client/manager_client.cpp
        client/thread.cpp
        common/global.cpp
        common/myhelper.cpp
        common/row_item.cpp
        common/txn_row_man.cpp
        common/parser.cpp
        common/mystats.cpp
        common/buffer/lru_index.cpp
        common/buffer/lru.cpp
        common/buffer/buffer.cpp
        server/concurrency_control/row_mvcc.cpp
        common/storage/catalog.cpp
        common/storage/table.cpp
#        server/workload/tpcc_wl.cpp
        common/workload/ycsb_wl.cpp
        common/workload/wl.cpp
        server/manager_server.bak.cpp
        util/arena.cpp
        config.cpp
        )
if (${WITH_RPC})
    set(SRC_FILES
        ${SRC_FILES}
        api/api_cc/api_cc.cpp
        api/api_txn/api_txn.cpp
        api/proto/api.grpc.pb.cc
        api/proto/api.pb.cc
        api/proto/dbx1000_service_util.cpp)
else()
    set(SRC_FILES
        ${SRC_FILES}
        api/api_single_machine/cs_api.cpp)
endif ()
#add_executable(rundb system/main.cpp ${SRC_FILES})
#target_link_libraries(rundb libleveldb.a libsnappy.a libjemalloc.so libpthread.so)

if(${WITH_RPC})
    add_executable(main_test_cc test/main_test_cc.cpp ${SRC_FILES} server/manager_server.cpp server/manager_server.h test/other_test.cpp)
    add_executable(main_test_txn test/main_test_txn.cpp ${SRC_FILES})
    target_link_libraries(main_test_cc libleveldb.a libsnappy.a ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY} libjsoncpp.a pthread)
    target_link_libraries(main_test_txn libleveldb.a libsnappy.a ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY} libjsoncpp.a pthread)
else()
    add_executable(main_test test/main_test.cpp ${SRC_FILES})
    target_link_libraries(main_test libleveldb.a libsnappy.a pthread)
endif()

### ******************************* test query *******************************
add_executable(test_query client/benchmarks/test_query.cpp
        client/benchmarks/query.cpp
        client/benchmarks/ycsb_query.cpp
        common/global.cpp
        util/arena.cpp)
target_link_libraries(test_query pthread)

### ******************************* test buffer *******************************
#add_executable(test_buffer test/test_buffer.cpp
#        common/buffer/buffer.cpp common/buffer/lru_index.cpp common/buffer/lru.cpp common/global.cpp)
#target_link_libraries(test_buffer libleveldb.a libsnappy.a libpthread.so)
#add_executable(test_lru test/test_lru.cpp common/buffer/lru.cpp common/buffer/lru_index.cpp common/global.cpp)

### ******************************* other test *******************************
add_executable(other_test test/other_test.cpp)