cmake_minimum_required(VERSION 2.8)
project(Dbx1000)

SET (CMAKE_C_COMPILER "gcc")
SET (CMAKE_CXX_COMPILER "g++")
SET (CMAKE_CXX_FLAGS "-g3 -std=c++11 -Wno-deprecated-declarations" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)

add_definitions(-DNOGRAPHITE=1)


### ******************************* grpc *******************************:
#add_definitions(-DWITH_RPC)
#option(WITH_RPC "if use rpc then on, else off?" ON)
if (WITH_RPC)
    message("WITH_RPC : " ${WITH_RPC})
endif ()

if (${WITH_RPC})
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
endif()
### ******************************* grpc *******************************


# include header files
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)


# lib files
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/libs)


set(SRC_FILES
        common/buffer/buffer.cpp
        common/buffer/lru.cpp
        common/buffer/lru_index.cpp
        common/buffer/test_buffer.cpp
        common/index/index.cpp
        common/index/test_index.cpp
        common/lock_table/lock_table.cpp
        common/lock_table/test_lock_table.cpp
        common/storage/disk/file_io.cpp
        common/storage/disk/memory_io.cpp
        common/storage/tablespace/page.cpp
        common/storage/tablespace/row_item.cpp
        common/storage/tablespace/tablespace.cpp
        common/storage/tablespace/test_page.cpp
        common/storage/tablespace/test_tablespace.cpp
        common/storage/catalog.cpp
        common/storage/table.cpp
        common/workload/ycsb_wl.cpp
        common/workload/wl.cpp
        common/global.cpp
        common/myhelper.cpp
        common/mystats.cpp
        common/parser.cpp
        common/parser_host.cpp
        instance/benchmarks/ycsb_query.cpp
        instance/benchmarks/query.cpp
        instance/concurrency_control/row_mvcc.cpp
        instance/txn/ycsb_txn.cpp
        instance/txn/txn.cpp
        instance/manager_client.cpp
        instance/thread.cpp
#        buffer_server/manager_server.cpp
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
#    set(SRC_FILES
#        ${SRC_FILES}
#        api/api_single_machine/cs_api.cpp)
endif ()

#add_executable(rundb system/main.cpp ${SRC_FILES})
#target_link_libraries(rundb libleveldb.a libsnappy.a libjemalloc.so libpthread.so)

if(${WITH_RPC})
    add_executable(main_test_cc test/main_test_cc.cpp ${SRC_FILES})
    add_executable(main_test_txn test/main_test_instance.cpp ${SRC_FILES})
    target_link_libraries(main_test_cc libleveldb.a libsnappy.a ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY} libjsoncpp.a pthread)
    target_link_libraries(main_test_txn libleveldb.a libsnappy.a ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY} libjsoncpp.a pthread)
else()
    add_executable(main_test test/main_test.cpp ${SRC_FILES})
    target_link_libraries(main_test libleveldb.a libsnappy.a pthread)
endif()

### ******************************* other test *******************************
set(SERVER_FILE
        common/buffer/buffer.cpp
        common/buffer/lru.cpp
        common/buffer/lru_index.cpp
        common/buffer/test_buffer.cpp
        common/index/index.cpp
        common/index/test_index.cpp
        common/lock_table/lock_table.cpp
        common/lock_table/test_lock_table.cpp
        common/storage/disk/file_io.cpp
        common/storage/disk/memory_io.cpp
        common/storage/tablespace/page.cpp
        common/storage/tablespace/tablespace.cpp
        common/storage/tablespace/test_page.cpp
        common/storage/tablespace/test_tablespace.cpp
        common/storage/catalog.cpp
        common/storage/table.cpp
        common/workload/wl.cpp
        common/workload/ycsb_wl.cpp
        common/global.cpp
        common/myhelper.cpp
        common/mystats.cpp
        common/parser.cpp
#        common/row_item.cpp
#        common/txn_row_man.cpp
#        server/concurrency_control/row_mvcc.cpp
#        server/manager_server.bak.cpp
        buffer_server/manager_server.cpp
        util/arena.cpp
        config.cpp
        )
add_executable(other_test test/other_test.cpp)
add_executable(test_server test/test_server.cpp ${SERVER_FILE})
target_link_libraries(test_server libjsoncpp.a pthread)

add_executable(parser_host common/parser_host.cpp)
target_link_libraries(parser_host libjsoncpp.a)

add_executable(gen_db test/gen_db.cpp ${SRC_FILES})
target_link_libraries(gen_db libjsoncpp.a pthread)

add_executable(main_test_instance test/main_test_instance.cpp test/gen_db.cpp ${SRC_FILES})
target_link_libraries(main_test_instance libjsoncpp.a pthread)