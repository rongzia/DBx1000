cmake_minimum_required(VERSION 2.8)
project(Dbx1000)

SET(CMAKE_C_COMPILER "gcc")
SET(CMAKE_CXX_COMPILER "g++")
SET(CMAKE_CXX_FLAGS "-g3 -std=c++11 -Wno-deprecated-declarations" CACHE INTERNAL "compiler options" FORCE)
SET(CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET(CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)

add_definitions(-DNOGRAPHITE=1)


### ******************************* grpc *******************************:
set(PROTOBUF_ROOT $ENV{HOME}/.local/protobuf-3.6.1)
include(${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/cmake/protobuf.cmake)
set(GRPC_ROOT $ENV{HOME}/.local/grpc-v1.23.x)
include(${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/cmake/grpc.cmake)
find_library(ZLIB_LIBRARY libz.so /usr/lib/x86_64-linux-gnu/)

execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
        -I ${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto
        --cpp_out=${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto
        ${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto/dbx1000_service.proto)
execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
        -I ${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto
        --grpc_out=${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto/dbx1000_service.proto)

include_directories(${Protobuf_INCLUDE_DIR})
include_directories(${gRPC_INCLUDE_DIR})
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
        util/arena.cpp
        config.cpp

        instance/benchmarks/ycsb_query.cpp
        instance/benchmarks/query.cpp
        instance/concurrency_control/row_mvcc.cpp
        instance/txn/ycsb_txn.cpp
        instance/txn/txn.cpp
        instance/manager_instance.cpp
        instance/thread.cpp

        buffer_manager/server_lock_table/server_lock_table.cpp
        buffer_manager/manager_server.cpp
        )
set(SRC_FILES
        ${SRC_FILES}
        rpc_handler/proto/dbx1000_service.grpc.pb.cc
        rpc_handler/proto/dbx1000_service.pb.cc
        rpc_handler/buffer_manager_handler.cpp
        rpc_handler/dbx1000_service_helper.cpp
        rpc_handler/instance_handler.cpp)

add_executable(main_test_bm test/main_test_bm.cpp ${SRC_FILES})
add_executable(main_test_instance test/main_test_instance.cpp ${SRC_FILES})
target_link_libraries(main_test_bm ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY} libjsoncpp.a pthread)
target_link_libraries(main_test_instance ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY} libjsoncpp.a pthread)

### ******************************* other test *******************************
add_executable(other_test test/other_test.cpp)

add_executable(parser_host common/parser_host.cpp)
target_link_libraries(parser_host libjsoncpp.a)

add_executable(gen_db test/gen_db.cpp ${SRC_FILES})
target_link_libraries(gen_db libjsoncpp.a pthread)