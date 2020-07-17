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
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/protobuf.cmake)
set(GRPC_ROOT $ENV{HOME}/.local/grpc-v1.23.x)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/grpc.cmake)
find_library(ZLIB_LIBRARY libz.so /usr/lib/x86_64-linux-gnu/)

#execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
#        -I ${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto
#        --cpp_out=${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto
#        ${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto/dbx1000_service.proto)
#execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
#        -I ${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto
#        --grpc_out=${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto
#        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
#        ${CMAKE_CURRENT_SOURCE_DIR}/rpc_handler/proto/dbx1000_service.proto)
#
#execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
#        -I ${CMAKE_CURRENT_SOURCE_DIR}/shared_disk/proto
#        --cpp_out=${CMAKE_CURRENT_SOURCE_DIR}/shared_disk/proto
#        ${CMAKE_CURRENT_SOURCE_DIR}/shared_disk/proto/shared_disk_service.proto)
#execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
#        -I ${CMAKE_CURRENT_SOURCE_DIR}/shared_disk/proto
#        --grpc_out=${CMAKE_CURRENT_SOURCE_DIR}/shared_disk/proto
#        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
#        ${CMAKE_CURRENT_SOURCE_DIR}/shared_disk/proto/shared_disk_service.proto)

include_directories(${Protobuf_INCLUDE_DIR})
include_directories(${gRPC_INCLUDE_DIR})
### ******************************* grpc *******************************

# include header files
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)

# lib files
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/libs)






set(COMMON_FILES
        common/index/index.cpp
        common/index/test_index.cpp
        common/lock_table/lock_table.cpp
        common/lock_table/test_lock_table.cpp
        common/storage/catalog.cpp
        common/storage/row_handler.cpp
        common/storage/table.cpp
        common/workload/ycsb_wl.cpp
        common/workload/wl.cpp
        common/global.cpp
        common/myhelper.cpp
        common/mystats.cpp
        common/parser.cpp
        common/parser_host.cpp
        util/arena.cpp
        util/parse_result.cpp
        config.cpp
        )
set(INSTANCE_FILES
        instance/benchmarks/ycsb_query.cpp
        instance/benchmarks/query.cpp
        instance/concurrency_control/row_mvcc.cpp
#        instance/concurrency_control/occ.cpp
#        instance/concurrency_control/row_occ.cpp
        instance/txn/ycsb_txn.cpp
        instance/txn/txn.cpp
        instance/manager_instance.cpp
        instance/thread.cpp
        )
set(RPC_HANDLER_FILES
        rpc_handler/proto/dbx1000_service.grpc.pb.cc
        rpc_handler/proto/dbx1000_service.pb.cc
        rpc_handler/dbx1000_service_helper.cpp
        rpc_handler/instance_handler.cpp
        rpc_handler/lock_service_handler.cpp)



add_library(disk SHARED common/storage/disk/file_io.cpp common/storage/disk/memory_io.cpp)
add_library(tablespace SHARED common/storage/tablespace/page.cpp common/storage/tablespace/row_item.cpp common/storage/tablespace/tablespace.cpp)
target_link_libraries(tablespace libjsoncpp.a)
add_library(shared_disk SHARED shared_disk/proto/shared_disk_service.grpc.pb.cc shared_disk/proto/shared_disk_service.pb.cc shared_disk/shared_disk_service.cpp)
target_link_libraries(shared_disk disk ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY})
file(GLOB buffer_src ${PROJECT_SOURCE_DIR}/common/buffer/*.cpp)
foreach(item ${buffer_src})
    if (${item} MATCHES "test*")
        list(REMOVE_ITEM buffer_src ${item})
    endif ()
endforeach()
add_library(buffer SHARED common/buffer/buffer.cpp common/buffer/lru_index.cpp common/buffer/lru.cpp)
target_link_libraries(buffer disk shared_disk tablespace)
add_library(lock_service lock_service/manager_lock_service.cpp ${COMMON_FILES})
target_link_libraries(lock_service buffer shared_disk)

# main_lock_service
add_executable(main_lock_service test/main_lock_service.cpp ${RPC_HANDLER_FILES} ${INSTANCE_FILES} ${COMMON_FILES})
target_link_libraries(main_lock_service lock_service buffer shared_disk tablespace libjsoncpp.a pthread tbb)
# main_instance
add_executable(main_instance test/main_instance.cpp ${RPC_HANDLER_FILES} ${INSTANCE_FILES} ${COMMON_FILES})
target_link_libraries(main_instance lock_service buffer shared_disk tablespace libjsoncpp.a pthread tbb)
# disk_service
add_executable(disk_service test/disk_service.cpp)
target_link_libraries(disk_service shared_disk disk pthread tbb)
# gen_db
add_executable(gen_db test/gen_db.cpp ${RPC_HANDLER_FILES} ${INSTANCE_FILES} ${COMMON_FILES})
target_link_libraries(gen_db lock_service buffer disk libjsoncpp.a pthread tbb)

### ******************************* other test *******************************
# for buffer
add_executable(test_buffer common/buffer/test_buffer.cpp)
add_executable(test_lru common/buffer/test_lru.cpp)
add_executable(test_lru_index common/buffer/test_lru_index.cpp)
target_link_libraries(test_buffer buffer pthread tbb)
target_link_libraries(test_lru buffer libjsoncpp.a pthread tbb)
target_link_libraries(test_lru_index buffer pthread tbb)

add_executable(parse_result_test test/parse_result_test.cpp util/parse_result.cpp)

add_executable(other_test test/other_test.cpp)
target_link_libraries(other_test pthread tbb)


