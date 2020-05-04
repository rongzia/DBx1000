#cmake_minimum_required(VERSION 3.10)
#
#set(PROTOBUF_ROOT $ENV{HOME}/.local/protobuf-3.6.1)
#include(cmake/protobuf.cmake)
#set(GRPC_ROOT $ENV{HOME}/.local/grpc-v1.23.x)
#include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/grpc.cmake)
#
#execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
#        -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
#        --cpp_out=${CMAKE_CURRENT_SOURCE_DIR}/proto
#        ${CMAKE_CURRENT_SOURCE_DIR}/proto/api.proto)
#execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
#        -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
#        --grpc_out=${CMAKE_CURRENT_SOURCE_DIR}/proto
#        #            --plugin=protoc-gen-grpc=$ENV{HOME}/.local/grpc-v1.23.x/bin/grpc_cpp_plugin
#        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
#        ${CMAKE_CURRENT_SOURCE_DIR}/proto/api.proto)
#
#include_directories(${CMAKE_CURRENT_SOURCE_DIR}/proto)
#include_directories(${CMAKE_CURRENT_SOURCE_DIR}/api_cc)
#include_directories(${CMAKE_CURRENT_SOURCE_DIR}/api_txn)
#include_directories(${gRPC_INCLUDE_DIR})