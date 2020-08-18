set(ds_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/dbx1000_service.pb.cc")
set(ds_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/dbx1000_service.pb.h")
set(ds_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/dbx1000_service.grpc.pb.cc")
set(ds_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/dbx1000_service.grpc.pb.h")

### 仅当依赖这些文件时，该命令才会执行，否则会抱没有这些文件的错误
add_custom_command(
      OUTPUT "${ds_proto_srcs}" "${ds_proto_hdrs}" "${ds_grpc_srcs}" "${ds_grpc_hdrs}"
      COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${CMAKE_SOURCE_DIR}/rpc_handler/proto"
        --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${CMAKE_SOURCE_DIR}/rpc_handler/proto/dbx1000_service.proto"
      DEPENDS "${CMAKE_SOURCE_DIR}/rpc_handler/proto/dbx1000_service.proto")

#execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
#        -I ${CMAKE_SOURCE_DIR}/rpc_handler/proto
#        --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
#        ${CMAKE_SOURCE_DIR}/rpc_handler/proto/dbx1000_service.proto)
#execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
#        -I ${CMAKE_SOURCE_DIR}/rpc_handler/proto
#        --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
#        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
#        ${CMAKE_SOURCE_DIR}/rpc_handler/proto/dbx1000_service.proto)

include_directories(${PROJECT_SOURCE_DIR}/rpc_handler/grpc)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(RPC_HANDLER_FILES
        ${ds_grpc_srcs}
        ${ds_proto_srcs}
        rpc_handler/grpc/dbx1000_service_helper.cpp
        rpc_handler/grpc/instance_handler.cpp
        rpc_handler/grpc/lock_service_handler.cpp
        lock_service/manager_lock_service.cpp)

add_library(rpc_handler SHARED ${RPC_HANDLER_FILES})
target_link_libraries(rpc_handler ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY})