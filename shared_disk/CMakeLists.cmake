set(sds_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/shared_disk_service.pb.cc")
set(sds_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/shared_disk_service.pb.h")
set(sds_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/shared_disk_service.grpc.pb.cc")
set(sds_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/shared_disk_service.grpc.pb.h")

### 仅当依赖这些文件时，该命令才会执行，否则会抱没有这些文件的错误
add_custom_command(
      OUTPUT "${sds_proto_srcs}" "${sds_proto_hdrs}" "${sds_grpc_srcs}" "${sds_grpc_hdrs}"
      COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${CMAKE_SOURCE_DIR}/shared_disk/proto"
        --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${CMAKE_SOURCE_DIR}/shared_disk/proto/shared_disk_service.proto"
      DEPENDS "${CMAKE_SOURCE_DIR}/shared_disk/proto/shared_disk_service.proto")

#execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} 
#        -I ${CMAKE_SOURCE_DIR}/shared_disk/proto
#        --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
#        ${CMAKE_SOURCE_DIR}/shared_disk/proto/shared_disk_service.proto)
#execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
#        -I ${CMAKE_SOURCE_DIR}/shared_disk/proto
#        --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
#        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
#        ${CMAKE_SOURCE_DIR}/shared_disk/proto/shared_disk_service.proto)

include_directories(${PROJECT_SOURCE_DIR}/shared_disk)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(SHARED_DISK_FILES
        ${sds_grpc_srcs}
        ${sds_proto_srcs}
        shared_disk/shared_disk_service.cpp)

add_library(shared_disk SHARED ${SHARED_DISK_FILES})
target_link_libraries(shared_disk ${GRPC_STATIC_LIBRARY} ${PROTOBUF_LIBRARY} ${ZLIB_LIBRARY})