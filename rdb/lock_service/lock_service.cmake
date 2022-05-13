set(lock_service_srcs "${CMAKE_SOURCE_DIR}/rdb/lock_service/lock_service.pb.cc")
set(lock_service_hdrs "${CMAKE_SOURCE_DIR}/rdb/lock_service/lock_service.pb.h")


### 最保险的做法，该命令会立即被调用。
execute_process(COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        -I ${CMAKE_SOURCE_DIR}/rdb/lock_service
        --cpp_out=${CMAKE_SOURCE_DIR}/rdb/lock_service
        ${CMAKE_SOURCE_DIR}/rdb/lock_service/lock_service.proto)

include_directories(${PROJECT_SOURCE_DIR}/rdb/lock_service)
include_directories(${CMAKE_CURRENT_BINARY_DIR})


