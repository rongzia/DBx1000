set(async_echo_srcs "${CMAKE_SOURCE_DIR}/rdb/test_async_echo/echo.pb.cc")
set(async_echo_hdrs "${CMAKE_SOURCE_DIR}/rdb/test_async_echo/echo.pb.h")


### 最保险的做法，该命令会立即被调用。
execute_process(COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        -I ${CMAKE_SOURCE_DIR}/rdb/test_async_echo
        --cpp_out=${CMAKE_SOURCE_DIR}/rdb/test_async_echo
        ${CMAKE_SOURCE_DIR}/rdb/test_async_echo/echo.proto)

include_directories(${PROJECT_SOURCE_DIR}/rdb/test_async_echo)
include_directories(${CMAKE_CURRENT_BINARY_DIR})



# add_library(shared_disk SHARED ${SHARED_DISK_FILES})
# target_link_libraries(shared_disk ${BRPC_LIB} ${DYNAMIC_LIB})

