cmake_minimum_required(VERSION 3.14)
project(Dbx1000)

SET (CMAKE_C_COMPILER "gcc")
SET (CMAKE_CXX_COMPILER "g++")
SET (CMAKE_CXX_FLAGS "-std=c++17 -Wno-deprecated-declarations -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)

add_definitions(-DNOGRAPHITE=1)

include(${PROJECT_SOURCE_DIR}/rdb/brpc.cmake)

# include header files
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/benchmarks/ ${PROJECT_SOURCE_DIR}/concurrency_control/ ${PROJECT_SOURCE_DIR}/storage/ ${PROJECT_SOURCE_DIR}/system/)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/rr/include ${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/rdb)
# lib files
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/libs)

include(${PROJECT_SOURCE_DIR}/rdb/lock_service/lock_service.cmake)
# file(GLOB_RECURSE SRC_FILES benchmarks/*.cpp concurrency_control/*.cpp storage/*.cpp system/*.cpp config.cpp)
file(GLOB SRC_FILES benchmarks/*.cpp concurrency_control/*.cpp storage/*.cpp system/*.cpp config.cpp
rr/include/rr/*.cpp rdb/*.cpp rdb/lock_service/lock_service.pb.cc rdb/instance_lock_manager/*.cpp)
add_executable(rundb ${SRC_FILES})
target_link_libraries(rundb libjemalloc.so libpthread.so tbb ${BRPC_LIB} ${DYNAMIC_LIB})

# lock_server
file(GLOB SERVER_SRC benchmarks/*.cpp concurrency_control/*.cpp storage/*.cpp
system/global.cpp system/helper.cpp system/manager.cpp system/mem_alloc.cpp system/parser.cpp system/query.cpp system/stats.cpp  system/thread.cpp system/txn.cpp system/wl.cpp
config.cpp rr/include/rr/*.cpp rdb/*.cpp rdb/lock_service/lock_service.pb.cc rdb/lock_service/lock_service_impl.cpp rdb/lock_server/*.cpp rdb/instance_lock_manager/*.cpp)
add_executable(lock_server ${SERVER_SRC})
target_link_libraries(lock_server libpthread.so tbb ${BRPC_LIB} ${DYNAMIC_LIB})



# test
include(${PROJECT_SOURCE_DIR}/rdb/test_async_echo/async_echo.cmake)
add_executable(async_echo_client ${CMAKE_SOURCE_DIR}/rdb/test_async_echo/client.cpp ${async_echo_srcs})
add_executable(async_echo_server ${CMAKE_SOURCE_DIR}/rdb/test_async_echo/server.cpp ${async_echo_srcs})
target_link_libraries(async_echo_client ${BRPC_LIB} ${DYNAMIC_LIB})
target_link_libraries(async_echo_server ${BRPC_LIB} ${DYNAMIC_LIB})