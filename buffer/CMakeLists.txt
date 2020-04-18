#cmake_minimum_required(VERSION 2.8)
#project(buffer)
#
#SET (CMAKE_C_COMPILER "gcc")
#SET (CMAKE_CXX_COMPILER "g++")
#SET (CMAKE_CXX_FLAGS "-std=c++11 -Wno-deprecated-declarations -fno-rtti" CACHE INTERNAL "compiler options" FORCE)
#SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
#SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)
#
#add_definitions(-DNOGRAPHITE=1)

add_definitions(-DUSE_MEMORY_DB)
add_definitions(-Ddb_num_item_=1000000)
#add_definitions(-DWARMUP_LEVELDB)

add_executable(test_buffer
        test_buffer.cpp buffer.cpp lru_index.cpp lru.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../util/numbercomparator.cpp
        )
target_link_libraries(test_buffer libleveldb.a libsnappy.a libpthread.so)

add_executable(test_lru test_lru.cpp lru.cpp lru_index.cpp)