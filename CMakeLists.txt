cmake_minimum_required(VERSION 3.10)
project(DKV VERSION 0.1.0 LANGUAGES CXX)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 设置编译选项
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
endif()

# 设置输出目录
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# 包含头文件目录
include_directories(include)

# 查找线程库
find_package(Threads REQUIRED)

# 收集源文件
file(GLOB_RECURSE DKV_SOURCES "src/*.cpp")

# 创建DKV库
add_library(dkv_lib STATIC ${DKV_SOURCES})
target_link_libraries(dkv_lib Threads::Threads)

# 创建主程序
add_executable(dkv_server src/dkv_main.cpp)
target_link_libraries(dkv_server dkv_lib)

# 创建测试程序
add_executable(test_basic tests/test_basic.cpp)
target_link_libraries(test_basic dkv_lib)

add_executable(test_datatype_string tests/test_datatype_string.cpp)
target_link_libraries(test_datatype_string dkv_lib)

add_executable(test_datatype_hash tests/test_datatype_hash.cpp)
target_link_libraries(test_datatype_hash dkv_lib)

add_executable(test_datatype_list tests/test_datatype_list.cpp)
target_link_libraries(test_datatype_list dkv_lib)

add_executable(test_datatype_set tests/test_datatype_set.cpp)
target_link_libraries(test_datatype_set dkv_lib)

add_executable(test_datatype_zset tests/test_datatype_zset.cpp)
target_link_libraries(test_datatype_zset dkv_lib)

add_executable(test_datatype_bitmap tests/test_datatype_bitmap.cpp)
target_link_libraries(test_datatype_bitmap dkv_lib)

add_executable(test_server_management tests/test_server_management.cpp)
target_link_libraries(test_server_management dkv_lib)

add_executable(test_memory_allocator tests/test_memory_allocator.cpp)
target_link_libraries(test_memory_allocator dkv_lib)

add_executable(test_maxmemory tests/test_maxmemory.cpp)
target_link_libraries(test_maxmemory dkv_lib)

add_executable(test_rdb tests/test_rdb.cpp)
target_link_libraries(test_rdb dkv_lib)


# 启用测试
enable_testing()
add_test(NAME basic_tests COMMAND test_basic)
add_test(NAME datatype_string_tests COMMAND test_datatype_string)
add_test(NAME datatype_hash_tests COMMAND test_datatype_hash)
add_test(NAME datatype_list_tests COMMAND test_datatype_list)
add_test(NAME datatype_set_tests COMMAND test_datatype_set)
add_test(NAME datatype_zset_tests COMMAND test_datatype_zset)
add_test(NAME datatype_bitmap_tests COMMAND test_datatype_bitmap)
add_test(NAME server_management_tests COMMAND test_server_management)
add_test(NAME memory_allocator_tests COMMAND test_memory_allocator)
add_test(NAME maxmemory_tests COMMAND test_maxmemory)
add_test(NAME rdb_tests COMMAND test_rdb)


# 安装规则
install(TARGETS dkv_server DESTINATION bin)
install(TARGETS dkv_lib DESTINATION lib)
install(DIRECTORY include/ DESTINATION include)
