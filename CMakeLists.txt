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
    
    # 添加AddressSanitizer支持
    option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
    if(ENABLE_ASAN)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address")
    endif()
endif()

# 设置输出目录
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Script VM库路径配置
set(SCRIPT_VM_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/include/scriptvm")
set(SCRIPT_VM_LIB_DIR "${CMAKE_SOURCE_DIR}/lib")

# 根据构建类型选择Script VM库
if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "")
    set(SCRIPT_VM_LIB "${SCRIPT_VM_LIB_DIR}/debug/libdkv_script.so")
else()
    set(SCRIPT_VM_LIB "${SCRIPT_VM_LIB_DIR}/release/libdkv_script.so")
endif()

# 查找线程库
find_package(Threads REQUIRED)

# 查找Google Benchmark库
find_package(benchmark)

# 创建DKV库
file(GLOB_RECURSE DKV_SOURCES "src/*.cpp")
include_directories(include)
include_directories(${SCRIPT_VM_INCLUDE_DIR})
add_library(dkv_lib STATIC ${DKV_SOURCES})
target_link_libraries(dkv_lib Threads::Threads ${SCRIPT_VM_LIB})

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

add_executable(test_datatype_hyperloglog tests/test_datatype_hyperloglog.cpp)
target_link_libraries(test_datatype_hyperloglog dkv_lib)

add_executable(test_server_management tests/test_server_management.cpp)
target_link_libraries(test_server_management dkv_lib)

add_executable(test_memory_allocator tests/test_memory_allocator.cpp)
target_link_libraries(test_memory_allocator dkv_lib)

add_executable(test_maxmemory tests/test_maxmemory.cpp)
target_link_libraries(test_maxmemory dkv_lib)

add_executable(test_rdb tests/test_rdb.cpp)
target_link_libraries(test_rdb dkv_lib)

add_executable(test_aof tests/test_aof.cpp)
target_link_libraries(test_aof dkv_lib)

add_executable(test_concurrency tests/test_concurrency.cpp)
target_link_libraries(test_concurrency dkv_lib)

add_executable(test_mvcc tests/test_mvcc.cpp)
target_link_libraries(test_mvcc dkv_lib)

add_executable(test_raft tests/test_raft.cpp)
target_link_libraries(test_raft dkv_lib)

add_executable(test_raft_statemachine tests/test_raft_statemachine.cpp)
target_link_libraries(test_raft_statemachine dkv_lib)

add_executable(test_script tests/test_script.cpp)
target_link_libraries(test_script dkv_lib)

# 启用测试
enable_testing()
add_test(NAME basic_tests COMMAND test_basic)
add_test(NAME datatype_string_tests COMMAND test_datatype_string)
add_test(NAME datatype_hash_tests COMMAND test_datatype_hash)
add_test(NAME datatype_list_tests COMMAND test_datatype_list)
add_test(NAME datatype_set_tests COMMAND test_datatype_set)
add_test(NAME datatype_zset_tests COMMAND test_datatype_zset)
add_test(NAME datatype_bitmap_tests COMMAND test_datatype_bitmap)
add_test(NAME datatype_hyperloglog_tests COMMAND test_datatype_hyperloglog)
add_test(NAME server_management_tests COMMAND test_server_management)
add_test(NAME memory_allocator_tests COMMAND test_memory_allocator)
# add_test(NAME maxmemory_tests COMMAND test_maxmemory)
add_test(NAME rdb_tests COMMAND test_rdb)
add_test(NAME aof_tests COMMAND test_aof)
add_test(NAME concurrency_tests COMMAND test_concurrency)
add_test(NAME mvcc_tests COMMAND test_mvcc)
add_test(NAME raft_tests COMMAND test_raft)
add_test(NAME raft_statemachine_tests COMMAND test_raft_statemachine)
add_test(NAME script_tests COMMAND test_script)

# benchmark tests
if(benchmark_FOUND)
    add_executable(benchmark_hello tests/benchmark_hello.cpp)
    target_link_libraries(benchmark_hello dkv_lib pthread)
    target_link_libraries(benchmark_hello benchmark::benchmark)
    add_test(NAME benchmark_hello COMMAND benchmark_hello)
endif()

# 安装规则
install(TARGETS dkv_server DESTINATION bin)
install(TARGETS dkv_lib DESTINATION lib)
install(DIRECTORY include/ DESTINATION include)

add_custom_target(debug
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/debug
    COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Debug -S ${CMAKE_SOURCE_DIR} -B ${CMAKE_BINARY_DIR}/debug
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/debug
    COMMENT "Building Debug version in build/debug directory"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_custom_target(release
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/release
    COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release -S ${CMAKE_SOURCE_DIR} -B ${CMAKE_BINARY_DIR}/release
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/release
    COMMENT "Building Release version in build/release directory"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_custom_target(clean-all
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}
    COMMENT "Removing all build directories"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)
