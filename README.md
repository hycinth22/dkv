# DKV: 基于现代C++17的高性能键值存储系统

## 项目概述

DKV是一个基于现代C++17实现的高性能键值（Key-Value）存储系统，具备网络访问能力、数据持久化支持和高并发处理能力。当前版本(V0.1.0)实现了单机内存版的基本功能，支持多种数据类型，兼容Redis RESP协议。

## 主要功能特性

### 核心特性
- **现代C++17实现**：利用智能指针、原子操作、线程安全等现代C++特性
- **内存存储引擎**：基于哈希表的高效内存存储实现
- **多数据类型支持**：
  - String（字符串）：完整支持
  - Hash（哈希）
  - List（列表）
  - Set（集合）
  - ZSet（有序集合）
  - Bitmap（位图）
  - HyperLogLog：支持PFADD、PFCOUNT、PFMERGE等命令
- **持久化机制**：支持RDB快照持久化
- **高并发处理**：基于事件驱动的非阻塞I/O（epoll）
- **线程安全**：使用读写锁保护数据访问，支持多客户端并发连接
- **RESP协议兼容**：完全兼容Redis RESP协议，支持标准Redis客户端连接

### 命令支持
- **字符串操作**：SET、GET、DEL、EXISTS
- **数值操作**：INCR、DECR
- **过期时间**：EXPIRE、TTL（部分实现）
- **Hash操作**：HSET、HGET、HDEL、HGETALL等
- **List操作**：LPUSH、LPOP、RPUSH、RPOP等
- **Set操作**：SADD、SPOP、SMEMBERS等
- **ZSet操作**：ZADD、ZRANGE、ZSCORE等
- **Bitmap操作**：SETBIT、GETBIT、BITCOUNT等
- **HyperLogLog操作**：PFADD、PFCOUNT、PFMERGE
- **服务器管理**：支持命令行参数和配置文件

## 项目结构

```
dkv/
├── include/            # 头文件目录
│   ├── dkv_core.hpp           # 核心头文件，定义基本类型和接口
│   ├── dkv_datatypes.hpp      # 数据类型定义
│   ├── dkv_datatype_string.hpp # String数据类型定义
│   ├── dkv_datatype_hyperloglog.hpp # HyperLogLog数据类型定义
│   ├── dkv_network.hpp        # 网络通信模块
│   ├── dkv_server.hpp         # 服务器定义
│   ├── dkv_storage.hpp        # 存储引擎定义
│   └── dkv_resp.hpp           # RESP协议实现
├── src/                # 源代码目录
│   ├── dkv_core.cpp           # 核心功能实现
│   ├── dkv_datatype_string.cpp # String数据类型实现
│   ├── dkv_datatype_hyperloglog.cpp # HyperLogLog数据类型实现
│   ├── dkv_network.cpp        # 网络通信实现
│   ├── dkv_server.cpp         # 服务器实现
│   ├── dkv_storage.cpp        # 存储引擎实现
│   ├── dkv_resp.cpp           # RESP协议实现
│   └── dkv_main.cpp           # 主程序入口
├── tests/              # 测试代码
│   ├── test_basic.cpp         # 基本功能测试
│   ├── test_datatype_string.cpp # String数据类型测试
│   ├── test_datatype_hyperloglog.cpp # HyperLogLog数据类型测试
│   ├── test_runner.hpp        # 测试框架
│   └── test_client.py         # Python测试客户端
├── docs/               # 项目文档
│   ├── README.md              # 详细文档
│   └── V0.1_SUMMARY.md        # V0.1版本总结
├── build/              # 构建目录（编译时自动生成）
├── CMakeLists.txt      # CMake构建配置
└── config.conf         # 配置文件示例
```

## 构建方法

### 构建要求
- CMake 3.10或更高版本
- 支持C++17的编译器（GCC 7+、Clang 5+、MSVC 2019+）
- Git
- 线程库（由操作系统提供）

```bash
# 克隆仓库（示例命令）
# git clone <repository-url>

# 进入项目目录
cd dkv

# 创建构建目录
mkdir build && cd build

# 配置和构建
cmake ..
make

# 运行所有测试
make test

# 默认端口启动
./bin/dkv_server

# 指定端口启动
./bin/dkv_server -p 6380

# 使用配置文件启动
./bin/dkv_server -c ../config.conf

# 查看帮助
./bin/dkv_server --help
```

### 自定义构建目标

项目提供了几个自定义的CMake目标，方便开发和测试：

```bash
# 构建Debug版本
make debug

# 构建Release版本
make release

# 清理所有构建文件
make clean-all
```

### Windows系统

~~在Windows系统上，可以使用Visual Studio进行构建：~~

**注意：当前版本在Windows上还无法正常构建和运行，主要由于：**
1. 网络模块依赖Linux特有的epoll机制，Windows系统需要使用替代方案（如IOCP），目前暂未实现
2. 部分编译器builtin指令的兼容性问题，考虑切换为编译器无关实现，目前暂未处理

```powershell
# 创建构建目录
mkdir build
cd build

# 使用CMake配置（添加UTF-8编码支持和异常处理语义）
cmake .. -G "Visual Studio 17 2022" -A x64 -DCMAKE_CXX_FLAGS="/utf-8 /EHsc"

# 使用Visual Studio构建
cmake --build . --config Release

# 运行测试
Release\bin\test_basic.exe

# 运行测试
Release\bin\dkv_server.exe
```

## 配置选项

DKV支持通过配置文件进行详细配置，主要配置项包括：

```conf
# 基本配置
port 6380                  # 服务器端口
maxmemory 1073741824       # 最大内存限制（1GB）
threads 4                  # 工作线程数量

# RDB持久化配置
enable_rdb yes             # 启用RDB持久化
rdb_filename dump.rdb      # RDB文件名
rdb_save_interval 3600     # RDB保存间隔（秒）
rdb_save_changes 1000      # 触发RDB的变更次数

# AOF持久化配置
enable_aof yes             # 启用AOF持久化
aof_filename appendonly.aof # AOF文件名
aof_fsync_policy 1         # AOF刷盘策略（0=从不，1=每秒，2=每次写操作）

# 分布式配置（开发中）
#is_master no
#master_host 127.0.0.1
#master_port 6379
```

## 客户端连接

DKV兼容Redis RESP协议，可以使用标准Redis客户端进行连接：

```bash
# 使用redis-cli连接
redis-cli -p 6379

# 示例命令
SET mykey "Hello DKV"
GET mykey
DEL mykey
PFADD hll 1 2 3
PFCOUNT hll
```

## 测试用例

DKV提供了各种测试用例，包括：

1. **基本功能测试**：验证核心存储和命令执行功能
2. **数据类型测试**：针对String、Hash、List、Set、ZSet、Bitmap、HyperLogLog等各数据类型的专项测试
3. **服务器管理测试**：验证服务器配置、参数解析等功能
4. **内存管理测试**：验证内存分配器、最大内存限制等功能
5. **持久化测试**：验证RDB快照功能
3. **客户端测试**：通过Python客户端测试网络通信和协议兼容性


```bash
# 进入项目目录
cd dkv

# 创建构建目录
mkdir build && cd build

# 配置和构建
cmake ..
make

# 运行所有测试
make test

# 或者单独运行测试
./bin/test_basic
./bin/test_datatype_string
./bin/test_datatype_hash
./bin/test_datatype_list
./bin/test_datatype_set
./bin/test_datatype_zset
./bin/test_datatype_bitmap
./bin/test_datatype_hyperloglog
./bin/test_server_management
./bin/test_memory_allocator
./bin/test_maxmemory
./bin/test_rdb

# 使用Python客户端测试
python3 tests/test_client.py
```

## 技术特性

### 现代C++17特性
- 智能指针管理内存，避免内存泄漏
- 原子操作保证线程安全
- 移动语义优化性能
- 类型安全的枚举和强类型
- 模板元编程优化性能

### 高性能网络框架
- 基于Linux epoll的事件驱动模型
- 多线程Reactor模型，使用线程池
- 非阻塞I/O处理，支持高并发连接
- 自动客户端连接管理

### 线程安全设计
- 读写锁保护数据访问
- 原子操作管理状态
- 线程安全的命令执行
- 自动过期键清理

### 数据持久化
- RDB快照机制，支持定时或手动持久化
- 支持重启后数据恢复

## 当前版本限制

- V0.1.0版本主要实现单机内存存储功能
- AOF持久化功能正在开发中
- 分布式功能（主从复制、一致性哈希分片）尚未完全实现
- 部分高级命令和功能正在开发中

## 未来规划

1. 完善AOF持久化功能
2. 实现完整的分布式支持（主从复制、一致性哈希分片）
3. 添加更多高级命令和功能
4. 优化性能和内存使用
5. 增强安全性和稳定性
6. 提供更完善的文档和示例

## License

[MIT License](https://opensource.org/licenses/MIT)

## 贡献指南

欢迎对DKV项目进行贡献！如果您有任何想法或建议，请提交Issue或Pull Request。
