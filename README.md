# DKV

DKV是一个基于现代C++17实现的高性能键值（Key-Value）存储系统，具备数据持久化支持和高并发处理能力。

**现代C++17实现**：利用智能指针、移动语义、原子操作、线程安全等现代C++特性。类型安全的枚举和强类型。模板元编程。

**持久化**：支持RDB快照、AOF持久化写入与恢复。支持自动AOF重写。

**高并发处理**：基于多线程Reactor模型、事件驱动的非阻塞I/O（epoll），支持高并发连接处理。使用线程池并发处理Command。

**线程安全**：使用读写锁保护数据访问，支持多客户端并发连接

**TTL支持**：支持为key设置过期时间; 后台根据过期时间自动清理过期键值对

**内存限制与淘汰**：支持内存配额限制，支持8种淘汰策略NOEVICTION、{VOLATILE/ALLKEYS}_{LRU/LFU/RANDOM}、VOLATILE_TTL。

**事务支持**：支持MULTI、EXEC、DISCARD等事务命令，支持四种事务隔离级别

### 命令支持

| 类型        | 支持的命令 |
|-------------|-------------------------------------------------------|
| 通用         | EXISTS、EXPIRE、TTL、DEL                              |
| String      | GET、SET、INCR、DECR                                   |
| Hash        | HGET/HGETALL、HSET、HDEL、HEXIST、HKEYS/HVALS、HLEN   |
| List        | LPUSH/RPUSH、LPOP/RPOP、LLEN、LRANGE                  |
| Set         | SADD、SREM、SMEMBERS、SISMEMBER、SCARD                 |
| ZSet        | ZADD、ZREM、ZSCORE、ZRANK/ZREVRANK、ZRANGE/ZREVRANGE、 |
| Bitmap      | SETBIT、GETBIT、BITCOUNT、BITOP（AND、OR、XOR、NOT）    |
| HyperLogLog | PFADD、PFCOUNT、PFMERGE                                 |
| 服务器管理   | INFO、DBSIZE、FLUSH、SHUTDOWN、SAVE/BGSAVE、             |
| 事务        | MULTI、EXEC、DISCARD                                    |

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
- 支持C++17的编译器（GCC 7+、Clang 5+）
- 线程库（操作系统安装）

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
auto_aof_rewrite_percentage 100
auto_aof_rewrite_min_size 64mb

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

## 测试

测试代码位于tests目录下

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
./bin/test_datatype_hyperloglog
./bin/test_server_management
./bin/test_memory_allocator
./bin/test_maxmemory
./bin/test_rdb

# 也可以使用CMake test driver
ctest -v
ctest -R test_basic

# 使用Python客户端测试
python3 tests/test_client.py
```

## 未来规划

1. 实现分布式支持（主从复制、一致性哈希分片）
2. 添加更多高级命令和功能
3. 优化性能和内存使用
4. 增强安全性和稳定性

## License

[MIT License](https://opensource.org/licenses/MIT)

## Contributing

有任何想法或建议，欢迎提交Issue或Pull Request
