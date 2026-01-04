# DKV

基于现代C++17实现的高性能键值（Key-Value）存储系统

### Command Support

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
| 脚本执行     | EVALX 采用自设计的脚本语言，自实现编译到字节码和VM（见[dkv_script](https://github.com/hycinth22/dkv_script)）    |


**C++17**：利用智能指针、移动语义、原子操作、线程安全等现代C++特性。类型安全的枚举和强类型。模板元编程。

**持久化**：支持RDB快照、AOF持久化写入与恢复。支持自动AOF重写。

**高并发处理**：基于多线程Reactor模型、事件驱动的非阻塞I/O（epoll），支持高并发连接处理。使用线程池并发处理Command。

**TTL支持**：支持为key设置过期时间; 后台根据过期时间自动清理过期键值对

**内存限制与淘汰**：支持内存配额限制，8种淘汰策略NOEVICTION、{VOLATILE/ALLKEYS}_{LRU/LFU/RANDOM}、VOLATILE_TTL。

**事务支持**：支持MULTI、EXEC、DISCARD等事务命令，支持四种事务隔离级别

**主从复制**：基于RAFT协议

## Build

### 构建要求
- CMake 3.10或更高版本
- 支持C++17的编译器（GCC 7+、Clang 5+）
- 线程库（操作系统安装）
- Google Benchmark库（可选，如果需要性能测试）

```bash
# 进入项目目录
cd dkv

# 创建构建目录
mkdir build && cd build

# 配置和构建
cmake ..
make

# 默认端口启动
./bin/dkv_server

# 指定端口启动
./bin/dkv_server -p 6380

# 使用配置文件启动
./bin/dkv_server -c ../config.conf

# 查看帮助
./bin/dkv_server --help

# 运行所有测试
make test
# 也可以使用CMake test driver
ctest -v
ctest -R test_basic
# 或者单独运行测试
./bin/test_basic
./bin/test_datatype_string
./bin/test_datatype_hyperloglog
./bin/test_server_management
./bin/test_memory_allocator
./bin/test_maxmemory
./bin/test_rdb
./bin/test_aof
# 使用Python客户端测试
python3 tests/test_client.py

# 项目定义了几个自定义的CMake目标，方便开发和测试：
# 构建Debug版本
make debug

# 构建Release版本
make release

# 清理所有构建文件
make clean-all
```

## 配置文件示例

```conf
port 6379
maxmemory 1073741824  # 1GB
threads 4

# RDB持久化
enable_rdb yes
rdb_filename dump.rdb
rdb_save_interval 3600  # 1小时
rdb_save_changes 1000   # 1000次变更

# AOF持久化
enable_aof yes
aof_filename appendonly.aof
aof_fsync_policy 1  # 0=never, 1=everysec, 2=always
auto_aof_rewrite_percentage 100
auto_aof_rewrite_min_size 64mb

# 事务配置
transaction_isolation_level read_committed # read_uncommitted, read_committed, repeatable_read, serializable
```

## License

[MIT License](https://opensource.org/licenses/MIT)

## Contributing

有任何想法或建议，欢迎提交Issue或Pull Request
