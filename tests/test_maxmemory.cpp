#include <cassert>
#include <thread>
#include <chrono>
#include "dkv_server.hpp"
#include "dkv_core.hpp"
#include "test_runner.hpp"

// 测试maxmemory功能
void testMaxMemory(dkv::TestRunner& runner) {
    std::cout << "开始测试maxmemory功能..." << std::endl;
    
    // 测试函数：创建服务器并设置maxmemory和淘汰策略
    auto createServerWithMaxMemory = [](int port, size_t max_memory, dkv::EvictionPolicy policy = dkv::EvictionPolicy::NOEVICTION) -> dkv::DKVServer* {
        dkv::DKVServer* server = new dkv::DKVServer(port);
        server->setMaxMemory(max_memory);
        server->setEvictionPolicy(policy);
        server->setAOFEnabled(false);
        server->setRDBEnabled(false);
        if (!server->start()) {
            std::cerr << "服务器启动失败" << std::endl;
            delete server;
            return nullptr;
        }
        return server;
    };
    
    // 测试函数：创建客户端连接
    auto createClientConnection = [](int port) -> int {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::cerr << "创建socket失败" << std::endl;
            return -1;
        }
        
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        
        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            std::cerr << "连接服务器失败" << std::endl;
            close(sock);
            return -1;
        }
        
        return sock;
    };
    
    // 测试maxmemory限制功能
    dkv::DKVServer* mem_test_server = createServerWithMaxMemory(6381, 50*1024);
    if (mem_test_server) {
        int mem_test_sock = createClientConnection(6381);
        if (mem_test_sock >= 0) {
            auto sendMemCommand = [mem_test_sock](const std::string& cmd) -> std::string {
                send(mem_test_sock, cmd.c_str(), cmd.length(), 0);
                char buffer[1024] = {0};
                int bytes_read = recv(mem_test_sock, buffer, sizeof(buffer) - 1, 0);
                return std::string(buffer, bytes_read);
            };
            
            // 先添加一些数据，使内存使用接近限制
            std::cout << "添加数据以接近内存限制..." << std::endl;
            int succ = 0;
            for (int i = 0; i < 1000000000; ++i) {
                std::string cmd = "SET key" + std::to_string(i) + " " + std::string(10, 'a' + i) + "\r\n";
                std::string response = sendMemCommand(cmd);
                if (response[0] != '-') {
                    succ++;
                } else if (response.find("OOM") != std::string::npos) {
                    break;
                }
            }
            std::cout << "成功添加 " << succ << " 个键值对" << std::endl;
            
            // 测试当内存使用超过maxmemory时SET命令应该失败
            runner.runTest("测试maxmemory限制功能", [&]() {
                std::string set_response = sendMemCommand("SET overflow_key large_value_to_test_memory_limit\r\n");
                std::cout << "超出内存限制时SET响应: " << set_response << std::endl;
                return set_response.find("-OOM command not allowed") != std::string::npos;
            });
            
            // 验证GET等只读命令仍然可以正常工作
            runner.runTest("验证只读命令在内存限制下仍可工作", [&]() {
                std::string get_response = sendMemCommand("GET key0\r\n");
                std::cout << "内存限制下GET响应: " << get_response << std::endl;
                return get_response.find("$10") != std::string::npos; // key0的值应该是10个字符
            });
            
            // 验证EXISTS命令在内存限制下仍可工作
            runner.runTest("验证EXISTS命令在内存限制下仍可工作", [&]() {
                std::string exists_response = sendMemCommand("EXISTS key0\r\n");
                std::cout << "内存限制下EXISTS响应: " << exists_response << std::endl;
                return exists_response.find("$1\r\n1") != std::string::npos;
            });
            
            // 验证INFO命令在内存限制下仍可工作并显示内存使用情况
            runner.runTest("验证INFO命令显示内存使用情况", [&]() {
                std::string info_response = sendMemCommand("INFO\r\n");
                std::cout << "INFO响应(部分): " << info_response.substr(0, 100) << "..." << std::endl;
                return info_response.find("used_memory") != std::string::npos && 
                       info_response.find("max_memory") != std::string::npos;
            });
            
            // 清除数据库，验证SET命令可以再次工作
            runner.runTest("验证清除数据后SET命令恢复正常", [&]() {
                sendMemCommand("FLUSHDB\r\n");
                std::string new_set_response = sendMemCommand("SET new_key new_value_after_flush\r\n");
                std::cout << "清除后SET响应: " << new_set_response << std::endl;
                return new_set_response.find("+OK") != std::string::npos;
            });
            
            close(mem_test_sock);
        }
        
        mem_test_server->stop();
        delete mem_test_server;
    }
    
    std::cout << "maxmemory功能测试完成！" << std::endl;
}

// 测试不同的内存淘汰策略
void testEvictionPolicies(dkv::TestRunner& runner) {
    std::cout << "开始测试内存淘汰策略..." << std::endl;
    
    // 测试函数：创建客户端连接
    auto createClientConnection = [](int port) -> int {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::cerr << "创建socket失败" << std::endl;
            return -1;
        }
        
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        
        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            std::cerr << "连接服务器失败" << std::endl;
            close(sock);
            return -1;
        }
        
        return sock;
    };

    // 测试NOEVICTION策略
    {   
        std::cout << "\n测试NOEVICTION策略..." << std::endl;
        dkv::DKVServer* server = new dkv::DKVServer(6382);
        server->setAOFEnabled(false);
        server->setRDBEnabled(false);
        server->setMaxMemory(100*1024);
        server->setEvictionPolicy(dkv::EvictionPolicy::NOEVICTION);
        if (server->start()) {
            int sock = createClientConnection(6382);
            if (sock >= 0) {
                auto sendCommand = [sock](const std::string& cmd) -> std::string {
                    send(sock, cmd.c_str(), cmd.length(), 0);
                    char buffer[1024] = {0};
                    int bytes_read = recv(sock, buffer, sizeof(buffer) - 1, 0);
                    return std::string(buffer, bytes_read);
                };
                
                // 添加数据直到内存限制
                int count = 0;
                for (int i = 0; i < 10000; ++i) {
                    std::string cmd = "SET key" + std::to_string(i) + " " + std::string(10, 'a' + i % 26) + "\r\n";
                    std::string response = sendCommand(cmd);
                    if (response[0] != '-') {
                        count++;
                    } else if (response.find("OOM") != std::string::npos) {
                        break;
                    }
                }
                
                runner.runTest("测试NOEVICTION策略在内存满时拒绝写操作", [&]() {
                    std::string response = sendCommand("SET newkey newvalue\r\n");
                    return response.find("-OOM command not allowed") != std::string::npos;
                });
                
                close(sock);
            }
            
            server->stop();
            delete server;
        }
    }
    
    // 测试VOLATILE_LRU策略
    {   
        std::cout << "\n测试VOLATILE_LRU策略..." << std::endl;
        dkv::DKVServer* server = new dkv::DKVServer(6383);
        server->setAOFEnabled(false);
        server->setRDBEnabled(false);
        server->setMaxMemory(20*1024);
        server->setEvictionPolicy(dkv::EvictionPolicy::VOLATILE_LRU);
        if (server->start()) {
            int sock = createClientConnection(6383);
            if (sock >= 0) {
                auto sendCommand = [sock](const std::string& cmd) -> std::string {
                    send(sock, cmd.c_str(), cmd.length(), 0);
                    char buffer[1024] = {0};
                    int bytes_read = recv(sock, buffer, sizeof(buffer) - 1, 0);
                    return std::string(buffer, bytes_read);
                };
                
                // 添加一些带过期时间的键
                for (int i = 0; i < 20; ++i) {
                    auto key = "volatile_key" + std::to_string(i);
                    std::string response = sendCommand("SET " + key + " value" + std::to_string(i) + "\r\n");
                    ASSERT_CONTAINS(response, "+OK");
                    response = sendCommand("EXPIRE " + key + " 60\r\n");
                    ASSERT_CONTAINS(response, "$1");
                }
                
                // 添加一些不带过期时间的键
                for (int i = 0; i < 20; ++i) {
                    std::string response = sendCommand("SET permanent_key" + std::to_string(i) + " value" + std::to_string(i) + "\r\n");
                    ASSERT_CONTAINS(response, "+OK");
                }
                
                // 读取一些带过期时间的键，使其成为LRU中的热点
                for (int i = 15; i < 20; ++i) {
                    std::string response = sendCommand("GET volatile_key" + std::to_string(i) + "\r\n");
                    ASSERT_CONTAINS(response, "value");
                }
                
                // 添加更多数据触发淘汰
                int new_keys_added = 0;
                for (int i = 0; i < 100; ++i) {
                    std::string response = sendCommand("SET new_key" + std::to_string(i) + " new_value" + std::to_string(i) + "\r\n");
                    if (response.find("+OK") != std::string::npos) {
                        new_keys_added++;
                    }
                }
                
                runner.runTest("测试VOLATILE_LRU策略会淘汰带过期时间的键", [&]() {
                    // 检查是否有旧的volatile键被淘汰，而新的volatile键和permanent键仍然存在
                    int missing_volatile = 0;
                    for (int i = 0; i < 15; ++i) { // 检查非热点键
                        std::string response = sendCommand("GET volatile_key" + std::to_string(i) + "\r\n");
                        if (response.find("$-1") != std::string::npos) {
                            missing_volatile++;
                        }
                    }
                    
                    int missing_permanent = 0;
                    for (int i = 0; i < 20; ++i) { // 检查permanent键
                        std::string response = sendCommand("GET permanent_key" + std::to_string(i) + "\r\n");
                        if (response.find("$-1") != std::string::npos) {
                            missing_permanent++;
                        }
                    }
                    assert(missing_volatile > 0);
                    assert(missing_permanent == 0);
                    assert(new_keys_added > 0);
                    return true;
                });
                
                close(sock);
            }
            
            server->stop();
            delete server;
        }
    }
    
    // 测试ALLKEYS_LRU策略
    {   
        std::cout << "\n测试ALLKEYS_LRU策略..." << std::endl;
        dkv::DKVServer* server = new dkv::DKVServer(6384);
        server->setAOFEnabled(false);
        server->setRDBEnabled(false);
        server->setMaxMemory(40*1024);
        server->setEvictionPolicy(dkv::EvictionPolicy::ALLKEYS_LRU);
        if (server->start()) {
            int sock = createClientConnection(6384);
            if (sock >= 0) {
                auto sendCommand = [sock](const std::string& cmd) -> std::string {
                    send(sock, cmd.c_str(), cmd.length(), 0);
                    char buffer[1024] = {0};
                    int bytes_read = recv(sock, buffer, sizeof(buffer) - 1, 0);
                    return std::string(buffer, bytes_read);
                };
                
                // 添加一些键
                for (int i = 0; i < 200; ++i) {
                    std::string response = sendCommand("SET key" + std::to_string(i) + " value" + std::to_string(i) + "\r\n");
                    ASSERT_CONTAINS(response, "+OK");
                }
                
                // 读取一些键，使其成为LRU中的热点
                for (int i = 80; i < 200; ++i) {
                    sendCommand("GET key" + std::to_string(i) + "\r\n");
                }
                
                std::cout<< "开始触发淘汰" << std::endl;
                // 添加更多数据触发淘汰
                int new_keys_added = 0;
                for (int i = 0; i < 70; ++i) {
                    std::string response = sendCommand("SET new_key" + std::to_string(i) + " new_value" + std::to_string(i) + "\r\n");
                    if (response.find("+OK") != std::string::npos) {
                        new_keys_added++;
                    }
                }
                
                runner.runTest("测试ALLKEYS_LRU策略会淘汰最近最少使用的键", [&]() {
                    // 检查是否有旧的非热点键被淘汰，而热点键仍然存在
                    int missing_old = 0;
                    for (int i = 0; i < 80; ++i) { // 检查非热点键
                        std::string response = sendCommand("GET key" + std::to_string(i) + "\r\n");
                        if (response.find("$-1") != std::string::npos) {
                            missing_old++;
                        }
                    }
                    
                    int missing_new = 0;
                    for (int i = 80; i < 150; ++i) { // 检查热点键
                        std::string response = sendCommand("GET key" + std::to_string(i) + "\r\n");
                        if (response.find("$-1") != std::string::npos) {
                            missing_new++;
                        }
                    }
                    std::cout << "missing_old: " << missing_old << ", missing_new: " << missing_new << std::endl;
                    ASSERT_GT(missing_old, missing_new);
                    assert(new_keys_added > 0);
                    return true;
                });
                
                close(sock);
            }
            
            server->stop();
            delete server;
        }
    }
    
    // 测试VOLATILE_TTL策略
    {   
        std::cout << "\n测试VOLATILE_TTL策略..." << std::endl;
        dkv::DKVServer* server = new dkv::DKVServer(6385);
        server->setAOFEnabled(false);
        server->setRDBEnabled(false);
        server->setMaxMemory(80*1024);
        server->setEvictionPolicy(dkv::EvictionPolicy::VOLATILE_TTL);
        if (server->start()) {
            int sock = createClientConnection(6385);
            if (sock >= 0) {
                auto sendCommand = [sock](const std::string& cmd) -> std::string {
                    send(sock, cmd.c_str(), cmd.length(), 0);
                    char buffer[1024] = {0};
                    int bytes_read = recv(sock, buffer, sizeof(buffer) - 1, 0);
                    return std::string(buffer, bytes_read);
                };
                
                // 添加带不同TTL的键
                for (int i = 0; i < 100; ++i) {
                    std::string response = sendCommand("SET short_ttl_key" + std::to_string(i) + " value" + std::to_string(i) + "\r\n");
                    ASSERT_CONTAINS(response, "+OK");
                    response = sendCommand("EXPIRE short_ttl_key" + std::to_string(i) + " 60\r\n"); // 短TTL
                    ASSERT_CONTAINS(response, "$1");
                }
                
                for (int i = 0; i < 100; ++i) {
                    std::string response = sendCommand("SET long_ttl_key" + std::to_string(i) + " value" + std::to_string(i) + "\r\n");
                    ASSERT_CONTAINS(response, "+OK");
                    response = sendCommand("EXPIRE long_ttl_key" + std::to_string(i) + " 3600\r\n"); // 长TTL
                    ASSERT_CONTAINS(response, "$1");
                }
                
                // 添加不带TTL的键
                for (int i = 0; i < 100; ++i) {
                    std::string response = sendCommand("SET no_ttl_key" + std::to_string(i) + " value" + std::to_string(i) + "\r\n");
                    ASSERT_CONTAINS(response, "+OK");
                }
                
                // 添加更多数据触发淘汰
                int new_keys_added = 0;
                for (int i = 0; i < 10; ++i) {
                    std::string response = sendCommand("SET new_key" + std::to_string(i) + " new_value" + std::to_string(i) + "\r\n");
                    if (response.find("+OK") != std::string::npos) {
                        new_keys_added++;
                    }
                }
                
                runner.runTest("测试VOLATILE_TTL策略会优先淘汰TTL较短的键", [&]() {
                    // 检查是否有短TTL键被淘汰更多，而长TTL键保留更多
                    int missing_short_ttl = 0;
                    for (int i = 0; i < 100; ++i) {
                        std::string response = sendCommand("GET short_ttl_key" + std::to_string(i) + "\r\n");
                        if (response.find("$-1") != std::string::npos) {
                            missing_short_ttl++;
                        }
                    }
                    
                    int missing_long_ttl = 0;
                    for (int i = 0; i < 100; ++i) {
                        std::string response = sendCommand("GET long_ttl_key" + std::to_string(i) + "\r\n");
                        if (response.find("$-1") != std::string::npos) {
                            missing_long_ttl++;
                        }
                    }
                    
                    int missing_no_ttl = 0;
                    for (int i = 0; i < 100; ++i) {
                        std::string response = sendCommand("GET no_ttl_key" + std::to_string(i) + "\r\n");
                        if (response.find("$-1") != std::string::npos) {
                            missing_no_ttl++;
                        }
                    }
                    
                    std::cout << "missing_short_ttl: " << missing_short_ttl << ", missing_long_ttl: " << missing_long_ttl << ", missing_no_ttl: " << missing_no_ttl << std::endl;
                    std::cout << "new_keys_added: " << new_keys_added << std::endl;
                    ASSERT_GE(missing_short_ttl, missing_long_ttl);
                    ASSERT_EQ(missing_no_ttl, 0);
                    ASSERT_GT(new_keys_added, 0);
                    return true;
                });
                
                close(sock);
            }
            
            server->stop();
            delete server;
        }
    }
    
    std::cout << "内存淘汰策略测试完成！" << std::endl;
}

int main() {
    try {
        dkv::TestRunner runner;
        
        // 运行maxmemory测试
        testMaxMemory(runner);
        
        // 运行内存淘汰策略测试
        testEvictionPolicies(runner);
        
        // 打印测试总结
        runner.printSummary();
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试失败: " << e.what() << std::endl;
        return 1;
    }
}