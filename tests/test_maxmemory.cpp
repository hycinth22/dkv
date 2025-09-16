#include <cassert>
#include <thread>
#include <chrono>
#include "dkv_server.hpp"
#include "dkv_core.hpp"
#include "test_runner.hpp"

// 测试maxmemory功能
void testMaxMemory(dkv::TestRunner& runner) {
    std::cout << "开始测试maxmemory功能..." << std::endl;
    
    // 测试函数：创建服务器并设置maxmemory
    auto createServerWithMaxMemory = [](int port, size_t max_memory) -> dkv::DKVServer* {
        dkv::DKVServer* server = new dkv::DKVServer(port);
        server->setMaxMemory(max_memory);
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

int main() {
    try {
        dkv::TestRunner runner;
        
        // 运行maxmemory测试
        testMaxMemory(runner);
        
        // 打印测试总结
        runner.printSummary();
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试失败: " << e.what() << std::endl;
        return 1;
    }
}