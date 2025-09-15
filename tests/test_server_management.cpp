#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include "dkv_server.hpp"
#include "dkv_core.hpp"
#include "test_runner.hpp"

// 测试服务器管理命令
void testServerManagement(dkv::TestRunner& runner) {
    std::cout << "开始测试服务器管理命令..." << std::endl;
    
    // 创建服务器实例（使用非标准端口避免冲突）
    dkv::DKVServer server(6380);
    
    // 启动服务器
    if (!server.start()) {
        std::cerr << "服务器启动失败" << std::endl;
        return;
    }
    std::cout << "服务器启动成功" << std::endl;
    
    // 创建临时测试客户端
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "创建socket失败" << std::endl;
        server.stop();
        return;
    }
    
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(6380);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    
    // 连接服务器
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        std::cerr << "连接服务器失败" << std::endl;
        close(sock);
        server.stop();
        return;
    }
    std::cout << "客户端连接成功" << std::endl;
    
    // 测试函数：发送命令并接收响应
    auto sendCommand = [sock](const std::string& cmd) -> std::string {
        send(sock, cmd.c_str(), cmd.length(), 0);
        char buffer[1024] = {0};
        int bytes_read = recv(sock, buffer, sizeof(buffer) - 1, 0);
        return std::string(buffer, bytes_read);
    };
    
    // 先添加一些测试数据
    std::cout << "添加测试数据..." << std::endl;
    sendCommand("SET test_key test_value\r\n");
    sendCommand("SET another_key another_value\r\n");
    
    // 使用TestRunner进行各个测试点的测试
    runner.runTest("测试DBSIZE命令", [&]() {
        std::string dbsize_response = sendCommand("DBSIZE\r\n");
        std::cout << "DBSIZE 响应: " << dbsize_response << std::endl;
        return dbsize_response.find("$1\r\n2") != std::string::npos;
    });
    
    runner.runTest("测试INFO命令", [&]() {
        std::string info_response = sendCommand("INFO\r\n");
        std::cout << "INFO 响应 (部分): " << info_response.substr(0, 100) << "..." << std::endl;
        return info_response.find("# DKV Server Info") != std::string::npos && 
               info_response.find("version:1.0.0") != std::string::npos;
    });
    
    runner.runTest("测试FLUSHDB命令", [&]() {
        std::string flushdb_response = sendCommand("FLUSHDB\r\n");
        std::cout << "FLUSHDB 响应: " << flushdb_response << std::endl;
        return flushdb_response.find("+OK") != std::string::npos;
    });
    
    runner.runTest("验证数据库已清空", [&]() {
        std::string dbsize_response = sendCommand("DBSIZE\r\n");
        std::cout << "清空后 DBSIZE 响应: " << dbsize_response << std::endl;
        return dbsize_response.find("$1\r\n0") != std::string::npos;
    });
    
    // 关闭连接
    close(sock);
    
    std::cout << "服务器管理命令测试完成！" << std::endl;
    
    // 停止服务器
    server.stop();
    std::cout << "服务器已停止" << std::endl;
}

int main() {
    try {
        dkv::TestRunner runner;
        
        // 运行服务器管理命令测试
        testServerManagement(runner);
        
        // 打印测试总结
        runner.printSummary();
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试失败: " << e.what() << std::endl;
        return 1;
    }
}