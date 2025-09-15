#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include <fstream>
#include "dkv_server.hpp"
#include "dkv_core.hpp"
#include "test_runner.hpp"

// 测试RDB持久化功能
void testRDB(dkv::TestRunner& runner) {
    std::cout << "开始测试RDB持久化功能..." << std::endl;
    
    // 测试SAVE和BGSAVE命令
    runner.runTest("测试SAVE和BGSAVE命令", []() {
        // 创建服务器实例（使用非标准端口避免冲突）
        dkv::DKVServer server(6381);
        
        // 设置RDB配置
        server.setRDBEnabled(true);
        server.setRDBFilename("test_dump.rdb");
        server.setRDBSaveInterval(60); // 60秒
        server.setRDBSaveChanges(10);  // 10次变更
        
        // 启动服务器
        if (!server.start()) {
            std::cerr << "服务器启动失败" << std::endl;
            return false;
        }
        
        // 创建临时测试客户端
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::cerr << "创建socket失败" << std::endl;
            server.stop();
            return false;
        }
        
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(6381);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        
        // 连接服务器
        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            std::cerr << "连接服务器失败" << std::endl;
            close(sock);
            server.stop();
            return false;
        }
        
        // 测试函数：发送命令并接收响应
        auto sendCommand = [sock](const std::string& cmd) -> std::string {
            send(sock, cmd.c_str(), cmd.length(), 0);
            char buffer[1024] = {0};
            int bytes_read = recv(sock, buffer, sizeof(buffer) - 1, 0);
            return std::string(buffer, bytes_read);
        };
        
        // 清理之前可能存在的测试文件
        std::remove("test_dump.rdb");
        
        // 执行SET命令设置一些测试数据
        std::string response = sendCommand("*3\r\n$3\r\nSET\r\n$5\r\ntest1\r\n$6\r\nvalue1\r\n");
        assert(response.find("+OK") != std::string::npos);
        
        response = sendCommand("*3\r\n$3\r\nSET\r\n$5\r\ntest2\r\n$6\r\nvalue2\r\n");
        assert(response.find("+OK") != std::string::npos);
        
        // 执行SAVE命令
        std::cout << "执行SAVE命令..." << std::endl;
        response = sendCommand("*1\r\n$4\r\nSAVE\r\n");
        assert(response.find("+OK") != std::string::npos);
        
        // 检查RDB文件是否创建
        std::ifstream file("test_dump.rdb");
        bool file_exists = file.good();
        file.close();
        assert(file_exists);
        
        // 执行BGSAVE命令
        std::cout << "执行BGSAVE命令..." << std::endl;
        response = sendCommand("*1\r\n$6\r\nBGSAVE\r\n");
        assert(response.find("+Background saving started") != std::string::npos);
        
        // 等待异步保存完成
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        // 关闭连接和服务器
        close(sock);
        server.stop();
        
        return true;
    });
    
    // 测试RDB文件加载功能
    runner.runTest("测试RDB文件加载功能", []() {
        // 确保测试文件存在
        std::ifstream file("test_dump.rdb");
        if (!file.good()) {
            std::cerr << "测试文件不存在，跳过此测试" << std::endl;
            file.close();
            return false;
        }
        file.close();
        
        // 创建新的服务器实例
        dkv::DKVServer server(6382);
        
        // 设置RDB配置
        server.setRDBEnabled(true);
        server.setRDBFilename("test_dump.rdb");
        
        // 启动服务器（应该会自动加载RDB文件）
        if (!server.start()) {
            std::cerr << "服务器启动失败" << std::endl;
            return false;
        }
        
        // 创建临时测试客户端
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::cerr << "创建socket失败" << std::endl;
            server.stop();
            return false;
        }
        
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(6382);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        
        // 连接服务器
        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            std::cerr << "连接服务器失败" << std::endl;
            close(sock);
            server.stop();
            return false;
        }
        
        // 测试函数：发送命令并接收响应
        auto sendCommand = [sock](const std::string& cmd) -> std::string {
            send(sock, cmd.c_str(), cmd.length(), 0);
            char buffer[1024] = {0};
            int bytes_read = recv(sock, buffer, sizeof(buffer) - 1, 0);
            return std::string(buffer, bytes_read);
        };
        
        // 检查是否成功加载了之前保存的数据
        std::cout << "检查是否成功加载了RDB数据..." << std::endl;
        std::string response = sendCommand("*2\r\n$3\r\nGET\r\n$5\r\ntest1\r\n");
        std::cout << "response：" << response << std::endl;
        assert(response.find("$6\r\nvalue1") != std::string::npos);
        
        response = sendCommand("*2\r\n$3\r\nGET\r\n$5\r\ntest2\r\n");
        assert(response.find("$6\r\nvalue2") != std::string::npos);
        
        // 关闭连接和服务器
        close(sock);
        server.stop();
        
        return true;
    });
    
    // 测试自动保存功能
    runner.runTest("测试自动保存功能", []() {
        // 创建服务器实例
        dkv::DKVServer server(6383);
        
        // 设置RDB配置，使用短间隔进行测试
        server.setRDBEnabled(true);
        server.setRDBFilename("auto_dump.rdb");
        server.setRDBSaveInterval(5);  // 5秒
        server.setRDBSaveChanges(5);   // 5次变更
        
        // 清理之前可能存在的测试文件
        std::remove("auto_dump.rdb");
        
        // 启动服务器
        if (!server.start()) {
            std::cerr << "服务器启动失败" << std::endl;
            return false;
        }
        
        // 创建临时测试客户端
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::cerr << "创建socket失败" << std::endl;
            server.stop();
            return false;
        }
        
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(6383);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        
        // 连接服务器
        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            std::cerr << "连接服务器失败" << std::endl;
            close(sock);
            server.stop();
            return false;
        }
        
        // 测试函数：发送命令并接收响应
        auto sendCommand = [sock](const std::string& cmd) -> std::string {
            send(sock, cmd.c_str(), cmd.length(), 0);
            char buffer[1024] = {0};
            int bytes_read = recv(sock, buffer, sizeof(buffer) - 1, 0);
            return std::string(buffer, bytes_read);
        };
        
        // 执行多次命令以触发自动保存
        std::cout << "执行多次命令以触发自动保存..." << std::endl;
        for (int i = 0; i < 10; i++) {
            std::string key = "auto_key" + std::to_string(i);
            std::string value = "auto_value" + std::to_string(i);
            
            // 构造SET命令
            std::string cmd = "*3\r\n$3\r\nSET\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n$" + 
                              std::to_string(value.length()) + "\r\n" + value + "\r\n";
            
            std::string response = sendCommand(cmd);
            assert(response.find("+OK") != std::string::npos);
        }
        
        // 等待自动保存触发
        std::cout << "等待自动保存触发..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(8));
        
        // 检查自动保存的RDB文件是否创建
        std::ifstream file("auto_dump.rdb");
        bool file_exists = file.good();
        file.close();
        
        // 关闭连接和服务器
        close(sock);
        server.stop();
        
        return file_exists;
    });
    
    // 清理测试文件
    std::remove("test_dump.rdb");
    std::remove("auto_dump.rdb");
}

int main() {
    dkv::TestRunner runner;
    testRDB(runner);
    runner.printSummary();
    return 0;
}