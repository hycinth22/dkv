#include <cassert>
#include <thread>
#include <chrono>
#include <fstream>
#include "dkv_server.hpp"
#include "dkv_core.hpp"
#include "test_runner.hpp"

// 测试AOF持久化功能
void testAOF(dkv::TestRunner& runner) {
    std::cout << "开始测试AOF持久化功能..." << std::endl;
    
    // 测试AOF文件创建和命令追加
    runner.runTest("测试AOF文件创建和命令追加", []() {
        std::cout << "Prepareing..." << std::endl;
        // 清理之前可能存在的测试文件
        std::remove("test_aof.aof");

        // 创建服务器实例（使用非标准端口避免冲突）
        dkv::DKVServer server(6391);
        
        // 设置AOF配置
        server.setAOFEnabled(true);
        server.setAOFFilename("test_aof.aof");
        server.setAOFFsyncPolicy("everysec");

        // 启动服务器
        if (!server.start()) {
            std::cerr << "服务器启动失败" << std::endl;
            return false;
        }
        std::cout << "Prepared" << std::endl;

        // 创建临时测试客户端
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::cerr << "创建socket失败" << std::endl;
            server.stop();
            return false;
        }
        
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(6391);
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
        
        std::cout << "Tests start" << std::endl;

        // 执行SET命令设置一些测试数据
        std::string response = sendCommand("*3\r\n$3\r\nSET\r\n$7\r\naofkey1\r\n$10\r\nvalue_aof1\r\n");
        assert(response.find("+OK") != std::string::npos);
        
        response = sendCommand("*3\r\n$3\r\nSET\r\n$7\r\naofkey2\r\n$10\r\nvalue_aof2\r\n");
        assert(response.find("+OK") != std::string::npos);
        
        // 执行INCR命令
        response = sendCommand("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        assert(response.find("$1\r\n1") != std::string::npos);
        
        // 等待AOF写入（由于使用了每秒fsync策略）
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        // 检查AOF文件是否创建
        std::ifstream file("test_aof.aof");
        bool file_exists = file.good();
        file.close();
        assert(file_exists);
        
        // 关闭连接和服务器
        close(sock);
        server.stop();
        
        return true;
    });
    
    // 测试AOF文件加载功能
    runner.runTest("测试AOF文件加载功能", []() {
        // 确保测试文件存在
        std::ifstream file("test_aof.aof");
        if (!file.good()) {
            std::cerr << "AOF测试文件不存在，跳过此测试" << std::endl;
            file.close();
            return false;
        }
        file.close();
        
        // 创建新的服务器实例
        dkv::DKVServer server(6392);
        
        // 设置AOF配置
        server.setAOFEnabled(true);
        server.setAOFFilename("test_aof.aof");
        
        // 启动服务器（应该会自动加载AOF文件）
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
        addr.sin_port = htons(6392);
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
        std::cout << "检查是否成功加载了AOF数据..." << std::endl;
        std::string response = sendCommand("*2\r\n$3\r\nGET\r\n$7\r\naofkey1\r\n");
        std::cout << "GET aofkey1: " << response << std::endl;
        assert(response.find("$10\r\nvalue_aof1") != std::string::npos);
        
        response = sendCommand("*2\r\n$3\r\nGET\r\n$7\r\naofkey2\r\n");
        std::cout << "GET aofkey2: " << response << std::endl;
        assert(response.find("$10\r\nvalue_aof2") != std::string::npos);
        
        response = sendCommand("*2\r\n$3\r\nGET\r\n$7\r\ncounter\r\n");
        std::cout << "GET counter: " << response << std::endl;
        assert(response.find("$1\r\n1") != std::string::npos);
        
        // 关闭连接和服务器
        close(sock);
        server.stop();
        
        return true;
    });
    
    // 测试不同的fsync策略
    runner.runTest("测试不同的fsync策略", []() {
        // 测试NEVER策略
        dkv::DKVServer server_never(6393);
        server_never.setAOFEnabled(true);
        server_never.setAOFFilename("test_aof_never.aof");
        server_never.setAOFFsyncPolicy("never");
        
        // 测试ALWAYS策略
        dkv::DKVServer server_always(6394);
        server_always.setAOFEnabled(true);
        server_always.setAOFFilename("test_aof_always.aof");
        server_always.setAOFFsyncPolicy("always");
        
        // 清理测试文件
        std::remove("test_aof_never.aof");
        std::remove("test_aof_always.aof");
        
        // 启动服务器
        if (!server_never.start() || !server_always.start()) {
            std::cerr << "服务器启动失败" << std::endl;
            server_never.stop();
            server_always.stop();
            return false;
        }
        
        // 等待服务器初始化
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        // 停止服务器
        server_never.stop();
        server_always.stop();
        
        // 检查文件是否创建
        std::ifstream file_never("test_aof_never.aof");
        std::ifstream file_always("test_aof_always.aof");
        bool files_exist = file_never.good() && file_always.good();
        file_never.close();
        file_always.close();
        
        return files_exist;
    });
}

// 主函数用于单独运行测试
int main() {
    dkv::TestRunner runner;
    testAOF(runner);
    runner.printSummary();
    return 0;
}