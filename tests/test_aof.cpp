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
    
    // 测试AOF重写功能
    runner.runTest("测试AOF重写功能", []() {
        // 清理之前可能存在的测试文件
        std::remove("test_aof_rewrite.aof");
        std::remove("test_aof_rewrite_temp.aof");
        
        // 创建服务器实例
        dkv::DKVServer server(6395);
        
        // 设置AOF配置
        server.setAOFEnabled(true);
        server.setAOFFilename("test_aof_rewrite.aof");
        server.setAOFFsyncPolicy("everysec");
        
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
        addr.sin_port = htons(6395);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        
        // 连接服务器
        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            std::cerr << "连接服务器失败" << std::endl;
            close(sock);
            server.stop();
            return false;
        }
        
        // 测试函数：发送命令并接收响应
         auto _sendCommand = [](int sock, const std::string& cmd) -> std::string {
            send(sock, cmd.c_str(), cmd.length(), 0);
            char buffer[1024] = {0};
            int bytes_read = recv(sock, buffer, sizeof(buffer) - 1, 0);
            return std::string(buffer, bytes_read);
        };
        auto sendCommand = [_sendCommand, sock](const std::string& cmd) -> std::string {
            return _sendCommand(sock, cmd);
        };
        
        // 添加测试数据，包括多种数据类型
        sendCommand("*3\r\n$3\r\nSET\r\n$12\r\nrewrite_key1\r\n$12\r\nrewrite_val1\r\n");
        sendCommand("*3\r\n$3\r\nSET\r\n$12\r\nrewrite_key2\r\n$12\r\nrewrite_val2\r\n");
        sendCommand("*2\r\n$4\r\nINCR\r\n$13\r\nrewrite_count\r\n");
        sendCommand("*3\r\n$5\r\nLPUSH\r\n$12\r\nrewrite_list\r\n$12\r\nlist_item_1\r\n");
        sendCommand("*3\r\n$5\r\nLPUSH\r\n$12\r\nrewrite_list\r\n$12\r\nlist_item_2\r\n");
        sendCommand("*4\r\n$4\r\nHSET\r\n$12\r\nrewrite_hash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");
        // 添加HyperLogLog类型数据
        sendCommand("*3\r\n$5\r\nPFADD\r\n$19\r\nrewrite_hyperloglog\r\n$9\r\nelement_1\r\n");
        sendCommand("*3\r\n$5\r\nPFADD\r\n$19\r\nrewrite_hyperloglog\r\n$9\r\nelement_2\r\n");
        sendCommand("*3\r\n$5\r\nPFADD\r\n$19\r\nrewrite_hyperloglog\r\n$9\r\nelement_3\r\n");
        
        // 等待AOF写入
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // 记录原始AOF文件大小
        std::ifstream original_file("test_aof_rewrite.aof", std::ios::ate | std::ios::binary);
        std::streamsize original_size = original_file.tellg();
        original_file.close();
        
        // 执行AOF重写
        if (!server.rewriteAOF()) {
            std::cerr << "AOF重写失败" << std::endl;
            close(sock);
            server.stop();
            return false;
        }
        
        // 等待重写完成
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        // 记录重写后AOF文件大小
        std::ifstream rewritten_file("test_aof_rewrite.aof", std::ios::ate | std::ios::binary);
        std::streamsize rewritten_size = rewritten_file.tellg();
        rewritten_file.close();
        
        // 打印文件大小信息
        std::cout << "原始AOF文件大小: " << original_size << " 字节" << std::endl;
        std::cout << "重写后AOF文件大小: " << rewritten_size << " 字节" << std::endl;
        
        // 关闭连接和服务器
        close(sock);
        server.stop();
        
        // 创建新的服务器实例来测试重写后的AOF文件
        dkv::DKVServer new_server(6396);
        new_server.setAOFEnabled(true);
        new_server.setAOFFilename("test_aof_rewrite.aof");
        
        // 启动新服务器（应该会加载重写后的AOF文件）
        if (!new_server.start()) {
            std::cerr << "新服务器启动失败" << std::endl;
            return false;
        }
        
        // 创建新的测试客户端
        int new_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (new_sock < 0) {
            std::cerr << "创建新socket失败" << std::endl;
            new_server.stop();
            return false;
        }
        
        struct sockaddr_in new_addr;
        new_addr.sin_family = AF_INET;
        new_addr.sin_port = htons(6396);
        new_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        
        // 连接新服务器
        if (connect(new_sock, (struct sockaddr*)&new_addr, sizeof(new_addr)) != 0) {
            std::cerr << "连接新服务器失败" << std::endl;
            close(new_sock);
            new_server.stop();
            return false;
        }

        // 测试函数：发送命令并接收响应
        auto new_sendCommand = [_sendCommand, new_sock](const std::string& cmd) -> std::string {
            return _sendCommand(new_sock, cmd);
        };
        
        // 验证数据是否正确加载
        std::string response = new_sendCommand("*2\r\n$3\r\nGET\r\n$12\r\nrewrite_key1\r\n");
        ASSERT_CONTAINS(response, "$12\r\nrewrite_val1");

        response = new_sendCommand("*2\r\n$3\r\nGET\r\n$12\r\nrewrite_key2\r\n");
        ASSERT_CONTAINS(response, "$12\r\nrewrite_val2");
        
        response = new_sendCommand("*2\r\n$3\r\nGET\r\n$13\r\nrewrite_count\r\n");
        ASSERT_CONTAINS(response, "$1\r\n1");
        
        response = new_sendCommand("*2\r\n$4\r\nLLEN\r\n$12\r\nrewrite_list\r\n");
        ASSERT_CONTAINS(response, "$1\r\n2");
        
        response = new_sendCommand("*4\r\n$4\r\nHGET\r\n$12\r\nrewrite_hash\r\n$5\r\nfield\r\n");
        ASSERT_CONTAINS(response, "$5\r\nvalue");
        
        // 验证HyperLogLog数据是否正确恢复
        response = new_sendCommand("*2\r\n$7\r\nPFCOUNT\r\n$19\r\nrewrite_hyperloglog\r\n");
        ASSERT_CONTAINS(response, "$1\r\n3");
        
        // 关闭连接和新服务器
        close(new_sock);
        new_server.stop();
        
        // 检查重写后的文件是否存在
        std::ifstream check_file("test_aof_rewrite.aof");
        bool file_exists = check_file.good();
        check_file.close();
        
        return file_exists;
    });
}

// 主函数用于单独运行测试
int main() {
    dkv::TestRunner runner;
    testAOF(runner);
    runner.printSummary();
    return 0;
}