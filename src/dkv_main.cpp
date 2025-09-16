#include "dkv_server.hpp"
#include <iostream>
#include <string>
#include <csignal>
#include <cstring>
#include "dkv_logger.hpp"

namespace dkv {

// 全局服务器实例，用于信号处理
DKVServer* g_server = nullptr;
std::atomic<bool> g_should_exit{false};

// 信号处理函数
void signalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        DKV_LOG_INFO("收到中断信号，正在关闭服务器...");
        g_should_exit = true;
        if (g_server) {
            g_server->stop();
        }
    }
}

// 打印帮助信息
void printHelp() {
    std::cout << "DKV - 分布式键值存储系统 v0.1\n" << std::endl;
    std::cout << "用法: dkv_server [选项]\n" << std::endl;
    std::cout << "选项:" << std::endl;
    std::cout << "  -c, --config <file>    使用指定的配置文件" << std::endl;
    std::cout << "  -p, --port <port>      设置服务器端口（默认：6379）" << std::endl;
    std::cout << "  -r, --reactors <num>   设置子Reactor数量（默认：4）" << std::endl;
    std::cout << "  -w, --workers <num>    设置工作线程数量（默认：8）" << std::endl;
    std::cout << "  -l, --log-level <level> 设置日志等级（debug, info, warning, error, critical, 默认：info）" << std::endl;
    std::cout << "  -f, --log-file <file>  设置日志文件路径" << std::endl;
    std::cout << "  -v, --version          显示版本信息" << std::endl;
    std::cout << "  -h, --help             显示帮助信息" << std::endl;
    std::cout << "\n示例:" << std::endl;
    std::cout << "  dkv_server                    # 使用默认配置启动" << std::endl;
    std::cout << "  dkv_server -p 6380            # 在端口6380启动" << std::endl;
    std::cout << "  dkv_server -c config.conf     # 使用配置文件启动" << std::endl;
    std::cout << "  dkv_server -r 8 -w 16         # 使用8个子Reactor和16个工作线程" << std::endl;
    std::cout << "  dkv_server -l debug -f dkv.log # 启用调试日志并输出到文件" << std::endl;
}

// 打印版本信息
void printVersion() {
    std::cout << "DKV v0.1.0" << std::endl;
    std::cout << "基于现代C++17的分布式键值存储系统" << std::endl;
}

// 解析命令行参数
struct ServerConfig {
    int port = 6379;
    std::string config_file;
    bool show_help = false;
    bool show_version = false;
    size_t num_sub_reactors = 4;  // 默认子Reactor数量
    size_t num_workers = 8;       // 默认工作线程数量
    std::string log_level = "info"; // 默认日志等级
    std::string log_file;         // 日志文件路径，为空则不输出到文件
};

ServerConfig parseArguments(int argc, char* argv[]) {
    ServerConfig config;
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg == "-h" || arg == "--help") {
            config.show_help = true;
        } else if (arg == "-v" || arg == "--version") {
            config.show_version = true;
        } else if (arg == "-p" || arg == "--port") {
            if (i + 1 < argc) {
                config.port = std::stoi(argv[++i]);
            } else {
                std::cerr << "错误: -p/--port 需要指定端口号" << std::endl;
                exit(1);
            }
        } else if (arg == "-c" || arg == "--config") {
            if (i + 1 < argc) {
                config.config_file = argv[++i];
            } else {
                std::cerr << "错误: -c/--config 需要指定配置文件" << std::endl;
                exit(1);
            }
        } else if (arg == "-r" || arg == "--reactors") {
            if (i + 1 < argc) {
                config.num_sub_reactors = std::stoul(argv[++i]);
            } else {
                std::cerr << "错误: -r/--reactors 需要指定子Reactor数量" << std::endl;
                exit(1);
            }
        } else if (arg == "-w" || arg == "--workers") {
            if (i + 1 < argc) {
                config.num_workers = std::stoul(argv[++i]);
            } else {
                std::cerr << "错误: -w/--workers 需要指定工作线程数量" << std::endl;
                exit(1);
            }
        } else if (arg == "-l" || arg == "--log-level") {
            if (i + 1 < argc) {
                config.log_level = argv[++i];
            } else {
                std::cerr << "错误: -l/--log-level 需要指定日志等级" << std::endl;
                exit(1);
            }
        } else if (arg == "-f" || arg == "--log-file") {
            if (i + 1 < argc) {
                config.log_file = argv[++i];
            } else {
                std::cerr << "错误: -f/--log-file 需要指定日志文件路径" << std::endl;
                exit(1);
            }
        } else {
            std::cerr << "未知参数: " << arg << std::endl;
            std::cerr << "使用 -h 或 --help 查看帮助信息" << std::endl;
            exit(1);
        }
    }
    
    return config;
}

} // namespace dkv

int main(int argc, char* argv[]) {
    using namespace dkv;
    
    // 初始化日志系统
    auto& logger = Logger::getInstance();
    
    // 解析命令行参数
    ServerConfig config = parseArguments(argc, argv);
    
    // 处理帮助和版本信息
    if (config.show_help) {
        printHelp();
        return 0;
    }
    
    if (config.show_version) {
        printVersion();
        return 0;
    }
    
    // 配置日志系统
    if (config.log_level == "debug") {
        logger.setLogLevel(LogLevel::DEBUG);
    } else if (config.log_level == "info") {
        logger.setLogLevel(LogLevel::INFO);
    } else if (config.log_level == "warning") {
        logger.setLogLevel(LogLevel::WARNING);
    } else if (config.log_level == "error") {
        logger.setLogLevel(LogLevel::ERROR);
    } else if (config.log_level == "critical") {
        logger.setLogLevel(LogLevel::CRITICAL);
    } else {
        std::cerr << "无效的日志等级: " << config.log_level << ", 使用默认等级 (info)" << std::endl;
    }
    
    // 设置日志文件
    if (!config.log_file.empty()) {
        logger.setLogFile(config.log_file);
        DKV_LOG_INFO("日志文件已设置为: ", config.log_file);
    }
    
    // 设置信号处理
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);
    
    // 创建服务器实例
    DKV_LOG_INFO("正在创建服务器实例，端口: ", config.port, ", 子Reactor数量: ", config.num_sub_reactors, ", 工作线程数量: ", config.num_workers);
    DKVServer server(config.port, config.num_sub_reactors, config.num_workers);
    g_server = &server;
    
    // 加载配置文件（如果指定）
    if (!config.config_file.empty()) {
        if (!server.loadConfig(config.config_file)) {
            DKV_LOG_ERROR("加载配置文件失败: ", config.config_file);
            return 1;
        } else {
            DKV_LOG_INFO("成功加载配置文件: ", config.config_file);
        }
    }
    
    // 启动服务器
    DKV_LOG_INFO("正在启动服务器...");
    if (!server.start()) {
        DKV_LOG_ERROR("启动服务器失败");
        return 1;
    }
    
    DKV_LOG_INFO("服务器已成功启动，端口: ", config.port);
    
    // 等待服务器停止
    while (server.isRunning() && !g_should_exit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // 如果收到退出信号，确保服务器已停止
    if (g_should_exit && server.isRunning()) {
        server.stop();
    }
    
    DKV_LOG_INFO("服务器已停止");
    
    // 关闭日志文件
    logger.closeLogFile();
    
    return 0;
}
