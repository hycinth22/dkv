#ifndef DKV_LOGGER_HPP
#define DKV_LOGGER_HPP

#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <mutex>
#include <iomanip>

namespace dkv {

// 日志等级枚举
enum class LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARNING = 2,
    ERROR = 3,
    CRITICAL = 4
};

// 日志系统类
class Logger {
public:
    // 获取单例实例
    static Logger& getInstance() {
        static Logger instance;
        return instance;
    }

    // 设置日志等级
    void setLogLevel(LogLevel level) {
        log_level_ = level;
    }

    // 设置日志文件路径
    void setLogFile(const std::string& file_path) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (log_file_.is_open()) {
            log_file_.close();
        }
        log_file_.open(file_path, std::ios::out | std::ios::app);
        log_to_file_ = log_file_.is_open();
    }

    // 关闭日志文件
    void closeLogFile() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (log_file_.is_open()) {
            log_file_.close();
            log_to_file_ = false;
        }
    }

    // 设置是否输出到控制台
    void setConsoleOutput(bool enable) {
        console_output_ = enable;
    }

    // 日志输出方法
    template<typename... Args>
    void log(LogLevel level, Args&&... args) {
        if (level < log_level_) {
            return;
        }

        std::string log_entry = sprintLogEntry(level, std::forward<Args>(args)...);

        std::lock_guard<std::mutex> lock(mutex_);
        
        if (console_output_) {
            std::ostream& out = (level >= LogLevel::ERROR) ? std::cerr : std::cout;
            out << log_entry << std::endl;
        }

        if (log_to_file_ && log_file_.is_open()) {
            log_file_ << log_entry << std::endl;
        }
    }

    // 便捷日志方法
    template<typename... Args>
    void debug(Args&&... args) {
        log(LogLevel::DEBUG, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void info(Args&&... args) {
        log(LogLevel::INFO, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void warning(Args&&... args) {
        log(LogLevel::WARNING, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void error(Args&&... args) {
        log(LogLevel::ERROR, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void critical(Args&&... args) {
        log(LogLevel::CRITICAL, std::forward<Args>(args)...);
    }

    // 格式化日志方法
    template<typename... Args>
    void logf(LogLevel level, const std::string& format, Args&&... args) {
        if (level < log_level_) {
            return;
        }

        std::string log_entry = formatLogEntry(level, format, std::forward<Args>(args)...);

        std::lock_guard<std::mutex> lock(mutex_);
        
        if (console_output_) {
            std::ostream& out = (level >= LogLevel::ERROR) ? std::cerr : std::cout;
            out << log_entry << std::endl;
        }

        if (log_to_file_ && log_file_.is_open()) {
            log_file_ << log_entry << std::endl;
        }
    }

    // 便捷格式化日志方法
    template<typename... Args>
    void debugf(const std::string& format, Args&&... args) {
        logf(LogLevel::DEBUG, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void infof(const std::string& format, Args&&... args) {
        logf(LogLevel::INFO, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void warningf(const std::string& format, Args&&... args) {
        logf(LogLevel::WARNING, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void errorf(const std::string& format, Args&&... args) {
        logf(LogLevel::ERROR, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void criticalf(const std::string& format, Args&&... args) {
        logf(LogLevel::CRITICAL, format, std::forward<Args>(args)...);
    }

    

private:
    Logger() : log_level_(LogLevel::INFO), console_output_(true), log_to_file_(false) {
        // 默认构造函数
    }

    ~Logger() {
        if (log_file_.is_open()) {
            log_file_.close();
        }
    }

    // 格式化日志条目
    template<typename... Args>
    std::string sprintLogEntry(LogLevel level, Args&&... args) {
        std::stringstream ss;
        
        // 添加时间戳
        auto now = std::chrono::system_clock::now();
        auto now_c = std::chrono::system_clock::to_time_t(now);
        auto now_tm = std::localtime(&now_c);
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
        
        ss << "[" << std::put_time(now_tm, "%Y-%m-%d %H:%M:%S") << "." 
           << std::setw(3) << std::setfill('0') << now_ms.count() << "] ";
        
        // 添加日志等级
        ss << "[" << levelToString(level) << "] ";
        
        // 添加日志内容
        printArgs(ss, std::forward<Args>(args)...);
        
        return ss.str();
    }
    
    template<typename... Args>
    std::string formatLogEntry(LogLevel level, const std::string& format, Args&&... args) {
        std::stringstream ss;
        
        // 添加时间戳
        auto now = std::chrono::system_clock::now();
        auto now_c = std::chrono::system_clock::to_time_t(now);
        auto now_tm = std::localtime(&now_c);
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
        
        ss << "[" << std::put_time(now_tm, "%Y-%m-%d %H:%M:%S") << "." 
           << std::setw(3) << std::setfill('0') << now_ms.count() << "] ";
        
        // 添加日志等级
        ss << "[" << levelToString(level) << "] ";
        
        // 格式化版本
        printfArgs(ss, format, std::forward<Args>(args)...);
        
        return ss.str();
    }

    // 将日志等级转换为字符串
    std::string levelToString(LogLevel level) {
        switch (level) {
            case LogLevel::DEBUG:
                return "DEBUG";
            case LogLevel::INFO:
                return "INFO";
            case LogLevel::WARNING:
                return "WARNING";
            case LogLevel::ERROR:
                return "ERROR";
            case LogLevel::CRITICAL:
                return "CRITICAL";
            default:
                return "UNKNOWN";
        }
    }

    // 将所有参数直接转为字符串并拼接（不格式化）
    template<typename T, typename... Args>
    void printArgs(std::stringstream& ss, T&& arg, Args&&... args) {
        ss << std::forward<T>(arg);
        if constexpr (sizeof...(args) > 0) {
            printArgs(ss, std::forward<Args>(args)...);
        }
    }

    // 基本情况：无参数
    void printArgs(std::stringstream& /*unused*/) {}

    // 格式化版本 - 支持{}占位符
    template<typename T, typename... Args>
    void printfArgs(std::stringstream& ss, const std::string& format, size_t pos, T&& arg, Args&&... args) {
        // 查找下一个占位符
        size_t placeholder_pos = format.find("{}", pos);
        if (placeholder_pos == std::string::npos) {
            // 没有找到占位符，添加剩余的format字符串
            ss << format.substr(pos);
            return;
        }
        
        // 添加占位符前的字符串
        ss << format.substr(pos, placeholder_pos - pos);
        
        // 添加当前参数
        ss << std::forward<T>(arg);
        
        // 递归处理剩余的format和参数
        if constexpr (sizeof...(args) > 0) {
            printfArgs(ss, format, placeholder_pos + 2, std::forward<Args>(args)...);
        } else {
            // 没有更多参数，添加剩余的format字符串
            ss << format.substr(placeholder_pos + 2);
        }
    }
    
    // 外部接口
    template<typename... Args>
    void printfArgs(std::stringstream& ss, const std::string& format, Args&&... args) {
        if constexpr (sizeof...(args) > 0) {
            printfArgs(ss, format, 0, std::forward<Args>(args)...);
        } else {
            ss << format;
        }
    }
    
    LogLevel log_level_;
    bool console_output_;
    bool log_to_file_;
    std::ofstream log_file_;
    std::mutex mutex_;
};

// 全局日志宏，方便使用
#define DKV_LOG_DEBUG(...) Logger::getInstance().debug(__VA_ARGS__)
#define DKV_LOG_INFO(...) Logger::getInstance().info(__VA_ARGS__)
#define DKV_LOG_WARNING(...) Logger::getInstance().warning(__VA_ARGS__)
#define DKV_LOG_ERROR(...) Logger::getInstance().error(__VA_ARGS__)
#define DKV_LOG_CRITICAL(...) Logger::getInstance().critical(__VA_ARGS__)

// 格式化版本日志宏
#define DKV_LOG_DEBUGF(...) Logger::getInstance().debugf(__VA_ARGS__)
#define DKV_LOG_INFOF(...) Logger::getInstance().infof(__VA_ARGS__)
#define DKV_LOG_WARNINGF(...) Logger::getInstance().warningf(__VA_ARGS__)
#define DKV_LOG_ERRORF(...) Logger::getInstance().errorf(__VA_ARGS__)
#define DKV_LOG_CRITICALF(...) Logger::getInstance().criticalf(__VA_ARGS__)

} // namespace dkv

#endif // DKV_LOGGER_HPP