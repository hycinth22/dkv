#ifndef DKV_SCRIPT_HPP
#define DKV_SCRIPT_HPP

#include "dkv_script.h"
#include <string>
#include <stdexcept>

namespace dkv_script {

// C++ 风格的 DKV 命令处理函数类型
typedef std::function<std::string(const std::string&)> DkvCommandHandler;

// C++ 包装器类，提供更友好的接口
class DkvScript {
public:
    // 构造函数和析构函数
    DkvScript() : compile_result_(nullptr), vm_(nullptr) {}
    
    ~DkvScript() {
        cleanup();
    }
    
    // 禁止复制和移动，避免资源管理问题
    DkvScript(const DkvScript&) = delete;
    DkvScript& operator=(const DkvScript&) = delete;
    DkvScript(DkvScript&&) = delete;
    DkvScript& operator=(DkvScript&&) = delete;
    
    // 编译源代码
    void compile(const std::string& source) {
        // 先清理之前的资源
        cleanup();
        
        // 调用 C 接口编译
        ResultCode result = dkv_script_compile(source.c_str(), &compile_result_);
        if (result != SUCCESS) {
            throw std::runtime_error("Failed to compile script");
        }
    }
    
    // 设置 DKV 命令处理函数
    void setDkvCommandHandler(DkvCommandHandler handler) {
        handler_ = std::move(handler);
    }
    
    // 创建虚拟机
    void createVM() {
        if (!compile_result_) {
            throw std::runtime_error("No compiled result available");
        }
        
        // 调用 C 接口创建 VM
        ResultCode result = dkv_script_create_vm(compile_result_, &vm_);
        if (result != SUCCESS) {
            throw std::runtime_error("Failed to create VM");
        }
        
        // 设置 DKV 命令处理函数
        if (handler_) {
            // 保存 this 指针用于回调
            result = dkv_script_set_dkv_command_handler(vm_, &dkvCommandHandlerCallback, this);
            if (result != SUCCESS) {
                throw std::runtime_error("Failed to set DKV command handler");
            }
        }
    }
    
    // 运行虚拟机
    void run() {
        if (!vm_) {
            throw std::runtime_error("No VM available");
        }
        
        // 调用 C 接口运行 VM
        ResultCode result = dkv_script_run_vm(vm_);
        if (result != SUCCESS) {
            throw std::runtime_error("Failed to run VM");
        }
    }
    
    // 编译并运行脚本（便捷方法）
    void execute(const std::string& source) {
        compile(source);
        createVM();
        run();
    }
    
private:
    // C 回调函数，将调用转发给 C++ 处理函数
    static char* dkvCommandHandlerCallback(const char* command, void* user_data) {
        if (!command || !user_data) {
            return strdup("Error: Invalid parameters");
        }
        
        // 将 user_data 转换为 DkvScript* 指针
        DkvScript* instance = static_cast<DkvScript*>(user_data);
        if (!instance->handler_) {
            return strdup("Error: No DKV command handler set");
        }
        
        try {
            // 调用 C++ 处理函数
            std::string result = instance->handler_(command);
            // 将结果转换为 C 字符串（调用者负责释放）
            return strdup(result.c_str());
        } catch (const std::exception& e) {
            std::string error = "Error: " + std::string(e.what());
            return strdup(error.c_str());
        } catch (...) {
            return strdup("Error: Unknown exception");
        }
    }
    
    // 清理资源
    void cleanup() {
        if (vm_) {
            dkv_script_free_vm(vm_);
            vm_ = nullptr;
        }
        
        if (compile_result_) {
            dkv_script_free_compile_result(compile_result_);
            compile_result_ = nullptr;
        }
    }
    
    // 内部指针
    DkvScriptCompileResult* compile_result_;
    DkvScriptVM* vm_;
    
    // DKV 命令处理函数
    DkvCommandHandler handler_;
};

} // namespace dkv_script

#endif // DKV_SCRIPT_HPP
