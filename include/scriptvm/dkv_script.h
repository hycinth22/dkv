#ifndef DKV_SCRIPT_H
#define DKV_SCRIPT_H

#ifdef __cplusplus
extern "C" {
#endif

// 错误类型
typedef int ResultCode;
const ResultCode SUCCESS = 0;
const ResultCode ERROR = 1;

// DKV 命令处理函数指针类型
typedef char* (*DkvCommandHandlerFn)(const char* command, void* user_data);

// 前向声明
typedef struct DkvScriptVM DkvScriptVM;
typedef struct DkvScriptCompileResult DkvScriptCompileResult;

// 暴露给 C 的函数
ResultCode dkv_script_compile(const char* source, DkvScriptCompileResult** result);
ResultCode dkv_script_create_vm(DkvScriptCompileResult* compile_result, DkvScriptVM** vm);
ResultCode dkv_script_run_vm(DkvScriptVM* vm);
ResultCode dkv_script_set_dkv_command_handler(DkvScriptVM* vm, DkvCommandHandlerFn handler, void* user_data);
void dkv_script_free_compile_result(DkvScriptCompileResult* result);
void dkv_script_free_vm(DkvScriptVM* vm);

#ifdef __cplusplus
}
#endif

#endif // DKV_SCRIPT_H
