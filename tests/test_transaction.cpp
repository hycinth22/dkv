#include "test_runner.hpp"
#include "dkv_server.hpp"
#include "dkv_core.hpp"
#include <cassert>
#include <thread>
#include <chrono>

namespace dkv {
namespace test {

// 测试事务基本功能
void testTransactionBasic() {
    // 创建服务器实例
    DKVServer server(0); // 使用端口0让系统自动分配可用端口
    assert(server.start());
    
    // 测试MULTI命令
    Command multi_cmd(CommandType::MULTI, {});
    Response multi_resp = server.executeCommand(multi_cmd);
    assert(multi_resp.status == ResponseStatus::OK);
    assert(multi_resp.message == "OK");
    
    // 测试嵌套MULTI命令
    Response nested_multi_resp = server.executeCommand(multi_cmd);
    assert(nested_multi_resp.status == ResponseStatus::ERROR);
    assert(nested_multi_resp.message == "MULTI calls can not be nested");
    
    // 测试EXEC命令
    Command exec_cmd(CommandType::EXEC, {});
    Response exec_resp = server.executeCommand(exec_cmd);
    assert(exec_resp.status == ResponseStatus::OK);
    
    // 测试没有事务时的EXEC命令
    Response no_multi_exec_resp = server.executeCommand(exec_cmd);
    assert(no_multi_exec_resp.status == ResponseStatus::ERROR);
    assert(no_multi_exec_resp.message == "EXEC without MULTI");
    
    // 测试MULTI+DISCARD命令
    server.executeCommand(multi_cmd);
    Command discard_cmd(CommandType::DISCARD, {});
    Response discard_resp = server.executeCommand(discard_cmd);
    assert(discard_resp.status == ResponseStatus::OK);
    assert(discard_resp.message == "OK");
    
    // 测试没有事务时的DISCARD命令
    Response no_multi_discard_resp = server.executeCommand(discard_cmd);
    assert(no_multi_discard_resp.status == ResponseStatus::ERROR);
    assert(no_multi_discard_resp.message == "DISCARD without MULTI");
    
    // 停止服务器
    server.stop();
    
    std::cout << "Transaction basic test passed" << std::endl;
}

// 注册测试
REGISTER_TEST(testTransactionBasic);

} // namespace test
} // namespace dkv

// 主函数，运行所有测试
int main() {
    dkv::test::TestRunner::getInstance().runAllTests();
    return 0;
}