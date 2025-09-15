#ifndef TEST_RUNNER_HPP
#define TEST_RUNNER_HPP

#include <iostream>
#include <functional>
#include <string>

namespace dkv {

// 测试工具类
class TestRunner {
private:
    int passed_tests_;
    int total_tests_;
    
public:
    TestRunner() : passed_tests_(0), total_tests_(0) {}
    
    void runTest(const std::string& test_name, std::function<bool()> test_func) {
        total_tests_++;
        std::cout << "运行测试: " << test_name << " ... ";
        
        try {
            if (test_func()) {
                passed_tests_++;
                std::cout << "通过" << std::endl;
            } else {
                std::cout << "失败" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cout << "异常: " << e.what() << std::endl;
        }
    }
    
    void printSummary() {
        std::cout << "\n测试总结: " << passed_tests_ << "/" << total_tests_ << " 通过" << std::endl;
        if (passed_tests_ == total_tests_) {
            std::cout << "所有测试通过！" << std::endl;
        } else {
            int failed = total_tests_ - passed_tests_;
            std::cout << "有 " << failed << " 个测试失败" << std::endl;
            exit(failed);
        }
    }
};

} // namespace dkv

#endif // TEST_RUNNER_HPP