#ifndef TEST_RUNNER_HPP
#define TEST_RUNNER_HPP

#include <iostream>
#include <functional>
#include <string>
#include <cassert>
#include <sstream>

namespace dkv {

// 断言工具宏定义
#define ASSERT_EQ(actual, expected) do { \
    auto _actual = (actual); \
    auto _expected = (expected); \
    if (!(_actual == _expected)) { \
        std::cerr << "断言失败: " << #actual << " == " << #expected << "\n" \
                  << "  实际值: " << _actual << "\n" \
                  << "  期望值: " << _expected << "\n" \
                  << "  文件: " << __FILE__ << "\n" \
                  << "  行号: " << __LINE__ << std::endl; \
        assert(false); \
    } \
} while(0)

#define ASSERT_NE(actual, expected) do { \
    auto _actual = (actual); \
    auto _expected = (expected); \
    if (!(_actual != _expected)) { \
        std::cerr << "断言失败: " << #actual << " != " << #expected << "\n" \
                  << "  实际值: " << _actual << "\n" \
                  << "  期望值: 不等于 " << _expected << "\n" \
                  << "  文件: " << __FILE__ << "\n" \
                  << "  行号: " << __LINE__ << std::endl; \
        assert(false); \
    } \
} while(0)

#define ASSERT_TRUE(condition) do { \
    auto _condition = (condition); \
    if (!_condition) { \
        std::cerr << "断言失败: " << #condition << " 应为真\n" \
                  << "  文件: " << __FILE__ << "\n" \
                  << "  行号: " << __LINE__ << std::endl; \
        assert(false); \
    } \
} while(0)

#define ASSERT_FALSE(condition) do { \
    auto _condition = (condition); \
    if (_condition) { \
        std::cerr << "断言失败: " << #condition << " 应为假\n" \
                  << "  文件: " << __FILE__ << "\n" \
                  << "  行号: " << __LINE__ << std::endl; \
        assert(false); \
    } \
} while(0)

#define ASSERT_GT(actual, expected) do { \
    auto _actual = (actual); \
    auto _expected = (expected); \
    if (!(_actual > _expected)) { \
        std::cerr << "断言失败: " << #actual << " > " << #expected << "\n" \
                  << "  实际值: " << _actual << "\n" \
                  << "  期望值: 大于 " << _expected << "\n" \
                  << "  文件: " << __FILE__ << "\n" \
                  << "  行号: " << __LINE__ << std::endl; \
        assert(false); \
    } \
} while(0)

#define ASSERT_GE(actual, expected) do { \
    auto _actual = (actual); \
    auto _expected = (expected); \
    if (!(_actual >= _expected)) { \
        std::cerr << "断言失败: " << #actual << " >= " << #expected << "\n" \
                  << "  实际值: " << _actual << "\n" \
                  << "  期望值: 大于等于 " << _expected << "\n" \
                  << "  文件: " << __FILE__ << "\n" \
                  << "  行号: " << __LINE__ << std::endl; \
        assert(false); \
    } \
} while(0)

#define ASSERT_LT(actual, expected) do { \
    auto _actual = (actual); \
    auto _expected = (expected); \
    if (!(_actual < _expected)) { \
        std::cerr << "断言失败: " << #actual << " < " << #expected << "\n" \
                  << "  实际值: " << _actual << "\n" \
                  << "  期望值: 小于 " << _expected << "\n" \
                  << "  文件: " << __FILE__ << "\n" \
                  << "  行号: " << __LINE__ << std::endl; \
        assert(false); \
    } \
} while(0)

#define ASSERT_LE(actual, expected) do { \
    auto _actual = (actual); \
    auto _expected = (expected); \
    if (!(_actual <= _expected)) { \
        std::cerr << "断言失败: " << #actual << " <= " << #expected << "\n" \
                  << "  实际值: " << _actual << "\n" \
                  << "  期望值: 小于等于 " << _expected << "\n" \
                  << "  文件: " << __FILE__ << "\n" \
                  << "  行号: " << __LINE__ << std::endl; \
        assert(false); \
    } \
} while(0)

// 用于字符串包含的断言宏
#define ASSERT_CONTAINS(str, substr) do { \
    const auto& _str = (str); \
    const auto& _substr = (substr); \
    if (_str.find(_substr) == std::string::npos) { \
        std::cerr << "断言失败: 字符串包含检查\n" \
                  << "  字符串: \"" << _str << "\"\n" \
                  << "  应包含: \"" << _substr << "\"\n" \
                  << "  文件: " << __FILE__ << "\n" \
                  << "  行号: " << __LINE__ << std::endl; \
        assert(false); \
    } \
} while(0)

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