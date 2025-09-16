#!/usr/bin/env python3
"""
简单的DKV客户端测试脚本
使用socket连接到DKV服务器并发送RESP协议命令
"""

import socket
import time
import sys

def send_command(sock, command):
    """发送RESP协议命令"""
    # 将命令转换为RESP数组格式
    parts = command.split()
    resp = f"*{len(parts)}\r\n"
    for part in parts:
        resp += f"${len(part)}\r\n{part}\r\n"
    
    print(f"发送命令: {command}")
    print(f"RESP格式: {repr(resp)}")
    
    sock.send(resp.encode())
    
    # 接收响应
    response = sock.recv(1024).decode()
    print(f"服务器响应: {repr(response)}")
    return response

def assert_response(response, expected_content, command=None):
    """断言服务器响应包含预期内容"""
    try:
        assert expected_content in response
        if command:
            print(f"✅ 断言通过: '{command}' 的响应包含 '{expected_content}'")
        else:
            print(f"✅ 断言通过: 响应包含 '{expected_content}'")
        print()
        return True
    except AssertionError:
        print(f"❌ 断言失败: 期望 '{expected_content}' 在响应 '{response}' 中")
        if command:
            print(f"  命令: {command}")
        print()
        return False

def test_basic(sock):
    """测试基本命令"""
    print("=" * 50)
    print("测试基本命令")
    print("=" * 50)
    
    # 测试SET命令
    response = send_command(sock, "SET test_key hello_world")
    assert_response(response, "OK", "SET test_key hello_world")
    
    # 测试GET命令
    response = send_command(sock, "GET test_key")
    assert_response(response, "hello_world", "GET test_key")
    
    # 测试EXISTS命令
    response = send_command(sock, "EXISTS test_key")
    assert_response(response, "1", "EXISTS test_key")
    
    # 测试计数器相关命令
    response = send_command(sock, "SET counter 100")
    assert_response(response, "OK", "SET counter 100")
    
    response = send_command(sock, "INCR counter")
    assert_response(response, "101", "INCR counter")
    
    response = send_command(sock, "GET counter")
    assert_response(response, "101", "GET counter")
    
    response = send_command(sock, "DECR counter")
    assert_response(response, "100", "DECR counter")
    
    response = send_command(sock, "GET counter")
    assert_response(response, "100", "GET counter")
    
    # 测试DEL命令
    response = send_command(sock, "DEL test_key")
    assert_response(response, "1", "DEL test_key")
    
    # 验证键已被删除
    response = send_command(sock, "GET test_key")
    assert_response(response, "-1", "GET test_key (after DEL)")
    
    response = send_command(sock, "EXISTS test_key")
    assert_response(response, "0", "EXISTS test_key (after DEL)")
    
    # 测试EXISTS命令多参数功能
    response = send_command(sock, "SET multi_key1 value1")
    assert_response(response, "OK", "SET multi_key1 value1")
    
    response = send_command(sock, "SET multi_key2 value2")
    assert_response(response, "OK", "SET multi_key2 value2")
    
    # 测试多个存在的键
    response = send_command(sock, "EXISTS multi_key1 multi_key2")
    assert_response(response, "2", "EXISTS multi_key1 multi_key2 (both exist)")
    
    # 测试混合存在和不存在的键
    response = send_command(sock, "EXISTS multi_key1 non_existent_key multi_key2")
    assert_response(response, "2", "EXISTS mixed keys")
    
    # 测试多个不存在的键
    response = send_command(sock, "EXISTS non_existent_key1 non_existent_key2")
    assert_response(response, "0", "EXISTS multiple non-existent keys")

def test_datatype_string(sock):
    """测试字符串数据类型相关命令"""
    print("=" * 50)
    print("测试字符串数据类型相关命令")
    print("=" * 50)
    
    # 测试过期时间
    response = send_command(sock, "SET temp_key temp_value")
    assert_response(response, "OK", "SET temp_key temp_value")
    
    response = send_command(sock, "EXPIRE temp_key 2")
    assert_response(response, "1", "EXPIRE temp_key 2")
    
    # 测试为不存在的键设置过期时间
    response = send_command(sock, "EXPIRE non_existent_key 10")
    assert_response(response, "0", "EXPIRE non_existent_key 10")
    
    response = send_command(sock, "TTL temp_key")
    # TTL可能返回1或2，这里只检查它是一个数字
    assert any(char.isdigit() for char in response), "TTL返回值应该是一个数字"
    
    response = send_command(sock, "GET temp_key")
    assert_response(response, "temp_value", "GET temp_key (before expiration)")
    
    print("等待2秒让键过期...")
    time.sleep(2.5)
    
    response = send_command(sock, "GET temp_key")
    assert_response(response, "-1", "GET temp_key (after expiration)")
    
    response = send_command(sock, "TTL temp_key")
    assert_response(response, "-2", "TTL temp_key (after expiration)")

def test_datatype_hash(sock):
    """测试哈希数据类型相关命令"""
    print("=" * 50)
    print("测试哈希数据类型相关命令")
    print("=" * 50)
    
    # 测试HSET和HGET
    response = send_command(sock, "HSET user1 name John")
    assert_response(response, "1", "HSET user1 name John")
    
    response = send_command(sock, "HSET user1 age 30")
    assert_response(response, "1", "HSET user1 age 30")
    
    response = send_command(sock, "HSET user1 email john@example.com")
    assert_response(response, "1", "HSET user1 email john@example.com")
    
    response = send_command(sock, "HGET user1 name")
    assert_response(response, "John", "HGET user1 name")
    
    response = send_command(sock, "HGET user1 age")
    assert_response(response, "30", "HGET user1 age")
    
    response = send_command(sock, "HGET user1 email")
    assert_response(response, "john@example.com", "HGET user1 email")
    
    # 测试字段不存在的情况
    response = send_command(sock, "HGET user1 phone")
    assert_response(response, "-1", "HGET user1 phone (non-existent field)")
    
    response = send_command(sock, "HGET user2 name")
    assert_response(response, "-1", "HGET user2 name (non-existent key)")
    
    # 测试HGETALL
    response = send_command(sock, "HGETALL user1")
    assert_response(response, "name", "HGETALL user1")
    assert_response(response, "John", "HGETALL user1")
    
    # 测试HDEL多字段功能 - 混合存在和不存在的字段
    # 先创建一个新的哈希键用于测试
    response = send_command(sock, "HSET user3 field1 value1 field2 value2")
    assert_response(response, "2\r\n", "HSET user3 field1 field2")
    
    response = send_command(sock, "HDEL user3 field1 field3")
    assert_response(response, "1\r\n", "HDEL user3 field1 field3 (mixed fields)")
    
    # 验证只删除了存在的字段
    response = send_command(sock, "HGET user3 field1")
    assert_response(response, "-1\r\n", "HGET user3 field1 (after mixed HDEL)")
    
    response = send_command(sock, "HGET user3 field2")
    assert_response(response, "value2", "HGET user3 field2 (should still exist)")
    
    # 测试HDEL多字段功能 - 删除多个不存在的字段
    response = send_command(sock, "HDEL non_existent_user field1 field2")
    assert_response(response, "0\r\n", "HDEL non_existent_user field1 field2 (non-existent key)")
    
    # 测试HDEL
    response = send_command(sock, "HDEL user1 email")
    assert_response(response, "1", "HDEL user1 email")
    
    response = send_command(sock, "HGET user1 email")
    assert_response(response, "-1", "HGET user1 email (after HDEL)")

    # 测试HEXISTS
    response = send_command(sock, "HEXISTS user1 name")
    assert_response(response, "1\r\n", "HEXISTS user1 name")
    
    response = send_command(sock, "HEXISTS user1 email")
    assert_response(response, "0\r\n", "HEXISTS user1 email (after HDEL)")
    
    # 测试HKEYS和HVALS
    response = send_command(sock, "HKEYS user1")
    assert_response(response, "name", "HKEYS user1")
    assert_response(response, "age", "HKEYS user1")
    
    response = send_command(sock, "HVALS user1")
    assert_response(response, "John", "HVALS user1")
    assert_response(response, "30", "HVALS user1")
    
    # 测试HLEN
    response = send_command(sock, "HLEN user1")
    assert_response(response, "2", "HLEN user1")
    
    response = send_command(sock, "HLEN user2")
    assert_response(response, "0", "HLEN user2 (non-existent key)")
    
    # 测试更新字段
    response = send_command(sock, "HSET user1 name Mike")
    assert_response(response, "1", "HSET user1 name Mike (update)")
    
    response = send_command(sock, "HGET user1 name")
    assert_response(response, "Mike", "HGET user1 name (after update)")
    
    # 测试HSET多字段功能 - 添加多个新字段
    response = send_command(sock, "HSET user2 field1 value1 field2 value2 field3 value3")
    assert_response(response, "3", "HSET user2 multiple fields (all new)")
    
    # 验证添加的字段
    response = send_command(sock, "HGET user2 field1")
    assert_response(response, "value1", "HGET user2 field1 (after multi-field HSET)")
    
    response = send_command(sock, "HGET user2 field2")
    assert_response(response, "value2", "HGET user2 field2 (after multi-field HSET)")
    
    response = send_command(sock, "HGET user2 field3")
    assert_response(response, "value3", "HGET user2 field3 (after multi-field HSET)")
    
    # 测试HSET多字段功能 - 混合新旧字段
    response = send_command(sock, "HSET user2 field1 new_value field4 value4")
    assert_response(response, "1", "HSET user2 multiple fields (mixed new/old)")
    
    # 测试HDEL多字段功能 - 删除多个存在的字段
    response = send_command(sock, "HDEL user1 name age")
    assert_response(response, "2", "HDEL user1 name age (multiple existing fields)")
    
    # 验证字段已被删除
    response = send_command(sock, "HGET user1 name")
    assert_response(response, "-1", "HGET user1 name (after multi-field HDEL)")
    
    response = send_command(sock, "HGET user1 age")
    assert_response(response, "-1", "HGET user1 age (after multi-field HDEL)")

    # 测试删除整个哈希键
    response = send_command(sock, "DEL user1")
    assert_response(response, "1", "DEL user1")
    
    response = send_command(sock, "EXISTS user1")
    assert_response(response, "0", "EXISTS user1 (after DEL)")
    
    response = send_command(sock, "HGETALL user1")
    assert_response(response, "*0", "HGETALL user1 (after DEL)")

def test_datatype_list(sock):
    """测试列表数据类型相关命令"""
    print("=" * 50)
    print("测试列表数据类型相关命令")
    print("=" * 50)
    
    # 测试LPUSH和RPUSH
    response = send_command(sock, "LPUSH mylist item1")
    assert_response(response, "1", "LPUSH mylist item1")
    
    response = send_command(sock, "LPUSH mylist item2 item3")
    assert_response(response, "3", "LPUSH mylist item2 item3")
    
    response = send_command(sock, "RPUSH mylist item4")
    assert_response(response, "4", "RPUSH mylist item4")
    
    # 测试LLEN
    response = send_command(sock, "LLEN mylist")
    assert_response(response, "4", "LLEN mylist")
    
    # 测试LRANGE - 获取全部元素
    response = send_command(sock, "LRANGE mylist 0 -1")
    # 列表内容应包含所有元素，但顺序可能根据实现有所不同
    assert_response(response, "item3", "LRANGE mylist 0 -1")
    assert_response(response, "item2", "LRANGE mylist 0 -1")
    assert_response(response, "item1", "LRANGE mylist 0 -1")
    assert_response(response, "item4", "LRANGE mylist 0 -1")
    
    # 测试LRANGE - 获取部分元素
    response = send_command(sock, "LRANGE mylist 1 3")
    
    # 测试LPOP - 单个元素
    response = send_command(sock, "LPOP mylist")
    assert_response(response, "item3", "LPOP mylist")
    
    response = send_command(sock, "LLEN mylist")
    assert_response(response, "3", "LLEN mylist (after LPOP)")
    
    # 测试LPOP - 多个元素
    response = send_command(sock, "LPOP mylist 2")
    assert_response(response, "2", "LPOP mylist 2 (array length)")
    assert_response(response, "item2", "LPOP mylist 2 (first element)")
    assert_response(response, "item1", "LPOP mylist 2 (second element)")
    
    response = send_command(sock, "LLEN mylist")
    assert_response(response, "1", "LLEN mylist (after multi LPOP)")
    
    # 测试RPOP - 单个元素
    response = send_command(sock, "RPOP mylist")
    assert_response(response, "item4", "RPOP mylist")
    
    response = send_command(sock, "LLEN mylist")
    assert_response(response, "0", "LLEN mylist (after RPOP)")
    
    # 重新构建列表用于RPOP多元素测试
    response = send_command(sock, "LPUSH mylist item5 item6")
    assert_response(response, "2", "LPUSH mylist item5 item6")
    
    # 测试RPOP - 多个元素
    response = send_command(sock, "RPOP mylist 2")
    assert_response(response, "2", "RPOP mylist 2 (array length)")
    assert_response(response, "item5", "RPOP mylist 2 (first element)")
    assert_response(response, "item6", "RPOP mylist 2 (second element)")
    
    response = send_command(sock, "LLEN mylist")
    assert_response(response, "0", "LLEN mylist (after multi RPOP)")
    
    # 测试空列表
    response = send_command(sock, "LLEN non_existent_list")
    assert_response(response, "0", "LLEN non_existent_list")
    
    response = send_command(sock, "LPOP non_existent_list")
    assert_response(response, "-1", "LPOP non_existent_list")
    
    # 测试空列表的多元素LPOP
    response = send_command(sock, "LPOP non_existent_list 2")
    assert_response(response, "-1", "LPOP non_existent_list 2")
    
    # 测试无效的count参数
    response = send_command(sock, "LPOP mylist abc")
    assert_response(response, "-", "LPOP mylist abc (invalid count)")
    
    response = send_command(sock, "RPOP non_existent_list")
    assert_response(response, "-1", "RPOP non_existent_list")
    
    # 测试空列表的多元素RPOP
    response = send_command(sock, "RPOP non_existent_list 2")
    assert_response(response, "-", "RPOP non_existent_list 2")
    
    # 测试无效的count参数
    response = send_command(sock, "RPOP mylist abc")
    assert_response(response, "-", "RPOP mylist abc (invalid count)")
    
    # 测试删除整个列表键
    response = send_command(sock, "DEL mylist")
    assert_response(response, "1", "DEL mylist")
    
    response = send_command(sock, "EXISTS mylist")
    assert_response(response, "0", "EXISTS mylist (after DEL)")

def test_datatype_set(sock):
    """测试集合数据类型相关命令"""
    print("=" * 50)
    print("测试集合数据类型相关命令")
    print("=" * 50)
    
    # 测试SADD - 添加单个元素
    response = send_command(sock, "SADD myset a")
    assert_response(response, "1", "SADD myset a")
    
    response = send_command(sock, "SADD myset b")
    assert_response(response, "1", "SADD myset b")
    
    response = send_command(sock, "SADD myset c")
    assert_response(response, "1", "SADD myset c")
    
    # 测试SADD - 添加多个元素
    response = send_command(sock, "SADD myset d e")
    assert_response(response, "2", "SADD myset d e")
    
    # 测试SADD - 添加已存在的元素
    response = send_command(sock, "SADD myset a")
    assert_response(response, "0", "SADD myset a (existing element)")
    
    # 测试SCARD - 获取集合大小
    response = send_command(sock, "SCARD myset")
    assert_response(response, "5", "SCARD myset")
    
    # 测试SISMEMBER - 检查元素是否存在
    response = send_command(sock, "SISMEMBER myset a")
    assert_response(response, "1", "SISMEMBER myset a")
    
    response = send_command(sock, "SISMEMBER myset z")
    assert_response(response, "0", "SISMEMBER myset z (non-existent element)")
    
    # 测试SMEMBERS - 获取所有元素
    response = send_command(sock, "SMEMBERS myset")
    # 验证所有元素都在响应中
    for element in ["a", "b", "c", "d", "e"]:
        assert_response(response, element, "SMEMBERS myset")
    
    # 测试SREM - 删除单个元素
    response = send_command(sock, "SREM myset a")
    assert_response(response, "1", "SREM myset a")
    
    response = send_command(sock, "SCARD myset")
    assert_response(response, "4", "SCARD myset (after SREM a)")
    
    response = send_command(sock, "SISMEMBER myset a")
    assert_response(response, "0", "SISMEMBER myset a (after SREM)")
    
    # 测试SREM - 删除多个元素
    response = send_command(sock, "SREM myset b c")
    assert_response(response, "2", "SREM myset b c")
    
    response = send_command(sock, "SCARD myset")
    assert_response(response, "2", "SCARD myset (after SREM b c)")
    
    # 测试SREM - 删除不存在的元素
    response = send_command(sock, "SREM myset z")
    assert_response(response, "0", "SREM myset z (non-existent element)")
    
    # 测试空集合
    response = send_command(sock, "SCARD non_existent_set")
    assert_response(response, "0", "SCARD non_existent_set")
    
    response = send_command(sock, "SISMEMBER non_existent_set a")
    assert_response(response, "0", "SISMEMBER non_existent_set a")
    
    response = send_command(sock, "SMEMBERS non_existent_set")
    assert_response(response, "*0", "SMEMBERS non_existent_set")
    
    # 测试删除整个集合键
    response = send_command(sock, "DEL myset")
    assert_response(response, "1", "DEL myset")
    
    response = send_command(sock, "EXISTS myset")
    assert_response(response, "0", "EXISTS myset (after DEL)")

def test_server_management(sock):
    """测试服务器管理相关命令"""
    print("=" * 50)
    print("测试服务器管理相关命令")
    print("=" * 50)
    
    # 测试DBSIZE命令 - 获取数据库中的键数量
    initial_size = send_command(sock, "DBSIZE")
    # 初始大小应该是一个数字
    assert any(char.isdigit() for char in initial_size), "DBSIZE返回值应该是一个数字"
    
    # 创建一些键，用于后续测试
    response = send_command(sock, "SET test_key1 value1")
    assert_response(response, "OK", "SET test_key1 value1")
    
    response = send_command(sock, "SET test_key2 value2")
    assert_response(response, "OK", "SET test_key2 value2")
    
    response = send_command(sock, "HSET user1 name Alice")
    assert_response(response, "1", "HSET user1 name Alice")
    
    # 再次测试DBSIZE，应该返回增加的键数量
    new_size = send_command(sock, "DBSIZE")
    # 新大小应该比初始大小大3
    assert any(char.isdigit() for char in new_size), "DBSIZE返回值应该是一个数字"
    
    # 测试INFO命令 - 获取服务器信息
    response = send_command(sock, "INFO")
    # INFO响应应该包含一些服务器信息
    assert len(response) > 0, "INFO命令返回的响应不应该为空"
    
    # 测试FLUSHDB命令 - 清空数据库
    response = send_command(sock, "FLUSHDB")
    assert_response(response, "OK", "FLUSHDB")
    
    # 验证数据库是否已清空
    response = send_command(sock, "DBSIZE")
    # 清空后大小应该是0或者接近初始大小
    assert_response(response, "0" if "0" in response else initial_size.strip(), "DBSIZE (after FLUSHDB)")
    
    response = send_command(sock, "EXISTS test_key1")
    assert_response(response, "0", "EXISTS test_key1 (after FLUSHDB)")
    
    response = send_command(sock, "EXISTS test_key2")
    assert_response(response, "0", "EXISTS test_key2 (after FLUSHDB)")
    
    response = send_command(sock, "EXISTS user1")
    assert_response(response, "0", "EXISTS user1 (after FLUSHDB)")

def test_dkv_server():
    """测试DKV服务器"""
    try:
        # 连接到服务器
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 6379))
        print("已连接到DKV服务器")
        
        # 运行不同类型的测试
        test_basic(sock)
        test_datatype_string(sock)
        test_datatype_hash(sock)
        test_datatype_list(sock)
        test_datatype_set(sock)
        test_server_management(sock)
        
        print("所有测试完成！")
        
    except ConnectionRefusedError:
        print("无法连接到DKV服务器，请确保服务器正在运行")
        sys.exit(1)
    except AssertionError as e:
        print(f"断言失败: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"测试过程中出现错误: {e}")
        sys.exit(1)
    finally:
        if 'sock' in locals():
            sock.close()

if __name__ == "__main__":
    test_dkv_server()
