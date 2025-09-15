#!/usr/bin/env python3
"""
简单的DKV客户端测试脚本
使用socket连接到DKV服务器并发送RESP协议命令
"""

import socket
import time

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
    print()
    return response

def test_basic(sock):
    """测试基本命令"""
    print("=" * 50)
    print("测试基本命令")
    print("=" * 50)
    
    send_command(sock, "SET test_key hello_world")
    send_command(sock, "GET test_key")
    send_command(sock, "EXISTS test_key")
    send_command(sock, "SET counter 100")
    send_command(sock, "INCR counter")
    send_command(sock, "GET counter")
    send_command(sock, "DECR counter")
    send_command(sock, "GET counter")
    send_command(sock, "DEL test_key")
    send_command(sock, "GET test_key")
    send_command(sock, "EXISTS test_key")

def test_datatype_string(sock):
    """测试字符串数据类型相关命令"""
    print("=" * 50)
    print("测试字符串数据类型相关命令")
    print("=" * 50)
    
    # 测试过期时间
    send_command(sock, "SET temp_key temp_value")
    send_command(sock, "EXPIRE temp_key 2")
    send_command(sock, "TTL temp_key")
    send_command(sock, "GET temp_key")
    
    print("等待2秒让键过期...")
    time.sleep(2.5)
    
    send_command(sock, "GET temp_key")
    send_command(sock, "TTL temp_key")

def test_datatype_hash(sock):
    """测试哈希数据类型相关命令"""
    print("=" * 50)
    print("测试哈希数据类型相关命令")
    print("=" * 50)
    
    # 测试HSET和HGET
    send_command(sock, "HSET user1 name John")
    send_command(sock, "HSET user1 age 30")
    send_command(sock, "HSET user1 email john@example.com")
    send_command(sock, "HGET user1 name")
    send_command(sock, "HGET user1 age")
    send_command(sock, "HGET user1 email")
    
    # 测试字段不存在的情况
    send_command(sock, "HGET user1 phone")
    send_command(sock, "HGET user2 name")
    
    # 测试HGETALL
    send_command(sock, "HGETALL user1")
    
    # 测试HDEL
    send_command(sock, "HDEL user1 email")
    send_command(sock, "HGET user1 email")
    
    # 测试HEXISTS
    send_command(sock, "HEXISTS user1 name")
    send_command(sock, "HEXISTS user1 email")
    
    # 测试HKEYS和HVALS
    send_command(sock, "HKEYS user1")
    send_command(sock, "HVALS user1")
    
    # 测试HLEN
    send_command(sock, "HLEN user1")
    send_command(sock, "HLEN user2")
    
    # 测试更新字段
    send_command(sock, "HSET user1 name Mike")
    send_command(sock, "HGET user1 name")
    
    # 测试删除整个哈希键
    send_command(sock, "DEL user1")
    send_command(sock, "EXISTS user1")
    send_command(sock, "HGETALL user1")

def test_datatype_list(sock):
    """测试列表数据类型相关命令"""
    print("=" * 50)
    print("测试列表数据类型相关命令")
    print("=" * 50)
    
    # 测试LPUSH和RPUSH
    send_command(sock, "LPUSH mylist item1")
    send_command(sock, "LPUSH mylist item2 item3")
    send_command(sock, "RPUSH mylist item4")
    
    # 测试LLEN
    send_command(sock, "LLEN mylist")
    
    # 测试LRANGE - 获取全部元素
    send_command(sock, "LRANGE mylist 0 -1")
    
    # 测试LRANGE - 获取部分元素
    send_command(sock, "LRANGE mylist 1 3")
    
    # 测试LPOP
    send_command(sock, "LPOP mylist")
    send_command(sock, "LLEN mylist")
    
    # 测试RPOP
    send_command(sock, "RPOP mylist")
    send_command(sock, "LLEN mylist")
    
    # 测试空列表
    send_command(sock, "LLEN non_existent_list")
    send_command(sock, "LPOP non_existent_list")
    send_command(sock, "RPOP non_existent_list")
    
    # 测试删除整个列表键
    send_command(sock, "DEL mylist")
    send_command(sock, "EXISTS mylist")

def test_datatype_set(sock):
    """测试集合数据类型相关命令"""
    print("=" * 50)
    print("测试集合数据类型相关命令")
    print("=" * 50)
    
    # 测试SADD - 添加单个元素
    send_command(sock, "SADD myset a")
    send_command(sock, "SADD myset b")
    send_command(sock, "SADD myset c")
    
    # 测试SADD - 添加多个元素
    send_command(sock, "SADD myset d e")
    
    # 测试SADD - 添加已存在的元素
    send_command(sock, "SADD myset a")
    
    # 测试SCARD - 获取集合大小
    send_command(sock, "SCARD myset")
    
    # 测试SISMEMBER - 检查元素是否存在
    send_command(sock, "SISMEMBER myset a")
    send_command(sock, "SISMEMBER myset z")
    
    # 测试SMEMBERS - 获取所有元素
    send_command(sock, "SMEMBERS myset")
    
    # 测试SREM - 删除单个元素
    send_command(sock, "SREM myset a")
    send_command(sock, "SCARD myset")
    send_command(sock, "SISMEMBER myset a")
    
    # 测试SREM - 删除多个元素
    send_command(sock, "SREM myset b c")
    send_command(sock, "SCARD myset")
    
    # 测试SREM - 删除不存在的元素
    send_command(sock, "SREM myset z")
    
    # 测试空集合
    send_command(sock, "SCARD non_existent_set")
    send_command(sock, "SISMEMBER non_existent_set a")
    send_command(sock, "SMEMBERS non_existent_set")
    
    # 测试删除整个集合键
    send_command(sock, "DEL myset")
    send_command(sock, "EXISTS myset")

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
        
        print("所有测试完成！")
        
    except ConnectionRefusedError:
        print("无法连接到DKV服务器，请确保服务器正在运行")
    except Exception as e:
        print(f"测试过程中出现错误: {e}")
    finally:
        if 'sock' in locals():
            sock.close()

if __name__ == "__main__":
    test_dkv_server()
