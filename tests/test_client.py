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

def test_dkv_server():
    """测试DKV服务器"""
    try:
        # 连接到服务器
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 6379))
        print("已连接到DKV服务器")
        print("=" * 50)
        
        # 测试基本命令
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
        
        # 测试过期时间
        send_command(sock, "SET temp_key temp_value")
        send_command(sock, "EXPIRE temp_key 2")
        send_command(sock, "TTL temp_key")
        send_command(sock, "GET temp_key")
        
        print("等待2秒让键过期...")
        time.sleep(2.5)
        
        send_command(sock, "GET temp_key")
        send_command(sock, "TTL temp_key")
        
        print("测试完成！")
        
    except ConnectionRefusedError:
        print("无法连接到DKV服务器，请确保服务器正在运行")
    except Exception as e:
        print(f"测试过程中出现错误: {e}")
    finally:
        if 'sock' in locals():
            sock.close()

if __name__ == "__main__":
    test_dkv_server()
