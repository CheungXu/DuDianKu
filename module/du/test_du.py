#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
牍层测试脚本

验证写入接口、回调机制等功能
"""

import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from module.du import DuDatabase, DuRecord, DuManager


def test_write_interfaces():
    """测试写入接口"""
    print("=" * 60)
    print("测试1: 写入接口")
    print("=" * 60)
    
    manager = DuManager("data/test_dudianku.db")
    
    # 1. Agent发送消息
    send_id = manager.insert_send(
        content="你好，我是AI助手，有什么可以帮你的？",
        agent_id="agent_001",
        sender_name="AI助手"
    )
    print(f"✓ 发送入库: {send_id}")
    
    # 2. Agent接收消息
    receive_id = manager.insert_receive(
        content="请帮我写一段Python代码",
        agent_id="agent_001",
        sender_id="user_001",
        sender_name="张三"
    )
    print(f"✓ 接收入库: {receive_id}")
    
    # 3. 批量导入
    batch_records = [
        {
            "content": "历史消息1：你好",
            "agent_id": "agent_001",
            "direction": "receive",
            "sender_id": "user_001",
            "timestamp": "2026-03-30T10:00:00"
        },
        {
            "content": "历史消息2：你好，有什么可以帮你的？",
            "agent_id": "agent_001",
            "direction": "send",
            "timestamp": "2026-03-30T10:01:00"
        },
        {
            "content": "历史消息3：帮我写个函数",
            "agent_id": "agent_001",
            "direction": "receive",
            "sender_id": "user_001",
            "timestamp": "2026-03-30T10:02:00"
        },
    ]
    
    result = manager.insert_batch(batch_records, batch_id="batch_20260331")
    print(f"✓ 批量导入: 成功 {result['success']} 条, 失败 {result['failed']} 条")
    print(f"  批次ID: {result['batch_id']}")
    print(f"  记录IDs: {result['record_ids']}")
    print()
    
    return manager, send_id, receive_id


def test_query_interfaces(manager: DuManager):
    """测试查询接口"""
    print("=" * 60)
    print("测试2: 查询接口")
    print("=" * 60)
    
    # 1. 按ID查询
    records = manager.get_recent(limit=5)
    if records:
        record = manager.get_by_id(records[0].id)
        print(f"✓ 按ID查询: {record.id[:8]}... - {record.content[:30]}...")
    
    # 2. 按时间范围查询
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    records = manager.get_by_timestamp_range(
        f"{today}T00:00:00",
        f"{today}T23:59:59"
    )
    print(f"✓ 时间范围查询: 找到 {len(records)} 条记录")
    
    # 3. 按Agent查询
    records = manager.get_by_agent("agent_001", limit=10)
    print(f"✓ 按Agent查询: 找到 {len(records)} 条记录")
    
    # 4. 内容搜索
    records = manager.search_content("Python", limit=5)
    print(f"✓ 内容搜索: 找到 {len(records)} 条包含'Python'的记录")
    print()


def test_heat_calculation(manager: DuManager):
    """测试热度计算"""
    print("=" * 60)
    print("测试3: 热度计算")
    print("=" * 60)
    
    # 获取一条记录
    records = manager.get_recent(limit=1)
    if not records:
        print("没有记录可测试")
        return
    
    record_id = records[0].id
    
    # 模拟多次访问
    for _ in range(5):
        manager.get_by_id(record_id)
    
    # 重新计算热度
    heat_score = manager.recalculate_heat(record_id)
    
    record = manager.get_by_id(record_id)
    print(f"✓ 热度计算完成:")
    print(f"  访问次数: {record.access_count}")
    print(f"  热度评分: {record.heat_score}")
    print(f"  热度等级: {record.heat_level}")
    print(f"  衰减因子: {record.decay_factor}")
    print()


def test_callback_mechanism():
    """测试回调机制"""
    print("=" * 60)
    print("测试4: 回调机制")
    print("=" * 60)
    
    insert_records = []
    batch_results = []
    
    def on_insert(record):
        insert_records.append(record)
        print(f"  [回调] 新记录入库: {record.id[:8]}...")
    
    def on_batch(result):
        batch_results.append(result)
        print(f"  [回调] 批量完成: 成功 {result['success']} 条")
    
    manager = DuManager(
        db_path="data/test_dudianku.db",
        on_insert=on_insert,
        on_batch=on_batch
    )
    
    # 写入会触发回调
    manager.insert_send("测试回调1", "agent_002")
    manager.insert_receive("测试回调2", "agent_002", sender_id="user_002")
    
    print(f"✓ 回调触发成功: insert回调 {len(insert_records)} 次")
    print()


def test_hook_decorator():
    """测试Hook装饰器"""
    print("=" * 60)
    print("测试5: Hook装饰器")
    print("=" * 60)
    
    manager = DuManager("data/test_dudianku.db")
    
    # 注册Hook
    @manager.hook('after_insert')
    def log_insert(record):
        print(f"  [Hook] 记录已写入: {record.id[:8]}...")
    
    @manager.hook('after_insert')
    def update_stats(record):
        print(f"  [Hook] 更新统计: {record.sender_type}")
    
    # 写入会触发所有Hook
    manager.insert_send("测试Hook", "agent_003")
    
    print("✓ Hook装饰器测试通过")
    print()


def test_stats(manager: DuManager):
    """测试统计信息"""
    print("=" * 60)
    print("测试6: 统计信息")
    print("=" * 60)
    
    stats = manager.get_stats()
    
    print(f"总记录数: {stats.get('total_records', 0)}")
    print(f"按状态分布: {stats.get('by_status', {})}")
    print(f"按热度等级分布: {stats.get('by_heat_level', {})}")
    print(f"按来源分布: {stats.get('by_source', {})}")
    print(f"数据库大小: {stats.get('db_size_mb', 0)} MB")
    print("✓ 统计信息获取成功")
    print()


def cleanup():
    """清理测试数据"""
    import os
    test_db = "data/test_dudianku.db"
    if os.path.exists(test_db):
        os.remove(test_db)
        print(f"已清理测试数据库: {test_db}")


def main():
    """运行所有测试"""
    print("\n" + "=" * 60)
    print("       牍(Du)层写入接口测试")
    print("=" * 60 + "\n")
    
    try:
        # 运行测试
        manager, send_id, receive_id = test_write_interfaces()
        test_query_interfaces(manager)
        test_heat_calculation(manager)
        test_callback_mechanism()
        test_hook_decorator()
        test_stats(manager)
        
        print("=" * 60)
        print("       所有测试通过 ✓")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        cleanup()


if __name__ == "__main__":
    main()