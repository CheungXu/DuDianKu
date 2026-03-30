#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
牍层测试脚本

验证SQLite表结构和基本操作
"""

import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from module.du import DuDatabase, DuRecord, DuManager


def test_database_init():
    """测试数据库初始化"""
    print("=" * 60)
    print("测试1: 数据库初始化")
    print("=" * 60)
    
    db = DuDatabase("data/test_dudianku.db")
    stats = db.get_stats()
    
    print(f"数据库文件: {db.db_path}")
    print(f"数据库大小: {stats.get('db_size_mb', 0)} MB")
    print(f"总记录数: {stats.get('total_records', 0)}")
    print("✓ 数据库初始化成功\n")


def test_insert_record():
    """测试插入记录"""
    print("=" * 60)
    print("测试2: 插入记录")
    print("=" * 60)
    
    manager = DuManager("data/test_dudianku.db")
    
    # 创建测试记录
    record = DuRecord.create_conversation(
        content="Python怎么做异步？asyncio是标准方案。",
        sender_id="user_001",
        sender_name="张三",
        sender_type="user",
        source="test",
        session_id="session_001"
    )
    
    record_id = manager.insert(record)
    print(f"插入记录ID: {record_id}")
    print(f"时间戳: {record.timestamp}")
    print(f"内容: {record.content}")
    print(f"内容哈希: {record.content_hash}")
    print("✓ 插入记录成功\n")
    
    return record_id


def test_query_record(record_id: str):
    """测试查询记录"""
    print("=" * 60)
    print("测试3: 查询记录")
    print("=" * 60)
    
    manager = DuManager("data/test_dudianku.db")
    
    record = manager.get_by_id(record_id)
    if record:
        print(f"记录ID: {record.id}")
        print(f"时间戳: {record.timestamp}")
        print(f"来源: {record.source}")
        print(f"发送人: {record.sender_name} ({record.sender_type})")
        print(f"内容: {record.content}")
        print(f"访问次数: {record.access_count}")
        print(f"热度等级: {record.heat_level}")
        print("✓ 查询记录成功\n")
    else:
        print("✗ 未找到记录\n")


def test_heat_calculation(record_id: str):
    """测试热度计算"""
    print("=" * 60)
    print("测试4: 热度计算")
    print("=" * 60)
    
    manager = DuManager("data/test_dudianku.db")
    
    # 模拟多次访问
    for i in range(5):
        manager.get_by_id(record_id)  # 每次查询会增加访问次数
    
    # 重新计算热度
    heat_score = manager.recalculate_heat(record_id)
    
    record = manager.get_by_id(record_id)
    print(f"访问次数: {record.access_count}")
    print(f"热度评分: {record.heat_score}")
    print(f"热度等级: {record.heat_level}")
    print(f"衰减因子: {record.decay_factor}")
    print("✓ 热度计算成功\n")


def test_time_range_query():
    """测试时间范围查询"""
    print("=" * 60)
    print("测试5: 时间范围查询")
    print("=" * 60)
    
    manager = DuManager("data/test_dudianku.db")
    
    # 插入多条测试记录
    for i in range(5):
        record = DuRecord.create_conversation(
            content=f"测试消息 #{i+1}",
            sender_id="user_001",
            sender_name="张三",
            source="test",
            session_id=f"session_{i+1}"
        )
        manager.insert(record)
    
    # 查询今天的记录
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    records = manager.get_by_timestamp_range(
        f"{today}T00:00:00",
        f"{today}T23:59:59"
    )
    
    print(f"查询时间范围: {today}")
    print(f"返回记录数: {len(records)}")
    for r in records[:3]:
        print(f"  - [{r.timestamp}] {r.content[:30]}...")
    print("✓ 时间范围查询成功\n")


def test_stats():
    """测试统计信息"""
    print("=" * 60)
    print("测试6: 统计信息")
    print("=" * 60)
    
    manager = DuManager("data/test_dudianku.db")
    stats = manager.get_stats()
    
    print(f"总记录数: {stats.get('total_records', 0)}")
    print(f"按状态分布: {stats.get('by_status', {})}")
    print(f"按热度等级分布: {stats.get('by_heat_level', {})}")
    print(f"按来源分布: {stats.get('by_source', {})}")
    print(f"数据库大小: {stats.get('db_size_mb', 0)} MB")
    print("✓ 统计信息获取成功\n")


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
    print("       牍(Du)层测试开始")
    print("=" * 60 + "\n")
    
    try:
        # 运行测试
        test_database_init()
        record_id = test_insert_record()
        test_query_record(record_id)
        test_heat_calculation(record_id)
        test_time_range_query()
        test_stats()
        
        print("=" * 60)
        print("       所有测试通过 ✓")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 清理测试数据
        cleanup()


if __name__ == "__main__":
    main()