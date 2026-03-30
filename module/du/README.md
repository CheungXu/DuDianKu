# 牍(Du) - 最小记忆单元

> Agent长期记忆系统的底层存储，全量记录每一次交互。

---

## 快速开始

```python
from module.du import DuManager

# 初始化
manager = DuManager("data/memory.db")

# Agent发送消息
manager.insert_send("你好，我是AI助手", agent_id="agent_001")

# Agent接收消息  
manager.insert_receive("请帮我写代码", agent_id="agent_001", sender_id="user_001")

# 查询最近对话
records = manager.get_recent(limit=10)
```

---

## 核心功能

### 写入

| 接口 | 用途 | 示例 |
|------|------|------|
| `insert_send()` | Agent发出的消息 | 用户提问后Agent回复 |
| `insert_receive()` | Agent收到的消息 | 用户发送的指令 |
| `insert_batch()` | 批量导入历史 | 从其他系统迁移数据 |

### 查询

| 接口 | 用途 |
|------|------|
| `get_by_id()` | 按ID获取单条 |
| `get_recent()` | 最近N条记录 |
| `get_by_session()` | 按会话查询 |
| `get_by_agent()` | 按Agent查询 |
| `search_content()` | 内容搜索 |

### 热度管理

自动追踪记录的访问热度：

```
热度等级:
  hot  (≥80分) - 常用记忆，优先检索
  warm (≥40分) - 中等热度
  cold (<40分) - 冷数据，可归档

热度公式:
  heat_score = access_count × decay_factor × 10
  decay_factor = exp(-天数/30)  # 30天衰减周期
```

---

## 回调机制

支持写入后的自定义处理：

```python
# 方式1: 构造时传入
manager = DuManager(
    db_path="data/memory.db",
    on_insert=lambda r: print(f"新记录: {r.id}")
)

# 方式2: Hook装饰器
@manager.hook('after_insert')
def process_record(record):
    # 触发典层处理等
    pass
```

---

## 数据结构

每条记录包含：

```
核心: id, timestamp, content
来源: agent_id, session_id, channel
内容: content, content_hash, content_type
热度: access_count, heat_score, heat_level
扩展: metadata, tags, status
```

---

## 存储位置

```
data/dudianku.db  # SQLite数据库
```

无第三方依赖，纯Python实现。

---

## 文件说明

```
module/du/
├── __init__.py    # 导出接口
├── models.py      # 数据模型
├── database.py    # 数据库操作
├── manager.py     # 核心管理器
└── test_du.py     # 测试脚本
```

---

## 下一步

- [典层] 关键信息提取、向量化存储
- [库层] 知识图谱构建