# 牍(Du)层 - 最小记忆单元

> 原始数据载体，全量存储Agent的交互记录、碎片化信息等最基础的记忆颗粒。

---

## 一、设计原则

```
1. 轻量级：最小依赖，纯Python实现
2. 可扩展：支持任意Agent/模型接入
3. 解耦合：写入逻辑独立，回调灵活
4. 自动化：哈希、热度等自动计算
```

---

## 二、表结构设计

### 2.1 du_raw 表（28个字段）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              du_raw 表结构                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  核心标识 (3)                                                                │
│  ├── id              TEXT PRIMARY KEY     -- 全局唯一ID                     │
│  ├── timestamp       TEXT NOT NULL        -- 原始时间戳 (ISO 8601)          │
│  └── created_at      TEXT                 -- 入库时间                       │
│                                                                             │
│  来源追溯 (6)                                                                │
│  ├── source          TEXT                 -- 来源渠道 (agent/api/webhook)   │
│  ├── source_id       TEXT                 -- 来源系统消息ID                 │
│  ├── session_id      TEXT                 -- 会话ID                         │
│  ├── channel         TEXT                 -- 频道/Agent标识                 │
│  ├── thread_id       TEXT                 -- 线程ID                         │
│  └── parent_id       TEXT                 -- 父消息ID                       │
│                                                                             │
│  发送人/接收人 (5)                                                           │
│  ├── sender_id       TEXT                 -- 发送人ID                       │
│  ├── sender_name     TEXT                 -- 发送人名称                     │
│  ├── sender_type     TEXT                 -- 发送人类型 (agent/user/system) │
│  ├── receiver_id     TEXT                 -- 接收人ID                       │
│  └── receiver_name   TEXT                 -- 接收人名称                     │
│                                                                             │
│  内容 (6)                                                                    │
│  ├── type            TEXT                 -- 数据类型 (conversation/log)    │
│  ├── content_type    TEXT                 -- 内容类型 (text/json/card)      │
│  ├── content         TEXT                 -- 内容文本 (便于搜索)            │
│  ├── raw_content     TEXT                 -- 原始内容 (完整JSON)            │
│  ├── content_hash    TEXT                 -- 内容哈希 (SHA256前16位)        │
│  └── reply_to_id     TEXT                 -- 回复的消息ID                   │
│                                                                             │
│  热度统计 (5)                                                                │
│  ├── access_count    INTEGER DEFAULT 0    -- 访问次数                       │
│  ├── last_access_at  TEXT                 -- 最后访问时间                   │
│  ├── heat_score      REAL DEFAULT 0.0     -- 热度评分 (0.0-100.0)           │
│  ├── heat_level      TEXT DEFAULT 'cold'  -- 热度等级 (hot/warm/cold)       │
│  └── decay_factor    REAL DEFAULT 1.0     -- 衰减因子                       │
│                                                                             │
│  分类/标记 (4)                                                               │
│  ├── tags            TEXT                 -- 标签 (JSON array)              │
│  ├── category        TEXT                 -- 分类                           │
│  ├── importance_hint REAL DEFAULT 0.5     -- 重要性提示 (0.0-1.0)           │
│  └── is_key_memory   INTEGER DEFAULT 0    -- 是否关键记忆 (0/1)             │
│                                                                             │
│  状态 (3)                                                                    │
│  ├── status          TEXT DEFAULT 'active' -- 状态 (active/archived)        │
│  ├── processed_at    TEXT                 -- 处理时间                       │
│  └── process_result  TEXT                 -- 处理结果 (生成的典ID)          │
│                                                                             │
│  扩展 (2)                                                                    │
│  ├── metadata        TEXT                 -- 扩展元数据 (JSON)              │
│  └── extra_data      TEXT                 -- 额外数据 (大字段)              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 索引设计

```sql
CREATE INDEX idx_du_timestamp    ON du_raw(timestamp);
CREATE INDEX idx_du_source       ON du_raw(source);
CREATE INDEX idx_du_session      ON du_raw(session_id);
CREATE INDEX idx_du_sender       ON du_raw(sender_id);
CREATE INDEX idx_du_type         ON du_raw(type);
CREATE INDEX idx_du_status       ON du_raw(status);
CREATE INDEX idx_du_heat_score   ON du_raw(heat_score);
CREATE INDEX idx_du_heat_level   ON du_raw(heat_level);
CREATE INDEX idx_du_content_hash ON du_raw(content_hash);
CREATE INDEX idx_du_created_at   ON du_raw(created_at);
```

---

## 三、写入接口设计

### 3.1 核心接口

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              写入接口                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. insert_send()     Agent发送消息入库                                     │
│     └── sender_type = "agent"                                               │
│     └── sender_id = agent_id                                                │
│                                                                             │
│  2. insert_receive()  Agent接收消息入库                                     │
│     └── receiver_id = agent_id                                              │
│     └── sender_type = "user" (或其他)                                       │
│                                                                             │
│  3. insert_batch()    批量导入历史数据                                      │
│     └── 支持指定 batch_id                                                   │
│     └── 返回成功/失败统计                                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 数据格式

#### 输入格式（最小必填）

```python
# 发送消息
{
    "content": str,           # 必填：消息内容
    "agent_id": str,          # 必填：Agent标识
    "session_id": str,        # 可选：会话ID（自动生成）
    "metadata": dict,         # 可选：扩展元数据
}

# 接收消息
{
    "content": str,           # 必填：消息内容
    "agent_id": str,          # 必填：Agent标识
    "sender_id": str,         # 可选：原始发送者ID
    "session_id": str,        # 可选：会话ID
    "metadata": dict,         # 可选：扩展元数据
}

# 批量导入
{
    "records": [
        {
            "content": str,
            "agent_id": str,
            "timestamp": str,    # 历史时间
            "direction": str,    # "send" / "receive"
            "sender_id": str,    # receive时需要
        }
    ],
    "batch_id": str,           # 可选：批次ID
}
```

#### 存储格式（自动补全）

```python
{
    # 核心标识（自动生成）
    "id": "uuid",
    "timestamp": "ISO8601",
    "created_at": "ISO8601",
    
    # 来源追溯
    "source": "agent",
    "source_id": "",
    "session_id": "自动生成/指定",
    "channel": agent_id,
    
    # 发送人（根据direction自动设置）
    # send:   sender_id = agent_id, sender_type = "agent"
    # receive: receiver_id = agent_id, sender_type = "user"
    "sender_id": str,
    "sender_type": str,
    "receiver_id": str,
    
    # 内容
    "type": "conversation",
    "content_type": "text",
    "content": str,
    "raw_content": str,
    "content_hash": "自动计算",
    
    # 热度（自动初始化）
    "access_count": 0,
    "heat_score": 0.0,
    "heat_level": "cold",
    "decay_factor": 1.0,
    
    # 扩展
    "metadata": {},
}
```

---

## 四、关联关系设计

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              关联关系                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  会话链: session_id → 同一会话的所有消息                                    │
│                                                                             │
│  回复链: parent_id / reply_to_id → 消息回复关系                            │
│                                                                             │
│  批次链: batch_id → 批量导入的批次标识                                      │
│                                                                             │
│  时间链: timestamp → 时间索引                                               │
│                                                                             │
│  Agent链: agent_id → 按Agent分组查询（存储在metadata中）                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 五、自动处理逻辑

### 5.1 处理流程

```
输入数据 → 字段补全 → 自动处理 → 存储 → 回调触发

具体步骤:
  1. 必填字段校验 (content, agent_id)
  2. 自动生成 (id, timestamp, content_hash)
  3. 自动初始化 (热度字段)
  4. 根据 direction 设置 sender/receiver
  5. 存入数据库
  6. 触发回调 (如有)
```

### 5.2 热度计算

```python
# 热度评分公式
heat_score = access_count * decay_factor * 10  # 放大系数

# 衰减因子计算 (30天衰减周期)
days_since_last_access = (now - last_access_at) / 86400
decay_factor = exp(-days_since_last_access / 30)

# 热度等级划分
if heat_score >= 80:
    heat_level = 'hot'     # 热数据
elif heat_score >= 40:
    heat_level = 'warm'    # 温数据
else:
    heat_level = 'cold'    # 冷数据
```

---

## 六、回调机制

### 6.1 方案：函数回调 + Hook装饰器

```python
# 方式1: 构造时传入
manager = DuManager(
    db_path="data/memory.db",
    on_insert=lambda r: print(f"新记录: {r.id}"),
    on_batch=lambda s: print(f"批量完成: {s}"),
)

# 方式2: 方法注册
manager.on_insert(callback_func)
manager.on_batch(callback_func)
manager.on_error(error_handler)

# 方式3: Hook装饰器
@manager.hook('after_insert')
def process_new_record(record):
    # 触发典层处理
    pass
```

### 6.2 Hook点

```
before_insert   - 写入前（可修改数据）
after_insert    - 写入后（通知处理）
before_batch    - 批量写入前
after_batch     - 批量写入后
on_error        - 错误处理
```

---

## 七、API总览

```python
class DuManager:
    """牍层管理器"""
    
    # ========== 写入接口 ==========
    
    def insert_send(content, agent_id, session_id=None, metadata=None, **kwargs) -> str:
        """Agent发送消息入库"""
        pass
    
    def insert_receive(content, agent_id, sender_id=None, session_id=None, metadata=None, **kwargs) -> str:
        """Agent接收消息入库"""
        pass
    
    def insert_batch(records, batch_id=None) -> dict:
        """批量导入"""
        pass
    
    # ========== 查询接口 ==========
    
    def get_by_id(record_id) -> DuRecord:
        """根据ID获取"""
        pass
    
    def get_by_session(session_id, limit=100) -> List[DuRecord]:
        """根据会话ID获取"""
        pass
    
    def get_by_timestamp_range(start, end, limit=100) -> List[DuRecord]:
        """时间范围查询"""
        pass
    
    def get_recent(limit=50) -> List[DuRecord]:
        """获取最近记录"""
        pass
    
    def get_hot_records(limit=20) -> List[DuRecord]:
        """获取热门记录"""
        pass
    
    # ========== 热度管理 ==========
    
    def recalculate_heat(record_id) -> float:
        """重新计算热度"""
        pass
    
    def batch_recalculate_heat() -> int:
        """批量重算热度"""
        pass
    
    # ========== 回调注册 ==========
    
    def on_insert(callback):
        """注册写入后回调"""
        pass
    
    def on_batch(callback):
        """注册批量写入后回调"""
        pass
    
    def on_error(callback):
        """注册错误回调"""
        pass
    
    def hook(name):
        """Hook装饰器"""
        pass
```

---

## 八、使用示例

### 8.1 基本使用

```python
from module.du import DuManager

# 初始化
manager = DuManager("data/memory.db")

# Agent发送消息
record_id = manager.insert_send(
    content="你好，我是AI助手",
    agent_id="agent_001"
)

# Agent接收消息
record_id = manager.insert_receive(
    content="请帮我写一段代码",
    agent_id="agent_001",
    sender_id="user_001"
)

# 查询最近的对话
records = manager.get_recent(limit=10)
```

### 8.2 带回调使用

```python
def on_new_memory(record):
    """新记忆写入后触发"""
    print(f"[新记忆] {record.id}: {record.content[:50]}...")
    # 可以触发典层处理

manager = DuManager(
    db_path="data/memory.db",
    on_insert=on_new_memory
)

# 写入时会自动触发回调
manager.insert_send("测试消息", "agent_001")
```

### 8.3 批量导入

```python
records = [
    {"content": "历史消息1", "agent_id": "agent_001", "direction": "receive", "timestamp": "2026-03-30T10:00:00"},
    {"content": "历史消息2", "agent_id": "agent_001", "direction": "send", "timestamp": "2026-03-30T10:01:00"},
]

result = manager.insert_batch(records)
print(f"成功: {result['success']}, 失败: {result['failed']}")
```

---

## 九、文件结构

```
module/du/
├── __init__.py      # 模块入口
├── models.py        # 数据模型 (DuRecord)
├── database.py      # 数据库管理
├── manager.py       # 管理器 (CRUD操作)
├── test_du.py       # 测试脚本
└── README.md        # 本文档
```

---

## 十、依赖

```
Python >= 3.10
sqlite3 (Python原生)
无第三方依赖
```