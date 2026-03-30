# -*- coding: utf-8 -*-
"""
牍层数据模型定义
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
import uuid
import json
import hashlib


class SenderType(Enum):
    """发送人类型"""
    USER = "user"
    AGENT = "agent"
    SYSTEM = "system"
    BOT = "bot"


class DataType(Enum):
    """数据类型"""
    CONVERSATION = "conversation"
    COMMAND = "command"
    LOG = "log"
    EVENT = "event"
    NOTIFICATION = "notification"


class ContentType(Enum):
    """内容类型"""
    TEXT = "text"
    IMAGE = "image"
    FILE = "file"
    AUDIO = "audio"
    VIDEO = "video"
    CARD = "card"
    JSON = "json"


class HeatLevel(Enum):
    """热度等级"""
    HOT = "hot"      # 热数据: heat_score >= 80
    WARM = "warm"    # 温数据: 40 <= heat_score < 80
    COLD = "cold"    # 冷数据: heat_score < 40


class RecordStatus(Enum):
    """记录状态"""
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"
    PROCESSED = "processed"


@dataclass
class DuRecord:
    """
    牍层记录模型
    
    存储原始对话/交互记录，支持完整的溯源和热度追踪
    """
    
    # 核心标识
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # 来源追溯
    source: str = ""                    # 来源渠道 (feishu/telegram/web/api)
    source_id: str = ""                 # 来源系统消息ID
    session_id: str = ""                # 会话ID
    channel: str = ""                   # 频道/群组名称
    thread_id: str = ""                 # 线程ID
    parent_id: str = ""                 # 父消息ID
    
    # 发送人/接收人
    sender_id: str = ""                 # 发送人ID
    sender_name: str = ""               # 发送人名称
    sender_type: str = "user"           # 发送人类型
    receiver_id: str = ""               # 接收人ID
    receiver_name: str = ""             # 接收人名称
    
    # 内容
    type: str = "conversation"          # 数据类型
    content_type: str = "text"          # 内容类型
    content: str = ""                   # 内容文本（便于搜索）
    raw_content: str = ""               # 原始内容（完整JSON）
    content_hash: str = ""              # 内容哈希
    reply_to_id: str = ""               # 回复的消息ID
    
    # 热度统计
    access_count: int = 0               # 访问次数
    last_access_at: str = ""            # 最后访问时间
    heat_score: float = 0.0             # 热度评分 (0.0-100.0)
    heat_level: str = "cold"            # 热度等级 (hot/warm/cold)
    decay_factor: float = 1.0           # 衰减因子
    
    # 分类/标记
    tags: str = "[]"                    # 标签 (JSON array)
    category: str = ""                  # 分类
    importance_hint: float = 0.5        # 重要性提示 (0.0-1.0)
    is_key_memory: int = 0              # 是否关键记忆 (0/1)
    
    # 状态
    status: str = "active"              # 状态
    processed_at: str = ""              # 处理时间
    process_result: str = ""            # 处理结果
    
    # 扩展
    metadata: str = "{}"                # 扩展元数据 (JSON)
    extra_data: str = ""                # 额外数据
    
    def __post_init__(self):
        """初始化后处理"""
        # 计算内容哈希
        if self.content and not self.content_hash:
            self.content_hash = self._compute_hash(self.content)
        # 确保tags是JSON字符串
        if isinstance(self.tags, list):
            self.tags = json.dumps(self.tags, ensure_ascii=False)
        if isinstance(self.metadata, dict):
            self.metadata = json.dumps(self.metadata, ensure_ascii=False)
    
    @staticmethod
    def _compute_hash(content: str) -> str:
        """计算内容哈希"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'timestamp': self.timestamp,
            'created_at': self.created_at,
            'source': self.source,
            'source_id': self.source_id,
            'session_id': self.session_id,
            'channel': self.channel,
            'thread_id': self.thread_id,
            'parent_id': self.parent_id,
            'sender_id': self.sender_id,
            'sender_name': self.sender_name,
            'sender_type': self.sender_type,
            'receiver_id': self.receiver_id,
            'receiver_name': self.receiver_name,
            'type': self.type,
            'content_type': self.content_type,
            'content': self.content,
            'raw_content': self.raw_content,
            'content_hash': self.content_hash,
            'reply_to_id': self.reply_to_id,
            'access_count': self.access_count,
            'last_access_at': self.last_access_at,
            'heat_score': self.heat_score,
            'heat_level': self.heat_level,
            'decay_factor': self.decay_factor,
            'tags': self.tags,
            'category': self.category,
            'importance_hint': self.importance_hint,
            'is_key_memory': self.is_key_memory,
            'status': self.status,
            'processed_at': self.processed_at,
            'process_result': self.process_result,
            'metadata': self.metadata,
            'extra_data': self.extra_data,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DuRecord':
        """从字典创建"""
        return cls(**data)
    
    @classmethod
    def from_feishu_message(cls, msg: Dict[str, Any]) -> 'DuRecord':
        """
        从飞书消息创建记录
        
        Args:
            msg: 飞书消息数据
            
        Returns:
            DuRecord 实例
        """
        sender_type = "user"
        if msg.get('sender', {}).get('sender_type') == 'app':
            sender_type = "bot"
        
        content = msg.get('message', {}).get('content', '')
        if isinstance(content, dict):
            content = json.dumps(content, ensure_ascii=False)
        
        return cls(
            timestamp=datetime.fromtimestamp(
                msg.get('message', {}).get('create_time', 0) / 1000
            ).isoformat() if msg.get('message', {}).get('create_time') else datetime.now().isoformat(),
            source="feishu",
            source_id=msg.get('message', {}).get('message_id', ''),
            session_id=msg.get('message', {}).get('chat_id', ''),
            channel=msg.get('message', {}).get('chat_id', ''),
            sender_id=msg.get('sender', {}).get('id', {}).get('open_id', ''),
            sender_name=msg.get('sender', {}).get('id', {}).get('name', ''),
            sender_type=sender_type,
            type="conversation",
            content_type="text",
            content=content,
            raw_content=json.dumps(msg, ensure_ascii=False),
        )
    
    @classmethod
    def create_conversation(
        cls,
        content: str,
        sender_id: str,
        sender_name: str = "",
        sender_type: str = "user",
        source: str = "api",
        session_id: str = "",
        **kwargs
    ) -> 'DuRecord':
        """
        创建对话记录的便捷方法
        
        Args:
            content: 对话内容
            sender_id: 发送人ID
            sender_name: 发送人名称
            sender_type: 发送人类型
            source: 来源
            session_id: 会话ID
            **kwargs: 其他字段
            
        Returns:
            DuRecord 实例
        """
        return cls(
            type="conversation",
            content_type="text",
            content=content,
            raw_content=content,
            sender_id=sender_id,
            sender_name=sender_name,
            sender_type=sender_type,
            source=source,
            session_id=session_id,
            **kwargs
        )