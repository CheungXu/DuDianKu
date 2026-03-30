# -*- coding: utf-8 -*-
"""
牍层管理器

提供CRUD操作、热度管理、回调机制等功能
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Callable
import json
import math
import uuid
import hashlib
import logging

from .models import DuRecord, HeatLevel, RecordStatus
from .database import DuDatabase

logger = logging.getLogger(__name__)


class DuManager:
    """
    牍层管理器
    
    功能:
    - 记录的增删改查
    - 热度计算与更新
    - 时间范围查询
    - 回调机制支持
    
    使用示例:
        # 基本使用
        manager = DuManager("data/memory.db")
        record_id = manager.insert_send("你好", "agent_001")
        
        # 带回调使用
        manager = DuManager(
            db_path="data/memory.db",
            on_insert=lambda r: print(f"新记录: {r.id}")
        )
    """
    
    def __init__(
        self,
        db_path: str = "data/dudianku.db",
        on_insert: Callable[[DuRecord], None] = None,
        on_batch: Callable[[Dict], None] = None,
        on_error: Callable[[Exception, Dict], None] = None,
    ):
        """
        初始化管理器
        
        Args:
            db_path: 数据库文件路径
            on_insert: 写入后回调
            on_batch: 批量写入后回调
            on_error: 错误回调
        """
        self.db = DuDatabase(db_path)
        
        # 回调函数
        self._on_insert = on_insert
        self._on_batch = on_batch
        self._on_error = on_error
        
        # Hook函数列表
        self._hooks = {
            'before_insert': [],
            'after_insert': [],
            'before_batch': [],
            'after_batch': [],
        }
    
    # ==================== 自动处理 ====================
    
    @staticmethod
    def _generate_id() -> str:
        """生成唯一ID"""
        return str(uuid.uuid4())
    
    @staticmethod
    def _compute_hash(content: str) -> str:
        """计算内容哈希"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
    
    @staticmethod
    def _get_timestamp(iso_str: str = None) -> str:
        """获取时间戳"""
        if iso_str:
            return iso_str
        return datetime.now().isoformat()
    
    @staticmethod
    def _build_session_id(agent_id: str, session_id: str = None) -> str:
        """构建会话ID"""
        if session_id:
            return session_id
        date_str = datetime.now().strftime("%Y%m%d")
        return f"{agent_id}_{date_str}"
    
    # ==================== 写入接口 ====================
    
    def insert_send(
        self,
        content: str,
        agent_id: str,
        session_id: str = None,
        metadata: dict = None,
        **kwargs
    ) -> str:
        """
        Agent发送消息入库
        
        Args:
            content: 消息内容
            agent_id: Agent标识
            session_id: 会话ID（可选，自动生成）
            metadata: 扩展元数据
            **kwargs: 其他字段 (sender_name, channel, thread_id等)
            
        Returns:
            记录ID
        """
        # 参数校验
        if not content:
            raise ValueError("content is required")
        if not agent_id:
            raise ValueError("agent_id is required")
        
        # 构建记录
        now = datetime.now().isoformat()
        session_id = self._build_session_id(agent_id, session_id)
        
        record_data = {
            'id': self._generate_id(),
            'timestamp': kwargs.get('timestamp') or now,
            'created_at': now,
            
            # 来源
            'source': 'agent',
            'source_id': kwargs.get('source_id', ''),
            'session_id': session_id,
            'channel': kwargs.get('channel') or agent_id,
            'thread_id': kwargs.get('thread_id', ''),
            'parent_id': kwargs.get('parent_id', ''),
            
            # 发送人（Agent发送）
            'sender_id': agent_id,
            'sender_name': kwargs.get('sender_name', ''),
            'sender_type': 'agent',
            'receiver_id': kwargs.get('receiver_id', ''),
            'receiver_name': kwargs.get('receiver_name', ''),
            
            # 内容
            'type': kwargs.get('type', 'conversation'),
            'content_type': kwargs.get('content_type', 'text'),
            'content': content,
            'raw_content': kwargs.get('raw_content') or content,
            'content_hash': self._compute_hash(content),
            'reply_to_id': kwargs.get('reply_to_id', ''),
            
            # 热度初始化
            'access_count': 0,
            'last_access_at': '',
            'heat_score': 0.0,
            'heat_level': 'cold',
            'decay_factor': 1.0,
            
            # 分类
            'tags': json.dumps(kwargs.get('tags', []), ensure_ascii=False),
            'category': kwargs.get('category', ''),
            'importance_hint': kwargs.get('importance_hint', 0.5),
            'is_key_memory': kwargs.get('is_key_memory', 0),
            
            # 状态
            'status': 'active',
            'processed_at': '',
            'process_result': '',
            
            # 扩展
            'metadata': json.dumps(metadata or {}, ensure_ascii=False),
            'extra_data': kwargs.get('extra_data', ''),
        }
        
        # 执行 before_insert hooks
        for hook in self._hooks['before_insert']:
            record_data = hook(record_data) or record_data
        
        # 插入数据库
        record = DuRecord.from_dict(record_data)
        self._insert_record(record)
        
        # 执行 after_insert hooks
        for hook in self._hooks['after_insert']:
            try:
                hook(record)
            except Exception as e:
                logger.error(f"Hook error: {e}")
        
        # 触发回调
        if self._on_insert:
            try:
                self._on_insert(record)
            except Exception as e:
                logger.error(f"Callback error: {e}")
        
        logger.debug(f"Inserted send record: {record.id}")
        return record.id
    
    def insert_receive(
        self,
        content: str,
        agent_id: str,
        sender_id: str = None,
        sender_name: str = None,
        sender_type: str = 'user',
        session_id: str = None,
        metadata: dict = None,
        **kwargs
    ) -> str:
        """
        Agent接收消息入库
        
        Args:
            content: 消息内容
            agent_id: Agent标识
            sender_id: 原始发送者ID
            sender_name: 发送者名称
            sender_type: 发送者类型 (user/system/bot)
            session_id: 会话ID
            metadata: 扩展元数据
            **kwargs: 其他字段
            
        Returns:
            记录ID
        """
        # 参数校验
        if not content:
            raise ValueError("content is required")
        if not agent_id:
            raise ValueError("agent_id is required")
        
        # 构建记录
        now = datetime.now().isoformat()
        session_id = self._build_session_id(agent_id, session_id)
        
        record_data = {
            'id': self._generate_id(),
            'timestamp': kwargs.get('timestamp') or now,
            'created_at': now,
            
            # 来源
            'source': 'agent',
            'source_id': kwargs.get('source_id', ''),
            'session_id': session_id,
            'channel': kwargs.get('channel') or agent_id,
            'thread_id': kwargs.get('thread_id', ''),
            'parent_id': kwargs.get('parent_id', ''),
            
            # 接收人（Agent接收）
            'sender_id': sender_id or '',
            'sender_name': sender_name or '',
            'sender_type': sender_type,
            'receiver_id': agent_id,
            'receiver_name': kwargs.get('receiver_name', ''),
            
            # 内容
            'type': kwargs.get('type', 'conversation'),
            'content_type': kwargs.get('content_type', 'text'),
            'content': content,
            'raw_content': kwargs.get('raw_content') or content,
            'content_hash': self._compute_hash(content),
            'reply_to_id': kwargs.get('reply_to_id', ''),
            
            # 热度初始化
            'access_count': 0,
            'last_access_at': '',
            'heat_score': 0.0,
            'heat_level': 'cold',
            'decay_factor': 1.0,
            
            # 分类
            'tags': json.dumps(kwargs.get('tags', []), ensure_ascii=False),
            'category': kwargs.get('category', ''),
            'importance_hint': kwargs.get('importance_hint', 0.5),
            'is_key_memory': kwargs.get('is_key_memory', 0),
            
            # 状态
            'status': 'active',
            'processed_at': '',
            'process_result': '',
            
            # 扩展
            'metadata': json.dumps(metadata or {}, ensure_ascii=False),
            'extra_data': kwargs.get('extra_data', ''),
        }
        
        # 执行 before_insert hooks
        for hook in self._hooks['before_insert']:
            record_data = hook(record_data) or record_data
        
        # 插入数据库
        record = DuRecord.from_dict(record_data)
        self._insert_record(record)
        
        # 执行 after_insert hooks
        for hook in self._hooks['after_insert']:
            try:
                hook(record)
            except Exception as e:
                logger.error(f"Hook error: {e}")
        
        # 触发回调
        if self._on_insert:
            try:
                self._on_insert(record)
            except Exception as e:
                logger.error(f"Callback error: {e}")
        
        logger.debug(f"Inserted receive record: {record.id}")
        return record.id
    
    def insert_batch(
        self,
        records: List[Dict[str, Any]],
        batch_id: str = None,
    ) -> Dict[str, Any]:
        """
        批量导入历史数据
        
        Args:
            records: 记录列表，每条记录需包含:
                     - content: 消息内容
                     - agent_id: Agent标识
                     - direction: "send" 或 "receive"
                     - timestamp: 时间戳（可选）
                     - sender_id: 接收时的发送者ID
            batch_id: 批次ID（可选，自动生成）
            
        Returns:
            {
                'success': int,      # 成功数量
                'failed': int,       # 失败数量
                'batch_id': str,     # 批次ID
                'record_ids': list,  # 成功的记录ID
                'errors': list,      # 错误信息
            }
        """
        batch_id = batch_id or self._generate_id()
        result = {
            'success': 0,
            'failed': 0,
            'batch_id': batch_id,
            'record_ids': [],
            'errors': [],
        }
        
        # 执行 before_batch hooks
        for hook in self._hooks['before_batch']:
            hook(records)
        
        for i, record_data in enumerate(records):
            try:
                direction = record_data.get('direction', 'receive')
                content = record_data.get('content')
                agent_id = record_data.get('agent_id')
                
                if not content or not agent_id:
                    raise ValueError(f"Record {i}: content and agent_id required")
                
                if direction == 'send':
                    record_id = self.insert_send(
                        content=content,
                        agent_id=agent_id,
                        session_id=record_data.get('session_id'),
                        timestamp=record_data.get('timestamp'),
                        metadata={'batch_id': batch_id, **record_data.get('metadata', {})},
                        **{k: v for k, v in record_data.items() 
                           if k not in ['content', 'agent_id', 'direction', 'session_id', 'timestamp', 'metadata']}
                    )
                else:
                    record_id = self.insert_receive(
                        content=content,
                        agent_id=agent_id,
                        sender_id=record_data.get('sender_id'),
                        sender_name=record_data.get('sender_name'),
                        sender_type=record_data.get('sender_type', 'user'),
                        session_id=record_data.get('session_id'),
                        timestamp=record_data.get('timestamp'),
                        metadata={'batch_id': batch_id, **record_data.get('metadata', {})},
                        **{k: v for k, v in record_data.items() 
                           if k not in ['content', 'agent_id', 'direction', 'sender_id', 'sender_name', 
                                        'sender_type', 'session_id', 'timestamp', 'metadata']}
                    )
                
                result['success'] += 1
                result['record_ids'].append(record_id)
                
            except Exception as e:
                result['failed'] += 1
                result['errors'].append({
                    'index': i,
                    'error': str(e),
                })
                logger.error(f"Batch insert error at index {i}: {e}")
                
                if self._on_error:
                    self._on_error(e, record_data)
        
        # 执行 after_batch hooks
        for hook in self._hooks['after_batch']:
            hook(result)
        
        # 触发回调
        if self._on_batch:
            try:
                self._on_batch(result)
            except Exception as e:
                logger.error(f"Batch callback error: {e}")
        
        logger.info(f"Batch insert complete: {result['success']} success, {result['failed']} failed")
        return result
    
    def _insert_record(self, record: DuRecord):
        """插入记录到数据库"""
        sql = """
        INSERT INTO du_raw (
            id, timestamp, created_at,
            source, source_id, session_id, channel, thread_id, parent_id,
            sender_id, sender_name, sender_type, receiver_id, receiver_name,
            type, content_type, content, raw_content, content_hash, reply_to_id,
            access_count, last_access_at, heat_score, heat_level, decay_factor,
            tags, category, importance_hint, is_key_memory,
            status, processed_at, process_result,
            metadata, extra_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        params = (
            record.id, record.timestamp, record.created_at,
            record.source, record.source_id, record.session_id, record.channel,
            record.thread_id, record.parent_id,
            record.sender_id, record.sender_name, record.sender_type,
            record.receiver_id, record.receiver_name,
            record.type, record.content_type, record.content, record.raw_content,
            record.content_hash, record.reply_to_id,
            record.access_count, record.last_access_at, record.heat_score,
            record.heat_level, record.decay_factor,
            record.tags, record.category, record.importance_hint, record.is_key_memory,
            record.status, record.processed_at, record.process_result,
            record.metadata, record.extra_data
        )
        
        self.db.execute(sql, params)
    
    # ==================== 查询操作 ====================
    
    def get_by_id(self, record_id: str) -> Optional[DuRecord]:
        """根据ID获取记录"""
        sql = "SELECT * FROM du_raw WHERE id = ?"
        row = self.db.fetchone(sql, (record_id,))
        if row:
            self._update_access(record_id)
            return DuRecord.from_dict(row)
        return None
    
    def get_by_timestamp_range(
        self,
        start_time: str,
        end_time: str,
        limit: int = 100
    ) -> List[DuRecord]:
        """根据时间范围获取记录"""
        sql = """
        SELECT * FROM du_raw 
        WHERE timestamp BETWEEN ? AND ?
        ORDER BY timestamp DESC
        LIMIT ?
        """
        rows = self.db.fetchall(sql, (start_time, end_time, limit))
        return [DuRecord.from_dict(row) for row in rows]
    
    def get_by_session(self, session_id: str, limit: int = 100) -> List[DuRecord]:
        """根据会话ID获取记录"""
        sql = """
        SELECT * FROM du_raw 
        WHERE session_id = ?
        ORDER BY timestamp ASC
        LIMIT ?
        """
        rows = self.db.fetchall(sql, (session_id, limit))
        return [DuRecord.from_dict(row) for row in rows]
    
    def get_by_agent(
        self,
        agent_id: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[DuRecord]:
        """根据Agent ID获取记录"""
        sql = """
        SELECT * FROM du_raw 
        WHERE channel = ? OR sender_id = ? OR receiver_id = ?
        ORDER BY timestamp DESC
        LIMIT ? OFFSET ?
        """
        rows = self.db.fetchall(sql, (agent_id, agent_id, agent_id, limit, offset))
        return [DuRecord.from_dict(row) for row in rows]
    
    def search_content(self, keyword: str, limit: int = 50) -> List[DuRecord]:
        """搜索内容"""
        sql = """
        SELECT * FROM du_raw 
        WHERE content LIKE ?
        ORDER BY heat_score DESC, timestamp DESC
        LIMIT ?
        """
        rows = self.db.fetchall(sql, (f"%{keyword}%", limit))
        for row in rows:
            self._update_access(row['id'])
        return [DuRecord.from_dict(row) for row in rows]
    
    def get_by_heat_level(self, heat_level: str, limit: int = 100) -> List[DuRecord]:
        """根据热度等级获取记录"""
        sql = """
        SELECT * FROM du_raw 
        WHERE heat_level = ?
        ORDER BY heat_score DESC
        LIMIT ?
        """
        rows = self.db.fetchall(sql, (heat_level, limit))
        return [DuRecord.from_dict(row) for row in rows]
    
    def get_hot_records(self, limit: int = 20) -> List[DuRecord]:
        """获取热门记录"""
        return self.get_by_heat_level('hot', limit)
    
    def get_recent(self, limit: int = 50) -> List[DuRecord]:
        """获取最近记录"""
        sql = """
        SELECT * FROM du_raw 
        WHERE status = 'active'
        ORDER BY timestamp DESC
        LIMIT ?
        """
        rows = self.db.fetchall(sql, (limit,))
        return [DuRecord.from_dict(row) for row in rows]
    
    # ==================== 更新操作 ====================
    
    def update(self, record_id: str, **kwargs) -> bool:
        """更新记录"""
        if not kwargs:
            return False
        
        set_clause = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [record_id]
        
        sql = f"UPDATE du_raw SET {set_clause} WHERE id = ?"
        rowcount = self.db.execute(sql, tuple(values))
        
        return rowcount > 0
    
    def mark_as_key_memory(self, record_id: str) -> bool:
        """标记为关键记忆"""
        return self.update(record_id, is_key_memory=1, importance_hint=1.0)
    
    def mark_processed(self, record_id: str, dian_ids: List[str]) -> bool:
        """标记已处理"""
        return self.update(
            record_id,
            status='processed',
            processed_at=datetime.now().isoformat(),
            process_result=json.dumps(dian_ids)
        )
    
    # ==================== 热度管理 ====================
    
    def _update_access(self, record_id: str):
        """更新访问统计"""
        now = datetime.now().isoformat()
        sql = """
        UPDATE du_raw 
        SET access_count = access_count + 1,
            last_access_at = ?
        WHERE id = ?
        """
        self.db.execute(sql, (now, record_id))
    
    def recalculate_heat(self, record_id: str) -> float:
        """重新计算热度评分"""
        record = self.get_by_id(record_id)
        if not record:
            return 0.0
        
        # 计算衰减因子
        decay_factor = 1.0
        if record.last_access_at:
            last_access = datetime.fromisoformat(record.last_access_at)
            days_since = (datetime.now() - last_access).days
            decay_factor = math.exp(-days_since / 30.0)
        
        # 计算热度评分
        heat_score = record.access_count * decay_factor * 10
        
        # 确定热度等级
        if heat_score >= 80:
            heat_level = 'hot'
        elif heat_score >= 40:
            heat_level = 'warm'
        else:
            heat_level = 'cold'
        
        # 更新记录
        self.update(
            record_id,
            heat_score=round(heat_score, 2),
            heat_level=heat_level,
            decay_factor=round(decay_factor, 4)
        )
        
        return heat_score
    
    def batch_recalculate_heat(self) -> int:
        """批量重新计算所有记录的热度"""
        sql = "SELECT id FROM du_raw WHERE status = 'active'"
        rows = self.db.fetchall(sql)
        
        count = 0
        for row in rows:
            self.recalculate_heat(row['id'])
            count += 1
        
        logger.info(f"Recalculated heat for {count} records")
        return count
    
    # ==================== 删除操作 ====================
    
    def soft_delete(self, record_id: str) -> bool:
        """软删除记录"""
        return self.update(record_id, status='deleted')
    
    def archive(self, record_id: str) -> bool:
        """归档记录"""
        return self.update(record_id, status='archived')
    
    def hard_delete(self, record_id: str) -> bool:
        """硬删除记录"""
        sql = "DELETE FROM du_raw WHERE id = ?"
        rowcount = self.db.execute(sql, (record_id,))
        return rowcount > 0
    
    # ==================== 回调注册 ====================
    
    def on_insert(self, callback: Callable[[DuRecord], None]):
        """注册写入后回调"""
        self._on_insert = callback
    
    def on_batch(self, callback: Callable[[Dict], None]):
        """注册批量写入后回调"""
        self._on_batch = callback
    
    def on_error(self, callback: Callable[[Exception, Dict], None]):
        """注册错误回调"""
        self._on_error = callback
    
    def hook(self, name: str) -> Callable:
        """
        Hook装饰器
        
        使用示例:
            @manager.hook('after_insert')
            def process_record(record):
                print(f"新记录: {record.id}")
        """
        def decorator(func: Callable):
            if name in self._hooks:
                self._hooks[name].append(func)
            return func
        return decorator
    
    # ==================== 统计信息 ====================
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return self.db.get_stats()