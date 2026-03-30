# -*- coding: utf-8 -*-
"""
牍层管理器

提供CRUD操作、热度管理、查询等功能
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
import json
import math
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
    - 批量操作
    """
    
    def __init__(self, db_path: str = "data/dudianku.db"):
        """
        初始化管理器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db = DuDatabase(db_path)
    
    # ==================== 创建操作 ====================
    
    def insert(self, record: DuRecord) -> str:
        """
        插入一条记录
        
        Args:
            record: DuRecord 实例
            
        Returns:
            记录ID
        """
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
        logger.debug(f"Inserted record: {record.id}")
        return record.id
    
    def insert_batch(self, records: List[DuRecord]) -> int:
        """
        批量插入记录
        
        Args:
            records: DuRecord 列表
            
        Returns:
            成功插入的数量
        """
        count = 0
        for record in records:
            try:
                self.insert(record)
                count += 1
            except Exception as e:
                logger.error(f"Failed to insert record {record.id}: {e}")
        return count
    
    # ==================== 查询操作 ====================
    
    def get_by_id(self, record_id: str) -> Optional[DuRecord]:
        """
        根据ID获取记录
        
        Args:
            record_id: 记录ID
            
        Returns:
            DuRecord 或 None
        """
        sql = "SELECT * FROM du_raw WHERE id = ?"
        row = self.db.fetchone(sql, (record_id,))
        if row:
            # 更新访问统计
            self._update_access(record_id)
            return DuRecord.from_dict(row)
        return None
    
    def get_by_timestamp_range(
        self,
        start_time: str,
        end_time: str,
        limit: int = 100
    ) -> List[DuRecord]:
        """
        根据时间范围获取记录
        
        Args:
            start_time: 开始时间 (ISO 8601)
            end_time: 结束时间 (ISO 8601)
            limit: 返回数量限制
            
        Returns:
            DuRecord 列表
        """
        sql = """
        SELECT * FROM du_raw 
        WHERE timestamp BETWEEN ? AND ?
        ORDER BY timestamp DESC
        LIMIT ?
        """
        rows = self.db.fetchall(sql, (start_time, end_time, limit))
        return [DuRecord.from_dict(row) for row in rows]
    
    def get_by_session(self, session_id: str, limit: int = 100) -> List[DuRecord]:
        """
        根据会话ID获取记录
        
        Args:
            session_id: 会话ID
            limit: 返回数量限制
            
        Returns:
            DuRecord 列表
        """
        sql = """
        SELECT * FROM du_raw 
        WHERE session_id = ?
        ORDER BY timestamp ASC
        LIMIT ?
        """
        rows = self.db.fetchall(sql, (session_id, limit))
        return [DuRecord.from_dict(row) for row in rows]
    
    def get_by_sender(
        self,
        sender_id: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[DuRecord]:
        """
        根据发送人ID获取记录
        
        Args:
            sender_id: 发送人ID
            limit: 返回数量限制
            offset: 偏移量
            
        Returns:
            DuRecord 列表
        """
        sql = """
        SELECT * FROM du_raw 
        WHERE sender_id = ?
        ORDER BY timestamp DESC
        LIMIT ? OFFSET ?
        """
        rows = self.db.fetchall(sql, (sender_id, limit, offset))
        return [DuRecord.from_dict(row) for row in rows]
    
    def search_content(
        self,
        keyword: str,
        limit: int = 50
    ) -> List[DuRecord]:
        """
        搜索内容（LIKE匹配）
        
        Args:
            keyword: 关键词
            limit: 返回数量限制
            
        Returns:
            DuRecord 列表
        """
        sql = """
        SELECT * FROM du_raw 
        WHERE content LIKE ?
        ORDER BY heat_score DESC, timestamp DESC
        LIMIT ?
        """
        rows = self.db.fetchall(sql, (f"%{keyword}%", limit))
        # 更新访问统计
        for row in rows:
            self._update_access(row['id'])
        return [DuRecord.from_dict(row) for row in rows]
    
    def get_by_heat_level(
        self,
        heat_level: str,
        limit: int = 100
    ) -> List[DuRecord]:
        """
        根据热度等级获取记录
        
        Args:
            heat_level: 热度等级 (hot/warm/cold)
            limit: 返回数量限制
            
        Returns:
            DuRecord 列表
        """
        sql = """
        SELECT * FROM du_raw 
        WHERE heat_level = ?
        ORDER BY heat_score DESC
        LIMIT ?
        """
        rows = self.db.fetchall(sql, (heat_level, limit))
        return [DuRecord.from_dict(row) for row in rows]
    
    def get_hot_records(self, limit: int = 20) -> List[DuRecord]:
        """
        获取热门记录
        
        Args:
            limit: 返回数量限制
            
        Returns:
            DuRecord 列表
        """
        return self.get_by_heat_level('hot', limit)
    
    def get_recent(self, limit: int = 50) -> List[DuRecord]:
        """
        获取最近记录
        
        Args:
            limit: 返回数量限制
            
        Returns:
            DuRecord 列表
        """
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
        """
        更新记录
        
        Args:
            record_id: 记录ID
            **kwargs: 要更新的字段
            
        Returns:
            是否成功
        """
        if not kwargs:
            return False
        
        set_clause = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [record_id]
        
        sql = f"UPDATE du_raw SET {set_clause} WHERE id = ?"
        rowcount = self.db.execute(sql, tuple(values))
        
        return rowcount > 0
    
    def mark_as_key_memory(self, record_id: str) -> bool:
        """
        标记为关键记忆
        
        Args:
            record_id: 记录ID
            
        Returns:
            是否成功
        """
        return self.update(record_id, is_key_memory=1, importance_hint=1.0)
    
    def mark_processed(self, record_id: str, dian_ids: List[str]) -> bool:
        """
        标记已处理
        
        Args:
            record_id: 记录ID
            dian_ids: 生成的典ID列表
            
        Returns:
            是否成功
        """
        return self.update(
            record_id,
            status='processed',
            processed_at=datetime.now().isoformat(),
            process_result=json.dumps(dian_ids)
        )
    
    # ==================== 热度管理 ====================
    
    def _update_access(self, record_id: str):
        """
        更新访问统计（内部方法）
        
        Args:
            record_id: 记录ID
        """
        now = datetime.now().isoformat()
        sql = """
        UPDATE du_raw 
        SET access_count = access_count + 1,
            last_access_at = ?
        WHERE id = ?
        """
        self.db.execute(sql, (now, record_id))
    
    def recalculate_heat(self, record_id: str) -> float:
        """
        重新计算热度评分
        
        热度公式: heat_score = access_count * decay_factor
        衰减因子: decay_factor = exp(-days_since_last_access / 30)
        
        Args:
            record_id: 记录ID
            
        Returns:
            新的热度评分
        """
        # 获取当前记录
        record = self.get_by_id(record_id)
        if not record:
            return 0.0
        
        # 计算衰减因子
        decay_factor = 1.0
        if record.last_access_at:
            last_access = datetime.fromisoformat(record.last_access_at)
            days_since = (datetime.now() - last_access).days
            decay_factor = math.exp(-days_since / 30.0)  # 30天衰减周期
        
        # 计算热度评分
        heat_score = record.access_count * decay_factor * 10  # 放大系数
        
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
        """
        批量重新计算所有记录的热度
        
        Returns:
            更新的记录数
        """
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
        """
        软删除记录
        
        Args:
            record_id: 记录ID
            
        Returns:
            是否成功
        """
        return self.update(record_id, status='deleted')
    
    def archive(self, record_id: str) -> bool:
        """
        归档记录
        
        Args:
            record_id: 记录ID
            
        Returns:
            是否成功
        """
        return self.update(record_id, status='archived')
    
    def hard_delete(self, record_id: str) -> bool:
        """
        硬删除记录（谨慎使用）
        
        Args:
            record_id: 记录ID
            
        Returns:
            是否成功
        """
        sql = "DELETE FROM du_raw WHERE id = ?"
        rowcount = self.db.execute(sql, (record_id,))
        return rowcount > 0
    
    # ==================== 统计信息 ====================
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            统计信息字典
        """
        return self.db.get_stats()
    
    def get_timeline(
        self,
        start_date: str,
        end_date: str,
        granularity: str = 'day'
    ) -> List[Dict[str, Any]]:
        """
        获取时间线统计
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            granularity: 粒度 (day/hour)
            
        Returns:
            时间线统计列表
        """
        if granularity == 'hour':
            date_format = '%Y-%m-%d %H:00'
        else:
            date_format = '%Y-%m-%d'
        
        sql = f"""
        SELECT strftime('{date_format}', timestamp) as time_bucket,
               COUNT(*) as count
        FROM du_raw
        WHERE timestamp BETWEEN ? AND ?
        GROUP BY time_bucket
        ORDER BY time_bucket
        """
        
        rows = self.db.fetchall(sql, (start_date, end_date))
        return rows