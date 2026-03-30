# -*- coding: utf-8 -*-
"""
牍层数据库管理

SQLite数据库初始化、表创建、索引管理
"""

import sqlite3
import os
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class DuDatabase:
    """
    牍层数据库管理类
    
    负责:
    - 数据库初始化
    - 表结构创建
    - 索引管理
    - 连接管理
    """
    
    # 表创建SQL
    CREATE_DU_RAW_TABLE = """
    CREATE TABLE IF NOT EXISTS du_raw (
        -- 核心标识
        id              TEXT PRIMARY KEY,
        timestamp       TEXT NOT NULL,
        created_at      TEXT,
        
        -- 来源追溯
        source          TEXT,
        source_id       TEXT,
        session_id      TEXT,
        channel         TEXT,
        thread_id       TEXT,
        parent_id       TEXT,
        
        -- 发送人/接收人
        sender_id       TEXT,
        sender_name     TEXT,
        sender_type     TEXT,
        receiver_id     TEXT,
        receiver_name   TEXT,
        
        -- 内容
        type            TEXT,
        content_type    TEXT,
        content         TEXT,
        raw_content     TEXT,
        content_hash    TEXT,
        reply_to_id     TEXT,
        
        -- 热度统计
        access_count    INTEGER DEFAULT 0,
        last_access_at  TEXT,
        heat_score      REAL DEFAULT 0.0,
        heat_level      TEXT DEFAULT 'cold',
        decay_factor    REAL DEFAULT 1.0,
        
        -- 分类/标记
        tags            TEXT,
        category        TEXT,
        importance_hint REAL DEFAULT 0.5,
        is_key_memory   INTEGER DEFAULT 0,
        
        -- 状态
        status          TEXT DEFAULT 'active',
        processed_at    TEXT,
        process_result  TEXT,
        
        -- 扩展
        metadata        TEXT,
        extra_data      TEXT
    );
    """
    
    # 索引创建SQL
    CREATE_INDEXES = [
        "CREATE INDEX IF NOT EXISTS idx_du_timestamp ON du_raw(timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_du_source ON du_raw(source);",
        "CREATE INDEX IF NOT EXISTS idx_du_session ON du_raw(session_id);",
        "CREATE INDEX IF NOT EXISTS idx_du_sender ON du_raw(sender_id);",
        "CREATE INDEX IF NOT EXISTS idx_du_type ON du_raw(type);",
        "CREATE INDEX IF NOT EXISTS idx_du_status ON du_raw(status);",
        "CREATE INDEX IF NOT EXISTS idx_du_heat_score ON du_raw(heat_score);",
        "CREATE INDEX IF NOT EXISTS idx_du_heat_level ON du_raw(heat_level);",
        "CREATE INDEX IF NOT EXISTS idx_du_content_hash ON du_raw(content_hash);",
        "CREATE INDEX IF NOT EXISTS idx_du_created_at ON du_raw(created_at);",
    ]
    
    def __init__(self, db_path: str = "data/dudianku.db"):
        """
        初始化数据库
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self._ensure_dir()
        self._init_db()
        logger.info(f"DuDatabase initialized: {db_path}")
    
    def _ensure_dir(self):
        """确保数据库目录存在"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
    
    def _init_db(self):
        """初始化数据库表结构"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # 创建表
            cursor.execute(self.CREATE_DU_RAW_TABLE)
            logger.info("Table du_raw created/verified")
            
            # 创建索引
            for index_sql in self.CREATE_INDEXES:
                cursor.execute(index_sql)
            logger.info(f"{len(self.CREATE_INDEXES)} indexes created/verified")
            
            conn.commit()
    
    @contextmanager
    def get_connection(self):
        """
        获取数据库连接（上下文管理器）
        
        Yields:
            sqlite3.Connection: 数据库连接
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # 支持字典式访问
        try:
            yield conn
        finally:
            conn.close()
    
    def execute(self, sql: str, params: tuple = ()) -> int:
        """
        执行SQL语句
        
        Args:
            sql: SQL语句
            params: 参数元组
            
        Returns:
            影响的行数
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            return cursor.rowcount
    
    def fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """
        查询单条记录
        
        Args:
            sql: SQL语句
            params: 参数元组
            
        Returns:
            记录字典或None
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def fetchall(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        查询多条记录
        
        Args:
            sql: SQL语句
            params: 参数元组
            
        Returns:
            记录列表
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取数据库统计信息
        
        Returns:
            统计信息字典
        """
        stats = {}
        
        # 总记录数
        result = self.fetchone("SELECT COUNT(*) as count FROM du_raw")
        stats['total_records'] = result['count'] if result else 0
        
        # 各状态记录数
        result = self.fetchall(
            "SELECT status, COUNT(*) as count FROM du_raw GROUP BY status"
        )
        stats['by_status'] = {r['status']: r['count'] for r in result}
        
        # 各热度等级记录数
        result = self.fetchall(
            "SELECT heat_level, COUNT(*) as count FROM du_raw GROUP BY heat_level"
        )
        stats['by_heat_level'] = {r['heat_level']: r['count'] for r in result}
        
        # 各来源记录数
        result = self.fetchall(
            "SELECT source, COUNT(*) as count FROM du_raw GROUP BY source"
        )
        stats['by_source'] = {r['source']: r['count'] for r in result}
        
        # 数据库文件大小
        if os.path.exists(self.db_path):
            stats['db_size_mb'] = round(os.path.getsize(self.db_path) / 1024 / 1024, 2)
        
        return stats
    
    def vacuum(self):
        """执行VACUUM，优化数据库空间"""
        with self.get_connection() as conn:
            conn.execute("VACUUM")
            logger.info("Database vacuumed")
    
    def backup(self, backup_path: str):
        """
        备份数据库
        
        Args:
            backup_path: 备份文件路径
        """
        import shutil
        shutil.copy2(self.db_path, backup_path)
        logger.info(f"Database backed up to: {backup_path}")