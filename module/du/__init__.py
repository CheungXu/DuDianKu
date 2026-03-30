# -*- coding: utf-8 -*-
"""
牍(Du) - 最小记忆单元

原始数据载体，全量存储Agent的交互记录、碎片化信息等最基础的记忆颗粒。

职责:
- 全量存: 所有原始数据完整保留
- 可回溯: 支持按时间溯源查找
- 热度追踪: 记录访问次数，支持热/温/冷数据判断
"""

from .database import DuDatabase
from .models import DuRecord
from .manager import DuManager

__all__ = ['DuDatabase', 'DuRecord', 'DuManager']