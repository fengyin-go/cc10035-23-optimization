"""解析配置版本化与实体抽取一致性的共享支撑。

这里只放跨 dom_parser / entity_extract / relation_graph 三个领域服务复用的
版本表名、状态词、时间戳和全局锁。版本登记、批次流转等业务动作仍留在各自的
领域服务里，路由层不做业务判断。
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

# ---- 内存表名（store 中不存在时由 store.rows 惰性创建为空表） ----
PARSE_VERSION_TABLE = "parse_versions"        # 解析版本清单
EXTRACTION_BATCH_TABLE = "extraction_batches"  # 抽取批次（每次运行一条）
ENTITY_VERSION_TABLE = "entity_versions"      # 带来源的实体版本数据集
EXTRACTION_RECEIPT_TABLE = "extraction_receipts"  # 交付回执（并发去重）
EXTRACTION_CURSOR_TABLE = "extraction_cursors"    # 每个页面当前生效批次/版本

# ---- 解析版本状态 ----
VERSION_ACTIVE = "可消费"      # 当前允许下游消费的完整版本
VERSION_SUPERSEDED = "已替换"  # 被同页面更新版本顶替，已发布批次仍可读取

# ---- 抽取批次状态 ----
BATCH_RUNNING = "运行中"
BATCH_DELIVERED = "已交付"    # 结果已回传并写入实体版本数据集
BATCH_CONFLICTED = "冲突待裁决"  # 与页面当前生效版本冲突，等待接纳决策
BATCH_PUBLISHED = "已发布"    # 已发布，快照不可变
BATCH_DISCARDED = "已废弃"    # 冲突裁决中被放弃的在途批次

# 交付时效优先 vs 既有待处理数据优先
POLICY_FRESH_FIRST = "fresh_first"
POLICY_PENDING_FIRST = "pending_first"
ADMISSION_POLICIES = (POLICY_FRESH_FIRST, POLICY_PENDING_FIRST)

# 实体抽取主表里追加的来源字段（不改动既有 LIST_FIELDS 的列顺序）
PROVENANCE_FIELDS = [
    "解析版本ID",
    "解析签名",
    "运行ID",
    "生效字段",
]

# 版本化流水线的全局锁：保证交错回传、并发回执下的读改写原子性。
PIPELINE_LOCK = threading.RLock()


def now_iso() -> str:
    """统一的落库时间戳，便于按交付先后排序。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def next_id(rows: list[dict]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1
