"""关系图谱领域服务：构建实体语义关系图。"""
from __future__ import annotations

import hashlib
from typing import Any

from app.services.base import BaseModuleService
from app.services.versioning import (
    ENTITY_VERSION_TABLE,
    EXTRACTION_BATCH_TABLE,
    PIPELINE_LOCK,
    next_id,
    now_iso,
)
from app.store import store


RELATION_TYPES = [
    "从属关系", "引用关系", "时序关系", "同义关系",
    "因果关系", "对比关系", "并列关系", "层级关系",
]


class RelationGraphService(BaseModuleService):
    MODULE = "relation_graph"
    ENTITY = "关系边"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["边ID", "源实体", "目标实体", "关系类型", "抽取页面", "原文证据", "关联强度"]
    REQUIRED_FIELDS = ["边ID", "源实体", "目标实体"]
    STATUS_ORDER = ["已抽取", "已验证", "已入库", "已过期"]
    ACTION_RULES = {"验证关系": "已抽取", "确认入库": "已验证", "标记过期": "已抽取"}
    NEGATIVE_ACTIONS = []

    def build_relations(self, entity_extract_records: list[dict[str, Any]]) -> dict[str, Any]:
        """基于实体抽取记录构建关系图谱。消费 entity_extract 输出。"""
        if len(entity_extract_records) < 2:
            return {"ok": True, "relation_count": 0, "relations": [], "reason": "实体数量不足"}

        relations = []
        for i in range(len(entity_extract_records)):
            for j in range(i + 1, min(i + 4, len(entity_extract_records))):
                src = entity_extract_records[i]
                tgt = entity_extract_records[j]
                if src.get("页面来源") != tgt.get("页面来源"):
                    continue
                rel_type_idx = (i * 7 + j * 13) % len(RELATION_TYPES)
                edge_id = hashlib.md5(
                    f"{src.get('实体ID')}|{tgt.get('实体ID')}|{RELATION_TYPES[rel_type_idx]}".encode()
                ).hexdigest()[:10]
                strength = round(0.85 - (abs(i - j) * 0.08), 2)
                relations.append({
                    "边ID": edge_id,
                    "源实体": src.get("实体名称", src.get("实体ID", "")),
                    "目标实体": tgt.get("实体名称", tgt.get("实体ID", "")),
                    "关系类型": RELATION_TYPES[rel_type_idx],
                    "抽取页面": src.get("页面来源"),
                    "原文证据": f"从{src.get('页面来源')}中抽取的共现关系",
                    "关联强度": str(strength),
                })

        return {
            "ok": True,
            "entity_count": len(entity_extract_records),
            "relation_count": len(relations),
            "relations": relations,
            "relation_types_used": list(set(r["关系类型"] for r in relations)),
        }

    def build_relations_from_batch(self, run_id: str) -> dict[str, Any]:
        """沿数据流消费已发布/已交付批次的实体版本数据集建边。

        建边只使用该批次实际生效的字段集合，边记录解析版本与签名，
        保证图谱侧也能追溯每条边采用过的解析字段。
        """
        with PIPELINE_LOCK:
            batch = next(
                (b for b in store.rows(EXTRACTION_BATCH_TABLE) if b["run_id"] == run_id),
                None,
            )
            if batch is None:
                return {"ok": False, "reason": f"抽取运行 {run_id} 不存在"}
            snapshot = [
                row for row in store.rows(ENTITY_VERSION_TABLE) if row["run_id"] == run_id
            ]
            if not snapshot:
                return {"ok": False, "reason": "该批次没有实体版本数据，无法建边"}
            records = [
                {
                    "实体ID": row["entity_id"],
                    "实体类型": row["entity_type"],
                    "实体名称": row["entity_name"],
                    "页面来源": row["source_uri"],
                }
                for row in snapshot
            ]
            built = self.build_relations(records)
            if not built["ok"]:
                return built
            table = store.rows(self.MODULE)
            created = []
            for relation in built["relations"]:
                relation.update({
                    "解析版本ID": batch["parse_version_id"],
                    "解析签名": batch["config_signature"],
                    "来源运行ID": run_id,
                    "生效字段": list(batch["active_fields"]),
                    "建边时间": now_iso(),
                })
                entry = {"id": next_id(table), "status": "已抽取", "pending": True, "abnormal": False}
                entry.update(relation)
                table.append(entry)
                created.append(dict(entry))
        return {
            "ok": True,
            "run_id": run_id,
            "parse_version_id": batch["parse_version_id"],
            "config_signature": batch["config_signature"],
            "active_fields": list(batch["active_fields"]),
            "relation_count": len(created),
            "relations": created,
        }

    def edge_provenance(self) -> list[dict[str, Any]]:
        """图谱建边采用的字段集合：逐条边回显版本、签名与生效字段。"""
        with PIPELINE_LOCK:
            return [
                {
                    "边ID": row.get("边ID"),
                    "解析版本ID": row.get("解析版本ID"),
                    "解析签名": row.get("解析签名"),
                    "来源运行ID": row.get("来源运行ID"),
                    "生效字段": row.get("生效字段", []),
                    "抽取页面": row.get("抽取页面"),
                }
                for row in store.rows(self.MODULE)
                if row.get("来源运行ID")
            ]

    def feed_knowledge_link(self, relation_ids: list[str]) -> dict[str, Any]:
        """将已验证的关系提供给知识链接模块。"""
        relations = []
        for rid in relation_ids:
            row = next((r for r in store.rows(self.MODULE) if r.get("边ID") == rid), None)
            if row and row.get("status") == "已验证":
                relations.append({
                    "edge_id": rid,
                    "source_entity": row.get("源实体"),
                    "target_entity": row.get("目标实体"),
                    "relation_type": row.get("关系类型"),
                    "evidence_page": row.get("抽取页面"),
                })
        return {
            "ok": True,
            "requested_ids": len(relation_ids),
            "found_relations": len(relations),
            "relations": relations,
        }

    def compute_graph_stats(self) -> dict[str, Any]:
        """图谱统计。"""
        rows = store.rows(self.MODULE)
        entities: set[str] = set()
        rel_type_dist: dict[str, int] = {}
        for row in rows:
            entities.add(str(row.get("源实体", "")))
            entities.add(str(row.get("目标实体", "")))
            rtype = row.get("关系类型", "未知")
            rel_type_dist[rtype] = rel_type_dist.get(rtype, 0) + 1
        return {
            "total_relations": len(rows),
            "unique_entities": len(entities) - (1 if "" in entities else 0),
            "relation_type_distribution": rel_type_dist,
            "validated": sum(1 for r in rows if r.get("status") == "已验证"),
            "ingested": sum(1 for r in rows if r.get("status") == "已入库"),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["graph_stats"] = self.compute_graph_stats()
        base["provenanced_edges"] = len(self.edge_provenance())
        return base