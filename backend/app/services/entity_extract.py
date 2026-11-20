"""实体抽取领域服务：从DOM解析结果中识别命名实体。

在原有抽取能力之上增加配置改动下的结果一致性保障：

- 每次抽取作为一个「批次（run）」启动，记录消费的解析版本ID、配置签名与
  实际生效字段；
- 交付结果写入带来源的实体版本数据集（entity_versions），并回写解析版本清单；
- 旧配置抽取在途而新配置已交付不同字段时按接纳策略（fresh_first /
  pending_first）处理版本冲突，冲突可查询、可裁决；
- 同一运行并发提交同一签名回执按 replay_key 去重，只保留该次运行的记录；
- 已发布批次快照不可变，字段合计与解析版本字段数一致，配置记录随结果落库。
"""
from __future__ import annotations

import hashlib
import re
from typing import Any

from app.services.base import BaseModuleService
from app.services.versioning import (
    BATCH_CONFLICTED,
    BATCH_DELIVERED,
    BATCH_DISCARDED,
    BATCH_PUBLISHED,
    BATCH_RUNNING,
    ENTITY_VERSION_TABLE,
    EXTRACTION_BATCH_TABLE,
    EXTRACTION_CURSOR_TABLE,
    EXTRACTION_RECEIPT_TABLE,
    PARSE_VERSION_TABLE,
    PIPELINE_LOCK,
    POLICY_FRESH_FIRST,
    POLICY_PENDING_FIRST,
    ADMISSION_POLICIES,
    VERSION_ACTIVE,
    next_id,
    now_iso,
)
from app.store import store


ENTITY_TYPE_MAP = {
    "title": re.compile(r"<title[^>]*>(.*?)</title>", re.I),
    "heading": re.compile(r"<h[1-6][^>]*>(.*?)</h[1-6]>", re.I),
    "link": re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.I),
    "meta_value": re.compile(r'<meta[^>]+content=["\']([^"\']+)["\']', re.I),
}

# 批次终结状态：终结后不允许再交付或重投
TERMINAL_STATUSES = (BATCH_PUBLISHED, BATCH_DISCARDED)


class EntityExtractService(BaseModuleService):
    MODULE = "entity_extract"
    ENTITY = "实体条目"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["实体ID", "实体类型", "实体名称", "页面来源", "原文片段", "置信度", "抽取时间"]
    REQUIRED_FIELDS = ["实体ID", "实体类型", "实体名称"]
    STATUS_ORDER = ["已抽取", "已校验", "已入库", "已废弃"]
    ACTION_RULES = {"校验实体": "已抽取", "确认入库": "已校验", "标记废弃": "已抽取"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 批次生命周期：启动 -> 交付（可能冲突）-> 裁决 -> 发布
    # ------------------------------------------------------------------
    def start_extraction(
        self,
        dom_parser_config_id: int,
        *,
        conflict_policy: str = POLICY_FRESH_FIRST,
    ) -> dict[str, Any]:
        """消费解析版本清单启动一次抽取运行，固定本次采用的签名与字段集合。"""
        if conflict_policy not in ADMISSION_POLICIES:
            return {"ok": False, "reason": f"未知接纳策略：{conflict_policy}"}
        from app.services.dom_parser import DomParserService
        dps = DomParserService()
        entry = store.find(dps.MODULE, dom_parser_config_id)
        if entry is None:
            return {"ok": False, "reason": "解析配置不存在"}
        # 登记解析版本（幂等），确保实体抽取消费的是清单里可消费的完整版本
        registered = dps.register_parse_version(dom_parser_config_id)
        if not registered["ok"]:
            return registered
        version = registered["version"]
        with PIPELINE_LOCK:
            batches = store.rows(EXTRACTION_BATCH_TABLE)
            run_seq = next_id(batches)
            run_id = f"RUN-{run_seq:04d}-{version['config_signature'][:6]}"
            batch = {
                "id": run_seq,
                "run_id": run_id,
                "config_id": dom_parser_config_id,
                "config_name": version["config_name"],
                "source_uri": version["source_uri"],
                "parse_version_id": version["id"],
                "config_signature": version["config_signature"],
                "active_fields": list(version["schema_fields"]),
                "expected_field_count": version["field_count"],
                "status": BATCH_RUNNING,
                "conflict_policy": conflict_policy,
                "conflict_of": None,
                "conflict_reason": None,
                "entity_count": 0,
                "last_receipt_id": None,
                "immutable": False,
                "created_at": now_iso(),
                "delivered_at": None,
                "published_at": None,
            }
            batches.append(batch)
        return {"ok": True, "run_id": run_id, "batch": dict(batch), "parse_version": dict(version)}

    def deliver_result(
        self,
        run_id: str,
        *,
        entities: list[dict[str, Any]] | None = None,
        conflict_policy: str | None = None,
        replay_key: str | None = None,
    ) -> dict[str, Any]:
        """回传一次抽取结果：写实体版本数据集、回执，并按策略处理版本冲突。"""
        with PIPELINE_LOCK:
            batch = self._find_batch(run_id)
            if batch is None:
                return {"ok": False, "reason": f"抽取运行 {run_id} 不存在"}
            if batch["status"] in TERMINAL_STATUSES:
                return {
                    "ok": False,
                    "reason": f"批次已{batch['status']}，不可再交付",
                    "run_id": run_id,
                }
            receipts = store.rows(EXTRACTION_RECEIPT_TABLE)
            # 并发提交同一签名的回执：同 run + 同 replay_key 只保留首次记录
            replay_key = replay_key or f"{run_id}-{next_id(receipts):04d}"
            duplicate = next(
                (r for r in receipts
                 if r["run_id"] == run_id and r["replay_key"] == replay_key),
                None,
            )
            if duplicate is not None:
                return {
                    "ok": True,
                    "deduplicated": True,
                    "receipt": dict(duplicate),
                    "batch": dict(batch),
                    "message": "同一签名回执已存在，沿用该次运行的既有记录",
                }

            version = self._parse_version(batch["parse_version_id"])
            if version is None:
                return {"ok": False, "reason": "解析版本清单中找不到对应版本，拒绝回写"}
            built = self._build_entities(batch, version, entities)
            if not built["ok"]:
                return built

            effective = self._effective_version_id(batch["source_uri"])
            conflict = effective is not None and effective != version["id"]
            policy = conflict_policy or batch["conflict_policy"]
            if conflict_policy is not None and conflict_policy in ADMISSION_POLICIES:
                batch["conflict_policy"] = conflict_policy

            receipt = {
                "id": next_id(receipts),
                "receipt_no": f"RC-{len(receipts) + 1:04d}",
                "run_id": run_id,
                "replay_key": replay_key,
                "config_signature": batch["config_signature"],
                "parse_version_id": version["id"],
                "entity_count": len(built["entities"]),
                "conflict": conflict,
                "policy": policy,
                "delivered_at": now_iso(),
            }

            # 无论是否冲突，先把该次运行的结果写入带来源的实体版本数据集
            self._write_versioned_entities(batch, version, built["entities"], receipt)
            batch["entity_count"] = len(built["entities"])
            batch["last_receipt_id"] = receipt["id"]
            batch["delivered_at"] = receipt["delivered_at"]

            accepted = True
            if conflict:
                manifest = self._current_manifest(batch["source_uri"])
                batch["conflict_of"] = effective
                batch["conflict_reason"] = (
                    f"页面当前生效版本为 v{effective}，"
                    f"本批次基于 v{version['id']}({batch['config_signature'][:6]})"
                )
                if manifest is not None:
                    batch["conflict_reason"] += (
                        f"；清单可消费版本为 v{manifest['id']}({manifest['config_signature'][:6]})"
                    )
                in_flight = self._has_older_running_batch(batch)
                if policy == POLICY_PENDING_FIRST and in_flight:
                    # 交付时效让位于既有待处理数据：挂起等待人工裁决
                    accepted = False
                    batch["status"] = BATCH_CONFLICTED
                    receipt["accepted"] = False
                    receipt["decision"] = BATCH_CONFLICTED
                else:
                    # fresh_first，或没有在途旧批次：直接接纳新版本
                    self._adopt_batch(batch, version)
                    receipt["accepted"] = True
                    receipt["decision"] = BATCH_DELIVERED
            else:
                self._adopt_batch(batch, version)
                receipt["accepted"] = True
                receipt["decision"] = BATCH_DELIVERED

            receipts.append(receipt)
            return {
                "ok": True,
                "deduplicated": False,
                "accepted": accepted,
                "conflict": conflict,
                "policy": policy,
                "receipt": dict(receipt),
                "batch": dict(batch),
            }

    def resolve_conflict(self, run_id: str, decision: str) -> dict[str, Any]:
        """对冲突待裁决批次做出接纳/废弃决定。"""
        with PIPELINE_LOCK:
            batch = self._find_batch(run_id)
            if batch is None:
                return {"ok": False, "reason": f"抽取运行 {run_id} 不存在"}
            if batch["status"] != BATCH_CONFLICTED:
                return {"ok": False, "reason": f"批次当前状态为{batch['status']}，无需裁决"}
            if decision == "废弃":
                batch["status"] = BATCH_DISCARDED
                return {"ok": True, "batch": dict(batch)}
            if decision != "接纳":
                return {"ok": False, "reason": "裁决动作只支持 接纳 / 废弃"}
            version = self._parse_version(batch["parse_version_id"])
            if version is None:
                return {"ok": False, "reason": "原解析版本已丢失，无法接纳"}
            self._adopt_batch(batch, version)
            # 裁决接纳后补记一条接受回执，保留该次运行的处理轨迹
            receipts = store.rows(EXTRACTION_RECEIPT_TABLE)
            receipt = {
                "id": next_id(receipts),
                "receipt_no": f"RC-{len(receipts) + 1:04d}",
                "run_id": run_id,
                "replay_key": f"{run_id}-resolve-{next_id(receipts):04d}",
                "config_signature": batch["config_signature"],
                "parse_version_id": version["id"],
                "entity_count": batch["entity_count"],
                "conflict": True,
                "policy": "manual_adopt",
                "accepted": True,
                "decision": BATCH_DELIVERED,
                "delivered_at": now_iso(),
            }
            receipts.append(receipt)
            batch["last_receipt_id"] = receipt["id"]
            return {"ok": True, "batch": dict(batch), "receipt": dict(receipt)}

    def redeliver(self, run_id: str, **kwargs: Any) -> dict[str, Any]:
        """重投：冲突挂起或交付失败的批次可重新回传；已发布批次拒绝重投。"""
        with PIPELINE_LOCK:
            batch = self._find_batch(run_id)
            if batch is None:
                return {"ok": False, "reason": f"抽取运行 {run_id} 不存在"}
            if batch["immutable"] or batch["status"] == BATCH_PUBLISHED:
                return {"ok": False, "reason": "已发布批次不可变，拒绝重投", "run_id": run_id}
        return self.deliver_result(run_id, **kwargs)

    def publish_batch(self, run_id: str) -> dict[str, Any]:
        """发布批次：校验字段合计与解析版本一致后冻结快照。"""
        with PIPELINE_LOCK:
            batch = self._find_batch(run_id)
            if batch is None:
                return {"ok": False, "reason": f"抽取运行 {run_id} 不存在"}
            if batch["status"] == BATCH_PUBLISHED:
                return {"ok": True, "batch": dict(batch), "reused": True}
            if batch["status"] != BATCH_DELIVERED:
                return {"ok": False, "reason": f"批次状态为{batch['status']}，不可发布"}
            version = self._parse_version(batch["parse_version_id"])
            if version is None:
                return {"ok": False, "reason": "解析版本清单中找不到原版本，拒绝发布"}
            snapshot = [
                row for row in store.rows(ENTITY_VERSION_TABLE) if row["run_id"] == run_id
            ]
            # 字段合计一致：每个生效字段产出一条实体，且签名与原版本吻合
            if len(snapshot) != version["field_count"]:
                return {
                    "ok": False,
                    "reason": "字段合计不一致",
                    "snapshot_count": len(snapshot),
                    "version_field_count": version["field_count"],
                }
            if {row["source_field"] for row in snapshot} != set(version["schema_fields"]):
                return {"ok": False, "reason": "快照字段集合与解析版本字段不一致"}
            if any(row["config_signature"] != version["config_signature"] for row in snapshot):
                return {"ok": False, "reason": "快照签名与解析版本签名不一致"}
            batch["status"] = BATCH_PUBLISHED
            batch["immutable"] = True
            batch["published_at"] = now_iso()
            for row in snapshot:
                row["published"] = True
            # 主实体表中属于本批次的条目进入已入库
            for row in store.rows(self.MODULE):
                if row.get("运行ID") == run_id:
                    row["status"] = "已入库"
                    row["pending"] = False
            return {
                "ok": True,
                "batch": dict(batch),
                "parse_version": dict(version),
                "field_total": len(snapshot),
            }

    # ------------------------------------------------------------------
    # 查询：批次、冲突、页面刷新明细、当前实体集引用的版本
    # ------------------------------------------------------------------
    def list_batches(self, source_uri: str | None = None) -> list[dict[str, Any]]:
        with PIPELINE_LOCK:
            rows = [dict(b) for b in store.rows(EXTRACTION_BATCH_TABLE)]
        if source_uri:
            rows = [b for b in rows if b["source_uri"] == source_uri]
        return rows

    def get_batch(self, run_id: str) -> dict[str, Any] | None:
        batch = self._find_batch(run_id)
        return dict(batch) if batch else None

    def list_conflicts(self) -> list[dict[str, Any]]:
        with PIPELINE_LOCK:
            return [
                dict(b) for b in store.rows(EXTRACTION_BATCH_TABLE)
                if b["status"] == BATCH_CONFLICTED
            ]

    def list_receipts(self, run_id: str | None = None) -> list[dict[str, Any]]:
        with PIPELINE_LOCK:
            rows = [dict(r) for r in store.rows(EXTRACTION_RECEIPT_TABLE)]
        if run_id:
            rows = [r for r in rows if r["run_id"] == run_id]
        return rows

    def entity_version_dataset(
        self,
        source_uri: str | None = None,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """带来源的实体版本数据集：每条实体都可追溯版本、签名、生效字段。"""
        with PIPELINE_LOCK:
            rows = [dict(r) for r in store.rows(ENTITY_VERSION_TABLE)]
        if source_uri:
            rows = [r for r in rows if r["source_uri"] == source_uri]
        if run_id:
            rows = [r for r in rows if r["run_id"] == run_id]
        return rows

    def page_refresh(self, source_uri: str) -> dict[str, Any]:
        """同一页面刷新：返回新旧版本明细、当前实体集、在途批次与回执。"""
        with PIPELINE_LOCK:
            from app.services.dom_parser import DomParserService
            versions = [
                dict(v) for v in DomParserService().list_versions(source_uri)
            ]
            batches = [
                dict(b) for b in store.rows(EXTRACTION_BATCH_TABLE)
                if b["source_uri"] == source_uri
            ]
            receipts = [
                dict(r) for r in store.rows(EXTRACTION_RECEIPT_TABLE)
                if any(b["run_id"] == r["run_id"] for b in batches)
            ]
            snapshot = [
                dict(r) for r in store.rows(ENTITY_VERSION_TABLE)
                if r["source_uri"] == source_uri
            ]
            cursor = next(
                (dict(c) for c in store.rows(EXTRACTION_CURSOR_TABLE)
                 if c["source_uri"] == source_uri),
                None,
            )
            current_entities = [
                dict(r) for r in store.rows(self.MODULE)
                if r.get("页面来源") == source_uri and r.get("status") != "已废弃"
            ]
        return {
            "source_uri": source_uri,
            "parse_versions": versions,
            "current_cursor": cursor,
            "current_entities": current_entities,
            "batches": batches,
            "receipts": receipts,
            "entity_version_dataset": snapshot,
        }

    def extract_from_parse(self, dom_parser_config_id: int) -> dict[str, Any]:
        """兼容旧调用链：启动并同步交付一次抽取，返回结构保持原有形状。"""
        started = self.start_extraction(dom_parser_config_id)
        if not started["ok"]:
            return started
        run_id = started["run_id"]
        delivered = self.deliver_result(run_id)
        if not delivered["ok"]:
            return delivered
        snapshot = self.entity_version_dataset(run_id=run_id)
        entities = [
            {
                "实体ID": row["entity_id"],
                "实体类型": row["entity_type"],
                "实体名称": row["entity_name"],
                "页面来源": row["source_uri"],
                "原文片段": row["snippet"],
                "置信度": row["confidence"],
                "抽取时间": row["extracted_at"][:10],
                "解析版本ID": row["parse_version_id"],
                "解析签名": row["config_signature"],
                "运行ID": row["run_id"],
                "生效字段": row["active_fields"],
            }
            for row in snapshot
        ]
        return {
            "ok": True,
            "source_uri": started["batch"]["source_uri"],
            "entity_count": len(entities),
            "entities": entities,
            "run_id": run_id,
            "parse_version_id": started["batch"]["parse_version_id"],
            "config_signature": started["batch"]["config_signature"],
        }

    def compute_entity_distribution(self) -> dict[str, int]:
        """计算实体类型分布，供聚合视图消费。"""
        rows = store.rows(self.MODULE)
        dist: dict[str, int] = {}
        for row in rows:
            etype = row.get("实体类型", "未知")
            dist[etype] = dist.get(etype, 0) + 1
        return dist

    def extract_for_relation_graph(self) -> list[dict[str, Any]]:
        """将已入库实体提供给关系图谱模块构建关系，并带上版本来源。"""
        rows = [r for r in store.rows(self.MODULE) if r.get("status") == "已入库"]
        return [
            {
                "entity_id": r.get("实体ID"),
                "entity_type": r.get("实体类型"),
                "entity_name": r.get("实体名称"),
                "source_page": r.get("页面来源"),
                "run_id": r.get("运行ID"),
                "parse_version_id": r.get("解析版本ID"),
                "config_signature": r.get("解析签名"),
                "active_fields": r.get("生效字段"),
            }
            for r in rows
        ]

    def _guess_type(self, field_name: str) -> str:
        field_lower = field_name.lower()
        if any(k in field_lower for k in ("name", "名称", "title", "标题")):
            return "命名实体"
        if any(k in field_lower for k in ("price", "价格", "amount", "金额")):
            return "数值实体"
        if any(k in field_lower for k in ("date", "日期", "time", "时间")):
            return "时间实体"
        if any(k in field_lower for k in ("url", "link", "链接")):
            return "链接实体"
        return "属性实体"

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------
    def _find_batch(self, run_id: str) -> dict[str, Any] | None:
        return next(
            (b for b in store.rows(EXTRACTION_BATCH_TABLE) if b["run_id"] == run_id),
            None,
        )

    def _parse_version(self, version_id: int) -> dict[str, Any] | None:
        return next(
            (v for v in store.rows(PARSE_VERSION_TABLE) if int(v.get("id", 0)) == version_id),
            None,
        )

    def _current_manifest(self, source_uri: str) -> dict[str, Any] | None:
        return next(
            (v for v in reversed(store.rows(PARSE_VERSION_TABLE))
             if v["source_uri"] == source_uri and v["status"] == VERSION_ACTIVE),
            None,
        )

    def _page_cursor(self, source_uri: str) -> dict[str, Any] | None:
        return next(
            (c for c in store.rows(EXTRACTION_CURSOR_TABLE)
             if c["source_uri"] == source_uri),
            None,
        )

    def _effective_version_id(self, source_uri: str) -> int | None:
        """页面当前实体集实际引用的解析版本：游标优先，清单可消费版本兜底。"""
        cursor = self._page_cursor(source_uri)
        if cursor is not None:
            return cursor["current_parse_version_id"]
        manifest = self._current_manifest(source_uri)
        return manifest["id"] if manifest else None

    def _has_older_running_batch(self, incoming: dict[str, Any]) -> bool:
        """是否存在引用更旧解析版本、仍在运行的同页面批次（既有待处理数据）。"""
        for batch in store.rows(EXTRACTION_BATCH_TABLE):
            if (
                batch["source_uri"] == incoming["source_uri"]
                and batch["run_id"] != incoming["run_id"]
                and batch["status"] == BATCH_RUNNING
                and batch["parse_version_id"] < incoming["parse_version_id"]
            ):
                return True
        return False

    def _build_entities(
        self,
        batch: dict[str, Any],
        version: dict[str, Any],
        custom: list[dict[str, Any]] | None,
    ) -> dict[str, Any]:
        fields = list(version["schema_fields"])
        source_uri = version["source_uri"]
        if custom is None:
            entities = []
            for idx, field in enumerate(fields):
                eid = hashlib.md5(f"{source_uri}|{field}".encode()).hexdigest()[:8]
                confidence = round(0.95 - (0.05 * (idx % 3)), 2)
                entities.append({
                    "entity_id": eid,
                    "entity_type": self._guess_type(field),
                    "entity_name": field,
                    "source_field": field,
                    "snippet": f"提取自 {source_uri} #{idx + 1}",
                    "confidence": str(confidence),
                })
            return {"ok": True, "entities": entities}
        if len(custom) != len(fields):
            return {
                "ok": False,
                "reason": "回传实体数与解析版本生效字段数不一致",
                "entity_count": len(custom),
                "field_count": len(fields),
            }
        cleaned = []
        for item in custom:
            if not item.get("entity_name"):
                return {"ok": False, "reason": "回传实体缺少 entity_name"}
            field = item.get("source_field") or item["entity_name"]
            if field not in fields:
                return {"ok": False, "reason": f"回传字段 {field} 不在解析版本字段集合内"}
            cleaned.append({
                "entity_id": item.get("entity_id")
                or hashlib.md5(f"{source_uri}|{field}".encode()).hexdigest()[:8],
                "entity_type": item.get("entity_type") or self._guess_type(field),
                "entity_name": item["entity_name"],
                "source_field": field,
                "snippet": item.get("snippet", f"提取自 {source_uri}"),
                "confidence": str(item.get("confidence", "0.90")),
            })
        return {"ok": True, "entities": cleaned}

    def _write_versioned_entities(
        self,
        batch: dict[str, Any],
        version: dict[str, Any],
        entities: list[dict[str, Any]],
        receipt: dict[str, Any],
    ) -> None:
        table = store.rows(ENTITY_VERSION_TABLE)
        # 重投/重复交付以最新结果为准：先移除该运行既有快照（已发布运行在入口已拦截），
        # 保证实体版本数据集中每个运行的行数与解析版本字段合计始终一致。
        for row in [r for r in table if r["run_id"] == batch["run_id"]]:
            table.remove(row)
        for item in entities:
            table.append({
                "id": next_id(table),
                "run_id": batch["run_id"],
                "parse_version_id": version["id"],
                "config_id": batch["config_id"],
                "config_signature": version["config_signature"],
                "source_uri": batch["source_uri"],
                "entity_id": item["entity_id"],
                "entity_type": item["entity_type"],
                "entity_name": item["entity_name"],
                "source_field": item["source_field"],
                "active_fields": list(batch["active_fields"]),
                "snippet": item["snippet"],
                "confidence": item["confidence"],
                "extracted_at": now_iso(),
                "receipt_id": receipt["id"],
                "published": False,
            })

    def _adopt_batch(self, batch: dict[str, Any], version: dict[str, Any]) -> None:
        """接纳批次：切换页面当前实体集，并把结果回写到主实体表与游标。"""
        batch["status"] = BATCH_DELIVERED
        batch["conflict_of"] = None
        batch["conflict_reason"] = None
        snapshot = [
            row for row in store.rows(ENTITY_VERSION_TABLE)
            if row["run_id"] == batch["run_id"]
        ]
        main_table = store.rows(self.MODULE)
        # 同页面旧批次条目整体废弃，当前实体集只引用新接纳版本
        for row in main_table:
            if row.get("页面来源") == batch["source_uri"] and row.get("运行ID") != batch["run_id"]:
                row["status"] = "已废弃"
                row["pending"] = False
        existing_ids = {row.get("实体ID") for row in main_table if row.get("运行ID") == batch["run_id"]}
        extracted_date = now_iso()[:10]
        for row in snapshot:
            if row["entity_id"] in existing_ids:
                continue
            main_table.append({
                "id": next_id(main_table),
                "status": "已抽取",
                "pending": True,
                "abnormal": False,
                "实体ID": row["entity_id"],
                "实体类型": row["entity_type"],
                "实体名称": row["entity_name"],
                "页面来源": row["source_uri"],
                "原文片段": row["snippet"],
                "置信度": row["confidence"],
                "抽取时间": extracted_date,
                "解析版本ID": row["parse_version_id"],
                "解析签名": row["config_signature"],
                "运行ID": row["run_id"],
                "生效字段": list(row["active_fields"]),
            })
        cursors = store.rows(EXTRACTION_CURSOR_TABLE)
        cursor = next(
            (c for c in cursors if c["source_uri"] == batch["source_uri"]),
            None,
        )
        if cursor is None:
            cursors.append({
                "id": next_id(cursors),
                "source_uri": batch["source_uri"],
                "current_run_id": batch["run_id"],
                "current_parse_version_id": version["id"],
                "current_signature": version["config_signature"],
                "active_fields": list(version["schema_fields"]),
                "updated_at": now_iso(),
            })
        else:
            cursor.update({
                "current_run_id": batch["run_id"],
                "current_parse_version_id": version["id"],
                "current_signature": version["config_signature"],
                "active_fields": list(version["schema_fields"]),
                "updated_at": now_iso(),
            })

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["entity_distribution"] = self.compute_entity_distribution()
        base["ready_for_relation"] = len(self.extract_for_relation_graph())
        batches = store.rows(EXTRACTION_BATCH_TABLE)
        base["extraction_batches"] = {
            "total": len(batches),
            "running": sum(1 for b in batches if b["status"] == BATCH_RUNNING),
            "delivered": sum(1 for b in batches if b["status"] == BATCH_DELIVERED),
            "conflicted": sum(1 for b in batches if b["status"] == BATCH_CONFLICTED),
            "published": sum(1 for b in batches if b["status"] == BATCH_PUBLISHED),
        }
        base["entity_version_rows"] = len(store.rows(ENTITY_VERSION_TABLE))
        return base
