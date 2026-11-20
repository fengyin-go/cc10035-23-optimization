"""DOM解析领域服务：管理DOM解析配置，驱动下游实体抽取流水线。"""
from __future__ import annotations

import hashlib
import re
from typing import Any

from app.services.base import BaseModuleService
from app.services.versioning import (
    PARSE_VERSION_TABLE,
    PIPELINE_LOCK,
    VERSION_ACTIVE,
    VERSION_SUPERSEDED,
    next_id,
    now_iso,
)
from app.store import store


CSS_SELECTOR_RE = re.compile(r"^[.#]?[a-zA-Z_][\w\-]*(?:\s*[>+~]\s*[.#]?[a-zA-Z_][\w\-]*)*$")
XPath_RE = re.compile(r"^(//|[.]/|/)[\w\[\]@='\"\-,.()*/]+")


class DomParserService(BaseModuleService):
    MODULE = "dom_parser"
    ENTITY = "解析配置"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["配置名称", "目标URI", "解析引擎", "选择器规则", "输出Schema", "等待策略", "渲染模式"]
    REQUIRED_FIELDS = ["配置名称", "目标URI", "解析引擎"]
    STATUS_ORDER = ["草稿", "已配置", "解析中", "已停止"]
    ACTION_RULES = {"确认配置": "草稿", "启动解析": "已配置", "停止解析": "解析中"}
    NEGATIVE_ACTIONS = []

    def validate_selector(self, selector_rule: str) -> dict[str, Any]:
        """校验选择器规则语法，返回类型与合法标志。"""
        is_css = bool(CSS_SELECTOR_RE.match(selector_rule.strip()))
        is_xpath = bool(XPath_RE.match(selector_rule.strip()))
        return {
            "selector": selector_rule,
            "syntax_type": "css" if is_css else ("xpath" if is_xpath else "unknown"),
            "valid": is_css or is_xpath,
        }

    def simulate_parse(self, entry: dict[str, Any]) -> dict[str, Any]:
        """模拟一次DOM解析，输出解析字段摘要和哈希签名。"""
        uri = entry.get("目标URI", "")
        engine = entry.get("解析引擎", "beautifulsoup")
        selector = entry.get("选择器规则", "")
        schema = entry.get("输出Schema", "")
        selectors = [s.strip() for s in selector.split(",") if s.strip()]
        fields = [f.strip() for f in schema.split(",") if f.strip()]
        parser_hash = hashlib.sha256(
            f"{uri}|{engine}|{selector}|{schema}".encode()
        ).hexdigest()[:12]
        return {
            "config_name": entry.get("配置名称"),
            "target_uri": uri,
            "parser_engine": engine,
            "selector_count": len(selectors),
            "schema_fields": fields,
            "dom_hash": parser_hash,
            "extracted_records": 0,
        }

    def feed_entity_extract(self, entry_id: int) -> dict[str, Any]:
        """将解析结果输出为实体抽取的输入：被 entity_extract 消费。

        输出中携带解析版本清单里当前可消费版本的签名与字段集合，
        实体抽取侧据此确认结果应回写到哪个解析版本。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": "解析配置不存在"}
        parse_result = self.simulate_parse(entry)
        source_uri = parse_result["target_uri"]
        # 从store读取entity_extract模块数据，计算当前URI已有实体
        existing = [
            r for r in store.rows("entity_extract")
            if r.get("页面来源", "") == source_uri
        ]
        manifest = self.current_manifest(source_uri)
        return {
            "ok": True,
            "config_id": entry_id,
            "source_uri": source_uri,
            "parse_hash": parse_result["dom_hash"],
            "fields_for_extraction": parse_result["schema_fields"],
            "existing_entity_count": len(existing),
            # 消费解析版本清单：当前可消费的完整版本
            "parse_version": manifest,
        }

    # ------------------------------------------------------------------
    # 解析版本清单：配置每次登记都生成不可变版本，同页面旧版本自动替换
    # ------------------------------------------------------------------
    def register_parse_version(self, config_id: int) -> dict[str, Any]:
        """登记一个解析版本：配置签名 + 字段集合一起落库并标记可消费。

        同一页面（目标URI）已有可消费版本时，旧版本标记为「已替换」；
        已发布批次仍按其记录的版本ID读取原版本，字段合计不受影响。
        """
        entry = store.find(self.MODULE, config_id)
        if entry is None:
            return {"ok": False, "reason": "解析配置不存在"}
        parse_result = self.simulate_parse(entry)
        signature = parse_result["dom_hash"]
        fields = list(parse_result["schema_fields"])
        source_uri = parse_result["target_uri"]
        with PIPELINE_LOCK:
            table = store.rows(PARSE_VERSION_TABLE)
            # 同一配置同一签名重复登记：幂等返回既有版本，不产生新版本
            existing = next(
                (v for v in table
                 if v["config_id"] == config_id and v["config_signature"] == signature),
                None,
            )
            if existing is not None:
                return {"ok": True, "version": existing, "reused": True}
            # 同页面旧的可消费版本让位
            for version in table:
                if version["source_uri"] == source_uri and version["status"] == VERSION_ACTIVE:
                    version["status"] = VERSION_SUPERSEDED
                    version["superseded_at"] = now_iso()
            record = {
                "id": next_id(table),
                "config_id": config_id,
                "config_name": entry.get("配置名称"),
                "source_uri": source_uri,
                "parser_engine": parse_result["parser_engine"],
                "config_signature": signature,
                "selector_rules": entry.get("选择器规则", ""),
                "schema_fields": fields,
                "field_count": len(fields),
                "status": VERSION_ACTIVE,
                "consumable": True,
                "registered_at": now_iso(),
                "superseded_at": None,
            }
            table.append(record)
        return {"ok": True, "version": record, "reused": False}

    def current_manifest(self, source_uri: str) -> dict[str, Any] | None:
        """返回某页面当前可消费的完整解析版本（清单标记）。"""
        with PIPELINE_LOCK:
            for version in reversed(store.rows(PARSE_VERSION_TABLE)):
                if version["source_uri"] == source_uri and version["status"] == VERSION_ACTIVE:
                    return dict(version)
        return None

    def get_parse_version(self, version_id: int) -> dict[str, Any] | None:
        return next(
            (v for v in store.rows(PARSE_VERSION_TABLE) if int(v.get("id", 0)) == version_id),
            None,
        )

    def list_versions(self, source_uri: str | None = None) -> list[dict[str, Any]]:
        """解析版本清单：可按页面过滤，登记顺序即版本先后。"""
        with PIPELINE_LOCK:
            rows = [dict(v) for v in store.rows(PARSE_VERSION_TABLE)]
        if source_uri:
            rows = [v for v in rows if v["source_uri"] == source_uri]
        return rows

    def version_manifest_view(self) -> dict[str, Any]:
        """供接口回显：每个页面当前可消费版本 + 完整版本清单。"""
        with PIPELINE_LOCK:
            versions = [dict(v) for v in store.rows(PARSE_VERSION_TABLE)]
        uris = sorted({v["source_uri"] for v in versions})
        current = {uri: self.current_manifest(uri) for uri in uris}
        return {"versions": versions, "current_by_uri": current}

    def compute_parse_quality(self, entry_id: int) -> dict[str, Any]:
        """计算解析质量指标，被质量检查模块消费。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": "配置不存在"}
        selector = entry.get("选择器规则", "")
        selectors = [s.strip() for s in selector.split(",") if s.strip()]
        uri = entry.get("目标URI", "")
        return {
            "config_id": entry_id,
            "selector_validity_ratio": 1.0 if selectors else 0.0,
            "selector_count": len(selectors),
            "uri_format_valid": uri.startswith("http") if uri else False,
            "has_schema": bool(entry.get("输出Schema", "").strip()),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["parser_quality"] = {
            "total_configs": len(rows),
            "with_selectors": sum(1 for r in rows if r.get("选择器规则", "").strip()),
            "with_schema": sum(1 for r in rows if r.get("输出Schema", "").strip()),
            "valid_uris": sum(1 for r in rows if (r.get("目标URI", "") or "").startswith("http")),
        }
        versions = store.rows(PARSE_VERSION_TABLE)
        base["parse_versions"] = {
            "total": len(versions),
            "consumable": sum(1 for v in versions if v.get("status") == VERSION_ACTIVE),
            "superseded": sum(1 for v in versions if v.get("status") == VERSION_SUPERSEDED),
        }
        return base