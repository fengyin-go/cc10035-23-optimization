"""并发与回归测试：直接调用领域服务，验证锁保护下的回执去重与旧链路兼容。"""
from __future__ import annotations

import concurrent.futures
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.dom_parser import DomParserService
from app.services.entity_extract import EntityExtractService
from app.services.relation_graph import RelationGraphService
from app.services.versioning import (
    EXTRACTION_RECEIPT_TABLE,
    BATCH_RUNNING,
)
from app.store import store

FAIL = 0


def check(name: str, condition: bool) -> None:
    global FAIL
    print(("  ✓ " if condition else "  ✗ ") + name)
    if not condition:
        FAIL += 1


def make_config(label: str, schema: str) -> int:
    entry, missing = DomParserService().create_entry({
        "配置名称": label,
        "目标URI": f"https://concurrency.example/{label}",
        "解析引擎": "beautifulsoup",
        "选择器规则": ".a, .b",
        "输出Schema": schema,
    })
    assert not missing
    return entry["id"]


def main() -> int:
    dps = DomParserService()
    ees = EntityExtractService()
    rgs = RelationGraphService()

    print("== 并发1：多线程同时登记同页面配置版本，清单始终只有一个可消费版本 ==")
    cid = make_config("并发配置A", "f1,f2,f3,f4")
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: dps.register_parse_version(cid), range(8)))
    check("8次并发登记全部成功", all(r["ok"] for r in results))
    check("只产生一个版本（幂等）", len({r["version"]["id"] for r in results}) == 1)
    uri = f"https://concurrency.example/并发配置A"
    check("页面只有一个可消费版本",
          sum(1 for v in dps.list_versions(uri) if v["status"] == "可消费") == 1)

    print("== 并发2：同运行多线程同 replay_key 交付，只保留一条回执 ==")
    started = ees.start_extraction(cid)
    run_id = started["run_id"]

    def deliver(_: int) -> dict:
        return ees.deliver_result(run_id, replay_key="same-signature-key")

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        deliveries = list(pool.map(deliver, range(8)))
    receipts = [r for r in store.rows(EXTRACTION_RECEIPT_TABLE) if r["run_id"] == run_id]
    check("8次并发交付只保留1条回执", len(receipts) == 1)
    check("仅首次为真实写入，其余标记去重",
          sum(1 for d in deliveries if not d.get("deduplicated")) == 1
          and sum(1 for d in deliveries if d.get("deduplicated")) == 7)
    check("批次实体数稳定为版本字段数4", ees.get_batch(run_id)["entity_count"] == 4)
    check("实体版本数据集该批次恰好4条",
          len(ees.entity_version_dataset(run_id=run_id)) == 4)

    print("== 并发3：运行中批次并发启动不串号，各运行状态独立 ==")
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        starts = list(pool.map(lambda i: ees.start_extraction(cid), range(4)))
    run_ids = {s["run_id"] for s in starts}
    check("4次并发启动产生4个独立运行", len(run_ids) == 4)
    check("新运行均为运行中状态",
          all(ees.get_batch(rid)["status"] == BATCH_RUNNING for rid in run_ids))
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        deliveries = list(pool.map(
            lambda rid: ees.deliver_result(rid, replay_key=f"key-{rid}"), run_ids))
    check("各运行独立交付成功且实体数均为4",
          all(d["ok"] and d["receipt"]["entity_count"] == 4 for d in deliveries))
    check("每个运行各自只留1条回执",
          all(len([r for r in store.rows(EXTRACTION_RECEIPT_TABLE) if r["run_id"] == rid]) == 1
              for rid in run_ids))

    print("== 回归1：旧调用链 extract_from_parse 仍可用且带版本来源 ==")
    cid3 = make_config("回归配置", "name,price")
    legacy = ees.extract_from_parse(cid3)
    check("旧链路返回ok与实体", legacy["ok"] and legacy["entity_count"] == 2)
    check("旧链路结果附带解析版本与签名",
          legacy["parse_version_id"] and len(legacy["config_signature"]) == 12)
    check("旧链路实体逐条带来源字段",
          all(e["解析版本ID"] == legacy["parse_version_id"] for e in legacy["entities"]))

    print("== 回归2：dom_parser 既有模拟解析与质量接口形状不变 ==")
    sim = dps.simulate_parse(store.find("dom_parser", cid3))
    check("simulate_parse 仍输出 dom_hash 与字段",
          sim["dom_hash"] and sim["schema_fields"] == ["name", "price"])
    feed = dps.feed_entity_extract(cid3)
    check("feed_entity_extract 新增 parse_version 且原字段保留",
          feed["ok"] and feed["parse_hash"] and feed["parse_version"]["status"] == "可消费")
    quality = dps.compute_parse_quality(cid3)
    check("compute_parse_quality 正常", quality["has_schema"] is True)

    print("== 回归3：关系图谱 build_relations 原始接口不变，来源接口可叠加 ==")
    built = rgs.build_relations(legacy["entities"])
    check("原始建边接口正常", built["ok"] and built["relation_count"] >= 1)
    run_published = ees.publish_batch(legacy["run_id"])
    check("旧链路批次可发布", run_published["ok"])
    edge = rgs.build_relations_from_batch(legacy["run_id"])
    check("批次建边带来源", edge["ok"] and edge["parse_version_id"] == legacy["parse_version_id"])

    print("== 回归4：领域快照包含版本化统计 ==")
    snap_d = dps.domain_snapshot()
    snap_e = ees.domain_snapshot()
    snap_g = rgs.domain_snapshot()
    check("DOM快照含 parse_versions 统计", "parse_versions" in snap_d)
    check("实体快照含 extraction_batches 统计", snap_e["extraction_batches"]["total"] >= 4)
    check("图谱快照含 provenanced_edges", snap_g["provenanced_edges"] >= 1)

    print(f"\n结果：失败 {FAIL} 项")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
