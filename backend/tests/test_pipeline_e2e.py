"""版本化抽取流水线端到端场景测试（仅用标准库，打真实 HTTP 接口）。

覆盖：
1. 依次登记旧/新解析配置，解析版本清单自动替换可消费标记；
2. 旧配置抽取在途、新配置先交付不同字段时的版本冲突与两种接纳策略；
3. 交错回传结果、冲突裁决、重投后活动字段核对；
4. 并发提交同一签名回执只保留该次运行的记录；
5. 已发布批次不可变、字段合计与原解析版本一致、配置记录与结果一起落库；
6. 页面刷新返回新旧版本明细、当前实体集与各批次引用的版本；
7. 图谱建边沿用批次字段集合，边带来源签名。
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000/api"
PASS = 0
FAIL = 0


def call(method: str, path: str, payload: dict | None = None, expect_error: bool = False):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = json.loads(exc.read().decode())
        if expect_error:
            return exc.code, body
        raise AssertionError(f"{method} {path} 意外失败 {exc.code}: {body}") from None


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def create_config(label: str, schema: str) -> int:
    status, body = call("POST", "/dom_parser", {
        "values": {
            "配置名称": label,
            "目标URI": "https://example.com/product/1",
            "解析引擎": "beautifulsoup",
            "选择器规则": ".title, .price, .sku",
            "输出Schema": schema,
        }
    })
    check(f"登记{label}", status == 200 and body["ok"])
    return body["entry"]["id"]


def main() -> int:
    print("== 场景1：依次登记旧配置与新配置，解析版本清单维护可消费版本 ==")
    old_id = create_config("旧版商品解析配置", "product_name,price,sku")
    new_id = create_config("新版商品解析配置", "product_name,price,sku,rating,review_count")

    _, v1 = call("POST", "/pipeline/parse-versions/register", {"config_id": old_id})
    _, v2 = call("POST", "/pipeline/parse-versions/register", {"config_id": new_id})
    check("两次登记生成不同版本ID", v1["version"]["id"] != v2["version"]["id"])
    check("版本携带配置签名", len(v1["version"]["config_signature"]) == 12)
    check("新旧版本签名不同",
          v1["version"]["config_signature"] != v2["version"]["config_signature"])
    check("旧版本字段集合为3项", v1["version"]["schema_fields"] == ["product_name", "price", "sku"])
    check("新版本字段集合为5项", len(v2["version"]["schema_fields"]) == 5)

    _, manifest = call("GET", "/pipeline/parse-versions?source_uri=https%3A%2F%2Fexample.com%2Fproduct%2F1")
    versions = manifest["filtered"]
    check("清单包含两个版本", len(versions) == 2)
    check("旧版本标记已替换", versions[0]["status"] == "已替换" and versions[0]["consumable"] is True)
    check("新版本标记可消费", versions[1]["status"] == "可消费")
    check("清单当前指针指向新版本",
          manifest["current_by_uri"]["https://example.com/product/1"]["id"] == v2["version"]["id"])
    # 重复登记同配置同签名幂等
    _, v1_again = call("POST", "/pipeline/parse-versions/register", {"config_id": old_id})
    check("同签名重复登记幂等复用", v1_again["reused"] is True
          and v1_again["version"]["id"] == v1["version"]["id"])

    print("== 场景2：旧配置抽取在途，新配置先交付 —— fresh_first 直接接纳 ==")
    _, run_a = call("POST", "/pipeline/extraction/runs",
                    {"config_id": old_id, "conflict_policy": "fresh_first"})
    _, run_b = call("POST", "/pipeline/extraction/runs",
                    {"config_id": new_id, "conflict_policy": "fresh_first"})
    ra, rb = run_a["run_id"], run_b["run_id"]
    check("运行A固定旧版本与3字段",
          run_a["batch"]["parse_version_id"] == v1["version"]["id"]
          and run_a["batch"]["active_fields"] == ["product_name", "price", "sku"])
    check("运行B固定新版本与5字段",
          run_b["batch"]["parse_version_id"] == v2["version"]["id"]
          and len(run_b["batch"]["active_fields"]) == 5)
    check("两个批次均为运行中",
          run_a["batch"]["status"] == "运行中" and run_b["batch"]["status"] == "运行中")

    # 交错回传：B（新配置）先交付
    _, del_b = call("POST", "/pipeline/extraction/deliver", {"run_id": rb})
    check("B先交付识别无冲突（启动时固定的就是当前版本）",
          del_b["conflict"] is False and del_b["accepted"] is True)
    check("B交付5条实体", del_b["receipt"]["entity_count"] == 5)

    # A（旧配置）后交付：页面当前已是v2，发生版本冲突，fresh_first 直接接纳
    _, del_a = call("POST", "/pipeline/extraction/deliver", {"run_id": ra})
    check("A后交付识别版本冲突", del_a["conflict"] is True)
    check("fresh_first 下A仍被接纳", del_a["accepted"] is True
          and del_a["batch"]["status"] == "已交付")
    check("回执记录冲突与策略", del_a["receipt"]["policy"] == "fresh_first")

    print("== 场景3：pending_first 策略 —— 新配置交付让位于在途旧批次 ==")
    _, run_c = call("POST", "/pipeline/extraction/runs",
                    {"config_id": old_id, "conflict_policy": "pending_first"})
    _, run_d = call("POST", "/pipeline/extraction/runs",
                    {"config_id": new_id, "conflict_policy": "pending_first"})
    rc, rd = run_c["run_id"], run_d["run_id"]
    # C（旧）保持在途，D（新）先交付 -> 冲突挂起
    _, del_d = call("POST", "/pipeline/extraction/deliver", {"run_id": rd})
    check("pending_first 下新批次挂起待裁决",
          del_d["accepted"] is False and del_d["batch"]["status"] == "冲突待裁决")
    _, conflicts = call("GET", "/pipeline/extraction/conflicts")
    conflict_row = next(b for b in conflicts["items"] if b["run_id"] == rd)
    check("冲突记录引用的新旧版本可查明",
          conflict_row["parse_version_id"] == v2["version"]["id"]
          and conflict_row["conflict_of"] == v1["version"]["id"]
          and conflict_row["config_signature"] == v2["version"]["config_signature"])

    # 重投挂起批次，改用 fresh_first -> 接纳，核对重投后活动字段
    _, redel_d = call("POST", "/pipeline/extraction/redeliver",
                      {"run_id": rd, "conflict_policy": "fresh_first"})
    check("重投后批次被接纳", redel_d["accepted"] is True
          and redel_d["batch"]["status"] == "已交付")
    check("重投后活动字段仍为新版本5字段",
          redel_d["batch"]["active_fields"] == v2["version"]["schema_fields"])
    _, pub_d = call("POST", "/pipeline/extraction/publish", {"run_id": rd})
    check("重投后批次可发布且字段合计一致",
          pub_d["ok"] and pub_d["field_total"] == 5 and pub_d["batch"]["immutable"] is True)
    _, ev_d = call("GET", f"/pipeline/extraction/entity-versions?run_id={rd}")
    check("重投发布后实体版本数据集无重复行",
          ev_d["total"] == 5
          and len({r["source_field"] for r in ev_d["items"]}) == 5)

    print("== 场景4：并发提交同一签名回执只保留该次运行的记录 ==")
    _, run_e = call("POST", "/pipeline/extraction/runs", {"config_id": new_id})
    re_id = run_e["run_id"]
    _, rc1 = call("POST", "/pipeline/extraction/deliver",
                  {"run_id": re_id, "replay_key": "sig-abc-001"})
    _, rc2 = call("POST", "/pipeline/extraction/deliver",
                  {"run_id": re_id, "replay_key": "sig-abc-001"})
    check("同 replay_key 第二次提交被去重", rc2["deduplicated"] is True)
    check("去重回执沿用首次记录", rc2["receipt"]["id"] == rc1["receipt"]["id"])
    _, receipts = call("GET", f"/pipeline/extraction/receipts?run_id={re_id}")
    check("该运行只保留一条回执记录", receipts["total"] == 1)

    print("== 场景5：发布批次不可变，字段合计与原版本一致，配置记录随结果落库 ==")
    _, pub_b = call("POST", "/pipeline/extraction/publish", {"run_id": rb})
    check("B发布成功", pub_b["batch"]["status"] == "已发布" and pub_b["batch"]["immutable"] is True)
    check("发布时字段合计与版本字段数一致", pub_b["field_total"] == 5)
    # 发布后重投被拒绝
    code, err = call("POST", "/pipeline/extraction/redeliver",
                     {"run_id": rb}, expect_error=True)
    check("已发布批次拒绝重投", code == 400 and "不可变" in err["detail"])
    # 已发布批次仍可读取原版本且签名一致
    _, ev_b = call("GET", f"/pipeline/extraction/entity-versions?run_id={rb}")
    check("发布批次实体版本数据集5条", ev_b["total"] == 5)
    check("每条实体带来源：版本ID/签名/配置ID/生效字段",
          all(row["parse_version_id"] == v2["version"]["id"]
              and row["config_signature"] == v2["version"]["config_signature"]
              and row["config_id"] == new_id
              and row["active_fields"] == v2["version"]["schema_fields"]
              and row["published"] is True
              for row in ev_b["items"]))
    check("实体字段合计覆盖版本全部字段",
          sorted(r["source_field"] for r in ev_b["items"])
          == sorted(v2["version"]["schema_fields"]))
    # 发布后再次发布幂等
    _, pub_b2 = call("POST", "/pipeline/extraction/publish", {"run_id": rb})
    check("重复发布幂等", pub_b2.get("reused") is True)
    # 字段合计不一致的伪造交付被拒
    _, run_f = call("POST", "/pipeline/extraction/runs", {"config_id": old_id})
    bad_entities = [{"entity_name": "only_one", "source_field": "product_name"}]
    code, err = call("POST", "/pipeline/extraction/deliver",
                     {"run_id": run_f["run_id"], "entities": bad_entities}, expect_error=True)
    check("回传实体数与字段数不一致被拒绝", code == 400 and "不一致" in err["detail"])

    print("== 场景6：页面刷新返回新旧版本明细与各批次引用的版本 ==")
    _, refresh = call("GET", "/pipeline/extraction/pages/https%3A%2F%2Fexample.com%2Fproduct%2F1/refresh")
    check("刷新包含新旧两个解析版本", len(refresh["parse_versions"]) == 2)
    check("刷新包含全部6个批次", len(refresh["batches"]) == 6)
    check("当前游标指向最新接纳批次的版本",
          refresh["current_cursor"]["current_parse_version_id"] == v2["version"]["id"]
          and refresh["current_cursor"]["current_signature"] == v2["version"]["config_signature"]
          and refresh["current_cursor"]["active_fields"] == v2["version"]["schema_fields"])
    batch_versions = {b["run_id"]: b["parse_version_id"] for b in refresh["batches"]}
    check("当前实体集和在途批次引用的版本可查明",
          batch_versions[ra] == v1["version"]["id"]
          and batch_versions[rb] == v2["version"]["id"]
          and batch_versions[rc] == v1["version"]["id"]
          and batch_versions[rd] == v2["version"]["id"])
    check("当前实体集条目均带解析签名与运行ID",
          all(e.get("解析签名") and e.get("运行ID") for e in refresh["current_entities"]))
    check("刷新回执数量与交付次数匹配",
          len(refresh["receipts"]) >= 5)

    print("== 场景7：图谱建边采用批次字段集合，边记录版本来源 ==")
    _, edges = call("POST", "/pipeline/graph/build-from-batch", {"run_id": rb})
    check("基于已发布批次建边成功", edges["ok"] and edges["relation_count"] > 0)
    check("建边沿用批次版本与签名",
          edges["parse_version_id"] == v2["version"]["id"]
          and edges["config_signature"] == v2["version"]["config_signature"])
    check("建边采用的字段集合为版本5字段",
          edges["active_fields"] == v2["version"]["schema_fields"])
    _, prov = call("GET", "/pipeline/graph/edge-provenance")
    check("每条新边可追溯版本、签名、运行ID、字段集合",
          all(e["解析版本ID"] == v2["version"]["id"]
              and e["来源运行ID"] == rb
              and len(e["生效字段"]) == 5
              for e in prov["items"]))

    print("== 场景8：通用资源接口仍可读取带新来源字段的实体主表 ==")
    _, entities_page = call("GET", "/entity_extract?page=1&size=200")
    versioned = [e for e in entities_page["items"] if e.get("运行ID")]
    check("主实体表存在带来源字段的版本化条目",
          len(versioned) >= 5
          and all(e.get("解析版本ID") and e.get("解析签名") and e.get("生效字段")
                  for e in versioned))
    rb_rows = [e for e in entities_page["items"] if e.get("运行ID") == rb]
    check("已发布批次原条目仍在主表且保留原版本签名（被后续接纳切换后标记废弃）",
          len(rb_rows) == 5
          and all(e["解析版本ID"] == v2["version"]["id"] for e in rb_rows))
    current = [e for e in refresh["current_entities"] if e.get("运行ID")]
    check("当前实体集引用的版本与游标一致（v2）",
          current and all(e["解析版本ID"] == v2["version"]["id"] for e in current))
    # 已发布批次的原始快照不受切换影响，字段合计仍一致
    _, ev_b_again = call("GET", f"/pipeline/extraction/entity-versions?run_id={rb}")
    check("已发布批次仍可读取原版本快照且字段合计一致",
          ev_b_again["total"] == 5
          and {r["source_field"] for r in ev_b_again["items"]}
          == set(v2["version"]["schema_fields"]))

    print(f"\n结果：通过 {PASS} 项，失败 {FAIL} 项")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
