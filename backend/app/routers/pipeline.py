"""解析版本化与实体抽取一致性流水线接口。

路由层只做参数接收与结果包装，版本登记、冲突裁决、回执去重、发布冻结等
业务判断全部在 dom_parser / entity_extract / relation_graph 领域服务内。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.dom_parser import DomParserService
from app.services.entity_extract import EntityExtractService
from app.services.relation_graph import RelationGraphService
from app.services.versioning import ADMISSION_POLICIES

router = APIRouter(prefix="/api/pipeline", tags=["解析版本流水线"])

_dom = DomParserService()
_entity = EntityExtractService()
_graph = RelationGraphService()


class RegisterVersionPayload(BaseModel):
    config_id: int


class StartRunPayload(BaseModel):
    config_id: int
    conflict_policy: str = "fresh_first"


class DeliverPayload(BaseModel):
    run_id: str
    entities: list[dict[str, Any]] | None = None
    conflict_policy: str | None = None
    replay_key: str | None = None


class RunIdPayload(BaseModel):
    run_id: str


class ResolvePayload(BaseModel):
    run_id: str
    decision: str = Field(description="接纳 或 废弃")


class BuildEdgePayload(BaseModel):
    run_id: str


@router.post("/parse-versions/register")
def register_parse_version(payload: RegisterVersionPayload) -> dict[str, Any]:
    result = _dom.register_parse_version(payload.config_id)
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


@router.get("/parse-versions")
def list_parse_versions(source_uri: str | None = None) -> dict[str, Any]:
    """解析模块用清单标记当前可消费的完整版本。"""
    return _dom.version_manifest_view() | {
        "filtered": _dom.list_versions(source_uri),
        "filter_uri": source_uri,
    }


@router.post("/extraction/runs")
def start_run(payload: StartRunPayload) -> dict[str, Any]:
    if payload.conflict_policy not in ADMISSION_POLICIES:
        raise HTTPException(status_code=400, detail=f"未知接纳策略：{payload.conflict_policy}")
    result = _entity.start_extraction(payload.config_id, conflict_policy=payload.conflict_policy)
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


@router.post("/extraction/deliver")
def deliver_result(payload: DeliverPayload) -> dict[str, Any]:
    result = _entity.deliver_result(
        payload.run_id,
        entities=payload.entities,
        conflict_policy=payload.conflict_policy,
        replay_key=payload.replay_key,
    )
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result["reason"])
    return result


@router.post("/extraction/redeliver")
def redeliver(payload: DeliverPayload) -> dict[str, Any]:
    result = _entity.redeliver(
        payload.run_id,
        entities=payload.entities,
        conflict_policy=payload.conflict_policy,
        replay_key=payload.replay_key,
    )
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result["reason"])
    return result


@router.post("/extraction/resolve-conflict")
def resolve_conflict(payload: ResolvePayload) -> dict[str, Any]:
    result = _entity.resolve_conflict(payload.run_id, payload.decision)
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result["reason"])
    return result


@router.post("/extraction/publish")
def publish_batch(payload: RunIdPayload) -> dict[str, Any]:
    result = _entity.publish_batch(payload.run_id)
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result["reason"])
    return result


@router.get("/extraction/runs")
def list_runs(source_uri: str | None = None) -> dict[str, Any]:
    return {"items": _entity.list_batches(source_uri), "total": len(_entity.list_batches(source_uri))}


@router.get("/extraction/conflicts")
def list_conflicts() -> dict[str, Any]:
    items = _entity.list_conflicts()
    return {"items": items, "total": len(items)}


@router.get("/extraction/receipts")
def list_receipts(run_id: str | None = None) -> dict[str, Any]:
    items = _entity.list_receipts(run_id)
    return {"items": items, "total": len(items)}


@router.get("/extraction/entity-versions")
def entity_versions(
    source_uri: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """带来源的实体版本数据集：每次切换保存实际生效的字段。"""
    items = _entity.entity_version_dataset(source_uri=source_uri, run_id=run_id)
    return {"items": items, "total": len(items)}


@router.get("/extraction/pages/{source_uri:path}/refresh")
def page_refresh(source_uri: str) -> dict[str, Any]:
    """刷新后返回新旧版本明细，核对实体数据、解析签名及重投后的活动字段。"""
    return _entity.page_refresh(source_uri)


@router.post("/graph/build-from-batch")
def build_from_batch(payload: BuildEdgePayload) -> dict[str, Any]:
    result = _graph.build_relations_from_batch(payload.run_id)
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result["reason"])
    return result


@router.get("/graph/edge-provenance")
def edge_provenance() -> dict[str, Any]:
    items = _graph.edge_provenance()
    return {"items": items, "total": len(items)}
