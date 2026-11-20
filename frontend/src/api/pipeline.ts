/** 解析版本化与实体抽取一致性流水线接口封装。 */
import { request } from './client'

async function postJson<T>(path: string, payload: Record<string, unknown> = {}): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(payload) })
  if (!response.ok) {
    const body = await response.json().catch(() => ({ detail: '请求失败' }))
    throw new Error(body.detail ?? `接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}，数据未更新`)
  return (await response.json()) as T
}

export interface ParseVersion {
  id: number
  config_id: number
  config_name: string
  source_uri: string
  config_signature: string
  schema_fields: string[]
  field_count: number
  status: string
  consumable: boolean
  registered_at: string
  superseded_at: string | null
}

export interface ExtractionBatch {
  id: number
  run_id: string
  config_id: number
  config_name: string
  source_uri: string
  parse_version_id: number
  config_signature: string
  active_fields: string[]
  expected_field_count: number
  status: string
  conflict_policy: string
  conflict_of: number | null
  conflict_reason: string | null
  entity_count: number
  immutable: boolean
  created_at: string
  delivered_at: string | null
  published_at: string | null
}

export interface Receipt {
  id: number
  receipt_no: string
  run_id: string
  replay_key: string
  config_signature: string
  parse_version_id: number
  entity_count: number
  conflict: boolean
  policy: string
  accepted?: boolean
  decision: string
  delivered_at: string
}

export interface EntityVersionRow {
  id: number
  run_id: string
  parse_version_id: number
  config_id: number
  config_signature: string
  source_uri: string
  entity_id: string
  entity_type: string
  entity_name: string
  source_field: string
  active_fields: string[]
  confidence: string
  extracted_at: string
  receipt_id: number
  published: boolean
}

export interface PageRefresh {
  source_uri: string
  parse_versions: ParseVersion[]
  current_cursor: {
    source_uri: string
    current_run_id: string
    current_parse_version_id: number
    current_signature: string
    active_fields: string[]
    updated_at: string
  } | null
  current_entities: Array<Record<string, unknown>>
  batches: ExtractionBatch[]
  receipts: Receipt[]
  entity_version_dataset: EntityVersionRow[]
}

export const pipelineApi = {
  registerVersion: (configId: number) =>
    postJson<{ ok: boolean; reused: boolean; version: ParseVersion }>(
      '/api/pipeline/parse-versions/register',
      { config_id: configId },
    ),
  listVersions: () =>
    getJson<{ versions: ParseVersion[]; current_by_uri: Record<string, ParseVersion> }>(
      '/api/pipeline/parse-versions',
    ),
  startRun: (configId: number, conflictPolicy: string) =>
    postJson<{ ok: boolean; run_id: string; batch: ExtractionBatch }>(
      '/api/pipeline/extraction/runs',
      { config_id: configId, conflict_policy: conflictPolicy },
    ),
  deliver: (runId: string, replayKey?: string, conflictPolicy?: string) =>
    postJson<Record<string, unknown>>('/api/pipeline/extraction/deliver', {
      run_id: runId,
      replay_key: replayKey || null,
      conflict_policy: conflictPolicy || null,
    }),
  redeliver: (runId: string, conflictPolicy?: string) =>
    postJson<Record<string, unknown>>('/api/pipeline/extraction/redeliver', {
      run_id: runId,
      conflict_policy: conflictPolicy || null,
    }),
  resolveConflict: (runId: string, decision: string) =>
    postJson<Record<string, unknown>>('/api/pipeline/extraction/resolve-conflict', {
      run_id: runId,
      decision,
    }),
  publish: (runId: string) =>
    postJson<Record<string, unknown>>('/api/pipeline/extraction/publish', { run_id: runId }),
  listRuns: () => getJson<{ items: ExtractionBatch[]; total: number }>(
    '/api/pipeline/extraction/runs',
  ),
  listConflicts: () => getJson<{ items: ExtractionBatch[]; total: number }>(
    '/api/pipeline/extraction/conflicts',
  ),
  listReceipts: () => getJson<{ items: Receipt[]; total: number }>(
    '/api/pipeline/extraction/receipts',
  ),
  entityVersions: () => getJson<{ items: EntityVersionRow[]; total: number }>(
    '/api/pipeline/extraction/entity-versions',
  ),
  pageRefresh: (sourceUri: string) =>
    getJson<PageRefresh>(
      `/api/pipeline/extraction/pages/${encodeURIComponent(sourceUri)}/refresh`,
    ),
  buildEdges: (runId: string) =>
    postJson<Record<string, unknown>>('/api/pipeline/graph/build-from-batch', { run_id: runId }),
  edgeProvenance: () => getJson<{ items: Array<Record<string, unknown>>; total: number }>(
    '/api/pipeline/graph/edge-provenance',
  ),
}
