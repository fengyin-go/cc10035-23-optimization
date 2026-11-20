<template>
  <section class="page" data-module="pipeline">
    <header class="page-head">
      <div>
        <h2>解析版本流水线</h2>
        <p class="page-desc">
          配置改动下的实体抽取一致性：解析版本清单、带来源的实体版本数据集、版本冲突裁决、
          回执去重与已发布批次不可变
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" @click="loadAll">刷新全部数据</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">解析版本数</span>
        <strong class="stat-value">{{ versions.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">抽取批次数</span>
        <strong class="stat-value">{{ batches.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">冲突待裁决</span>
        <strong class="stat-value">{{ conflicts.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已发布批次</span>
        <strong class="stat-value">{{ publishedCount }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">实体版本数据集</span>
        <strong class="stat-value">{{ entityRows.length }}</strong>
      </article>
    </div>

    <h3>① 解析版本清单（配置签名 + 字段集合，当前可消费版本）</h3>
    <form class="filter-bar" @submit.prevent="registerVersion">
      <label class="filter-item">
        <span>解析配置ID</span>
        <input v-model="registerConfigId" data-testid="register-config-id" placeholder="dom_parser 配置 id" required />
      </label>
      <button class="btn primary" type="submit">登记/复用解析版本</button>
    </form>
    <table class="data-table">
      <thead>
        <tr>
          <th>版本ID</th><th>配置</th><th>页面URI</th><th>配置签名</th>
          <th>字段集合</th><th>字段数</th><th>清单状态</th><th>登记时间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="version in versions" :key="version.id" :data-status="version.status">
          <td>v{{ version.id }}</td>
          <td>{{ version.config_name }} (#{{ version.config_id }})</td>
          <td>{{ version.source_uri }}</td>
          <td><code>{{ version.config_signature }}</code></td>
          <td>{{ version.schema_fields.join(', ') }}</td>
          <td>{{ version.field_count }}</td>
          <td>{{ version.status }}</td>
          <td>{{ version.registered_at }}</td>
        </tr>
        <tr v-if="!versions.length"><td colspan="8" class="empty-state">暂无解析版本</td></tr>
      </tbody>
    </table>

    <h3>② 抽取批次（启动时固定版本、签名与生效字段）</h3>
    <form class="filter-bar" @submit.prevent="startRun">
      <label class="filter-item">
        <span>解析配置ID</span>
        <input v-model="runConfigId" placeholder="dom_parser 配置 id" required />
      </label>
      <label class="filter-item">
        <span>接纳策略</span>
        <select v-model="conflictPolicy">
          <option value="fresh_first">fresh_first（交付时效优先）</option>
          <option value="pending_first">pending_first（既有待处理优先）</option>
        </select>
      </label>
      <button class="btn primary" type="submit">启动抽取运行</button>
    </form>
    <table class="data-table">
      <thead>
        <tr>
          <th>运行ID</th><th>解析版本</th><th>签名</th><th>生效字段</th>
          <th>状态</th><th>策略</th><th>冲突说明</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="batch in batches" :key="batch.run_id" :data-status="batch.status">
          <td>{{ batch.run_id }}</td>
          <td>v{{ batch.parse_version_id }}</td>
          <td><code>{{ batch.config_signature.slice(0, 8) }}</code></td>
          <td>{{ batch.active_fields.join(', ') }}</td>
          <td>{{ batch.status }}</td>
          <td>{{ batch.conflict_policy }}</td>
          <td>{{ batch.conflict_reason ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="deliver(batch.run_id)">回传结果</button>
            <button
              v-if="batch.status === '冲突待裁决'"
              class="link"
              type="button"
              @click="resolve(batch.run_id, '接纳')"
            >接纳</button>
            <button
              v-if="batch.status === '冲突待裁决'"
              class="link"
              type="button"
              @click="resolve(batch.run_id, '废弃')"
            >废弃</button>
            <button
              v-if="!batch.immutable && batch.status !== '运行中'"
              class="link"
              type="button"
              @click="redeliver(batch.run_id)"
            >重投(fresh)</button>
            <button
              v-if="batch.status === '已交付'"
              class="link"
              type="button"
              @click="publish(batch.run_id)"
            >发布</button>
            <button
              v-if="batch.status === '已发布' || batch.status === '已交付'"
              class="link"
              type="button"
              @click="buildEdges(batch.run_id)"
            >图谱建边</button>
          </td>
        </tr>
        <tr v-if="!batches.length"><td colspan="8" class="empty-state">暂无抽取批次</td></tr>
      </tbody>
    </table>

    <h3>③ 交付回执（同运行同签名并发提交只保留一条）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>回执号</th><th>运行ID</th><th>replay_key</th><th>版本</th>
          <th>实体数</th><th>冲突</th><th>策略/裁决</th><th>接纳</th><th>交付时间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="receipt in receipts" :key="receipt.id">
          <td>{{ receipt.receipt_no }}</td>
          <td>{{ receipt.run_id }}</td>
          <td>{{ receipt.replay_key }}</td>
          <td>v{{ receipt.parse_version_id }}</td>
          <td>{{ receipt.entity_count }}</td>
          <td>{{ receipt.conflict ? '是' : '否' }}</td>
          <td>{{ receipt.policy }} / {{ receipt.decision }}</td>
          <td>{{ receipt.accepted === undefined ? '—' : receipt.accepted ? '已接纳' : '挂起' }}</td>
          <td>{{ receipt.delivered_at }}</td>
        </tr>
        <tr v-if="!receipts.length"><td colspan="9" class="empty-state">暂无回执</td></tr>
      </tbody>
    </table>

    <h3>④ 带来源的实体版本数据集</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>运行ID</th><th>版本</th><th>签名</th><th>实体ID</th>
          <th>实体名称</th><th>来源字段</th><th>生效字段</th><th>已发布</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in entityRows" :key="row.id">
          <td>{{ row.run_id }}</td>
          <td>v{{ row.parse_version_id }}</td>
          <td><code>{{ row.config_signature.slice(0, 8) }}</code></td>
          <td>{{ row.entity_id }}</td>
          <td>{{ row.entity_name }}</td>
          <td>{{ row.source_field }}</td>
          <td>{{ row.active_fields.join(', ') }}</td>
          <td>{{ row.published ? '是' : '否' }}</td>
        </tr>
        <tr v-if="!entityRows.length"><td colspan="8" class="empty-state">暂无实体版本数据</td></tr>
      </tbody>
    </table>

    <h3>⑤ 页面刷新核对（新旧版本明细、当前实体集引用的版本与活动字段）</h3>
    <form class="filter-bar" @submit.prevent="refreshPage">
      <label class="filter-item">
        <span>页面URI</span>
        <input v-model="refreshUri" style="min-width: 320px" placeholder="https://example.com/product/1" required />
      </label>
      <button class="btn primary" type="submit">刷新核对</button>
    </form>
    <template v-if="pageDetail">
      <p class="status-legend">
        <span class="legend-item">
          当前游标：{{ pageDetail.current_cursor?.current_run_id }}
          / v{{ pageDetail.current_cursor?.current_parse_version_id }}
          / {{ pageDetail.current_cursor?.active_fields.join(', ') }}
        </span>
        <span class="legend-item">批次数：{{ pageDetail.batches.length }}</span>
        <span class="legend-item">实体版本行数：{{ pageDetail.entity_version_dataset.length }}</span>
      </p>
      <table class="data-table">
        <thead>
          <tr><th>版本</th><th>签名</th><th>字段集合</th><th>清单状态</th><th>引用该版本的批次</th></tr>
        </thead>
        <tbody>
          <tr v-for="version in pageDetail.parse_versions" :key="version.id">
            <td>v{{ version.id }}</td>
            <td><code>{{ version.config_signature }}</code></td>
            <td>{{ version.schema_fields.join(', ') }}</td>
            <td>{{ version.status }}</td>
            <td>
              {{ pageDetail.batches
                .filter((b) => b.parse_version_id === version.id)
                .map((b) => `${b.run_id}(${b.status})`)
                .join('、') || '—' }}
            </td>
          </tr>
        </tbody>
      </table>
    </template>

    <footer class="page-foot">
      <span>版本记录与抽取结果一起落库；已发布批次快照不可变，仍按原版本读取且字段合计一致</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  pipelineApi,
  type EntityVersionRow,
  type ExtractionBatch,
  type PageRefresh,
  type ParseVersion,
  type Receipt,
} from '@/api/pipeline'

const versions = ref<ParseVersion[]>([])
const batches = ref<ExtractionBatch[]>([])
const conflicts = ref<ExtractionBatch[]>([])
const receipts = ref<Receipt[]>([])
const entityRows = ref<EntityVersionRow[]>([])
const pageDetail = ref<PageRefresh | null>(null)
const errorMessage = ref('')

const registerConfigId = ref('')
const runConfigId = ref('')
const conflictPolicy = ref('fresh_first')
const refreshUri = ref('https://example.com/product/1')

const publishedCount = computed(
  () => batches.value.filter((batch) => batch.status === '已发布').length,
)

async function guard(action: () => Promise<void>) {
  errorMessage.value = ''
  try {
    await action()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function loadAll() {
  const [versionData, runData, conflictData, receiptData, entityData] = await Promise.all([
    pipelineApi.listVersions(),
    pipelineApi.listRuns(),
    pipelineApi.listConflicts(),
    pipelineApi.listReceipts(),
    pipelineApi.entityVersions(),
  ])
  versions.value = versionData.versions
  batches.value = runData.items
  conflicts.value = conflictData.items
  receipts.value = receiptData.items
  entityRows.value = entityData.items
}

async function registerVersion() {
  await guard(async () => {
    await pipelineApi.registerVersion(Number(registerConfigId.value))
    await loadAll()
  })
}

async function startRun() {
  await guard(async () => {
    await pipelineApi.startRun(Number(runConfigId.value), conflictPolicy.value)
    await loadAll()
  })
}

async function deliver(runId: string) {
  await guard(async () => {
    await pipelineApi.deliver(runId)
    await loadAll()
  })
}

async function redeliver(runId: string) {
  await guard(async () => {
    await pipelineApi.redeliver(runId, 'fresh_first')
    await loadAll()
  })
}

async function resolve(runId: string, decision: string) {
  await guard(async () => {
    await pipelineApi.resolveConflict(runId, decision)
    await loadAll()
  })
}

async function publish(runId: string) {
  await guard(async () => {
    await pipelineApi.publish(runId)
    await loadAll()
  })
}

async function buildEdges(runId: string) {
  await guard(async () => {
    await pipelineApi.buildEdges(runId)
  })
}

async function refreshPage() {
  await guard(async () => {
    pageDetail.value = await pipelineApi.pageRefresh(refreshUri.value)
  })
}

onMounted(loadAll)
</script>
