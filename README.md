# Web语义抽取与内容理解中心

面向Web结构化数据采集场景，提供页面DOM解析、实体识别、关系抽取、内容分类和知识图谱构建的语义抽取平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| DOM解析 | `dom_parser` | 解析配置 | 配置名称、目标URI、解析引擎 |
| 实体抽取 | `entity_extract` | 实体条目 | 实体ID、实体类型、实体名称 |
| 关系图谱 | `relation_graph` | 关系边 | 边ID、源实体、目标实体 |
| 内容分类 | `content_classifier` | 分类标签 | 标签ID、页面URI、一级分类 |
| 模板规则 | `template_rule` | 规则模板 | 模板名称、适用站点、提取字段 |
| 文本分块 | `text_segment` | 文本块 | 块ID、来源页面、块类型 |
| 情感标注 | `sentiment_tag` | 情感记录 | 记录ID、文本片段、情感倾向 |
| Schema对齐 | `schema_align` | 对齐规则 | 规则名称、源Schema、目标Schema |
| 种子调度 | `feed_scheduler` | 调度任务 | 任务名称、URL种子、抓取深度 |
| 质量检查 | `quality_check` | 检查规则 | 规则名称、检查字段、检查类型 |
| 探针选择 | `probe_selector` | 探针配置 | 探针名称、目标页面、选择器列表 |
| 页面快照 | `page_snapshot` | 快照记录 | 快照ID、页面URI、DOM哈希 |
| 抽取指标 | `extract_metric` | 指标记录 | 记录时间、所属任务、成功页面数 |
| 语义结果 | `nlu_result` | 语义记录 | 记录ID、输入文本、意图标签 |
| 知识链接 | `knowledge_link` | 链接记录 | 链接ID、源实体、外部URI |
| 输出目标 | `output_target` | 输出配置 | 配置名称、输出类型、目标地址 |
| 抽取工坊 | `extract_studio` | 交互任务 | 任务名称、示例页面、标注区域 |
| 聚合视图 | `aggregate_view` | 视图配置 | 视图名称、统计维度、聚合字段 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 解析版本流水线（配置改动下的抽取一致性）

DOM 解析配置改动时，通过 `/api/pipeline/*` 接口保证实体抽取结果可追溯、冲突可裁决：

- **解析版本清单**（`parse_versions`）：每次登记解析配置生成不可变版本，含配置签名、
  字段集合与「可消费/已替换」标记，同页面旧版本自动让位，同签名重复登记幂等。
- **抽取批次**（`extraction_batches`）：每次抽取运行启动时固定解析版本ID、签名与生效字段；
  交付结果写入带来源的实体版本数据集（`entity_versions`），并回写主实体表与页面游标。
- **版本冲突**：旧配置抽取在途、新配置先交付不同字段时，按 `fresh_first`（交付时效优先，
  直接接纳）或 `pending_first`（既有待处理优先，挂起待裁决）处理；冲突批次可通过
  `resolve-conflict` 人工接纳/废弃，也可 `redeliver` 重投。
- **回执去重**：同一运行同一 `replay_key` 的并发提交只保留首次回执（全局锁保护）。
- **发布不可变**：`publish` 校验快照字段合计与解析版本一致后冻结，已发布批次拒绝重投，
  但仍按原版本读取，配置记录与结果一起落库。
- **图谱建边**：`graph/build-from-batch` 只采用批次生效字段，边记录版本、签名与字段集合，
  可通过 `graph/edge-provenance` 追溯。
- **页面刷新核对**：`extraction/pages/{uri}/refresh` 返回新旧版本明细、当前实体集、
  各批次引用的版本与全部回执。

前端「版本流水线」导航（`/pipeline`，`PipelineView.vue`）提供上述流程的可视化操作。
后端测试：`backend/tests/test_pipeline_e2e.py`（HTTP 端到端，需先启动服务）与
`backend/tests/test_concurrency_regression.py`（并发与旧链路回归，直接运行即可）。

领域：数据工程
子方向：数据采集、爬虫与RPA
