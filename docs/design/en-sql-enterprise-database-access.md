# 企业数据库接入方案（Oracle / MPP / HBase / HDFS）

状态：设计已评审，待实施。日期：2026-11-02。实施分支：`feature/en-sql`。

本文定义 WeKnora 接入企业侧数据库的能力：用户在知识库/助手中用自然语言提问，系统在企业库上执行受控查询并作答。目标是让 5TB 级 MPP、数百张表的 Oracle、以及 Hadoop 生态数据**可被可靠提问**，同时不让业务数据全量进入 RAG，也不把数据库凭据与写权限暴露给模型。

## 0. 已确认的决策

| 问题 | 决策 |
|---|---|
| 首要目标 | **精确记录查找** + **统计/聚合问数**（两者都是"算得准"的问题，不是语义检索问题） |
| 量级 | MPP 侧约 5TB、Oracle 大量表 → **业务数据全量走 RAG 不可行**（见 §1） |
| 接入形态 | 语义层/本体选表 + 受控 SQL 直查；RAG 仅承载**元数据层** |
| 组件边界 | 独立 **data-gateway**（Java + JDBC），运行在企业内网侧 |
| SQL 来源 | LLM 组装 + **AST 校验器**强制拦截（模板化查询列为二期） |
| 语义层构建 | **自动抽取 + LLM 增强 + 人工抽检** |
| 安全治理 | 只读账号 + 网关侧连接隔离 + **全量审计日志**（行列级权限、敏感脱敏、配额限流暂不做） |
| 技术栈 | 网关 Java + JDBC；WeKnora 侧 Go（不引数据库驱动） |

## 1. 为什么全量 RAG 不成立

以仓库默认配置 `chunk_size=512` / `chunk_overlap=50`（`config/config.yaml:45`）估算 5TB 可索引文本：

| 指标 | 量级 |
|---|---|
| 行数（50% 可索引文本、均 2KB/行） | ≈ 12 亿行 |
| chunk 数（1 行 ≈ 1 chunk） | ≈ 10 亿 |
| 向量存储（1024 维 float32） | ≈ 4 TB（pgvector 不可行） |
| embedding 成本（即便 5% 抽样） | 6.4×10¹⁰ tokens，一次全量数万元，且每次增量同步重付 |

更根本的是语义错配：RAG 解决"这段话讲了什么"，而数据库的价值是"**算得准**"——求和、分组、join、时间聚合恰是向量检索最不擅长的。因此采用**分层**：

- **L0 目录层（进 RAG）**：表结构、字段口径、样例、统计分布、本体关系。规模万级~十万级 chunk，可行。回答"库里有什么、字段什么含义、哪些表能关联"。
- **L1 取数层（不进 RAG）**：需要算的时候直查库，结果直接进上下文，不向量化。
- **L2 精选层（进 RAG，可选二期）**：人工/规则选中的热点表、维表、码表（万行内），复用现有 Connector 同步管线。

## 2. 总体架构

```
┌──────────────── 企业内网 ────────────────┐        ┌──── WeKnora 侧 ────┐
│                                          │        │                    │
│  ┌────────────────────────────────────┐  │  HTTP  │  语义目录知识库      │
│  │  data-gateway (Java + JDBC)        │  │◀──────│  (表卡片 → 现有     │
│  │  ├ 元数据采集 (information_schema) │  │        │   分块/embedding     │
│  │  ├ SQL AST 校验器 (只读/白名单)     │  │        │   /pgvector 检索)   │
│  │  ├ 执行器 (行数/超时/并发上限)      │  │        │                    │
│  │  └ 审计日志 (谁/问了什么/SQL/行数)  │  │        │  编排工具:          │
│  └────────────┬───────────────────────┘  │        │  enterprise_query   │
│               │ JDBC                    │        │  ├ 检索选表         │
│  ┌────────────┴───────────────────────┐  │        │  ├ 生成SQL(LLM)     │
│  │ Oracle / MPP-PG / MPP-MySQL /      │  │        │  ├ 校验             │
│  │ HiveServer2 / HBase / HDFS(WebHDFS)│  │        │  └ 核查+回灌作答    │
│  └────────────────────────────────────┘  │        │                    │
└──────────────────────────────────────────┘        └────────────────────┘
```

### 2.1 三条边界原则

1. **业务数据永不过 WeKnora 存储**。只有表卡片进向量索引。业务行只在查询时现取、当次进 LLM 上下文、随会话消失。
2. **SQL 的信任边界在网关**。网关是唯一持有数据库凭据、唯一执行 SQL 的地方。WeKnora 生成的 SQL 是"建议"，必须过网关校验器才生效。
3. **WeKnora 主仓不引入数据库驱动**。不改 `go.mod` 依赖树、不引 CGO、不装 Oracle Instant Client——Hadoop 生态客户端集中在 Java 侧。

### 2.2 为什么不走 MCP 形态

WeKnora Agent 已支持 MCP（`internal/agent/tools/definitions.go` 中的 `discover_mcp_tools` / `call_mcp_tool`）。网关也可以额外以 MCP Server 暴露给 Cursor/Claude 等外部客户端复用同一能力。但**主链路走 WeKnora 原生工具**，原因：

- 审计要求"谁问了什么"，需要把 `tenant_id` / `user_id` / `space_id` 作为一等参数从 WeKnora 传下去；MCP 调用上下文里拿不到可靠的租户身份。
- SQL 校验、候选表白名单、审计落库需要与 WeKnora 的租户/空间体系对齐。
- MCP 形态保留为二期：把网关再包一层 MCP，供外部客户端使用。

## 3. 语义层数据模型

### 3.1 结构化本体（存 WeKnora 现有 postgres，与业务库无关）

| 表 | 关键字段 | 用途 |
|---|---|---|
| `db_catalog_datasources` | id, tenant_id, name, dialect(oracle/pg/mysql/hive/hdfs), host, port, database, credential_ref, scope_json, status, last_synced_at | 一个企业库一个连接 |
| `db_catalog_tables` | id, datasource_id, schema_name, table_name, table_type, row_count_estimate, comment, biz_summary, card_kb_id | 表的"身份证" |
| `db_catalog_columns` | id, table_id, column_name, data_type, nullable, is_pk, comment, sample_values, value_domain, sensitivity | 字段级元数据 + 敏感标记 |
| `db_catalog_relations` | id, from_table_id, to_table_id, from_column, to_column, relation_type, confidence | **本体图**：外键 + LLM 推断的 join 路径，SQL 生成时直接给出 JOIN 片段 |
| `db_catalog_metrics` | id, name, table_id, expression_sql, description, caliber_note | 指标口径（如"销售额=含税金额-退款"），聚合问数优先复用 |
| `db_catalog_sync_jobs` | id, datasource_id, status, started_at, finished_at, tables_scanned, tables_changed, error | 采集任务审计 |

DDL 走现有迁移机制（`migrations/versioned/`）。

### 3.2 表卡片（进向量索引）

每张表渲染为一段 markdown，作为文档进一个专用知识库（建议名「企业数据目录」），**复用现有知识入库管线**（分块 → embedding → pgvector）：

```markdown
# 表：ods.order_header（订单主表）
业务含义：电商订单主表，一行一笔订单（含拆单前的主单）
更新方式：T+1 分区覆盖；order_status 状态机 CREATED/PAID/SHIPPED/CANCELLED
典型问法："上月各区域订单量"、"订单已取消但已收款的有哪些"
关联：ods.order_item.order_id → ods.order_header.id（外键）

| 列 | 类型 | 说明 | 示例 |
|---|---|---|---|
| id | bigint | 订单主键 | 100234 |
| user_id | bigint | 用户 ID（敏感） | 88213 |
| region_code | varchar(8) | 区域编码，字典表 dim.region.code | CN-SH |
| pay_amount | numeric(18,2) | 实付金额（元） | 1299.00 |
```

**表卡片是选表的唯一向量索引**。问题"上月华东区卖了多少"命中 `ods.order_header`、`dim.region` 卡片，比让 LLM 盲猜表名可靠得多。

### 3.3 构建流程（离线任务）

```
采集(网关, JDBC 只读元数据查询)
  → 结构化元数据(表/列/类型/注释/主键/外键/样例/统计)
  → LLM 增强(表业务含义、字段口径、典型问法、示例 SQL)
  → relations(外键 + LLM 推断的常见 join)
  → 生成表卡片 markdown → 走现有 knowledge 管线入库(企业数据目录 KB)
  → 写 db_catalog_*
  → 增量刷新: 按表 comment/统计时间戳比对, 变了才重新生成卡片
```

约束：
- 采集只读元数据（Oracle `all_tab_columns`/`all_constraints`、PG/MySQL `information_schema`、Hive Metastore/DESCRIBE、HDFS `WebHDFS`），**不做业务全扫**。
- 样例行 `LIMIT 10` 或分区首行；`row_count` 用统计信息近似，避免 `COUNT(*)` 拖垮业务库。
- 采集任务走 Asynq 队列（复用现有 `internal/background` 体系），可断点续跑。

## 4. 查询链路

```
用户问题
  ↓
① 检索选表      企业数据目录 KB 向量检索 top-k 表卡片
                + 沿 db_catalog_relations 扩展 1–2 跳邻居
                → 候选表集（含 join 路径、字段白名单）
  ↓
② SQL 生成      LLM(candidate table cards + join 路径 + 指标口径 + 问题 + 用户上下文)
                → SQL 草案 + 依据说明
  ↓
③ 校验          网关 AST 校验器（§5.2）→ 通过 / 拦截并返回原因
  ↓
④ 执行          网关执行器：行数上限、超时、并发上限、只读连接
  ↓
⑤ 结果核查      量级合理性、空结果、截断标记、超时降级
  ↓
⑥ 回灌作答      结果行 → markdown 上下文 → LLM 作答
                附引用：命中表、SQL、行数、截断标记
```

### 4.1 选表规则

- top-k 默认 8（可配），按语义相似度排序。
- 扩展：候选表的 1 跳外键邻居上限 6 个，2 跳不扩展（避免爆炸）。
- 候选集是**唯一表白名单**：SQL 里出现候选集外的表 → 直接拦截。
- 无候选表命中时：不猜表，直接回复"没有找到相关数据表"并列出目录中相关表的提示。

### 4.2 核查（⑤）

| 情形 | 处理 |
|---|---|
| 空结果 | 返回"查询成功但无匹配数据"，附执行的 SQL 供用户核对 |
| 行数达上限 | 标记 `truncated=true`，提示"仅返回前 N 行"，避免 LLM 把部分结果当全量 |
| 超时 | 降级为建议缩小范围/加过滤条件，并回传失败原因 |
| 量级异常（单行金额 > 阈值等） | 不阻断，标记到上下文，让 LLM 提示核对 |
| SQL 被拦截 | 把拦截原因回喂 LLM，允许一次修正重试（最多 1 次），再失败则如实说明 |

## 5. 安全与审计

### 5.1 连接与凭据

- 数据库凭据**只存在网关**，WeKnora 侧只存 `credential_ref`（引用网关内的凭据条目 ID），不存密码。
- 网关侧使用只读账号；连接池按数据源隔离，设最大连接数。
- WeKnora 侧若将来需要代管凭据，复用现有 `SYSTEM_AES_KEY` 加密机制（与模型 API key 同路径），密钥不落 Docker env / compose 文件。

### 5.2 SQL AST 校验器（网关侧，强制）

| 规则 | 说明 |
|---|---|
| 单语句 | 拒绝多语句（分号拼接） |
| 只读 | 仅 `SELECT`/`WITH`；拒 `INSERT/UPDATE/DELETE/MERGE/DDL/PRAGMA/CALL/EXEC` |
| 表白名单 | 表 ∈ 本次候选集 ∩ 数据源 scope；拒绝跨库/跨 schema（除显式允许） |
| 列白名单 | 拒绝 `SELECT *`；列须在 `db_catalog_columns` 内 |
| 函数黑名单 | 禁 `pg_read_file`/`dblink`/`utl_http`/`xp_cmdshell` 等外部访问与系统函数 |
| 扫描保护 | 无 `WHERE` 的表扫描默认拒绝（除非该表在 scope 中标记 `allow_full_scan`） |
| 资源上限 | 行数上限（默认 1000）、单查询超时（默认 30s）、并发上限（按租户） |

拦截时返回结构化错误：`{code, reason, offending_fragment}`，供 LLM 修正一次。

### 5.3 审计日志（本次必做）

网关侧 `audit_log` 表 + 可选文件输出，记录：

| 字段 | 说明 |
|---|---|
| `ts` | 时间戳 |
| `tenant_id` / `user_id` / `space_id` / `request_id` | 调用方身份（由 WeKnora 透传） |
| `datasource_id` / `table` | 数据源与命中表 |
| `question` | 原始问题（可选脱敏策略，默认落库） |
| `sql` | 最终执行的 SQL |
| `rows_returned` / `truncated` / `duration_ms` | 执行结果指标 |
| `status` / `error` | 成功、拦截、失败 |

另在 WeKnora 侧记录调用事件（谁在哪个会话问了什么），便于与网关审计对账。

## 6. 接口契约

### 6.1 网关 HTTP API（WeKnora → 网关）

```
POST /v1/metadata/collect      # 采集元数据（分页、按 schema）
POST /v1/sql/validate          # 校验 SQL（可选，供调试）
POST /v1/sql/execute           # 执行并返回结果集 + 审计事件
GET  /v1/health
```

`/v1/sql/execute` 请求：

```json
{
  "request_id": "...",
  "tenant_id": "...", "user_id": "...", "space_id": "...",
  "datasource_id": "...",
  "sql": "SELECT ... ",
  "allowed_tables": ["ods.order_header", "dim.region"],
  "max_rows": 1000, "timeout_ms": 30000
}
```

响应：

```json
{ "columns": ["..."], "rows": [["..."]], "row_count": 128,
  "truncated": false, "duration_ms": 340, "audit_id": "..." }
```

### 6.2 WeKnora 侧新增 Agent 工具

新增 `internal/agent/tools/enterprise_query.go`，工具名 `enterprise_query`，参数：

```json
{ "question": "...", "datasource_id": "..." }
```

工具内部完成 §4 的 ①–⑥（① 用现有检索组件，② 用现有 LLM 调用封装，③⑥ 走网关）。工具注册沿用 `internal/agent/tools/definitions.go` 的既有模式，并加入 `AvailableToolDefinitions` / `DefaultAllowedTools`。

## 7. WeKnora 侧改造清单

| # | 改动 | 位置 | 说明 |
|---|---|---|---|
| 1 | 语义层建表 | `migrations/versioned/` | §3.1 六张表 |
| 2 | 本体模型与仓储 | `internal/types/`、`internal/application/repository/` | 参照现有 datasource repo 模式 |
| 3 | 元数据采集任务 | `internal/background/` | Asynq 任务，复用现有任务队列 |
| 4 | 表卡片生成 | `internal/application/service/` | LLM 调用现有封装；生成 markdown 后走现有 knowledge 管线 |
| 5 | Agent 工具 | `internal/agent/tools/enterprise_query.go` | §6.2 |
| 6 | 网关客户端 | `internal/application/service/` 或 `internal/client/` | HTTP 客户端 + 超时/重试 |
| 7 | 数据源/连接配置 | `internal/types/datasource.go`、`internal/datasource/connector.go` | 企业库连接配置（不进 Connector 体系，见下） |
| 8 | 前端配置页 | `frontend/src/views/knowledge/settings/` | 企业库连接配置、语义层管理、审计查询 |
| 9 | 审计查询 API | `internal/handler/`、`internal/router/` | 审计日志查询（按租户隔离） |

**说明（7）**：企业库**不注册为 `ConnectorType*`**。现有 Connector 接口是"列举资源 + 抓取正文"模型（`ListResources`/`FetchAll`/`FetchIncremental`），与"执行 SQL 返回结果集"语义不符，强行套用会扭曲接口。企业库走独立的 catalog + 查询网关体系，L2 精选小表同步（可选二期）才复用 Connector。

**不改动**：`go.mod` 依赖树、docreader、现有向量检索实现、现有同步调度器。

## 8. 分期计划

| 阶段 | 内容 | 产出 | 验收 |
|---|---|---|---|
| **P0** | 网关骨架：JDBC 连接管理、元数据采集、AST 校验器、执行器、审计表 | 可独立部署的 data-gateway + `/v1/sql/execute` | 用 Oracle/PG 各 1 库验证采集与拦截 |
| **P1** | 语义层：本体建表、采集任务、表卡片生成、入库、检索选表 | 企业数据目录 KB 可用 | 20 条真实问题的选表命中率 ≥ 80% |
| **P2** | 查询链路：Agent 工具、SQL 生成、校验、核查、回灌 | 端到端可问答 | SQL 可执行率 ≥ 90%，结果正确率 ≥ 85% |
| **P3** | Hadoop 生态：HiveServer2 / HBase / HDFS 元数据与取数 | 支持三类 Hadoop 源 | Hive 表问答验证通过 |
| **P4** | 前端：连接配置、语义层管理、审计查询 | 可运营 | — |
| **P5（可选）** | L2 精选小表 RAG 同步；模板化查询；网关 MCP 暴露 | — | — |

## 9. 测试与验收策略

**固定回归集**：整理 20–50 条真实业务问题，标注期望命中表、期望 SQL 关键片段、期望结果集，纳入回归。每次改动跑。

| 指标 | 目标 |
|---|---|
| 选表命中率 | ≥ 80% |
| SQL 可执行率（过校验且不报错） | ≥ 90% |
| 结果正确率（与期望结果集一致） | ≥ 85% |
| 拦截准确率（越权 SQL 必被拦） | 100%（安全项，不设妥协目标） |

测试层次：
- 网关单测：AST 校验器规则用例（多语句、DDL、`SELECT *`、跨库、非候选表、危险函数、注释绕过）。
- 契约测试：WeKnora ↔ 网关 API 契约（可用容器内 mock 网关）。
- 端到端：固定回归集 + 人工抽检。

## 10. 风险与未决事项

| 风险 | 缓解 |
|---|---|
| Oracle JDBC 在容器内需确认字符集/NLS 设置 | P0 阶段用目标环境实测；必要时改用 `oracle.jdbc.thin` 并固定 `NLS_LANG` |
| HBase 直连需 HiveServer2 桥接或 Phoenix | P3 阶段确定；先把 Hive 表链路做通 |
| 语义层质量决定选表准确率 | 人工抽检 UI（P4）+ 回归集持续回流修正 |
| 业务库被查询拖垮 | 只读账号 + 超时 + 行数上限 + 并发上限 + 禁全扫 |
| LLM 生成 SQL 越权 | 网关 AST 校验器强制（模型侧提示词只是辅助，不是边界） |

**未决**（需在 P0 前定稿）：
1. 网关的部署位置与网络打通方式（企业内网 IP、认证方式）。
2. 审计日志的保留期限与是否需要外发到 SIEM。
3. 元数据采集的频率与"业务口径"由谁维护（数据治理角色）。
4. 5TB 场景下的典型问题是否集中在少数几张宽表（影响 L0/L1 分工）。
