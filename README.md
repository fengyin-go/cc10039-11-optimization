# 数据湖分区与存储治理中心

管理数据湖分区策略、存储优化、冷热分层和小文件合并的数据湖治理平台

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

健康检查：`curl http://127.0.0.1:8014/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5194/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8014`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 分区策略 | `partition_policy` | 分区策略 | 策略名称、分区维度、分区粒度 |
| 湖表注册 | `lake_table` | 湖表 | 表名、所属业务域、存储格式 |
| 冷热分层 | `storage_tier` | 分层策略 | 策略名称、热层阈值、冷层阈值 |
| 小文件合并 | `file_compaction` | 合并任务 | 任务编号、目标湖表、文件阈值 |
| 数据目录 | `data_catalog` | 目录条目 | 条目名称、数据层级、所属湖表 |
| 湖内质量监控 | `quality_monitor` | 监控规则 | 规则名称、监控指标、告警阈值 |
| 保留策略 | `retention_mgr` | 保留策略 | 策略名称、适用湖表、保留天数 |
| 入湖管道 | `ingest_pipeline` | 入湖任务 | 任务名称、来源系统、目标湖表 |
| 快照版本 | `snapshot_version` | 快照版本 | 湖表名、快照时间戳、版本号 |
| 访问审计 | `access_audit` | 访问记录 | 访问者、目标湖表、操作类型 |
| Schema演进 | `schema_evolution` | Schema变更 | 湖表名、变更类型、变更字段 |
| 负载均衡配置 | `load_balance` | 分片配置 | 湖表名、当前分片数、分片策略 |
| 元数据同步 | `metadata_sync` | 同步任务 | 任务名称、源端类型、目标端类型 |
| 数据回填任务 | `backfill_job` | 回填任务 | 任务名称、目标湖表、回填时间范围 |
| 数据字典管理 | `data_dictionary` | 字典条目 | 字典名称、适用范围、值域列表 |
| 任务调度 | `task_scheduler` | 调度任务 | 任务名称、调度表达式、关联管道 |
| 存储告警规则 | `alert_rule` | 告警规则 | 规则名称、告警指标、告警级别 |
| 存储成本分析 | `cost_report` | 成本报告 | 报告周期、目标业务域、存储总量 |
| 湖表血缘 | `lineage_graph` | 血缘关系 | 上游源、中间转换、下游消费 |
| 治理报告 | `govern_report` | 治理报告 | 报告周期、治理指标、优化建议 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 分区策略归档与合并输入交接

同一湖表同时只允许一套生效的分区粒度。分区策略生命周期只允许
`待审批 → 已生效 → 已归档` 顺序流转（流转记录写在 `生命周期流转` 里）。

归档旧策略时，湖表上处于 `待调度` 的合并任务不会让归档失败，也不会让任务悬空：
系统会把这些任务的策略版本、分区粒度、数据范围写入「归档交接分区清单」
（`交接状态=待领取`）。小文件合并启动时优先从该清单领取输入，领取结果
（来源、策略版本、粒度、日期范围、调度记录编号）写入合并调度记录；
失败请求也会留痕，可按 `请求任务编号` 追溯来源。

- 归档时若仍有 `执行中` 合并任务或 `已生效` 保留策略，归档会被明确拒绝。
- 交接输入的粒度固定为被归档策略的粒度；任务自行声明了另一套粒度时，
  清单标记为 `粒度冲突`，触发会被拒绝（每项合并只能使用一套粒度）。
- 收缩后的新粒度策略若与旧策略已交接日期重叠，启用时记录
  `重叠日期范围` 与 `有效数据范围`；合并触发时若同一段日期已被另一套
  粒度占用（包括尚未启动的交接清单），会被拒绝，避免新旧方案重复处理同一输入。
- 旧策略已归档但任务没有任何交接清单时，触发会明确失败，而不是显示可执行却取不到输入。

相关查询接口：

| 接口 | 说明 |
| --- | --- |
| `GET /api/cross/partition-handovers?table_name=&task_no=` | 归档交接分区清单 |
| `GET /api/cross/compaction-runs?table_name=&task_no=&status=` | 合并调度记录（含失败原因） |
| `GET /api/cross/partition-coverage?table_name=` | 分区日期覆盖、粒度覆盖与同一输入冲突核对 |

领域：数据工程
子方向：数仓与数据湖
