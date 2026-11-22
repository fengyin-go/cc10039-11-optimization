"""分区策略领域服务：管理数据湖表的动态分区策略及生命周期。

分区策略 → 湖表注册 → 冷热分层 → 小文件合并 → 保留策略
构成数据湖存储治理的主链路。策略生效时会联动下游模块。

生命周期只允许 待审批 → 已生效 → 已归档 单向顺序流转。策略归档时，
如果湖表上还有待调度的合并任务，不会让任务悬空：归档动作会把这些任务的
策略版本、分区粒度和数据范围锁存到「归档交接分区清单」，合并任务启动时
从清单领取实际输入。这样旧策略退场后任务仍可沿原计划执行，也不会因为
新旧粒度并存而重复处理同一段日期。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.partition_handover import (
    format_date_range,
    normalize_granularity,
    parse_explicit_range,
    ranges_overlap,
    subtract_range,
)
from app.store import store

LIFECYCLE_ORDER = ["待审批", "已生效", "已归档"]


class PartitionPolicyService(BaseModuleService):
    MODULE = "partition_policy"
    ENTITY = "分区策略"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["策略名称", "分区维度", "分区粒度", "目标湖表", "生命周期"]
    REQUIRED_FIELDS = ["策略名称", "分区维度", "分区粒度"]
    STATUS_ORDER = ["待审批", "已生效", "已归档"]
    ACTION_RULES = {"启用策略": "已生效", "归档策略": "已归档"}
    NEGATIVE_ACTIONS = []

    # ---- 基础辅助 ----

    def _policies_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return store.find_by_field(self.MODULE, "目标湖表", table_name)

    def _next_version(self, table_name: str, exclude_id: int | None = None) -> str:
        """按湖表生成单调递增的策略版本号，保证归档清单能指回具体版本。"""
        highest = 0
        for policy in self._policies_for_table(table_name):
            if exclude_id is not None and int(policy.get("id", 0)) == exclude_id:
                continue
            version = str(policy.get("策略版本", ""))
            if version.startswith("v") and version[1:].isdigit():
                highest = max(highest, int(version[1:]))
        return f"v{highest + 1}"

    def _policy_range(self, policy: dict[str, Any]) -> tuple[Any, Any] | None:
        return parse_explicit_range(policy)

    def _lifecycle_flow(self, policy: dict[str, Any]) -> list[str]:
        flow = policy.setdefault("生命周期流转", [])
        if not isinstance(flow, list):
            flow = []
            policy["生命周期流转"] = flow
        return flow

    def _record_lifecycle(self, policy: dict[str, Any], status: str) -> None:
        flow = self._lifecycle_flow(policy)
        step = f"{status}@{datetime.datetime.now().isoformat()}"
        if not flow or not flow[-1].startswith(f"{status}@"):
            flow.append(step)

    def _archive_handovers_for_table(self, table_name: str) -> list[dict[str, Any]]:
        """返回该湖表所有已归档策略留下的交接清单。"""
        archived_ids = {
            int(policy.get("id", 0))
            for policy in self._policies_for_table(table_name)
            if policy.get("status") == "已归档"
        }
        return [
            item for item in store.handovers(table_name=table_name)
            if int(item.get("策略ID", 0)) in archived_ids
        ]

    def _handed_over_ranges(self, table_name: str) -> list[tuple[Any, Any]]:
        ranges = []
        for item in self._archive_handovers_for_table(table_name):
            parsed = parse_explicit_range(item)
            if parsed:
                ranges.append(parsed)
        return ranges

    # ---- 登记 ----

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记分区策略：分配策略版本，并保留数据范围等流转字段。"""
        entry, missing = super().create_entry(values)
        if entry is None:
            return entry, missing

        table_name = entry.get("目标湖表", "")
        entry["策略版本"] = str(values.get("策略版本") or self._next_version(table_name))
        for field in ("数据范围", "数据范围起", "数据范围止"):
            if values.get(field) not in (None, ""):
                entry[field] = values.get(field)
        entry["生命周期"] = entry.get("status", "待审批")
        entry["生命周期流转"] = [f"待审批@{datetime.datetime.now().isoformat()}"]
        return entry, missing

    # ---- 生命周期 ----

    def enable_policy(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启用分区策略：联动湖表注册分配分区键，并触发冷热分层检查。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "分区策略不存在"
        # 生命周期只允许按顺序流转：只有待审批可以启用
        if entry.get("status") != "待审批":
            return None, f"分区策略状态为{entry.get('status')}，不可启用（只允许待审批→已生效→已归档顺序流转）"

        table_name = entry.get("目标湖表", "")
        partition_key = entry.get("分区维度", "")
        granularity = normalize_granularity(entry.get("分区粒度", ""))
        if not granularity:
            return None, f"分区粒度「{entry.get('分区粒度')}」无法归一为单一粒度，每项合并只能使用一套分区粒度"

        # 同一湖表同时只允许一套生效粒度：要切换必须先归档旧策略
        active_policies = [
            policy for policy in self._policies_for_table(table_name)
            if policy.get("status") == "已生效" and int(policy.get("id", 0)) != entry_id
        ]
        if active_policies:
            active = active_policies[0]
            return None, (
                f"湖表 {table_name} 已有生效分区策略「{active.get('策略名称')}」"
                f"（版本 {active.get('策略版本', '-')}，粒度 {active.get('分区粒度', '-')}），"
                f"请先归档旧策略再启用新粒度"
            )

        entry["策略版本"] = entry.get("策略版本") or self._next_version(
            table_name, exclude_id=entry_id)
        entry["分区粒度"] = granularity

        # 检查新粒度声明的日期范围是否与旧策略已交接范围重叠
        declared_range = self._policy_range(entry)
        handover_ranges = self._handed_over_ranges(table_name)
        overlap_ranges = [
            (max(declared_range[0], old[0]), min(declared_range[1], old[1]))
            for old in handover_ranges if ranges_overlap(declared_range, old)
        ] if declared_range else []
        if overlap_ranges:
            remaining = subtract_range(declared_range, handover_ranges)
            entry["重叠日期范围"] = "；".join(
                format_date_range(item) for item in overlap_ranges)
            entry["有效数据范围"] = "；".join(
                format_date_range(item) for item in remaining) or "无（已全部交接旧策略）"
            entry["abnormal"] = True
        else:
            entry.pop("重叠日期范围", None)
            entry.pop("有效数据范围", None)
            entry["abnormal"] = False

        # 1) 联动湖表注册：将分区键写入对应湖表
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        if lake_tables:
            for lt in lake_tables:
                lt["分区键"] = partition_key
                lt["pending"] = True
            store.log_event("partition_to_lake", "partition_policy", "lake_table", {
                "policy_id": entry_id,
                "policy_version": entry.get("策略版本"),
                "table_name": table_name,
                "partition_key": partition_key,
                "granularity": granularity,
            })

        # 2) 检查冷热分层策略是否需要更新
        tier_entries = store.find_by_field("storage_tier", "生效湖表", table_name)
        if tier_entries:
            store.log_event("partition_to_tier", "partition_policy", "storage_tier", {
                "policy_id": entry_id,
                "table_name": table_name,
                "affected_tiers": len(tier_entries),
            })

        # 3) 自身状态流转
        entry["status"] = "已生效"
        entry["生命周期"] = "已生效"
        entry["pending"] = False
        entry["启用时间"] = datetime.datetime.now().isoformat()
        self._record_lifecycle(entry, "已生效")

        # 4) 记录粒度切换与旧交接范围的冲突情况
        if handover_ranges or overlap_ranges:
            store.log_event("partition_granularity_switch", "partition_policy", "file_compaction", {
                "policy_id": entry_id,
                "policy_version": entry.get("策略版本"),
                "table_name": table_name,
                "granularity": granularity,
                "handover_ranges": [format_date_range(item) for item in handover_ranges],
                "overlap_ranges": [format_date_range(item) for item in overlap_ranges],
            })

        # 5) 创建分区周期快照记录
        snapshot_key = store.create_snapshot(self.MODULE)
        return entry, (
            f"分区策略 {entry.get('策略名称')} 已生效，版本={entry.get('策略版本')}，"
            f"粒度={granularity}，快照保存在 {snapshot_key}"
        )

    def archive_policy(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """归档分区策略：把待调度任务的输入交接出去后才归档。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "分区策略不存在"
        if entry.get("status") != "已生效":
            return None, f"分区策略状态为{entry.get('status')}，不可归档（只允许待审批→已生效→已归档顺序流转）"

        table_name = entry.get("目标湖表", "")
        policy_version = entry.get("策略版本") or self._next_version(
            table_name, exclude_id=entry_id)
        entry["策略版本"] = policy_version
        granularity = normalize_granularity(entry.get("分区粒度", ""))

        compactions = store.find_by_field("file_compaction", "目标湖表", table_name)

        # 执行中的任务已经持有输入，归档前必须先结束，避免半途换输入
        running = [c for c in compactions if c.get("status") == "执行中"]
        if running:
            return None, (
                f"湖表 {table_name} 仍有 {len(running)} 个执行中的合并任务，"
                f"请等待其完成后再归档分区策略"
            )

        # 保留策略仍在生效时不允许归档，避免清理动作失去分区口径
        retentions = store.find_by_field("retention_mgr", "适用湖表", table_name)
        active_retentions = [r for r in retentions if r.get("status") == "已生效"]
        if active_retentions:
            return None, f"湖表 {table_name} 仍有 {len(active_retentions)} 个活跃保留策略，不可归档"

        # 待调度任务：逐条锁存分区版本、粒度和数据范围到归档交接清单
        pending = [c for c in compactions if c.get("status") == "待调度"]
        policy_range = self._policy_range(entry)
        handed_over: list[dict[str, Any]] = []
        for task in pending:
            task_no = str(task.get("任务编号", ""))
            existing = [
                item for item in store.handovers(task_no=task_no)
                if int(item.get("策略ID", 0)) == entry_id
            ]
            if existing:
                handed_over.append(existing[0])
                continue

            task_range = parse_explicit_range(task)
            input_range = task_range or policy_range
            task_granularity = normalize_granularity(task.get("分区粒度", ""))
            handover_id = store.next_handover_id()
            handover = {
                "id": handover_id,
                "交接清单编号": f"HO-{handover_id:04d}",
                "任务编号": task_no,
                "待调度任务ID": task.get("id"),
                "目标湖表": table_name,
                "策略ID": entry_id,
                "策略名称": entry.get("策略名称", ""),
                "策略版本": policy_version,
                # 交接输入固定按被归档策略的粒度，任务自行声明的异粒度不覆盖它
                "分区粒度": granularity,
                "数据范围": format_date_range(input_range),
                "数据范围起": input_range[0].isoformat() if input_range else "",
                "数据范围止": input_range[1].isoformat() if input_range else "",
                "交接状态": "待领取",
                "生命周期": "已归档",
                "归档时间": datetime.datetime.now().isoformat(),
            }
            if task_granularity and task_granularity != granularity:
                handover["任务声明粒度"] = task_granularity
                handover["交接状态"] = "粒度冲突"
            store.add_handover(handover)
            handed_over.append(handover)

        handover_ranges = [
            item.get("数据范围", "") for item in handed_over if item.get("数据范围")
        ]
        entry["status"] = "已归档"
        entry["生命周期"] = "已归档"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["归档时间"] = datetime.datetime.now().isoformat()
        entry["交接日期范围"] = "；".join(handover_ranges)
        self._record_lifecycle(entry, "已归档")

        store.log_event("partition_policy_archived", "partition_policy", "file_compaction", {
            "policy_id": entry_id,
            "policy_version": policy_version,
            "table_name": table_name,
            "granularity": granularity,
            "handover_count": len(handed_over),
            "handover_ranges": handover_ranges,
        })

        if handed_over:
            return entry, (
                f"分区策略 {entry.get('策略名称')} 已归档，{len(handed_over)} 个待调度任务已交接，"
                f"交接日期范围={entry['交接日期范围'] or '未标注'}"
            )
        return entry, f"分区策略 {entry.get('策略名称')} 已归档，无待调度任务需要交接"

    # ---- 生命周期管理 ----

    def lifecycle_report(self, entry_id: int) -> dict[str, Any] | None:
        """生成分区策略的完整生命周期报告，汇总下游依赖状态。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None

        table_name = entry.get("目标湖表", "")
        # 汇总下游模块状态
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        tiers = store.find_by_field("storage_tier", "生效湖表", table_name)
        compactions = store.find_by_field("file_compaction", "目标湖表", table_name)
        retentions = store.find_by_field("retention_mgr", "适用湖表", table_name)
        cross_events = store.events_for_module(self.MODULE)
        handovers = store.handovers(table_name=table_name)

        return {
            "policy": entry,
            "lifecycle_order": LIFECYCLE_ORDER,
            "lifecycle_flow": list(entry.get("生命周期流转", [])),
            "linked_lake_tables": len(lake_tables),
            "linked_storage_tiers": len(tiers),
            "linked_compactions": len(compactions),
            "linked_retentions": len(retentions),
            "cross_module_events": len(cross_events),
            "handover_count": len(handovers),
            "handover_date_ranges": sorted({
                item.get("数据范围", "") for item in handovers if item.get("数据范围")
            }),
            "downstream_health": {
                "has_errors": any(
                    t.get("abnormal") for t in tiers + compactions + retentions
                ),
                "lake_table_ready": any(t.get("status") == "已注册" for t in lake_tables),
            },
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """覆盖基类方法，加入业务逻辑分支。"""
        if action == "启用策略":
            return self.enable_policy(entry_id)
        if action == "归档策略":
            return self.archive_policy(entry_id)
        return super().run_action(entry_id, action)
