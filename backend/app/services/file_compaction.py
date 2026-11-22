"""小文件合并领域服务：调度和执行小文件合并任务以减少元数据开销。

小文件合并具备幂等追踪能力：同一湖表的同一批次合并任务不会重复执行。
上游接收冷热分层迁移事件，下游联动保留策略的清理动作。

合并启动时先领取输入：优先读「归档交接分区清单」，没有交接时回退到当前
生效的分区策略，两者都没有才按任务自带范围执行。领取结果（策略版本、
分区粒度、数据范围）写入调度记录，失败请求同样留痕，保证任何一次触发
都能追溯到来源任务和当时的口径。每项合并只允许一套分区粒度。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.partition_handover import (
    format_date_range,
    granularities_in,
    intersect_range,
    normalize_granularity,
    parse_explicit_range,
    ranges_overlap,
)
from app.store import store


class FileCompactionService(BaseModuleService):
    MODULE = "file_compaction"
    ENTITY = "合并任务"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["任务编号", "目标湖表", "文件阈值", "合并后文件数", "执行状态"]
    REQUIRED_FIELDS = ["任务编号", "目标湖表", "文件阈值"]
    STATUS_ORDER = ["待调度", "执行中", "已完成", "执行失败"]
    ACTION_RULES = {"触发合并": "执行中", "取消任务": "执行失败"}
    NEGATIVE_ACTIONS = []

    # 幂等追踪：已完成的 task_id 集合
    _completed_tasks: set[str] = set()

    # ---- 登记 ----

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记合并任务：绑定登记时可见的分区口径，避免归档后无输入可领。"""
        entry, missing = super().create_entry(values)
        if entry is None:
            return entry, missing

        table_name = entry.get("目标湖表", "")
        for field in ("分区粒度", "数据范围", "数据范围起", "数据范围止", "策略版本"):
            if values.get(field) not in (None, ""):
                entry[field] = values.get(field)

        active_policy = self._active_policy(table_name)
        if active_policy is not None:
            if not entry.get("策略版本"):
                entry["策略版本"] = active_policy.get("策略版本", "")
            if not entry.get("分区粒度"):
                entry["分区粒度"] = normalize_granularity(active_policy.get("分区粒度", ""))
            policy_range = parse_explicit_range(active_policy)
            if policy_range and not parse_explicit_range(entry):
                entry["数据范围"] = format_date_range(policy_range)
                entry["数据范围起"] = policy_range[0].isoformat()
                entry["数据范围止"] = policy_range[1].isoformat()
        elif not parse_explicit_range(entry):
            # 没有生效策略但已有归档策略：登记即按旧策略版本落口径，保证可追溯
            archived = self._archived_policies(table_name)
            if archived:
                policy = archived[-1]
                task_range = parse_explicit_range(policy)
                entry.setdefault("策略版本", policy.get("策略版本", ""))
                entry.setdefault(
                    "分区粒度", normalize_granularity(policy.get("分区粒度", "")))
                if task_range:
                    entry["数据范围"] = format_date_range(task_range)
                    entry["数据范围起"] = task_range[0].isoformat()
                    entry["数据范围止"] = task_range[1].isoformat()
        return entry, missing

    # ---- 输入领取 ----

    def _archived_policies(self, table_name: str) -> list[dict[str, Any]]:
        return [
            policy for policy in store.find_by_field(
                "partition_policy", "目标湖表", table_name)
            if policy.get("status") == "已归档"
        ]

    def _active_policy(self, table_name: str) -> dict[str, Any] | None:
        for policy in store.find_by_field("partition_policy", "目标湖表", table_name):
            if policy.get("status") == "已生效":
                return policy
        return None

    def resolve_input(
        self, entry: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str]:
        """解析本次合并实际可用的输入；返回 (输入描述, 失败原因)。

        输入描述含来源、策略版本、分区粒度、数据范围和交接清单编号。
        旧策略已归档但没有任何交接清单时明确失败，不允许“显示可执行却取不到输入”。
        """
        table_name = entry.get("目标湖表", "")
        task_no = str(entry.get("任务编号", ""))

        handovers = [
            item for item in store.handovers(table_name=table_name, task_no=task_no)
            if str(item.get("任务编号", "")) == task_no
        ]
        handover = handovers[-1] if handovers else None
        if handover is not None:
            handover_status = handover.get("交接状态", "")
            if handover_status == "粒度冲突":
                return None, (
                    f"归档交接分区清单 {handover.get('交接清单编号')} 的输入粒度为 "
                    f"{handover.get('分区粒度')}，任务 {task_no} 声明 "
                    f"{handover.get('任务声明粒度')}，每项合并只能使用一套分区粒度"
                )
            handover_range = parse_explicit_range(handover)
            if not handover_range:
                return None, (
                    f"归档交接分区清单 {handover.get('交接清单编号')} 未记录有效数据范围，"
                    f"任务 {task_no} 实际取不到输入"
                )
            return {
                "来源": "归档交接分区清单",
                "交接清单编号": handover.get("交接清单编号", ""),
                "策略ID": handover.get("策略ID"),
                "策略版本": handover.get("策略版本", ""),
                "分区粒度": handover.get("分区粒度", ""),
                "数据范围": format_date_range(handover_range),
                "range": handover_range,
                "handover": handover,
                "policy": None,
            }, ""

        active_policy = self._active_policy(table_name)
        if active_policy is not None:
            policy_range = parse_explicit_range(active_policy)
            task_range = parse_explicit_range(entry)
            input_range = intersect_range(policy_range, task_range)
            if input_range is None:
                input_range = task_range or policy_range
            return {
                "来源": "生效分区策略",
                "交接清单编号": "",
                "策略ID": active_policy.get("id"),
                "策略版本": active_policy.get("策略版本", ""),
                "分区粒度": normalize_granularity(active_policy.get("分区粒度", "")),
                "数据范围": format_date_range(input_range),
                "range": input_range,
                "handover": None,
                "policy": active_policy,
            }, ""

        archived = self._archived_policies(table_name)
        if archived:
            return None, (
                f"湖表 {table_name} 的旧分区策略已归档且没有任务 {task_no} 的归档交接分区清单，"
                f"任务显示可执行但实际取不到输入"
            )

        # 没有策略约束的任务：按任务自带范围执行，保持兼容
        task_range = parse_explicit_range(entry)
        return {
            "来源": "任务自带范围",
            "交接清单编号": "",
            "策略ID": None,
            "策略版本": "",
            "分区粒度": normalize_granularity(entry.get("分区粒度", "")),
            "数据范围": format_date_range(task_range),
            "range": task_range,
            "handover": None,
            "policy": None,
        }, ""

    def _run_conflict(
        self, table_name: str, granularity: str,
        input_range: tuple[Any, Any] | None,
        task_no: str = "",
    ) -> str:
        """同一输入已被另一套粒度占住时拒绝，避免新旧方案重复处理。

        既看已领取的调度记录，也看待领取的归档交接清单：旧策略归档后
        尚未启动的任务仍占着那段日期，新粒度不能抢先吃掉同一份输入。
        """
        if not granularity or not input_range:
            return ""
        for run in store.compaction_runs(table_name=table_name):
            if run.get("状态") in ("失败", "已取消"):
                continue
            if run.get("任务编号") == task_no:
                continue
            if not granularities_in(run.get("分区粒度", "")):
                continue
            if normalize_granularity(run.get("分区粒度", "")) == granularity:
                continue
            if ranges_overlap(parse_explicit_range(run), input_range):
                return (
                    f"数据范围 {run.get('数据范围')} 已由调度记录 {run.get('调度记录编号')} "
                    f"按粒度 {run.get('分区粒度')} 领取，当前任务粒度 {granularity} 会重复处理同一输入"
                )
        for handover in store.handovers(table_name=table_name):
            if str(handover.get("任务编号", "")) == task_no:
                continue
            if handover.get("交接状态") in ("已取消", "粒度冲突"):
                continue
            handover_granularity = normalize_granularity(handover.get("分区粒度", ""))
            if not handover_granularity or handover_granularity == granularity:
                continue
            if ranges_overlap(parse_explicit_range(handover), input_range):
                return (
                    f"数据范围 {handover.get('数据范围')} 仍由归档交接分区清单 "
                    f"{handover.get('交接清单编号')} 按粒度 {handover_granularity} 保留"
                    f"（任务 {handover.get('任务编号')} 尚未启动），当前任务粒度 {granularity} "
                    f"会重复处理同一输入"
                )
        return ""

    def _record_run(
        self, entry: dict[str, Any] | None, *, status: str, reason: str = "",
        input_info: dict[str, Any] | None = None,
        request_task_no: str = "", request_table: str = "",
    ) -> dict[str, Any]:
        info = input_info or {}
        run_no = store.next_run_no()
        run = {
            "id": len(store._compaction_runs) + 1,
            "调度记录编号": run_no,
            "任务编号": str(entry.get("任务编号", "")) if entry else request_task_no,
            "请求任务编号": request_task_no or (
                str(entry.get("任务编号", "")) if entry else ""),
            "任务ID": entry.get("id") if entry else None,
            "目标湖表": entry.get("目标湖表", "") if entry else request_table,
            "来源": info.get("来源", ""),
            "交接清单编号": info.get("交接清单编号", ""),
            "策略ID": info.get("策略ID"),
            "策略版本": info.get("策略版本", ""),
            "分区粒度": info.get("分区粒度", ""),
            "数据范围": info.get("数据范围", ""),
            "数据范围起": info["range"][0].isoformat() if info.get("range") else "",
            "数据范围止": info["range"][1].isoformat() if info.get("range") else "",
            "状态": status,
            "失败原因": reason,
            "触发时间": datetime.datetime.now().isoformat(),
        }
        store.add_compaction_run(run)
        return run

    # ---- 动作 ----

    def trigger_compaction(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """触发小文件合并任务：先领取实际输入，再按单一粒度调度。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            self._record_run(
                None, status="失败", reason=f"合并任务 {entry_id} 不存在",
                request_task_no=f"ID-{entry_id}")
            return None, "合并任务不存在"

        task_no = str(entry.get("任务编号", ""))
        table_name = entry.get("目标湖表", "")

        if entry.get("status") != "待调度":
            reason = f"合并任务状态为{entry.get('status')}，不可触发"
            self._record_run(entry, status="失败", reason=reason)
            return None, reason

        # 幂等检查：同一任务编号已完成的不再触发
        if task_no in self._completed_tasks:
            reason = f"任务 {task_no} 已经完成，不可重复触发"
            self._record_run(entry, status="失败", reason=reason)
            return None, reason

        # 检查同湖表是否有其他执行中的合并
        active_compactions = [
            c for c in store.rows(self.MODULE)
            if c.get("目标湖表") == table_name
            and c.get("status") == "执行中"
            and c.get("id") != entry_id
        ]
        if active_compactions:
            reason = (
                f"湖表 {table_name} 已有合并任务在执行中"
                f"（任务编号：{active_compactions[0].get('任务编号')}），不可并发触发"
            )
            self._record_run(entry, status="失败", reason=reason)
            return None, reason

        # 1) 领取实际输入（优先归档交接清单）
        input_info, error = self.resolve_input(entry)
        if error:
            self._record_run(entry, status="失败", reason=error)
            store.log_event("compaction_input_missing", "file_compaction", "partition_policy", {
                "task_no": task_no,
                "table_name": table_name,
                "reason": error,
            })
            return None, error

        # 2) 每项合并只能使用一套分区粒度
        declared = granularities_in(entry.get("分区粒度", ""))
        if len(declared) > 1:
            reason = f"任务 {task_no} 声明了多套分区粒度（{'、'.join(declared)}），每项合并只能使用一套"
            self._record_run(entry, status="失败", reason=reason, input_info=input_info)
            return None, reason
        if declared and input_info.get("分区粒度") and declared[0] != input_info["分区粒度"]:
            reason = (
                f"任务声明粒度 {declared[0]} 与输入来源粒度 {input_info['分区粒度']} 不一致，"
                f"不可混用"
            )
            self._record_run(entry, status="失败", reason=reason, input_info=input_info)
            return None, reason
        granularity = input_info.get("分区粒度") or (declared[0] if declared else "")
        input_info["分区粒度"] = granularity

        # 3) 新旧方案不能处理同一输入
        conflict = self._run_conflict(
            table_name, granularity, input_info.get("range"), task_no=task_no)
        if conflict:
            self._record_run(entry, status="失败", reason=conflict, input_info=input_info)
            return None, conflict

        # 4) 自身状态流转并落调度记录
        entry["status"] = "执行中"
        entry["pending"] = True
        entry["abnormal"] = False
        entry["分区粒度"] = granularity
        entry["策略版本"] = input_info.get("策略版本", "")
        entry["输入来源"] = input_info.get("来源", "")
        entry["数据范围"] = input_info.get("数据范围", "")

        run = self._record_run(entry, status="已领取", input_info=input_info)
        entry["调度记录编号"] = run["调度记录编号"]

        # 5) 交接清单标记为已领取，避免同一输入被再次分配
        handover = input_info.get("handover")
        if handover is not None:
            handover["交接状态"] = "已领取"
            handover["领取时间"] = run["触发时间"]
            handover["关联调度记录"] = run["调度记录编号"]

        store.log_event("compaction_triggered", "file_compaction", "file_compaction", {
            "task_no": task_no,
            "table_name": table_name,
            "file_threshold": entry.get("文件阈值", ""),
            "input_source": input_info.get("来源", ""),
            "policy_version": input_info.get("策略版本", ""),
            "granularity": granularity,
            "data_range": input_info.get("数据范围", ""),
            "run_no": run["调度记录编号"],
        })

        return entry, (
            f"合并任务 {task_no} 已触发，输入来源={input_info.get('来源')}，"
            f"策略版本={input_info.get('策略版本') or '-'}，粒度={granularity}，"
            f"数据范围={input_info.get('数据范围') or '未标注'}，调度记录={run['调度记录编号']}"
        )

    def complete_compaction(self, entry_id: int, result_file_count: str = "") -> tuple[dict[str, Any] | None, str]:
        """完成合并任务，记录幂等标识，通知保留策略清理旧文件。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "合并任务不存在"
        if entry.get("status") != "执行中":
            return None, f"合并任务状态为{entry.get('status')}，不可完成"

        task_no = entry.get("任务编号", "")
        table_name = entry.get("目标湖表", "")

        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        if result_file_count:
            entry["合并后文件数"] = result_file_count

        # 调度记录闭环，保留策略版本和日期口径供事后追溯
        for run in store.compaction_runs(task_no=task_no):
            if run.get("状态") == "已领取" and run.get("任务ID") == entry_id:
                run["状态"] = "已完成"
                run["完成时间"] = datetime.datetime.now().isoformat()
                break

        # 幂等追踪
        self._completed_tasks.add(task_no)

        store.log_event("compaction_completed", "file_compaction", "retention_mgr", {
            "task_no": task_no,
            "table_name": table_name,
            "result_file_count": result_file_count,
            "policy_version": entry.get("策略版本", ""),
            "granularity": entry.get("分区粒度", ""),
            "data_range": entry.get("数据范围", ""),
        })

        # 通知保留策略：合并完成后的旧文件可清理
        retentions = store.find_by_field("retention_mgr", "适用湖表", table_name)
        for r in retentions:
            if r.get("status") == "已生效":
                r["pending"] = True

        return entry, f"合并任务 {task_no} 已完成，结果文件数={result_file_count}，已通知保留策略清理旧文件"

    def cancel_compaction(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """取消任务：退回已领取的交接清单，并记录失败请求来源。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "合并任务不存在"
        if entry.get("status") not in ("待调度", "执行中"):
            return None, f"合并任务状态为{entry.get('status')}，不可取消"

        task_no = str(entry.get("任务编号", ""))
        run_no = entry.get("调度记录编号", "")
        for run in store.compaction_runs(task_no=task_no):
            if run_no and run.get("调度记录编号") != run_no:
                continue
            if run.get("状态") == "已领取":
                run["状态"] = "已取消"
                run["取消时间"] = datetime.datetime.now().isoformat()

        for handover in store.handovers(table_name=entry.get("目标湖表", ""), task_no=task_no):
            if handover.get("交接状态") == "已领取":
                handover["交接状态"] = "待领取"
                handover.pop("领取时间", None)
                handover.pop("关联调度记录", None)

        entry["status"] = "执行失败"
        entry["pending"] = False
        entry["abnormal"] = False
        store.log_event("compaction_cancelled", "file_compaction", "file_compaction", {
            "task_no": task_no,
            "table_name": entry.get("目标湖表", ""),
            "run_no": run_no,
        })
        return entry, f"合并任务 {task_no} 已取消，交接清单已退回待领取"

    def is_task_completed(self, task_no: str) -> bool:
        """幂等检查：查询指定任务编号是否已完成。"""
        return task_no in self._completed_tasks

    def compaction_status_for_table(self, table_name: str) -> dict[str, Any]:
        """查询指定湖表的合并任务状态摘要。"""
        compactions = store.find_by_field(self.MODULE, "目标湖表", table_name)
        return {
            "table_name": table_name,
            "total_tasks": len(compactions),
            "pending": sum(1 for c in compactions if c.get("status") == "待调度"),
            "running": sum(1 for c in compactions if c.get("status") == "执行中"),
            "completed": sum(1 for c in compactions if c.get("status") == "已完成"),
            "failed": sum(1 for c in compactions if c.get("status") == "执行失败"),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "触发合并":
            return self.trigger_compaction(entry_id)
        if action == "取消任务":
            return self.cancel_compaction(entry_id)
        return super().run_action(entry_id, action)
