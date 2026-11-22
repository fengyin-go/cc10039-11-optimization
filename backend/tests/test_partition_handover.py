"""归档交接 + 合并输入领取的端到端场景测试。

覆盖题目要求：日分区任务暂缓启动 → 归档旧策略并切换到月分区 → 触发合并，
核对原计划状态、分区日期覆盖，以及新旧方案是否处理同一输入。
"""
from __future__ import annotations

import copy
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.seed import SEED_ROWS
from app.services.file_compaction import FileCompactionService
from app.services.partition_handover import coverage_items
from app.services.partition_policy import PartitionPolicyService
from app.store import store

TABLE = "dwd_order_di"


@pytest.fixture(autouse=True)
def clean_store():
    """每个用例重置内存仓库，避免共享单例串味。"""
    store._tables = {name: [copy.deepcopy(row) for row in rows]
                     for name, rows in SEED_ROWS.items()}
    store._events = []
    store._snapshots = {}
    store._handovers = []
    store._compaction_runs = []
    FileCompactionService._completed_tasks = set()
    yield


def _register_table(lake_svc, table_name: str = TABLE) -> dict:
    entry, missing = lake_svc.create_entry({
        "表名": table_name,
        "所属业务域": "交易域",
        "存储格式": "iceberg",
    })
    assert entry is not None and not missing
    entry, message = lake_svc.register_table(entry["id"])
    assert entry is not None, message
    return entry


def _create_day_policy(pp_svc, table_name: str = TABLE) -> dict:
    entry, missing = pp_svc.create_entry({
        "策略名称": "日分区策略",
        "分区维度": "dt",
        "分区粒度": "日",
        "目标湖表": table_name,
        "数据范围": "2026-01-01~2026-01-31",
    })
    assert entry is not None and not missing
    entry, message = pp_svc.enable_policy(entry["id"])
    assert entry is not None, message
    return entry


def _create_pending_task(fc_svc, table_name: str = TABLE, task_no: str = "FILE-9001") -> dict:
    entry, missing = fc_svc.create_entry({
        "任务编号": task_no,
        "目标湖表": table_name,
        "文件阈值": "100",
        "数据范围": "2026-01-01~2026-01-31",
    })
    assert entry is not None and not missing
    return entry


class TestArchiveHandoverScenario:
    def test_day_to_month_switch_keeps_original_plan(self):
        from app.services.lake_table import LakeTableService

        lake_svc = LakeTableService()
        pp_svc = PartitionPolicyService()
        fc_svc = FileCompactionService()

        _register_table(lake_svc)
        day_policy = _create_day_policy(pp_svc)
        task = _create_pending_task(fc_svc)

        # 1) 登记日分区任务后暂缓启动：任务保持待调度
        assert task["status"] == "待调度"
        assert task["分区粒度"] == "日"
        assert task["策略版本"] == day_policy["策略版本"] == "v1"

        # 2) 归档旧分区策略：待调度任务被写入交接清单
        archived, message = pp_svc.archive_policy(day_policy["id"])
        assert archived is not None, message
        assert archived["status"] == "已归档"
        assert archived["生命周期"] == "已归档"
        assert archived["生命周期流转"][0].startswith("待审批@")
        assert archived["生命周期流转"][-1].startswith("已归档@")
        assert archived["交接日期范围"] == "2026-01-01~2026-01-31"

        handovers = store.handovers(table_name=TABLE, task_no="FILE-9001")
        assert len(handovers) == 1
        handover = handovers[0]
        assert handover["策略版本"] == "v1"
        assert handover["分区粒度"] == "日"
        assert handover["数据范围"] == "2026-01-01~2026-01-31"
        assert handover["交接状态"] == "待领取"

        # 3) 启用月分区新策略：识别与旧交接范围重叠，给出有效范围
        month_policy, missing = pp_svc.create_entry({
            "策略名称": "月分区策略",
            "分区维度": "month",
            "分区粒度": "月",
            "目标湖表": TABLE,
            "数据范围": "2026-01-01~2026-12-31",
        })
        assert month_policy is not None and not missing
        month_policy, message = pp_svc.enable_policy(month_policy["id"])
        assert month_policy is not None, message
        assert month_policy["分区粒度"] == "月"
        assert month_policy["策略版本"] == "v2"
        assert "2026-01-01~2026-01-31" in month_policy["重叠日期范围"]
        assert "2026-02-01~2026-12-31" in month_policy["有效数据范围"]

        # 4) 触发合并：按原计划从交接清单领取日分区输入
        triggered, message = fc_svc.trigger_compaction(task["id"])
        assert triggered is not None, message
        assert triggered["status"] == "执行中"
        assert triggered["分区粒度"] == "日"
        assert triggered["策略版本"] == "v1"
        assert triggered["输入来源"] == "归档交接分区清单"
        assert triggered["数据范围"] == "2026-01-01~2026-01-31"

        runs = store.compaction_runs(task_no="FILE-9001")
        assert len(runs) == 1
        run = runs[0]
        assert run["状态"] == "已领取"
        assert run["分区粒度"] == "日"
        assert run["数据范围起"] == "2026-01-01"
        assert run["数据范围止"] == "2026-01-31"
        assert run["交接清单编号"] == handover["交接清单编号"]
        assert run["策略版本"] == "v1"

        # 5) 交接清单被标记领取，避免重复分配
        assert store.handovers(task_no="FILE-9001")[0]["交接状态"] == "已领取"

        # 6) 日期覆盖核对：交接范围仍可辨认，且没有冲突
        coverage = coverage_items(TABLE)
        assert "2026-01-01~2026-01-31" in coverage["handover_date_ranges"]
        assert coverage["has_conflict"] is False
        assert {item["分区粒度"] for item in coverage["items"]} == {"日"}

    def test_new_month_task_cannot_reprocess_handed_over_days(self):
        from app.services.lake_table import LakeTableService

        lake_svc = LakeTableService()
        pp_svc = PartitionPolicyService()
        fc_svc = FileCompactionService()

        _register_table(lake_svc)
        day_policy = _create_day_policy(pp_svc)
        first_task = _create_pending_task(fc_svc, task_no="FILE-9101")
        archived, message = pp_svc.archive_policy(day_policy["id"])
        assert archived is not None, message
        triggered, message = fc_svc.trigger_compaction(first_task["id"])
        assert triggered is not None, message
        completed, message = fc_svc.complete_compaction(first_task["id"], "12")
        assert completed is not None, message

        month_policy, missing = pp_svc.create_entry({
            "策略名称": "月分区策略",
            "分区维度": "month",
            "分区粒度": "月",
            "目标湖表": TABLE,
            "数据范围": "2026-01-01~2026-01-31",
        })
        assert month_policy is not None and not missing
        month_policy, message = pp_svc.enable_policy(month_policy["id"])
        assert month_policy is not None, message

        second_task = _create_pending_task(fc_svc, task_no="FILE-9102")
        assert second_task["分区粒度"] == "月"
        result, message = fc_svc.trigger_compaction(second_task["id"])
        assert result is None
        assert "重复处理同一输入" in message

        failed_runs = [
            run for run in store.compaction_runs(task_no="FILE-9102")
            if run["状态"] == "失败"
        ]
        assert len(failed_runs) == 1
        assert failed_runs[0]["请求任务编号"] == "FILE-9102"
        assert failed_runs[0]["分区粒度"] == "月"

        coverage = coverage_items(TABLE)
        assert coverage["has_conflict"] is True
        assert coverage["conflicts"][0]["类型"] == "不同分区粒度命中同一输入"

    def test_unknown_task_failure_is_traceable(self):
        fc_svc = FileCompactionService()
        result, message = fc_svc.trigger_compaction(987654)
        assert result is None
        assert "不存在" in message
        runs = store.compaction_runs()
        assert runs[-1]["状态"] == "失败"
        assert runs[-1]["请求任务编号"] == "ID-987654"

    def test_archive_blocked_while_compaction_running(self):
        from app.services.lake_table import LakeTableService

        lake_svc = LakeTableService()
        pp_svc = PartitionPolicyService()
        fc_svc = FileCompactionService()

        _register_table(lake_svc)
        day_policy = _create_day_policy(pp_svc)
        task = _create_pending_task(fc_svc)
        triggered, message = fc_svc.trigger_compaction(task["id"])
        assert triggered is not None, message

        archived, message = pp_svc.archive_policy(day_policy["id"])
        assert archived is None
        assert "执行中" in message
        assert store.handovers(task_no="FILE-9001") == []

    def test_archived_policy_without_handover_must_not_look_runnable(self):
        from app.services.lake_table import LakeTableService

        lake_svc = LakeTableService()
        pp_svc = PartitionPolicyService()
        fc_svc = FileCompactionService()

        _register_table(lake_svc)
        day_policy = _create_day_policy(pp_svc)
        task = _create_pending_task(fc_svc)

        # 模拟历史数据：旧策略已归档，但没有留下交接清单
        day_policy["status"] = "已归档"
        result, message = fc_svc.trigger_compaction(task["id"])
        assert result is None
        assert "取不到输入" in message
        assert task["status"] == "待调度"
        assert store.compaction_runs(task_no="FILE-9001")[-1]["状态"] == "失败"


class TestPartitionPolicyLifecycle:
    def test_lifecycle_only_flows_forward(self):
        pp_svc = PartitionPolicyService()
        entry, missing = pp_svc.create_entry({
            "策略名称": "待审批策略",
            "分区维度": "dt",
            "分区粒度": "日",
            "目标湖表": "t_lifecycle",
        })
        assert entry is not None and not missing
        # 待审批不能直接归档
        result, message = pp_svc.archive_policy(entry["id"])
        assert result is None
        assert "顺序流转" in message
        # 启用后不能再次启用
        entry, message = pp_svc.enable_policy(entry["id"])
        assert entry is not None, message
        result, message = pp_svc.enable_policy(entry["id"])
        assert result is None
        # 归档后不可再次启用
        entry, message = pp_svc.archive_policy(entry["id"])
        assert entry is not None, message
        result, message = pp_svc.enable_policy(entry["id"])
        assert result is None

    def test_multiple_granularities_rejected_for_one_merge(self):
        from app.services.lake_table import LakeTableService

        lake_svc = LakeTableService()
        fc_svc = FileCompactionService()
        _register_table(lake_svc)
        task, missing = fc_svc.create_entry({
            "任务编号": "FILE-9201",
            "目标湖表": TABLE,
            "文件阈值": "100",
            "分区粒度": "日/月",
            "数据范围": "2026-03-01~2026-03-31",
        })
        assert task is not None and not missing
        result, message = fc_svc.trigger_compaction(task["id"])
        assert result is None
        assert "每项合并只能使用一套" in message

    def test_archive_records_granularity_conflict_instead_of_silently_switching(self):
        from app.services.lake_table import LakeTableService

        lake_svc = LakeTableService()
        pp_svc = PartitionPolicyService()
        fc_svc = FileCompactionService()

        _register_table(lake_svc)
        day_policy = _create_day_policy(pp_svc)
        task, missing = fc_svc.create_entry({
            "任务编号": "FILE-9301",
            "目标湖表": TABLE,
            "文件阈值": "100",
            "分区粒度": "月",
            "数据范围": "2026-01-01~2026-01-31",
        })
        assert task is not None and not missing

        archived, message = pp_svc.archive_policy(day_policy["id"])
        assert archived is not None, message
        handover = store.handovers(task_no="FILE-9301")[0]
        # 交接输入固定按被归档策略的日粒度，任务的月粒度作为冲突信息保留
        assert handover["分区粒度"] == "日"
        assert handover["任务声明粒度"] == "月"
        assert handover["交接状态"] == "粒度冲突"

        result, message = fc_svc.trigger_compaction(task["id"])
        assert result is None
        assert "每项合并只能使用一套分区粒度" in message
        assert task["status"] == "待调度"
        assert store.compaction_runs(task_no="FILE-9301")[-1]["状态"] == "失败"

    def test_pending_task_without_declared_granularity_uses_policy_granularity(self):
        from app.services.lake_table import LakeTableService

        lake_svc = LakeTableService()
        pp_svc = PartitionPolicyService()
        fc_svc = FileCompactionService()

        _register_table(lake_svc)
        day_policy = _create_day_policy(pp_svc)
        task, missing = fc_svc.create_entry({
            "任务编号": "FILE-9401",
            "目标湖表": TABLE,
            "文件阈值": "100",
        })
        assert task is not None and not missing
        assert task["分区粒度"] == "日"

        archived, message = pp_svc.archive_policy(day_policy["id"])
        assert archived is not None, message
        triggered, message = fc_svc.trigger_compaction(task["id"])
        assert triggered is not None, message
        assert triggered["分区粒度"] == "日"
        assert triggered["数据范围"] == "2026-01-01~2026-01-31"

    def test_pending_handover_blocks_new_granularity_before_original_task_starts(self):
        from app.services.lake_table import LakeTableService

        lake_svc = LakeTableService()
        pp_svc = PartitionPolicyService()
        fc_svc = FileCompactionService()

        _register_table(lake_svc)
        day_policy = _create_day_policy(pp_svc)
        original = _create_pending_task(fc_svc, task_no="FILE-9501")
        archived, message = pp_svc.archive_policy(day_policy["id"])
        assert archived is not None, message

        month_policy, missing = pp_svc.create_entry({
            "策略名称": "月分区策略",
            "分区维度": "month",
            "分区粒度": "月",
            "目标湖表": TABLE,
            "数据范围": "2026-01-01~2026-01-31",
        })
        assert month_policy is not None and not missing
        month_policy, message = pp_svc.enable_policy(month_policy["id"])
        assert month_policy is not None, message

        # 旧任务尚未启动，交接清单仍占着 1 月；新月任务不能抢同一段输入
        newcomer = _create_pending_task(fc_svc, task_no="FILE-9502")
        result, message = fc_svc.trigger_compaction(newcomer["id"])
        assert result is None
        assert "尚未启动" in message

        # 原计划不受影响，仍按日粒度领取
        triggered, message = fc_svc.trigger_compaction(original["id"])
        assert triggered is not None, message
        assert triggered["分区粒度"] == "日"

        coverage = coverage_items(TABLE)
        assert coverage["overlap_with_active_policy"]
        assert coverage["overlap_with_active_policy"][0]["重叠日期范围"] == (
            "2026-01-01~2026-01-31")
        # 旧任务完成后，调度记录仍占着 1 月，新任务不能重复处理同一输入
        completed, message = fc_svc.complete_compaction(original["id"], "8")
        assert completed is not None, message
        relaxed, message = fc_svc.trigger_compaction(newcomer["id"])
        assert relaxed is None
        assert "重复处理同一输入" in message

    def test_cancel_returns_handover_and_frees_input(self):
        from app.services.lake_table import LakeTableService

        lake_svc = LakeTableService()
        pp_svc = PartitionPolicyService()
        fc_svc = FileCompactionService()

        _register_table(lake_svc)
        day_policy = _create_day_policy(pp_svc)
        task = _create_pending_task(fc_svc)
        archived, message = pp_svc.archive_policy(day_policy["id"])
        assert archived is not None, message

        triggered, message = fc_svc.trigger_compaction(task["id"])
        assert triggered is not None, message
        assert store.handovers(task_no="FILE-9001")[0]["交接状态"] == "已领取"

        cancelled, message = fc_svc.cancel_compaction(task["id"])
        assert cancelled is not None, message
        assert cancelled["status"] == "执行失败"
        assert store.handovers(task_no="FILE-9001")[0]["交接状态"] == "待领取"
        assert store.compaction_runs(task_no="FILE-9001")[-1]["状态"] == "已取消"
