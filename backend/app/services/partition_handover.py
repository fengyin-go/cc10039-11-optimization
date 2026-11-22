"""分区粒度与日期范围工具：归档交接、合并输入领取和覆盖核对共用一套口径。

这里只放纯函数和只读汇总，不承担状态流转；状态流转仍留在各领域服务里。
"""
from __future__ import annotations

import calendar
import datetime
import re
from typing import Any, Iterable

# 分区粒度别名统一归一到“日 / 月 / 小时 / 年”
_GRANULARITY_ALIASES: dict[str, str] = {
    "日": "日",
    "天": "日",
    "按日": "日",
    "按天": "日",
    "日分区": "日",
    "day": "日",
    "daily": "日",
    "d": "日",
    "1d": "日",
    "月": "月",
    "按月": "月",
    "月分区": "月",
    "month": "月",
    "monthly": "月",
    "m": "月",
    "1m": "月",
    "小时": "小时",
    "时": "小时",
    "按小时": "小时",
    "hour": "小时",
    "hourly": "小时",
    "h": "小时",
    "1h": "小时",
    "年": "年",
    "按年": "年",
    "年分区": "年",
    "year": "年",
    "yearly": "年",
    "y": "年",
    "1y": "年",
}

_GRANULARITY_SPLIT = re.compile(r"[,，/、;；+|\s]+")

_DATE_TOKEN = re.compile(
    r"(?P<y1>\d{4})[-/.](?P<m1>\d{1,2})[-/.](?P<d1>\d{1,2})"
    r"|(?P<ymd>\d{8})"
    r"|(?P<y2>\d{4})年(?P<m2>\d{1,2})月(?:(?P<d2>\d{1,2})日?)?"
    r"|(?P<y3>\d{4})[-/.](?P<m3>\d{1,2})"
)


def _canonical_granularity(token: str) -> str:
    value = token.strip().casefold()
    if not value:
        return ""
    if value in _GRANULARITY_ALIASES:
        return _GRANULARITY_ALIASES[value]
    # 兼容“日分区策略”这类带修饰的写法
    if "小时" in value or "hour" in value:
        return "小时"
    if "月" in value or "month" in value:
        return "月"
    if "日" in value or "天" in value or "day" in value:
        return "日"
    if "年" in value or "year" in value:
        return "年"
    return ""


def granularities_in(value: Any) -> list[str]:
    """拆出一段文本里声明的全部粒度（去重、保序）。

    已知口径归一成“日 / 月 / 小时 / 年”；无法归一的词原样保留，
    这样自增字段（例如自定义周期）仍能作为单一粒度参与调度。
    """
    if value is None:
        return []
    tokens = [token for token in _GRANULARITY_SPLIT.split(str(value)) if token.strip()]
    found: list[str] = []
    for token in tokens:
        canonical = _canonical_granularity(token) or token.strip()
        if canonical and canonical not in found:
            found.append(canonical)
    return found


def normalize_granularity(value: Any) -> str:
    """单套粒度归一：原文即唯一粒度时返回原文；声明多套时返回空串，由调用方拒绝。"""
    found = granularities_in(value)
    if len(found) == 1:
        return found[0]
    return ""


def _month_end(year: int, month: int) -> datetime.date:
    return datetime.date(year, month, calendar.monthrange(year, month)[1])


def _parse_date_tokens(value: Any) -> list[dict[str, Any]]:
    text = str(value or "")
    tokens: list[dict[str, Any]] = []
    for match in _DATE_TOKEN.finditer(text):
        try:
            if match.group("y1"):
                day = datetime.date(
                    int(match.group("y1")), int(match.group("m1")), int(match.group("d1")))
                tokens.append({"kind": "day", "date": day})
            elif match.group("ymd"):
                raw = match.group("ymd")
                day = datetime.date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
                tokens.append({"kind": "day", "date": day})
            elif match.group("y2"):
                year = int(match.group("y2"))
                month = int(match.group("m2"))
                if match.group("d2"):
                    day = datetime.date(year, month, int(match.group("d2")))
                    tokens.append({"kind": "day", "date": day})
                else:
                    tokens.append({"kind": "month", "year": year, "month": month})
            elif match.group("y3"):
                tokens.append({
                    "kind": "month",
                    "year": int(match.group("y3")),
                    "month": int(match.group("m3")),
                })
        except ValueError:
            # 非法日期不参与范围推断，交给调用方按“缺范围”处理
            continue
    return tokens


def parse_date_range(value: Any) -> tuple[datetime.date, datetime.date] | None:
    """把“2026-01-01~2026-01-31”“2026-01”等文本解析成闭区间日期范围。"""
    if isinstance(value, dict):
        value = value.get("数据范围") or value.get("range") or ""
    tokens = _parse_date_tokens(value)
    if not tokens:
        return None

    def start_of(token: dict[str, Any]) -> datetime.date:
        if token["kind"] == "day":
            return token["date"]
        return datetime.date(token["year"], token["month"], 1)

    def end_of(token: dict[str, Any]) -> datetime.date:
        if token["kind"] == "day":
            return token["date"]
        return _month_end(token["year"], token["month"])

    start = start_of(tokens[0])
    end = end_of(tokens[-1])
    if start > end:
        start, end = end, start
    return start, end


def parse_explicit_range(record: dict[str, Any]) -> tuple[datetime.date, datetime.date] | None:
    """优先读“数据范围”，其次读“数据范围起 / 数据范围止”。"""
    parsed = parse_date_range(record.get("数据范围"))
    if parsed:
        return parsed
    start = parse_date_range(record.get("数据范围起"))
    end = parse_date_range(record.get("数据范围止"))
    if start and end:
        return start[0], end[1]
    return None


def format_date_range(value: tuple[datetime.date, datetime.date] | None) -> str:
    if not value:
        return ""
    return f"{value[0].isoformat()}~{value[1].isoformat()}"


def range_payload(value: tuple[datetime.date, datetime.date] | None) -> dict[str, str]:
    if not value:
        return {"数据范围": "", "数据范围起": "", "数据范围止": ""}
    return {
        "数据范围": format_date_range(value),
        "数据范围起": value[0].isoformat(),
        "数据范围止": value[1].isoformat(),
    }


def ranges_overlap(
    left: tuple[datetime.date, datetime.date] | None,
    right: tuple[datetime.date, datetime.date] | None,
) -> bool:
    if not left or not right:
        return False
    return left[0] <= right[1] and right[0] <= left[1]


def range_contains(
    outer: tuple[datetime.date, datetime.date] | None,
    inner: tuple[datetime.date, datetime.date] | None,
) -> bool:
    if not outer or not inner:
        return False
    return outer[0] <= inner[0] and inner[1] <= outer[1]


def intersect_range(
    left: tuple[datetime.date, datetime.date] | None,
    right: tuple[datetime.date, datetime.date] | None,
) -> tuple[datetime.date, datetime.date] | None:
    if not ranges_overlap(left, right) or not left or not right:
        return None
    return max(left[0], right[0]), min(left[1], right[1])


def subtract_range(
    base: tuple[datetime.date, datetime.date] | None,
    cutters: Iterable[tuple[datetime.date, datetime.date] | None],
) -> list[tuple[datetime.date, datetime.date]]:
    """从 base 中扣除若干保留区间，返回剩余可执行区间。"""
    if not base:
        return []
    remaining: list[tuple[datetime.date, datetime.date]] = [base]
    for cutter in sorted((c for c in cutters if c), key=lambda item: item[0]):
        next_remaining: list[tuple[datetime.date, datetime.date]] = []
        for start, end in remaining:
            overlap = intersect_range((start, end), cutter)
            if not overlap:
                next_remaining.append((start, end))
                continue
            if start < overlap[0]:
                next_remaining.append((start, overlap[0] - datetime.timedelta(days=1)))
            if overlap[1] < end:
                next_remaining.append((overlap[1] + datetime.timedelta(days=1), end))
        remaining = next_remaining
    return [(start, end) for start, end in remaining if start <= end]


def manifest_range(manifest: dict[str, Any]) -> tuple[datetime.date, datetime.date] | None:
    return parse_explicit_range(manifest)


def run_range(run: dict[str, Any]) -> tuple[datetime.date, datetime.date] | None:
    return parse_explicit_range(run)


def coverage_items(table_name: str) -> dict[str, Any]:
    """汇总一张表的交接清单、调度记录和日期覆盖，供跨模块核对使用。"""
    # 延迟导入，避免工具模块和仓库初始化互相牵挂
    from app.store import store

    manifests = store.handovers(table_name=table_name)
    runs = store.compaction_runs(table_name=table_name)
    policies = store.find_by_field("partition_policy", "目标湖表", table_name)

    items: list[dict[str, Any]] = []
    for manifest in manifests:
        items.append({
            "来源": "归档交接分区清单",
            "交接清单ID": manifest.get("id"),
            "交接清单编号": manifest.get("交接清单编号", ""),
            "任务编号": manifest.get("任务编号", ""),
            "任务ID": manifest.get("待调度任务ID"),
            "策略版本": manifest.get("策略版本", ""),
            "分区粒度": manifest.get("分区粒度", ""),
            "数据范围": manifest.get("数据范围", ""),
            "状态": manifest.get("交接状态", ""),
            "日期范围": manifest_range(manifest),
        })
    for run in runs:
        items.append({
            "来源": "合并调度记录",
            "交接清单编号": run.get("交接清单编号", ""),
            "调度记录编号": run.get("调度记录编号", ""),
            "任务编号": run.get("任务编号", ""),
            "任务ID": run.get("任务ID"),
            "策略版本": run.get("策略版本", ""),
            "分区粒度": run.get("分区粒度", ""),
            "数据范围": run.get("数据范围", ""),
            "状态": run.get("状态", ""),
            "日期范围": run_range(run),
        })

    conflicts: list[dict[str, Any]] = []
    same_input: list[dict[str, Any]] = []
    for index, left in enumerate(items):
        for right in items[index + 1:]:
            if not ranges_overlap(left["日期范围"], right["日期范围"]):
                continue
            # 同一任务的交接清单和它的调度记录是同一份输入的两个视角，不算重复
            if (
                left.get("任务编号") == right.get("任务编号")
                and {left["来源"], right["来源"]} == {"归档交接分区清单", "合并调度记录"}
                and left.get("交接清单编号")
                and left.get("交接清单编号") == right.get("交接清单编号")
            ):
                continue
            pair = {
                "日期范围": format_date_range(
                    intersect_range(left["日期范围"], right["日期范围"])),
                "左": {key: value for key, value in left.items() if key != "日期范围"},
                "右": {key: value for key, value in right.items() if key != "日期范围"},
            }
            same_input.append(pair)
            if left.get("分区粒度") != right.get("分区粒度"):
                pair["类型"] = "不同分区粒度命中同一输入"
                conflicts.append(pair)

    # 生效策略声明的范围若仍覆盖已交接日期，也要在核对结果里点出来
    overlap_with_active_policy: list[dict[str, Any]] = []
    for policy in policies:
        if policy.get("status") != "已生效":
            continue
        policy_range = parse_explicit_range(policy)
        overlaps = [
            intersect_range(policy_range, parse_explicit_range(manifest))
            for manifest in manifests
        ]
        for overlap in overlaps:
            if overlap:
                overlap_with_active_policy.append({
                    "策略版本": policy.get("策略版本", ""),
                    "分区粒度": normalize_granularity(policy.get("分区粒度", "")),
                    "重叠日期范围": format_date_range(overlap),
                    "说明": "生效策略范围仍覆盖旧策略已交接日期，需按有效范围执行",
                })
                break

    coverage: dict[str, list[str]] = {}
    for item in items:
        granularity = item.get("分区粒度") or "未标注"
        coverage.setdefault(granularity, [])
        text = item.get("数据范围", "")
        if text and text not in coverage[granularity]:
            coverage[granularity].append(text)

    handover_ranges = [
        item["数据范围"] for item in items
        if item["来源"] == "归档交接分区清单" and item.get("数据范围")
    ]
    return {
        "table_name": table_name,
        "items": [
            {key: value for key, value in item.items() if key != "日期范围"}
            for item in items
        ],
        "coverage": coverage,
        "handover_date_ranges": sorted(set(handover_ranges)),
        "same_input_pairs": same_input,
        "conflicts": conflicts,
        "has_conflict": bool(conflicts),
        "overlap_with_active_policy": overlap_with_active_policy,
        "policies": [
            {
                "id": policy.get("id"),
                "策略名称": policy.get("策略名称", ""),
                "策略版本": policy.get("策略版本", ""),
                "分区粒度": policy.get("分区粒度", ""),
                "status": policy.get("status", ""),
                "数据范围": policy.get("数据范围", ""),
                "交接日期范围": policy.get("交接日期范围", ""),
            }
            for policy in policies
        ],
    }
