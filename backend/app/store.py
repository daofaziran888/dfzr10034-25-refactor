"""内存数据仓库：支持跨模块读写、快照、变更日志和事务性操作的增强存储层。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
import time as _time
from collections import defaultdict
from typing import Any

from app.seed import SEED_ROWS


class Store:
    """增强版存储层，支持跨模块读写、变更日志和快照。"""

    def __init__(self) -> None:
        # 采集侧并发读写的临界区；账本本身只追加，游标按消费方分别推进。
        self._ledger_lock = threading.RLock()
        self._seed()

    def _seed(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._change_log: list[dict[str, Any]] = []
        self._snapshots: dict[str, list[dict[str, Any]]] = {}
        self._pipeline_state: dict[str, dict[str, Any]] = {}
        # 血缘变动事件账本：只追加、按血缘定序；消费者位点按消费方分别保存。
        self._lineage_events: list[dict[str, Any]] = []
        self._lineage_cursors: dict[str, int] = {}
        # 已写入血缘关系但尚未确认（未拿到确认回执）的变动暂存区。
        self._lineage_stages: dict[str, dict[str, Any]] = {}

    def reset(self) -> None:
        """清空所有数据并重新种子化（测试用）。"""
        self._seed()

    # ── 基础操作 ──────────────────────────────────────────────

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def find_by_field(self, module: str, field: str, value: str) -> list[dict[str, Any]]:
        """按字段值查找记录列表。"""
        results: list[dict[str, Any]] = []
        for row in self.rows(module):
            if str(row.get(field, "")) == value:
                results.append(row)
        return results

    def next_id(self, module: str) -> int:
        rows = self.rows(module)
        return max((int(row.get("id", 0)) for row in rows), default=0) + 1

    def upsert(self, module: str, entry: dict[str, Any]) -> dict[str, Any]:
        """创建或更新记录。"""
        entry_id = entry.get("id")
        existing = self.find(module, entry_id) if entry_id else None
        if existing:
            for key, val in entry.items():
                existing[key] = val
            return existing
        else:
            if not entry_id:
                entry["id"] = self.next_id(module)
            self.rows(module).append(entry)
            return entry

    def update_status(self, module: str, entry_id: int, status: str, extra: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """更新记录状态。"""
        entry = self.find(module, entry_id)
        if entry is None:
            return None
        entry["status"] = status
        if extra:
            entry.update(extra)
        return entry

    # ── 变更日志 ──────────────────────────────────────────────

    def log_change(
        self, module: str, entry_id: int, change_type: str,
        before: Any = None, after: Any = None, source: str = "", scope: str = "",
        target: str | None = None, **extra: Any
    ) -> dict[str, Any]:
        """记录一次变更到 change_capture 表。

        target 用于把变更挂到业务资产编号上；extra 可补记 event_id / job_code /
        seq 等追踪字段，血缘变动事件靠它们与事件账本对齐。
        """
        log_entry = {
            "id": self.next_id("change_capture"),
            "记录编号": f"CHAN-{self.next_id('change_capture'):04d}",
            "目标资产": target or f"{module}#{entry_id}",
            "变更类型": change_type,
            "变更前值": str(before) if before is not None else "",
            "变更后值": str(after) if after is not None else "",
            "变更来源": source,
            "变更时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
            "影响范围": scope,
            "status": "已捕获",
            "pending": True,
            "abnormal": False,
        }
        for key, value in extra.items():
            log_entry.setdefault(key, value)
        self.rows("change_capture").append(log_entry)
        self._change_log.append(log_entry)
        return log_entry

    def get_change_log(self, module: str | None = None, entry_id: int | None = None) -> list[dict[str, Any]]:
        """查询变更日志，可按模块和条目过滤。"""
        results = list(self._change_log)
        if module:
            results = [r for r in results if r.get("目标资产", "").startswith(f"{module}#")]
        if entry_id is not None:
            target = f"#{entry_id}"
            results = [r for r in results if str(r.get("目标资产", "")).endswith(target)]
        return results

    # ── 快照 ──────────────────────────────────────────────────

    def create_snapshot(self, snapshot_id: str, modules: list[str] | None = None) -> dict[str, Any]:
        """创建指定模块的快照。"""
        target_modules = modules or self.module_names()
        snap = {}
        for mod in target_modules:
            snap[mod] = copy.deepcopy(self.rows(mod))
        self._snapshots[snapshot_id] = snap

        # 记录到 version_history
        for mod in target_modules:
            version_entry = {
                "id": self.next_id("version_history"),
                "快照编号": f"{snapshot_id}-{mod}",
                "目标资产": mod,
                "版本标签": snapshot_id,
                "Schema摘要": json.dumps(list(snap[mod][0].keys()) if snap[mod] else []),
                "行数记录": str(len(snap[mod])),
                "创建原因": "自动快照",
                "差异字段": "",
                "快照时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "历史版本",
                "pending": False,
                "abnormal": False,
            }
            self.rows("version_history").append(version_entry)

        return {"snapshot_id": snapshot_id, "modules": target_modules, "rows": sum(len(v) for v in snap.values())}

    def get_snapshot(self, snapshot_id: str) -> dict[str, list[dict[str, Any]]] | None:
        return self._snapshots.get(snapshot_id)

    def diff_snapshots(self, snap_a: str, snap_b: str, module: str | None = None) -> list[dict[str, Any]]:
        """比较两个快照的差异，生成差异报告。"""
        a = self._snapshots.get(snap_a, {})
        b = self._snapshots.get(snap_b, {})
        diffs: list[dict[str, Any]] = []
        modules_to_check = [module] if module else list(set(a) | set(b))
        for mod in modules_to_check:
            rows_a = a.get(mod, [])
            rows_b = b.get(mod, [])
            ids_a = {r["id"] for r in rows_a}
            ids_b = {r["id"] for r in rows_b}

            # 新增
            for rid in ids_b - ids_a:
                row_b = next(r for r in rows_b if r["id"] == rid)
                diffs.append({
                    "比较对象": f"{mod}#{rid}",
                    "比较维度": "新增记录",
                    "差异字段": "记录",
                    "源值": "-",
                    "目标值": str(row_b.get("id")),
                    "差异等级": "新增",
                })
            # 删除
            for rid in ids_a - ids_b:
                diffs.append({
                    "比较对象": f"{mod}#{rid}",
                    "比较维度": "删除记录",
                    "差异字段": "记录",
                    "源值": str(rid),
                    "目标值": "-",
                    "差异等级": "删除",
                })
            # 修改
            for rid in ids_a & ids_b:
                row_a = next(r for r in rows_a if r["id"] == rid)
                row_b = next(r for r in rows_b if r["id"] == rid)
                for key in set(row_a) | set(row_b):
                    va = str(row_a.get(key, ""))
                    vb = str(row_b.get(key, ""))
                    if va != vb:
                        diffs.append({
                            "比较对象": f"{mod}#{rid}",
                            "比较维度": key,
                            "差异字段": key,
                            "源值": va,
                            "目标值": vb,
                            "差异等级": "修改",
                        })

        # 将差异写入 diff_report 表
        for d in diffs:
            report = {
                "id": self.next_id("diff_report"),
                "记录编号": f"DIFF-{self.next_id('diff_report'):04d}",
                "比较对象": d["比较对象"],
                "比较维度": d["比较维度"],
                "差异字段": d["差异字段"],
                "源值": d["源值"],
                "目标值": d["目标值"],
                "差异等级": d["差异等级"],
                "报告时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "已生成",
                "pending": False,
                "abnormal": False,
            }
            self.rows("diff_report").append(report)

        return diffs

    # ── 跨模块操作 ────────────────────────────────────────────

    def cross_module_link(
        self, source_module: str, source_id: int, target_module: str, target_id: int,
        relation_type: str = "关联"
    ) -> dict[str, Any]:
        """在两个模块之间建立关联。"""
        link = {
            "source_module": source_module,
            "source_id": source_id,
            "target_module": target_module,
            "target_id": target_id,
            "relation_type": relation_type,
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        return link

    def read_cross_module(
        self, source_module: str, source_id: int, target_module: str
    ) -> list[dict[str, Any]]:
        """读取与源记录关联的目标模块记录。"""
        source = self.find(source_module, source_id)
        if not source:
            return []
        # 通过引用字段查找
        results = []
        for row in self.rows(target_module):
            # 检查记录是否引用了源模块的 ID
            for key, val in row.items():
                if isinstance(val, str) and f"{source_module}#{source_id}" in val:
                    results.append(row)
                    break
        return results

    # ── 血缘变动事件账本 ──────────────────────────────────────

    def append_lineage_event(self, event: dict[str, Any]) -> dict[str, Any]:
        """追加一条已确认的血缘变动事件，返回带全局序号和幂等键的副本。

        账本只追加不覆盖：同一张表连续多次变动各留一条，下游复算据此区分两次加工。
        """
        with self._ledger_lock:
            seq = len(self._lineage_events) + 1
            stored = dict(event)
            stored["seq"] = seq
            stored.setdefault("event_id", f"LEV-{seq:06d}")
            stored.setdefault("recorded_at", _time.strftime("%Y-%m-%d %H:%M:%S"))
            self._lineage_events.append(stored)
            return dict(stored)

    def lineage_events(
        self,
        *,
        lineage_id: int | None = None,
        asset_code: str | None = None,
        after_seq: int = 0,
    ) -> list[dict[str, Any]]:
        """读取账本中已确认的事件，保持确认次序（seq 升序）。"""
        with self._ledger_lock:
            events = list(self._lineage_events)
        if lineage_id is not None:
            events = [e for e in events if e.get("lineage_id") == lineage_id]
        if asset_code is not None:
            events = [
                e for e in events
                if asset_code in (e.get("target_asset"), e.get("upstream_asset"),
                                  e.get("downstream_asset"))
            ]
        return [e for e in events if int(e.get("seq", 0)) > after_seq]

    def find_lineage_event(self, event_id: str) -> dict[str, Any] | None:
        with self._ledger_lock:
            for event in self._lineage_events:
                if event.get("event_id") == event_id:
                    return dict(event)
        return None

    def find_lineage_event_by_key(self, idempotency_key: str) -> dict[str, Any] | None:
        """按幂等键查找已采集的变动；重复提交据此只生效一次。"""
        if not idempotency_key:
            return None
        with self._ledger_lock:
            for event in self._lineage_events:
                if event.get("idempotency_key") == idempotency_key:
                    return dict(event)
        return None

    def update_lineage_event(self, event_id: str, **updates: Any) -> dict[str, Any] | None:
        """更新账本事件（例如补记确认回执），返回更新后的副本。"""
        with self._ledger_lock:
            for event in self._lineage_events:
                if event.get("event_id") == event_id:
                    event.update(updates)
                    return dict(event)
        return None

    def lineage_cursor(self, consumer: str) -> int:
        """返回消费方已经交付到的账本位点。"""
        with self._ledger_lock:
            return int(self._lineage_cursors.get(consumer, 0))

    def advance_lineage_cursor(self, consumer: str, seq: int) -> int:
        """把消费方位点推进到指定事件；位点只前进不后退。"""
        with self._ledger_lock:
            current = int(self._lineage_cursors.get(consumer, 0))
            if seq > current:
                self._lineage_cursors[consumer] = seq
                return seq
            return current

    def reset_lineage_cursor(self, consumer: str) -> None:
        """让消费方回到账本起点，用于从实际确认位置补采。"""
        with self._ledger_lock:
            self._lineage_cursors[consumer] = 0

    @property
    def lineage_ledger_size(self) -> int:
        with self._ledger_lock:
            return len(self._lineage_events)

    # ── 管线状态 ──────────────────────────────────────────────

    def set_pipeline_state(self, pipeline: str, state: dict[str, Any]) -> None:
        self._pipeline_state[pipeline] = state

    def get_pipeline_state(self, pipeline: str) -> dict[str, Any] | None:
        return self._pipeline_state.get(pipeline)

    # ── 行数据操作 ────────────────────────────────────────────

    def count_by_status(self, module: str) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for row in self.rows(module):
            counts[row.get("status", "")] += 1
        return dict(counts)

    def count_by_field(self, module: str, field: str) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for row in self.rows(module):
            val = str(row.get(field, ""))
            if val:
                counts[val] += 1
        return dict(counts)

    def aggregate(self, module: str, field: str, operation: str = "count") -> Any:
        """对指定字段进行聚合计算。"""
        rows = self.rows(module)
        values = [row.get(field) for row in rows if row.get(field) is not None]
        if operation == "count":
            return len(values)
        elif operation == "sum":
            try:
                return sum(float(v) for v in values if v)
            except (ValueError, TypeError):
                return 0
        elif operation == "avg":
            try:
                nums = [float(v) for v in values if v]
                return sum(nums) / len(nums) if nums else 0
            except (ValueError, TypeError):
                return 0
        elif operation == "max":
            try:
                return max((float(v) for v in values if v), default=0)
            except (ValueError, TypeError):
                return 0
        elif operation == "min":
            try:
                return min((float(v) for v in values if v), default=0)
            except (ValueError, TypeError):
                return 0
        return len(values)

    # ── 运营概览 ──────────────────────────────────────────────

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()