"""变更捕获领域服务：Schema和元数据的变更捕获、版本对比和差异通知。

Pipeline 6: Change capture → diff reporting → version history

采集与交付统一走 `lineage_events`：
- 捕获侧只负责「确认发生了什么」，事件先进账本再触发差异/版本；
- 重复提交由采集器幂等键去重，同一来源的同一笔只生效一次；
- 交付位点由消费方自己持有，可从实际确认位置补采未交付的变动。
"""
from __future__ import annotations

import time as _time
from typing import Any

from app.services.base import BaseModuleService
from app.services.lineage_events import lineage_recorder
from app.store import store


class ChangeCaptureService(BaseModuleService):
    MODULE = "change_capture"
    ENTITY = "变更记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "目标资产", "变更类型", "变更前值", "变更后值", "变更来源", "变更时间", "影响范围"]
    REQUIRED_FIELDS = ["记录编号", "目标资产", "变更类型"]
    STATUS_ORDER = ["已捕获", "已对比", "已通知", "已确认"]
    ACTION_RULES = {"启动对比": "已对比", "发送通知": "已通知", "确认变更": "已确认"}
    NEGATIVE_ACTIONS = []

    def capture_change(self, asset_code: str, change_type: str, before: Any, after: Any,
                       source: str = "schema_evolution", job_code: str = "") -> dict[str, Any]:
        """捕获一次变更并自动触发差异对比和版本快照（Pipeline 6 入口）。

        采集结果写入事件账本并立即确认；重复提交（同资产/类型/来源/作业/前后值）
        只返回首笔结果，不再生成第二条变更或差异。
        """
        recorded = lineage_recorder.record_asset_change(
            asset_code=asset_code,
            change_type=change_type,
            before=before,
            after=after,
            source=source,
            job_code=job_code,
        )
        change = recorded["change"]
        event = recorded["event"]
        if recorded["duplicate"]:
            return {
                "change": change,
                "diff_report": {"diff_count": 0, "duplicate": True},
                "version_snapshot": {"created": False, "duplicate": True},
                "affected_downstream": lineage_recorder.downstream_assets(asset_code),
                "event": event,
                "duplicate": True,
                "pipeline": "change_capture->diff_report->version_history",
            }

        # Pipeline 6: 自动触发差异报告和版本快照
        diff_result = self._generate_diff_report(change)
        version_snap = self._create_version_snapshot(change)

        # 记录血缘中受影响的下游资产
        affected_downstream = lineage_recorder.downstream_assets(asset_code)
        event = store.update_lineage_event(
            event["event_id"],
            affected_downstream=list(affected_downstream),
        ) or event

        self.emit_event("change_captured", {
            "change_id": change["id"],
            "event_id": event["event_id"],
            "asset_code": asset_code,
            "change_type": change_type,
            "downstream_count": len(affected_downstream),
        })

        return {
            "change": change,
            "diff_report": diff_result,
            "version_snapshot": version_snap,
            "affected_downstream": affected_downstream,
            "event": event,
            "duplicate": False,
            "pipeline": "change_capture->diff_report->version_history",
        }

    def capture_lineage_change(self, lineage_id: int, *,
                               join_fields: str | None = None,
                               transform_logic: str | None = None,
                               job_code: str = "",
                               acknowledge: bool = True) -> dict[str, Any] | None:
        """从捕获侧登记一次血缘修订，返回本次事件的前后值。

        acknowledge=False 对应确认回执丢失：关系值已提交、事件已落账但未确认，
        重试时用同一 job_code 与请求值即可补上确认而不产生第二笔。
        """
        result = lineage_recorder.revise_relation(
            lineage_id,
            join_fields=join_fields,
            transform_logic=transform_logic,
            job_code=job_code,
            source="change_capture",
            acknowledge=acknowledge,
        )
        if result is None:
            return None
        event = result.get("event")
        if event is not None:
            self.emit_event("lineage_change_captured", {
                "event_id": event["event_id"],
                "lineage_id": lineage_id,
                "job_code": job_code,
                "confirmed": bool(event.get("confirmed")),
            })
        return result

    def pending_changes(self, consumer: str = "change_capture") -> list[dict[str, Any]]:
        """采集侧尚未交付的已确认变动。"""
        return lineage_recorder.pending_deliveries(consumer)

    def backfill(self, consumer: str = "change_capture") -> dict[str, Any]:
        """从实际确认位置补采未交付的变动。"""
        return lineage_recorder.backfill(consumer)

    def capture_status(self, consumer: str = "change_capture") -> dict[str, Any]:
        """采集位点与待补采量，供下游复跑前核对。"""
        return lineage_recorder.capture_status(consumer)

    def get_events_by_asset(self, asset_code: str) -> list[dict[str, Any]]:
        """按资产读取全部变动事件（含每次的前后值）。"""
        return lineage_recorder.list_events(asset_code=asset_code)

    def _compute_impact_scope(self, asset_code: str) -> str:
        """计算影响范围。"""
        downstream = lineage_recorder.downstream_assets(asset_code)
        if downstream:
            return f"影响{len(downstream)}个下游资产: {', '.join(downstream)}"
        return "无下游依赖"

    def _generate_diff_report(self, change: dict[str, Any]) -> dict[str, Any]:
        """生成差异报告。"""
        diff_id = store.next_id("diff_report")
        diff = {
            "id": diff_id,
            "记录编号": f"DIFF-CHG-{diff_id:04d}",
            "比较对象": change.get("目标资产", ""),
            "比较维度": change.get("变更类型", ""),
            "差异字段": change.get("变更类型", ""),
            "源值": change.get("变更前值", ""),
            "目标值": change.get("变更后值", ""),
            "差异等级": "修改",
            "报告时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "已生成",
            "pending": False,
            "abnormal": False,
            "change_id": change["id"],
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows("diff_report").append(diff)
        return {"diff_id": diff_id, "diff_count": 1}

    def _create_version_snapshot(self, change: dict[str, Any]) -> dict[str, Any]:
        """创建版本快照记录变更历史。"""
        store.create_snapshot(f"change-{change['id']}", [change.get("目标资产", "unknown")])
        return {"snapshot_id": f"change-{change['id']}", "created": True}

    def compare_versions(self, asset_code: str, version_a: str, version_b: str) -> list[dict[str, Any]]:
        """比较资产的两个版本差异。"""
        diffs = store.diff_snapshots(version_a, version_b, asset_code)
        return diffs

    def get_changes_by_asset(self, asset_code: str) -> list[dict[str, Any]]:
        """获取资产的所有变更记录。"""
        return store.find_by_field(self.MODULE, "目标资产", asset_code)

    def get_pending_changes(self) -> list[dict[str, Any]]:
        """获取未确认的变更。"""
        return [
            r for r in store.rows(self.MODULE)
            if r.get("status") in ("已捕获", "已对比")
        ]
