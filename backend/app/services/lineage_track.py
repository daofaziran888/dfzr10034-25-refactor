"""血缘追踪领域服务：端到端数据血缘关系的自动推导、影响层级计算和变更通知。

Pipeline 7: Lineage tracking → impact analysis → governance scoring

关系登记与关系修订共用 `lineage_events` 采集器：关系行只保存当前已提交值，
每次写入的前后值进入只追加的事件账本，查询详情时再合并回放。
"""
from __future__ import annotations

import time as _time
from typing import Any

from app.services.base import BaseModuleService
from app.services.lineage_events import lineage_recorder
from app.store import store


class LineageTrackService(BaseModuleService):
    MODULE = "lineage_track"
    ENTITY = "血缘关系"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["关系编号", "上游资产", "下游资产", "关联字段", "转换逻辑", "影响层级", "自动推导", "最后更新"]
    REQUIRED_FIELDS = ["关系编号", "上游资产", "下游资产"]
    STATUS_ORDER = ["自动生成", "已确认", "待验证", "已废弃"]
    ACTION_RULES = {"确认关系": "已确认", "提交验证": "待验证", "废弃关系": "已废弃"}
    NEGATIVE_ACTIONS = ["废弃关系"]

    def derive_lineage(self, upstream_asset: str, downstream_asset: str, join_fields: str,
                       transform_logic: str, auto_derived: bool = True,
                       job_code: str = "") -> dict[str, Any]:
        """自动推导血缘关系并触发影响分析和治理评分（Pipeline 7 入口）。

        完全相同的重复提交只保留一条关系、一条登记事件；返回结构与首次一致。
        """
        recorded = lineage_recorder.register_relation(
            upstream_asset=upstream_asset,
            downstream_asset=downstream_asset,
            join_fields=join_fields,
            transform_logic=transform_logic,
            auto_derived=auto_derived,
            impact_level=self._compute_impact_level(upstream_asset),
            job_code=job_code,
            source="lineage_track",
        )
        lineage = recorded["lineage"]
        if recorded["duplicate"]:
            return {
                "lineage": lineage,
                "impact_analysis": None,
                "governance_scoring": None,
                "event": None,
                "duplicate": True,
                "pipeline": "lineage_track->impact_analysis->govern_score",
            }

        # Pipeline 7: 触发影响分析
        impact = self._trigger_impact_analysis(lineage)
        # Pipeline 7: 触发治理评分
        score = self._trigger_governance_scoring(lineage)

        self.emit_event("lineage_derived", {
            "lineage_id": lineage["id"],
            "upstream": upstream_asset,
            "downstream": downstream_asset,
            "impact_report_id": impact.get("report_id") if impact else None,
            "event_id": recorded["event"]["event_id"] if recorded["event"] else None,
        })

        return {
            "lineage": lineage,
            "impact_analysis": impact,
            "governance_scoring": score,
            "event": recorded["event"],
            "duplicate": False,
            "pipeline": "lineage_track->impact_analysis->govern_score",
        }

    def revise_lineage(self, lineage_id: int, *,
                       join_fields: str | None = None,
                       transform_logic: str | None = None,
                       job_code: str = "",
                       acknowledge: bool = True) -> dict[str, Any] | None:
        """修订血缘关系（换连接键/转换逻辑），逐字段保留本次写入的前后值。"""
        return lineage_recorder.revise_relation(
            lineage_id,
            join_fields=join_fields,
            transform_logic=transform_logic,
            job_code=job_code,
            source="lineage_track",
            acknowledge=acknowledge,
        )

    def get_event_history(self, lineage_id: int) -> list[dict[str, Any]]:
        """按确认次序回放同一血缘的每一次变动，区分不同作业的加工。"""
        return lineage_recorder.list_events(lineage_id=lineage_id)

    def get_relation_detail(self, lineage_id: int) -> dict[str, Any] | None:
        """关系详情：当前已提交值 + 逐笔变动历史（原有字段、归属与次序）。"""
        lineage = store.find(self.MODULE, lineage_id)
        if lineage is None:
            return None
        history = lineage_recorder.list_events(lineage_id=lineage_id)
        return {
            "lineage": lineage,
            "current": {
                "关联字段": lineage.get("关联字段", ""),
                "转换逻辑": lineage.get("转换逻辑", ""),
                "最后更新": lineage.get("最后更新", ""),
            },
            "history": history,
            "history_count": len(history),
        }

    def recompute_manifest(self, lineage_id: int | None = None,
                           asset_code: str | None = None,
                           consumer: str | None = None) -> list[dict[str, Any]]:
        """由已采集事件生成下游复算清单，一笔变动一条。"""
        return lineage_recorder.build_recompute_manifest(
            lineage_id=lineage_id, asset_code=asset_code, consumer=consumer)

    def _compute_impact_level(self, asset_code: str) -> int:
        """计算资产的影响层级（基于下游链长度）。"""
        downstream = self.get_downstream_chain(asset_code)
        return len(downstream) + 1

    def _trigger_impact_analysis(self, lineage: dict[str, Any]) -> dict[str, Any]:
        """触发影响分析报告生成。"""
        report_id = store.next_id("impact_analysis")
        report = {
            "id": report_id,
            "报告编号": f"IMPA-LINE-{report_id:04d}",
            "变更资产": lineage["上游资产"],
            "影响下游": lineage["下游资产"],
            "影响类型": "血缘变更",
            "严重级别": "需关注" if int(lineage.get("影响层级", "1")) >= 3 else "一般",
            "影响行数": "0",
            "补救建议": f"上游{lineage['上游资产']}的变更将通过关联字段{lineage['关联字段']}影响下游{lineage['下游资产']}",
            "生成时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "已生成",
            "pending": True,
            "abnormal": False,
            "lineage_id": lineage["id"],
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows("impact_analysis").append(report)
        return {"report_id": report_id}

    def _trigger_governance_scoring(self, lineage: dict[str, Any]) -> dict[str, Any]:
        """触发治理评分计算。"""
        score_id = store.next_id("govern_score")
        score = {
            "id": score_id,
            "任务编号": f"GOVE-LINE-{score_id:04d}",
            "评估对象": lineage["下游资产"],
            "评分维度": "血缘完备性",
            "当前分数": "85",
            "改进基线": "确认血缘并填写转换逻辑",
            "评估周期": "按需",
            "预警级别": "正常",
            "整改状态": "待评估",
            "status": "待评估",
            "pending": True,
            "abnormal": False,
            "lineage_id": lineage["id"],
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows("govern_score").append(score)
        return {"score_id": score_id, "dimension": "血缘完备性"}

    def get_upstream_assets(self, asset_code: str) -> list[str]:
        """获取上游资产列表。"""
        upstream = []
        for row in store.rows(self.MODULE):
            if row.get("下游资产") == asset_code:
                us = row.get("上游资产", "")
                if us and us not in upstream:
                    upstream.append(us)
        return upstream

    def get_downstream_chain(self, asset_code: str) -> list[str]:
        """获取下游资产链（递归）。"""
        visited = set()
        chain = []

        def dfs(current):
            if current in visited:
                return
            visited.add(current)
            for row in store.rows(self.MODULE):
                if row.get("上游资产") == current:
                    ds = row.get("下游资产", "")
                    if ds and ds not in visited:
                        chain.append(ds)
                        dfs(ds)

        dfs(asset_code)
        return chain

    def get_full_lineage(self, asset_code: str) -> dict[str, Any]:
        """获取资产的完整血缘关系。"""
        return {
            "asset": asset_code,
            "upstream": self.get_upstream_assets(asset_code),
            "downstream": self.get_downstream_chain(asset_code),
            "direct_relations": [
                {
                    "relation_id": row["关系编号"],
                    "upstream": row["上游资产"],
                    "downstream": row["下游资产"],
                    "join_fields": row["关联字段"],
                    "transform": row["转换逻辑"],
                }
                for row in store.rows(self.MODULE)
                if row.get("上游资产") == asset_code or row.get("下游资产") == asset_code
            ],
        }
