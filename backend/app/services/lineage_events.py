"""血缘变动事件采集：关系登记与变更捕获共用的唯一写入与交付通道。

设计要点：
- 关系行只保留「当前已提交值」，历史前后值由只追加的事件账本保留，
  因此同一分钟内的两次变动不会互相覆盖成末次关系。
- 幂等键按「动作 + 血缘 + 作业 + 请求值」生成：同一作业重试同一笔只生效一次，
  不同作业对同一字段的有效修改则各有独立事件。
- 采集与交付分离：事件先落账（可处于未确认态），确认后才进入可交付队列；
  消费方按自己的位点补采，不依赖「事件时间」或「当前关系行」。
"""
from __future__ import annotations

import hashlib
import json
import threading
import time as _time
from typing import Any

from app.store import store

RELATION_KEY_FIELDS = ("上游资产", "下游资产", "关联字段", "转换逻辑")


def _text(value: Any) -> str:
    return str(value if value is not None else "").strip()


class LineageChangeRecorder:
    """血缘变动的唯一采集入口；两个领域服务都通过它写关系、记账和交付。"""

    def __init__(self) -> None:
        # 同一血缘的两次写入串行化，保证第二笔读到的 before 是第一笔的 after。
        self._lock = threading.RLock()

    # ── 幂等与查找 ────────────────────────────────────────────

    @staticmethod
    def idempotency_key(kind: str, *parts: Any) -> str:
        payload = json.dumps([_text(part) for part in parts], ensure_ascii=False)
        digest = hashlib.sha256(f"{kind}|{payload}".encode("utf-8")).hexdigest()
        return f"{kind}:{digest[:24]}"

    @staticmethod
    def find_relation(upstream_asset: str, downstream_asset: str,
                      join_fields: str, transform_logic: str) -> dict[str, Any] | None:
        expected = (upstream_asset, downstream_asset, join_fields, transform_logic)
        for row in store.rows("lineage_track"):
            if all(_text(row.get(field)) == _text(value)
                   for field, value in zip(RELATION_KEY_FIELDS, expected)):
                return row
        return None

    # ── 关系登记 ──────────────────────────────────────────────

    def register_relation(
        self,
        *,
        upstream_asset: str,
        downstream_asset: str,
        join_fields: str,
        transform_logic: str,
        auto_derived: bool = True,
        impact_level: int = 1,
        job_code: str = "",
        source: str = "lineage_track",
    ) -> dict[str, Any]:
        """登记关系；完全相同的重复提交只落一条关系和一条事件。"""
        with self._lock:
            existing = self.find_relation(
                upstream_asset, downstream_asset, join_fields, transform_logic)
            if existing is not None:
                return {
                    "lineage": existing,
                    "created": False,
                    "event": None,
                    "duplicate": True,
                }

            lineage_id = store.next_id("lineage_track")
            lineage = {
                "id": lineage_id,
                "关系编号": f"LINE-{lineage_id:04d}",
                "上游资产": upstream_asset,
                "下游资产": downstream_asset,
                "关联字段": join_fields,
                "转换逻辑": transform_logic,
                "影响层级": str(impact_level),
                "自动推导": "是" if auto_derived else "否",
                "最后更新": _time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "自动生成" if auto_derived else "已确认",
                "pending": not auto_derived,
                "abnormal": False,
                "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            store.rows("lineage_track").append(lineage)

            key = self.idempotency_key(
                "relation_registered", upstream_asset, downstream_asset,
                join_fields, transform_logic)
            event = self._append_event(
                kind="relation_registered",
                lineage=lineage,
                changes={
                    "上游资产": ("", upstream_asset),
                    "下游资产": ("", downstream_asset),
                    "关联字段": ("", join_fields),
                    "转换逻辑": ("", transform_logic),
                },
                field="关联字段",
                job_code=job_code,
                source=source,
                idempotency_key=key,
            )
            # 登记与关系写入同一次提交，视为已确认，立即可被消费。
            event = self.confirm_event(event["event_id"]) or event
            return {
                "lineage": lineage,
                "created": True,
                "event": event,
                "duplicate": False,
            }

    # ── 关系修订 ──────────────────────────────────────────────

    def revise_relation(
        self,
        lineage_id: int,
        *,
        join_fields: str | None = None,
        transform_logic: str | None = None,
        job_code: str = "",
        source: str = "change_capture",
        acknowledge: bool = True,
    ) -> dict[str, Any] | None:
        """修订已有关系，逐字段记录本次写入的前后值。

        acknowledge=False 模拟确认回执丢失：关系值已提交、事件已落账，
        但采集侧尚未确认，暂不进入交付队列。
        """
        with self._lock:
            lineage = store.find("lineage_track", lineage_id)
            if lineage is None:
                return None

            requested: dict[str, str] = {}
            if join_fields is not None:
                requested["关联字段"] = _text(join_fields)
            if transform_logic is not None:
                requested["转换逻辑"] = _text(transform_logic)

            key = self.idempotency_key(
                "relation_revised", lineage_id, job_code,
                ";".join(f"{field}={value}" for field, value in sorted(requested.items())),
            )
            previous_event = store.find_lineage_event_by_key(key)
            if previous_event is not None:
                # 同一作业的重试：不再改关系、不再记第二笔，只补确认。
                if acknowledge and not previous_event.get("confirmed"):
                    previous_event = self.confirm_event(previous_event["event_id"]) or previous_event
                return {
                    "lineage": lineage,
                    "event": previous_event,
                    "changed": False,
                    "duplicate": True,
                }

            changes: dict[str, tuple[str, str]] = {}
            for field, value in requested.items():
                current = _text(lineage.get(field))
                if current != value:
                    changes[field] = (current, value)

            if not changes:
                # 请求值与当前值一致且没有历史事件：幂等空操作。
                return {
                    "lineage": lineage,
                    "event": None,
                    "changed": False,
                    "duplicate": False,
                }

            for field, (_before, after) in changes.items():
                lineage[field] = after
            lineage["最后更新"] = _time.strftime("%Y-%m-%d %H:%M:%S")

            event = self._append_event(
                kind="relation_revised",
                lineage=lineage,
                changes=changes,
                field=next(iter(changes)),
                job_code=job_code,
                source=source,
                idempotency_key=key,
            )
            if acknowledge:
                event = self.confirm_event(event["event_id"]) or event
            return {
                "lineage": lineage,
                "event": event,
                "changed": True,
                "duplicate": False,
            }

    # ── 资产级变更捕获 ────────────────────────────────────────

    def record_asset_change(
        self,
        *,
        asset_code: str,
        change_type: str,
        before: Any,
        after: Any,
        source: str = "schema_evolution",
        job_code: str = "",
    ) -> dict[str, Any]:
        """采集一次资产级变更；同一来源/作业的重复提交只生效一次。"""
        with self._lock:
            key = self.idempotency_key(
                "asset_change", asset_code, change_type, source, job_code,
                before, after,
            )
            previous = store.find_lineage_event_by_key(key)
            if previous is not None:
                change = store.find("change_capture", int(previous.get("change_id", 0)))
                return {
                    "change": change,
                    "event": previous,
                    "duplicate": True,
                }

            downstream = self.downstream_assets(asset_code)
            change_id = store.next_id("change_capture")
            change = {
                "id": change_id,
                "记录编号": f"CHAN-{change_id:04d}",
                "目标资产": asset_code,
                "变更类型": change_type,
                "变更前值": _text(before),
                "变更后值": _text(after),
                "变更来源": source,
                "变更时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
                "影响范围": self._impact_scope(downstream),
                "status": "已捕获",
                "pending": True,
                "abnormal": False,
                "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            store.rows("change_capture").append(change)

            event = self._append_event(
                kind="asset_change",
                lineage=None,
                changes={change_type: (_text(before), _text(after))},
                field=change_type,
                job_code=job_code,
                source=source,
                idempotency_key=key,
                target_asset=asset_code,
                change_id=change_id,
            )
            event = self.confirm_event(event["event_id"]) or event
            return {"change": change, "event": event, "duplicate": False}

    # ── 事件账本 ──────────────────────────────────────────────

    def _append_event(
        self,
        *,
        kind: str,
        lineage: dict[str, Any] | None,
        changes: dict[str, tuple[str, str]],
        field: str,
        job_code: str,
        source: str,
        idempotency_key: str,
        target_asset: str | None = None,
        change_id: int | None = None,
    ) -> dict[str, Any]:
        before_value, after_value = changes.get(field, ("", ""))
        event = {
            "kind": kind,
            "lineage_id": lineage["id"] if lineage else None,
            "relation_code": lineage["关系编号"] if lineage else "",
            "upstream_asset": lineage["上游资产"] if lineage else (target_asset or ""),
            "downstream_asset": lineage["下游资产"] if lineage else "",
            "target_asset": target_asset or (lineage["上游资产"] if lineage else ""),
            "field": field,
            "before_value": _text(before_value),
            "after_value": _text(after_value),
            "changes": {
                name: [_text(pair[0]), _text(pair[1])]
                for name, pair in changes.items()
            },
            "job_code": job_code,
            "source": source,
            "idempotency_key": idempotency_key,
            "confirmed": False,
            "confirmed_at": None,
            "delivered_to": [],
            "change_id": change_id,
        }
        stored = store.append_lineage_event(event)

        # 资产变更已由 record_asset_change 落了变更行，这里只补追踪字段；
        # 血缘变动没有独立变更行，则同步追加一条到变更列表，使普通列表查询也能回放。
        if change_id is not None:
            for row in store.rows("change_capture"):
                if int(row.get("id", 0)) == int(change_id):
                    row.update({
                        "event_id": stored["event_id"],
                        "seq": stored["seq"],
                        "job_code": job_code,
                        "作业编号": job_code,
                        "lineage_id": event["lineage_id"],
                        "field": field,
                        "confirmed": False,
                    })
                    break
            return stored

        scope = (
            f"{event['upstream_asset']}->{event['downstream_asset']}"
            if event["downstream_asset"] else event["target_asset"]
        )
        store.log_change(
            "lineage_track" if lineage else "change_capture",
            lineage["id"] if lineage else 0,
            "血缘变动" if lineage else "资产变更",
            before=event["before_value"],
            after=event["after_value"],
            source=source,
            scope=scope,
            target=event["target_asset"],
            event_id=stored["event_id"],
            seq=stored["seq"],
            job_code=job_code,
            作业编号=job_code,
            lineage_id=event["lineage_id"],
            relation_code=event["relation_code"],
            field=field,
            kind=kind,
            confirmed=False,
        )
        return stored

    def get_event(self, event_id_or_key: str) -> dict[str, Any] | None:
        event = store.find_lineage_event(event_id_or_key)
        if event is None:
            event = store.find_lineage_event_by_key(event_id_or_key)
        return event

    def confirm_event(self, event_id_or_key: str) -> dict[str, Any] | None:
        """补记确认回执；重复确认不产生新事件。"""
        with self._lock:
            event = self.get_event(event_id_or_key)
            if event is None:
                return None
            if event.get("confirmed"):
                return event
            updated = store.update_lineage_event(
                event["event_id"],
                confirmed=True,
                confirmed_at=_time.strftime("%Y-%m-%d %H:%M:%S"),
            ) or event
            self._sync_change_row(updated, confirmed=True)
            return updated

    def list_events(
        self,
        *,
        lineage_id: int | None = None,
        asset_code: str | None = None,
    ) -> list[dict[str, Any]]:
        """按确认次序返回事件；同一血缘多次变动各占一条。"""
        return store.lineage_events(lineage_id=lineage_id, asset_code=asset_code)

    # ── 交付与补采 ────────────────────────────────────────────

    def pending_deliveries(self, consumer: str) -> list[dict[str, Any]]:
        """消费方尚未交付的已确认事件，按账本序号升序。"""
        pending = []
        for event in store.lineage_events():
            if not event.get("confirmed"):
                continue
            if consumer in (event.get("delivered_to") or []):
                continue
            pending.append(event)
        return pending

    def deliver(self, consumer: str) -> list[dict[str, Any]]:
        """向消费方交付已确认且未交付的事件，并推进其补采位点。"""
        with self._lock:
            delivered = []
            for event in self.pending_deliveries(consumer):
                delivered_to = list(event.get("delivered_to") or [])
                delivered_to.append(consumer)
                updated = store.update_lineage_event(
                    event["event_id"],
                    delivered_to=delivered_to,
                    delivered_at=_time.strftime("%Y-%m-%d %H:%M:%S"),
                ) or event
                store.advance_lineage_cursor(consumer, int(updated["seq"]))
                self._sync_change_row(updated, delivered=True)
                delivered.append(updated)
            return delivered

    def backfill(self, consumer: str) -> dict[str, Any]:
        """从消费方已确认位点补采尚未交付的变动（含确认回执丢失后补确认的）。"""
        with self._lock:
            cursor_before = store.lineage_cursor(consumer)
            delivered = self.deliver(consumer)
            return {
                "consumer": consumer,
                "from_seq": cursor_before,
                "to_seq": store.lineage_cursor(consumer),
                "delivered": delivered,
            }

    def capture_status(self, consumer: str) -> dict[str, Any]:
        return {
            "consumer": consumer,
            "cursor": store.lineage_cursor(consumer),
            "pending": len(self.pending_deliveries(consumer)),
            "ledger_size": store.lineage_ledger_size,
        }

    # ── 复算清单 ──────────────────────────────────────────────

    def downstream_assets(self, asset_code: str) -> list[str]:
        downstream: list[str] = []
        for row in store.rows("lineage_track"):
            if row.get("上游资产") == asset_code:
                name = row.get("下游资产", "")
                if name and name not in downstream:
                    downstream.append(name)
        return downstream

    def build_recompute_manifest(
        self,
        *,
        asset_code: str | None = None,
        lineage_id: int | None = None,
        consumer: str | None = None,
    ) -> list[dict[str, Any]]:
        """由采集结果生成复算清单：一笔事件一条，不按表或时间合并。"""
        events = self.list_events(lineage_id=lineage_id, asset_code=asset_code)
        if consumer is not None:
            events = [event for event in events if consumer in (event.get("delivered_to") or [])]
        manifest = []
        for event in events:
            if not event.get("confirmed"):
                continue
            targets = []
            if event.get("downstream_asset"):
                targets.append(event["downstream_asset"])
            for name in self.downstream_assets(event.get("target_asset", "")):
                if name not in targets:
                    targets.append(name)
            manifest.append({
                "event_id": event["event_id"],
                "seq": event["seq"],
                "lineage_id": event.get("lineage_id"),
                "relation_code": event.get("relation_code", ""),
                "upstream_asset": event.get("upstream_asset", ""),
                "downstream_asset": event.get("downstream_asset", ""),
                "job_code": event.get("job_code", ""),
                "field": event.get("field", ""),
                "before_value": event.get("before_value", ""),
                "after_value": event.get("after_value", ""),
                "recompute_assets": targets,
            })
        return manifest

    # ── 内部 ──────────────────────────────────────────────────

    def _sync_change_row(self, event: dict[str, Any], *, confirmed: bool = False,
                         delivered: bool = False) -> None:
        """把确认/交付状态回写到变更列表中对应的那一行。

        只补追踪标记，不改 status：状态流转仍由模块动作规则负责。
        """
        row = None
        for candidate in store.rows("change_capture"):
            if candidate.get("event_id") == event.get("event_id"):
                row = candidate
                break
        if row is None:
            return
        if confirmed:
            row["confirmed"] = True
            row["confirmed_at"] = event.get("confirmed_at")
        if delivered:
            row["delivered_to"] = list(event.get("delivered_to") or [])
            row["delivered_at"] = event.get("delivered_at")

    @staticmethod
    def _impact_scope(downstream: list[str]) -> str:
        if downstream:
            return f"影响{len(downstream)}个下游资产: {', '.join(downstream)}"
        return "无下游依赖"


lineage_recorder = LineageChangeRecorder()
