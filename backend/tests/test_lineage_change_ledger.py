"""血缘变动事件采集测试：关系登记与变更捕获共用同一套事件账本。

覆盖：
- 重复提交相同关系只生效一次；
- 同一血缘的并发写入串行定序，第二笔未确认时不影响首笔交付；
- 不同作业对同一字段的有效修改各自留痕；
- J41/J42 场景：第二笔确认回执丢失后重试，下游能区分两次加工变动；
- 采集位点补采与复算清单。
"""
from __future__ import annotations

import threading

import app.store as store_mod
from app.services.change_capture import ChangeCaptureService
from app.services.lineage_events import lineage_recorder
from app.services.lineage_track import LineageTrackService


def _fresh():
    store_mod.store.reset()
    return LineageTrackService(), ChangeCaptureService()


def _register(lt, up="DWD_ORDER", down="DWS_ORDER"):
    result = lt.derive_lineage(up, down, "order_id", "GROUP BY order_id", True, job_code="J40")
    return result["lineage"]["id"]


def _relations(up="SRC", down="DST"):
    return [
        row for row in store_mod.store.rows("lineage_track")
        if row.get("上游资产") == up and row.get("下游资产") == down
    ]


class TestRelationRegistration:

    def test_duplicate_relation_submission_applies_once(self):
        lt, _ = _fresh()
        first = lt.derive_lineage("SRC", "DST", "order_id", "直接引用", True)
        second = lt.derive_lineage("SRC", "DST", "order_id", "直接引用", True)

        assert first["duplicate"] is False
        assert second["duplicate"] is True
        assert len(_relations()) == 1
        assert len(lineage_recorder.list_events(lineage_id=first["lineage"]["id"])) == 1

    def test_single_relation_output_shape_unchanged(self):
        lt, _ = _fresh()
        result = lt.derive_lineage("SRC", "DST", "id", "ETL", True)
        assert set(result) >= {"lineage", "impact_analysis", "governance_scoring", "pipeline"}
        assert result["lineage"]["status"] == "自动生成"
        assert result["impact_analysis"] is not None
        assert result["governance_scoring"] is not None


class TestConcurrentWrites:

    def test_concurrent_writes_on_same_lineage_are_ordered(self):
        lt, cc = _fresh()
        lineage_id = _register(lt)
        barrier = threading.Barrier(2)
        results: dict[str, dict] = {}

        def write(job_code: str, join_fields: str, acknowledge: bool):
            barrier.wait()
            results[job_code] = cc.capture_lineage_change(
                lineage_id, join_fields=join_fields, job_code=job_code,
                acknowledge=acknowledge,
            )

        threads = [
            threading.Thread(target=write, args=("J41", "order_no", True)),
            threading.Thread(target=write, args=("J42", "order_key", False)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        events = [e for e in lt.get_event_history(lineage_id)
                  if e["kind"] == "relation_revised"]
        assert len(events) == 2
        # 两笔修复的前置值首尾相接，说明第二笔读到的是第一笔提交后的值。
        assert events[0]["after_value"] == events[1]["before_value"]
        assert events[0]["seq"] < events[1]["seq"]

    def test_first_capture_deliverable_while_second_unconfirmed(self):
        lt, cc = _fresh()
        lineage_id = _register(lt)
        cc.capture_lineage_change(lineage_id, transform_logic="SUM(net_amount)", job_code="J41")
        cc.capture_lineage_change(lineage_id, join_fields="order_no", job_code="J42",
                                  acknowledge=False)

        pending = cc.pending_changes("downstream")
        revised_jobs = [
            event["job_code"] for event in pending
            if event["job_code"] and event["kind"] == "relation_revised"
        ]
        assert revised_jobs == ["J41"]
        # 已登记关系的事件先落账先交付，J41 不必等 J42 确认。
        assert lineage_id in [event["lineage_id"] for event in pending]


class TestFieldRevisions:

    def test_effective_changes_from_two_jobs_are_both_queryable(self):
        lt, cc = _fresh()
        lineage_id = _register(lt)

        cc.capture_lineage_change(lineage_id, join_fields="order_no", job_code="J42")
        cc.capture_lineage_change(lineage_id, join_fields="order_key", job_code="J43")

        history = [e for e in lt.get_event_history(lineage_id)
                   if e["kind"] == "relation_revised"]
        assert [e["job_code"] for e in history] == ["J42", "J43"]
        assert [e["before_value"] for e in history] == ["order_id", "order_no"]
        assert [e["after_value"] for e in history] == ["order_no", "order_key"]

    def test_noop_revision_does_not_create_event(self):
        lt, cc = _fresh()
        lineage_id = _register(lt)
        before = store_mod.store.lineage_ledger_size
        result = cc.capture_lineage_change(lineage_id, join_fields="order_id", job_code="J44")
        assert result["changed"] is False
        assert store_mod.store.lineage_ledger_size == before


class TestJ41J42Scenario:

    def test_lost_ack_retry_keeps_two_distinct_machining_changes(self):
        lt, cc = _fresh()
        lineage_id = _register(lt)

        # J41 把净额字段换成含税金额（转换逻辑），确认回执正常返回。
        j41 = cc.capture_lineage_change(
            lineage_id, transform_logic="SUM(tax_inclusive_amount)", job_code="J41")
        # J42 随后修订连接键，关系值已提交，但确认回执在采集侧超时。
        j42 = cc.capture_lineage_change(
            lineage_id, join_fields="order_no", job_code="J42", acknowledge=False)
        assert j41["event"]["confirmed"] is True
        assert j42["event"]["confirmed"] is False

        # 回执丢失后重试：补确认，不生成第二笔。
        ledger_before = store_mod.store.lineage_ledger_size
        retry = cc.capture_lineage_change(
            lineage_id, join_fields="order_no", job_code="J42")
        assert retry["duplicate"] is True
        assert retry["event"]["confirmed"] is True
        assert store_mod.store.lineage_ledger_size == ledger_before

        # 下游复算：同一张表的两次加工各占一条，不会被合并成末次关系。
        manifest = lt.recompute_manifest(lineage_id=lineage_id)
        machining = [item for item in manifest if item["job_code"] in ("J41", "J42")]
        assert [item["job_code"] for item in machining] == ["J41", "J42"]
        assert len({item["event_id"] for item in machining}) == 2
        assert len({item["seq"] for item in machining}) == 2

    def test_relation_detail_replays_fields_owner_and_order(self):
        lt, cc = _fresh()
        lineage_id = _register(lt)
        cc.capture_lineage_change(
            lineage_id, transform_logic="SUM(tax_inclusive_amount)", job_code="J41")
        cc.capture_lineage_change(
            lineage_id, join_fields="order_no", job_code="J42", acknowledge=False)
        cc.capture_lineage_change(
            lineage_id, join_fields="order_no", job_code="J42")  # 超时后重试补确认

        detail = lt.get_relation_detail(lineage_id)
        revised = [e for e in detail["history"] if e["kind"] == "relation_revised"]
        assert [e["job_code"] for e in revised] == ["J41", "J42"]
        assert [e["field"] for e in revised] == ["转换逻辑", "关联字段"]
        assert [e["before_value"] for e in revised] == [
            "GROUP BY order_id", "order_id"]
        assert [e["after_value"] for e in revised] == [
            "SUM(tax_inclusive_amount)", "order_no"]
        assert all(e["relation_code"] == detail["lineage"]["关系编号"] for e in revised)
        # 关系行保留当前已提交值；历史前后值来自事件账本。
        assert detail["current"]["关联字段"] == "order_no"

    def test_change_list_contains_both_revisions(self):
        lt, cc = _fresh()
        lineage_id = _register(lt)
        relation_code = lt.get_entry(lineage_id)["关系编号"]
        cc.capture_lineage_change(lineage_id, transform_logic="SUM(tax)", job_code="J41")
        cc.capture_lineage_change(lineage_id, join_fields="order_no", job_code="J42")

        rows = [r for r in store_mod.store.rows("change_capture")
                if r.get("relation_code") == relation_code
                and r.get("kind") == "relation_revised"]
        assert [r["作业编号"] for r in rows] == ["J41", "J42"]
        assert [r["变更前值"] for r in rows] == ["GROUP BY order_id", "order_id"]
        assert [r["变更后值"] for r in rows] == ["SUM(tax)", "order_no"]
        assert [r["seq"] for r in rows] == sorted(r["seq"] for r in rows)


class TestBackfill:

    def test_backfill_resumes_from_confirmed_position(self):
        lt, cc = _fresh()
        lineage_id = _register(lt)
        cc.capture_lineage_change(lineage_id, transform_logic="SUM(tax)", job_code="J41")
        cc.capture_lineage_change(lineage_id, join_fields="order_no", job_code="J42",
                                  acknowledge=False)

        first = cc.backfill("scheduler")
        revised = [e for e in first["delivered"] if e["kind"] == "relation_revised"]
        assert [e["job_code"] for e in revised] == ["J41"]
        assert cc.capture_status("scheduler")["pending"] == 0

        # 回执补上后，从原确认位点继续补采第二笔。
        cc.capture_lineage_change(lineage_id, join_fields="order_no", job_code="J42")
        second = cc.backfill("scheduler")
        revised = [e for e in second["delivered"] if e["kind"] == "relation_revised"]
        assert [e["job_code"] for e in revised] == ["J42"]
        assert second["from_seq"] >= first["to_seq"]
        assert cc.capture_status("scheduler")["pending"] == 0

    def test_unconfirmed_event_not_in_recompute_manifest(self):
        lt, cc = _fresh()
        lineage_id = _register(lt)
        cc.capture_lineage_change(lineage_id, transform_logic="SUM(tax)", job_code="J41")
        cc.capture_lineage_change(lineage_id, join_fields="order_no", job_code="J42",
                                  acknowledge=False)

        manifest = lt.recompute_manifest(lineage_id=lineage_id)
        assert "J42" not in [item["job_code"] for item in manifest]
        assert "J41" in [item["job_code"] for item in manifest]


class TestAssetChangeCapture:

    def test_duplicate_asset_change_submission_applies_once(self):
        _, cc = _fresh()
        first = cc.capture_change("T6_ASSET", "schema_change", "a,b", "a,b,c",
                                  source="ddl", job_code="J41")
        second = cc.capture_change("T6_ASSET", "schema_change", "a,b", "a,b,c",
                                   source="ddl", job_code="J41")
        assert first["duplicate"] is False
        assert second["duplicate"] is True
        changes = store_mod.store.find_by_field("change_capture", "目标资产", "T6_ASSET")
        assert len(changes) == 1
        assert changes[0]["event_id"] == first["event"]["event_id"]
        assert len(store_mod.store.find_by_field("diff_report", "比较对象", "T6_ASSET")) == 1

    def test_single_capture_output_shape_unchanged(self):
        _, cc = _fresh()
        result = cc.capture_change("T6_ASSET", "schema_change", "a", "a,b")
        assert result["change"]["status"] == "已捕获"
        assert result["diff_report"]["diff_count"] == 1
        assert result["version_snapshot"]["created"] is True
        assert "affected_downstream" in result
