import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from railwatch.anomaly import AnomalyService
from railwatch.domain import ProductError
from railwatch.quality import Quality
from railwatch.report import Reports
from railwatch.service import RailwayService


class AnomalyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = RailwayService(Path(self.temp.name) / "anomaly.sqlite3")
        self.anomalies = self.service.anomalies
        self.service.register_source({
            "source_id": "news", "host": "example.org", "kinds": ["freight"],
            "max_age_days": 3650, "trust_rank": 10, "version": 1, "active": True})
        # 阈值：相邻期间超过 3 倍即核查（100 -> 400 触发，100 -> 200 不触发）。
        self.anomalies.put_config({"project_key": "rail-a", "metric": "cargo", "ratio": "3"})

    def tearDown(self):
        self.temp.cleanup()

    # 合成数据辅助 --------------------------------------------------------
    def claim(self, amount, period, metric="cargo", project="rail-a"):
        return {"project_key": project, "kind": "freight", "metric": metric, "period": period,
                "amount": str(amount), "unit": "tonne",
                "quote": f"Freight reached {amount} tonnes in the period ending {period}."}

    def capture(self, external_id, amount, period, published=None):
        claim = self.claim(amount, period)
        published = published or f"{period}T12:00:00Z"
        return {"source_id": "news", "external_id": external_id,
                "url": f"https://example.org/article/{external_id}",
                "fetched_at": published, "published_at": published, "title": "Rail report " + external_id,
                "body": json.dumps({"text": claim["quote"], "claims": [claim]}),
                "media_type": "application/json", "metadata": {"origin": "synthetic"}}

    def ingest_accepted(self, external_id, amount, period, expected_version=0):
        result = self.service.replay(self.capture(external_id, amount, period))
        claim_id = result["claim_ids"][0]
        self.service.review(claim_id, {"decision": "accepted", "reviewer": "analyst",
                                       "reason": "Quote matches synthetic text",
                                       "expected_version": expected_version})
        return claim_id

    def assert_code(self, code, action, *args, **kwargs):
        with self.assertRaises(ProductError) as caught:
            action(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    # 场景一：量级跳变自动立案并阻止发布 -----------------------------------
    def test_jump_opens_tracked_anomaly_with_full_context(self):
        first = self.ingest_accepted("a-1", 100, "2026-08-31", 0)
        second = self.ingest_accepted("a-2", 400, "2026-09-30")
        items = self.anomalies.queue("rail-a", "cargo", "open")
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item["state"], "open")
        self.assertEqual(item["before"]["claim_id"], first)
        self.assertEqual(item["after"]["claim_id"], second)
        self.assertEqual(Decimal(item["observed_ratio"]), Decimal("4"))
        self.assertEqual(item["threshold_ratio"], "3")
        # 原始采集与候选上下文完整保留。
        self.assertEqual(item["before"]["url"], "https://example.org/article/a-1")
        self.assertIn("Freight reached 100 tonnes", item["before"]["quote"])
        self.assertEqual(item["after"]["capture_state"], "parsed")
        self.assertEqual(item["after"]["content_hash"],
                         self.service.capture(self.service.claim(second)["capture_id"])["content_hash"])
        # 未解决时当前指标不得发布。
        self.assert_code("anomaly_unresolved", self.service.publish,
                         {"name": "blocked", "project_key": "rail-a"})
        self.assertFalse(Quality(self.service).publication_readiness("rail-a")["ready"])

    def test_small_variation_does_not_open_anomaly(self):
        self.ingest_accepted("b-1", 100, "2026-08-31", 0)
        self.ingest_accepted("b-2", 200, "2026-09-30")  # 2 倍 < 3 倍阈值
        self.assertEqual(self.anomalies.queue("rail-a"), [])
        release = self.service.publish({"name": "clean", "project_key": "rail-a"})
        self.assertEqual(release["anomaly_count"], 0)

    def test_scan_is_idempotent(self):
        self.ingest_accepted("c-1", 100, "2026-08-31", 0)
        self.ingest_accepted("c-2", 400, "2026-09-30")
        again = self.anomalies.scan("rail-a", "cargo")
        self.assertEqual(again["opened"], [])
        self.assertEqual(again["tracked_count"], 1)
        self.assertEqual(len(self.anomalies.queue()), 1)

    # 场景二：确认合理跳变后可发布 -----------------------------------------
    def test_confirm_reasonable_jump_allows_publication(self):
        self.ingest_accepted("d-1", 100, "2026-08-31", 0)
        self.ingest_accepted("d-2", 400, "2026-09-30")
        item = self.anomalies.queue()[0]
        result = self.anomalies.review(item["anomaly_id"], {
            "action": "confirm", "reviewer": "editor", "expected_version": item["version"],
            "reason": "New high-speed line opened, fourfold volume is plausible"})
        self.assertEqual(result["state"], "confirmed")
        self.assertFalse(result["blocks_publication"])
        release = self.service.publish({"name": "confirmed-release", "project_key": "rail-a"})
        self.assertEqual(release["anomaly_count"], 1)

    # 场景三：误报驳回 -----------------------------------------------------
    def test_dismiss_false_positive(self):
        self.ingest_accepted("e-1", 100, "2026-08-31", 0)
        self.ingest_accepted("e-2", 400, "2026-09-30")
        item = self.anomalies.queue()[0]
        result = self.anomalies.review(item["anomaly_id"], {
            "action": "dismiss", "reviewer": "editor", "expected_version": item["version"],
            "reason": "Unit correction note shows figures remain comparable"})
        self.assertEqual(result["resolution"], "dismissed")
        # 复核事件全程可追踪。
        events = self.anomalies.store.anomaly_events(item["anomaly_id"])
        self.assertEqual([event["event"] for event in events], ["opened", "review_dismiss"])
        self.assertTrue(Quality(self.service).publication_readiness("rail-a")["ready"])

    def test_review_uses_optimistic_version(self):
        self.ingest_accepted("f-1", 100, "2026-08-31", 0)
        self.ingest_accepted("f-2", 400, "2026-09-30")
        item = self.anomalies.queue()[0]
        self.assert_code("stale_version", self.anomalies.review, item["anomaly_id"],
                         {"action": "confirm", "reviewer": "editor", "expected_version": 99,
                          "reason": "stale"})

    def test_illegal_transition_rejected(self):
        self.ingest_accepted("g-1", 100, "2026-08-31", 0)
        self.ingest_accepted("g-2", 400, "2026-09-30")
        item = self.anomalies.queue()[0]
        # open 状态不能直接 accept_evidence。
        self.assert_code("invalid_transition", self.anomalies.review, item["anomaly_id"],
                         {"action": "accept_evidence", "reviewer": "editor",
                          "expected_version": item["version"], "reason": "no evidence yet"})

    # 场景四：要求来源补证，补证文章到达后自动关联，仍待人确认 --------------
    def test_evidence_request_then_supporting_article_arrives(self):
        self.ingest_accepted("h-1", 100, "2026-08-31", 0)
        second = self.ingest_accepted("h-2", 400, "2026-09-30")
        item = self.anomalies.queue()[0]
        self.anomalies.review(item["anomaly_id"], {
            "action": "request_evidence", "reviewer": "editor", "expected_version": 1,
            "reason": "Need official corroboration for the fourfold jump"})
        self.assertEqual(self.anomalies.get(item["anomaly_id"])["state"], "evidence_requested")
        # 补证到达前依然阻止发布。
        self.assert_code("anomaly_unresolved", self.service.publish,
                         {"name": "waiting", "project_key": "rail-a"})
        # 同序列更晚期间的补证文章到达并通过审核。
        support = self.ingest_accepted("h-3", 410, "2026-10-31")
        updated = self.anomalies.get(item["anomaly_id"])
        self.assertEqual(updated["state"], "evidence_received")
        self.assertEqual(updated["evidence"]["claim_id"], support)
        # 自动关联不等于自动放行：仍阻止发布。
        self.assertTrue(updated["blocks_publication"])
        self.assert_code("anomaly_unresolved", self.service.publish,
                         {"name": "still-waiting", "project_key": "rail-a"})
        # 复核人确认补证充分后放行。
        self.anomalies.review(updated["anomaly_id"], {
            "action": "accept_evidence", "reviewer": "editor",
            "expected_version": updated["version"],
            "reason": "Official monthly bulletin corroborates the level"})
        self.assertEqual(self.anomalies.get(item["anomaly_id"])["state"], "supplemented")
        self.service.publish({"name": "supported", "project_key": "rail-a"})

    def test_evidence_attached_even_before_supporting_claim_is_reviewed(self):
        self.ingest_accepted("h2-1", 100, "2026-08-31", 0)
        self.ingest_accepted("h2-2", 400, "2026-09-30")
        item = self.anomalies.queue()[0]
        self.anomalies.review(item["anomaly_id"], {
            "action": "request_evidence", "reviewer": "editor", "expected_version": 1,
            "reason": "Need corroboration"})
        # 补证文章入库但尚未审核：仍应自动关联并进入 evidence_received。
        pending = self.service.replay(self.capture("h2-3", 410, "2026-10-31"))
        updated = self.anomalies.get(item["anomaly_id"])
        self.assertEqual(updated["state"], "evidence_received")
        self.assertEqual(updated["evidence"]["claim_id"], pending["claim_ids"][0])
        self.assertEqual(updated["evidence"]["claim_state"], "proposed")
        # 补证被拒绝后锚点仍有效，事项保持待决；重新补证并审核后才能放行。
        self.service.review(pending["claim_ids"][0], {
            "decision": "rejected", "reviewer": "analyst", "expected_version": 0,
            "reason": "Unrelated source"})
        still = self.anomalies.get(item["anomaly_id"])
        self.assertEqual(still["state"], "evidence_received")
        self.assertTrue(still["blocks_publication"])

    def test_unrelated_article_is_not_attached_as_evidence(self):
        self.ingest_accepted("i-1", 100, "2026-08-31", 0)
        self.ingest_accepted("i-2", 400, "2026-09-30")
        item = self.anomalies.queue()[0]
        self.anomalies.review(item["anomaly_id"], {
            "action": "request_evidence", "reviewer": "editor", "expected_version": 1,
            "reason": "Need corroboration"})
        # 不同指标的文章不得关联。
        other = self.claim(500, "2026-10-31", metric="wagon_load")
        capture = {"source_id": "news", "external_id": "i-3", "url": "https://example.org/article/i-3",
                   "fetched_at": "2026-10-31T12:00:00Z", "published_at": "2026-10-31T12:00:00Z",
                   "title": "Other metric", "body": json.dumps({"text": other["quote"], "claims": [other]}),
                   "media_type": "application/json"}
        self.service.replay(capture)
        self.assertIsNone(self.anomalies.get(item["anomaly_id"])["evidence"])
        self.assertEqual(self.anomalies.get(item["anomaly_id"])["state"], "evidence_requested")

    # 场景五：旧候选被撤回，核查事项自动作废 -------------------------------
    def test_superseding_capture_voids_anomaly_and_reopens_cleanly(self):
        first = self.ingest_accepted("j-1", 100, "2026-08-31", 0)
        self.ingest_accepted("j-2", 400, "2026-09-30")
        anomaly_id = self.anomalies.queue()[0]["anomaly_id"]
        # 来源发布更正版，旧 capture 被 supersede，旧 accepted claim 被撤回。
        corrected = self.capture("j-2", 120, "2026-09-30", "2026-10-02T12:00:00Z")
        self.service.replay(corrected)
        self.assertEqual(self.service.claim(first)["state"], "accepted")
        voided = self.anomalies.get(anomaly_id)
        self.assertEqual(voided["state"], "void")
        self.assertFalse(voided["blocks_publication"])
        events = [event["event"] for event in self.anomalies.store.anomaly_events(anomaly_id)]
        self.assertIn("anchor_invalidated", events)
        # 作废后不再阻止发布（scope 因撤回进入 conflict，需先复核新候选）。
        self.service.review(self.service.review_queue("rail-a")[0]["id"], {
            "decision": "accepted", "reviewer": "analyst", "expected_version": 2,
            "reason": "Corrected figure accepted"})
        # 100 -> 120 不再超阈，无新事项，可发布。
        self.assertEqual(self.anomalies.queue(state="open"), [])
        self.service.publish({"name": "corrected", "project_key": "rail-a"})

    # 场景六：阈值配置变化的失效规则 ---------------------------------------
    def test_loosening_threshold_clears_open_item_but_keeps_human_decisions(self):
        self.ingest_accepted("k-1", 100, "2026-08-31", 0)
        self.ingest_accepted("k-2", 400, "2026-09-30")
        anomaly_id = self.anomalies.queue()[0]["anomaly_id"]
        # 放宽到 5 倍：4 倍不再超阈，待决事项自动解除。
        config = self.anomalies.put_config(
            {"project_key": "rail-a", "metric": "cargo", "ratio": "5"}, expected_version=1)
        self.assertEqual(config["version"], 2)
        cleared = self.anomalies.get(anomaly_id)
        self.assertEqual(cleared["state"], "config_cleared")
        self.assertFalse(cleared["blocks_publication"])
        self.service.publish({"name": "loosened", "project_key": "rail-a"})

    def test_human_decision_is_marked_stale_never_overwritten_by_config(self):
        self.ingest_accepted("l-1", 100, "2026-08-31", 0)
        self.ingest_accepted("l-2", 400, "2026-09-30")
        item = self.anomalies.queue()[0]
        self.anomalies.review(item["anomaly_id"], {
            "action": "confirm", "reviewer": "editor", "expected_version": 1,
            "reason": "Planned capacity expansion"})
        self.anomalies.put_config(
            {"project_key": "rail-a", "metric": "cargo", "ratio": "5"}, expected_version=1)
        stale = self.anomalies.get(item["anomaly_id"])
        self.assertEqual(stale["state"], "confirmed")  # 人工结论不被覆盖
        self.assertTrue(stale["config_stale"])
        self.assertEqual(stale["config_version"], 1)
        self.assertEqual(stale["current_config_version"], 2)

    def test_tightening_threshold_refreshes_open_item_and_flags_new_jumps(self):
        self.ingest_accepted("m-1", 100, "2026-08-31", 0)
        self.ingest_accepted("m-2", 250, "2026-09-30")  # 2.5 倍，阈值 3 时不立案
        self.assertEqual(self.anomalies.queue(), [])
        result = self.anomalies.put_config(
            {"project_key": "rail-a", "metric": "cargo", "ratio": "2"}, expected_version=1)
        self.assertEqual(result["version"], 2)
        items = self.anomalies.queue(state="open")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["threshold_ratio"], "2")
        self.assertEqual(items[0]["config_version"], 2)

    def test_config_requires_cas(self):
        self.assert_code("stale_version", self.anomalies.put_config,
                         {"project_key": "rail-a", "metric": "cargo", "ratio": "4"}, 7)

    # 场景七：历史快照不被后来的阈值重算或复核改写 --------------------------
    def test_snapshot_freezes_anomaly_explanation_independently_of_later_changes(self):
        self.ingest_accepted("n-1", 100, "2026-08-31", 0)
        self.ingest_accepted("n-2", 400, "2026-09-30")
        item = self.anomalies.queue()[0]
        self.anomalies.review(item["anomaly_id"], {
            "action": "confirm", "reviewer": "editor", "expected_version": 1,
            "reason": "Documented service expansion"})
        release = self.service.publish({"name": "frozen", "project_key": "rail-a"})
        snapshot_id = release["snapshot_id"]
        snapshot_report = Reports(self.service).snapshot(snapshot_id, "rail-a")
        frozen = snapshot_report["anomalies"][0]
        self.assertEqual(frozen["state"], "confirmed")
        self.assertEqual(frozen["threshold_ratio"], "3")
        self.assertEqual(frozen["observed_ratio"], "4")
        digest_before = self.service.snapshot(snapshot_id)["digest"]
        # 之后阈值放宽、新文章到达并产生新的跳变、复核动作发生……
        self.anomalies.put_config(
            {"project_key": "rail-a", "metric": "cargo", "ratio": "10"}, expected_version=1)
        self.ingest_accepted("n-3", 5000, "2026-10-31")  # 12.5 倍跳变，在新阈值下立案
        # 历史快照内容与指纹保持不变。
        stored = self.service.snapshot(snapshot_id)
        self.assertEqual(stored["digest"], digest_before)
        self.assertEqual(stored["anomalies"][0]["threshold_ratio"], "3")
        again = Reports(self.service).snapshot(snapshot_id, "rail-a")
        self.assertEqual(again["anomalies"][0]["state"], "confirmed")
        self.assertEqual(again["snapshot_digest"], digest_before)
        # 当前报告反映的是新状态，与历史快照解释不同但各自自洽。
        current = Reports(self.service).current("rail-a")
        self.assertEqual(len(current["anomalies"]), 2)
        self.assertTrue(any(mark["blocks_publication"]
                            for series in current["series"] for mark in series["anomaly_marks"]))

    def test_current_report_marks_blocked_series_consistently(self):
        self.ingest_accepted("o-1", 100, "2026-08-31", 0)
        self.ingest_accepted("o-2", 400, "2026-09-30")
        report = Reports(self.service).current("rail-a")
        self.assertEqual(report["anomaly_block_count"], 1)
        series = report["series"][0]
        self.assertTrue(series["has_blocking_anomaly"])
        self.assertEqual(series["anomaly_marks"][0]["state"], "open")
        readiness = Quality(self.service).publication_readiness("rail-a")
        self.assertEqual(readiness["blocking_anomalies"][0]["anomaly_id"],
                         report["anomalies"][0]["anomaly_id"])
        self.assertEqual(self.anomalies.queue()[0]["after"]["amount"], "400")

    def test_quality_view_surfaces_anomaly_states(self):
        self.ingest_accepted("p-1", 100, "2026-08-31", 0)
        self.ingest_accepted("p-2", 400, "2026-09-30")
        quality = Quality(self.service).claim_quality("rail-a")
        self.assertEqual(quality["anomaly_states"]["open"], 1)
        self.assertEqual(len(quality["blocking_anomaly_ids"]), 1)

    def test_decrease_jump_is_also_detected(self):
        self.ingest_accepted("q-1", 400, "2026-08-31", 0)
        self.ingest_accepted("q-2", 100, "2026-09-30")  # 四分之一，反向跳变
        item = self.anomalies.queue()[0]
        self.assertEqual(item["direction"], "decrease")
        self.assertTrue(item["blocks_publication"])

    def test_zero_baseline_opens_anomaly(self):
        self.ingest_accepted("r-1", 0, "2026-08-31", 0)
        self.ingest_accepted("r-2", 50, "2026-09-30")
        self.assertEqual(len(self.anomalies.queue(state="open")), 1)

    def test_inactive_config_suppresses_detection(self):
        self.anomalies.put_config(
            {"project_key": "rail-a", "metric": "cargo", "ratio": "3", "active": False},
            expected_version=1)
        self.ingest_accepted("s-1", 100, "2026-08-31", 0)
        self.ingest_accepted("s-2", 400, "2026-09-30")
        self.assertEqual(self.anomalies.queue(), [])
        self.assertTrue(Quality(self.service).publication_readiness("rail-a")["ready"])


if __name__ == "__main__":
    unittest.main()
