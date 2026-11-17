import json
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from railwatch.api import create_app


class AnomalyApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.client = TestClient(create_app(Path(self.temp.name) / "api.sqlite3"))
        self.client.put("/sources/news", json={
            "source_id": "news", "host": "example.org", "kinds": ["freight"],
            "max_age_days": 3650, "trust_rank": 10, "version": 1, "active": True})
        config = {"project_key": "rail-a", "metric": "cargo", "ratio": "3"}
        response = self.client.put("/anomaly/config", json=config)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["version"], 1)

    def tearDown(self):
        self.client.close()
        self.temp.cleanup()

    def capture(self, external_id, amount, period):
        claim = {"project_key": "rail-a", "kind": "freight", "metric": "cargo",
                 "period": period, "amount": str(amount), "unit": "tonne",
                 "quote": f"Freight reached {amount} tonnes in the period ending {period}."}
        return {"source_id": "news", "external_id": external_id,
                "url": f"https://example.org/article/{external_id}",
                "fetched_at": f"{period}T12:00:00Z", "published_at": f"{period}T12:00:00Z",
                "title": "Rail report", "body": json.dumps({"text": claim["quote"], "claims": [claim]}),
                "media_type": "application/json"}

    def ingest(self, external_id, amount, period):
        response = self.client.post("/captures", json=self.capture(external_id, amount, period))
        claim_id = response.json()["claim_ids"][0]
        self.client.post(f"/claims/{claim_id}/reviews",
                         json={"decision": "accepted", "reviewer": "analyst",
                               "reason": "Synthetic evidence checked", "expected_version": 0})
        return claim_id

    def test_config_cas_rejects_stale_version(self):
        response = self.client.put("/anomaly/config",
                                   json={"project_key": "rail-a", "metric": "cargo",
                                         "ratio": "4", "expected_version": 9})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"], "stale_version")

    def test_full_review_workflow_blocks_then_allows_publication(self):
        self.ingest("a-1", 100, "2026-08-31")
        self.ingest("a-2", 400, "2026-09-30")
        queue = self.client.get("/anomaly/items?state=open").json()
        self.assertEqual(len(queue), 1)
        item = queue[0]
        self.assertEqual(item["observed_ratio"], "4")
        self.assertTrue(item["blocks_publication"])
        # 未解决时发布被拒。
        blocked = self.client.post("/snapshots", json={"name": "blocked", "project_key": "rail-a"})
        self.assertEqual(blocked.status_code, 409)
        self.assertEqual(blocked.json()["error"], "anomaly_unresolved")
        # 复核确认合理跳变。
        reviewed = self.client.post(f"/anomaly/items/{item['anomaly_id']}/reviews", json={
            "action": "confirm", "reviewer": "editor", "reason": "New line opened",
            "expected_version": item["version"]})
        self.assertEqual(reviewed.status_code, 200)
        self.assertEqual(reviewed.json()["state"], "confirmed")
        release = self.client.post("/snapshots", json={"name": "allowed", "project_key": "rail-a"})
        self.assertEqual(release.status_code, 201)
        self.assertEqual(release.json()["anomaly_count"], 1)
        # 快照报告携带冻结解释。
        report = self.client.get(f"/reports/snapshots/{release.json()['snapshot_id']}?project_key=rail-a").json()
        self.assertEqual(report["anomalies"][0]["state"], "confirmed")
        self.assertEqual(report["anomaly_block_count"], 0)

    def test_readiness_and_current_report_share_the_same_blocker(self):
        self.ingest("b-1", 100, "2026-08-31")
        self.ingest("b-2", 400, "2026-09-30")
        readiness = self.client.get("/quality/readiness?project_key=rail-a").json()
        self.assertFalse(readiness["ready"])
        current = self.client.get("/reports/current?project_key=rail-a").json()
        self.assertEqual(current["anomaly_block_count"], 1)
        self.assertEqual(readiness["blocking_anomalies"][0]["anomaly_id"],
                         current["anomalies"][0]["anomaly_id"])
        self.assertTrue(current["series"][0]["has_blocking_anomaly"])

    def test_threshold_change_clears_blocker_and_old_snapshot_stays_frozen(self):
        self.ingest("c-1", 100, "2026-08-31")
        second = self.ingest("c-2", 400, "2026-09-30")
        item = self.client.get("/anomaly/items?state=open").json()[0]
        self.client.post(f"/anomaly/items/{item['anomaly_id']}/reviews", json={
            "action": "confirm", "reviewer": "editor", "reason": "Planned expansion",
            "expected_version": item["version"]})
        release = self.client.post("/snapshots", json={"name": "v1", "project_key": "rail-a"}).json()
        # 阈值放宽：新的待决跳变（随后制造）会被解除；已确认事项仅标记过期。
        updated = self.client.put("/anomaly/config", json={
            "project_key": "rail-a", "metric": "cargo", "ratio": "5",
            "expected_version": 1})
        self.assertEqual(updated.json()["version"], 2)
        detail = self.client.get(f"/anomaly/items/{item['anomaly_id']}").json()
        self.assertEqual(detail["state"], "confirmed")
        self.assertTrue(detail["config_stale"])
        # 历史快照不被重算改写。
        snapshot = self.client.get(f"/snapshots/{release['snapshot_id']}").json()
        self.assertEqual(snapshot["anomalies"][0]["threshold_ratio"], "3")
        self.assertEqual(snapshot["digest"], release["digest"])

    def test_evidence_flow_through_api(self):
        self.ingest("d-1", 100, "2026-08-31")
        self.ingest("d-2", 400, "2026-09-30")
        item = self.client.get("/anomaly/items?state=open").json()[0]
        self.client.post(f"/anomaly/items/{item['anomaly_id']}/reviews", json={
            "action": "request_evidence", "reviewer": "editor",
            "reason": "Need corroboration", "expected_version": item["version"]})
        self.ingest("d-3", 410, "2026-10-31")
        updated = self.client.get(f"/anomaly/items/{item['anomaly_id']}").json()
        self.assertEqual(updated["state"], "evidence_received")
        self.assertIsNotNone(updated["evidence"])
        # 自动关联不自动放行。
        self.assertEqual(
            self.client.post("/snapshots", json={"name": "held", "project_key": "rail-a"}).status_code, 409)
        events = self.client.get(f"/anomaly/items/{item['anomaly_id']}/events").json()
        self.assertIn("evidence_arrived", [event["event"] for event in events])
        accepted = self.client.post(f"/anomaly/items/{item['anomaly_id']}/reviews", json={
            "action": "accept_evidence", "reviewer": "editor",
            "reason": "Bulletin corroborates", "expected_version": updated["version"]})
        self.assertEqual(accepted.json()["state"], "supplemented")

    def test_unknown_config_returns_404(self):
        response = self.client.get("/anomaly/config/rail-a/missing")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"], "unknown_config")


if __name__ == "__main__":
    unittest.main()
