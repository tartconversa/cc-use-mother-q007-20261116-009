import json
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from railwatch.api import create_app
from railwatch.domain import Capture, ProductError, SourcePolicy
from railwatch.extract import Extractor


class ExtractApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.client = TestClient(create_app(Path(self.temp.name) / "api.sqlite3"))
        self.policy = {"source_id": "news", "host": "example.org",
                       "kinds": ["freight", "milestone"], "max_age_days": 30,
                       "trust_rank": 10, "version": 1, "active": True}
        self.client.put("/sources/news", json=self.policy)

    def tearDown(self):
        self.client.close()
        self.temp.cleanup()

    def claim(self, amount="100"):
        return {"project_key": "rail-a", "kind": "freight", "metric": "cargo",
                "period": "2026-11-15", "amount": amount, "unit": "tonne",
                "quote": "Freight reached " + amount + " tonnes today."}

    def capture(self, body=None, media_type="application/json"):
        claim = self.claim()
        if body is None:
            body = json.dumps({"text": claim["quote"], "claims": [claim]})
        return {"source_id": "news", "external_id": "article-1",
                "url": "https://example.org/rail", "fetched_at": "2026-11-16T00:00:00Z",
                "published_at": "2026-11-15T12:00:00Z", "title": "Rail report",
                "body": body, "media_type": media_type}

    def test_health(self):
        self.assertEqual(self.client.get("/health").json()["status"], "ok")

    def test_source_endpoint(self):
        self.assertEqual(self.client.get("/sources/news").json()["host"], "example.org")

    def test_source_list(self):
        self.assertEqual(len(self.client.get("/sources").json()), 1)

    def test_capture_endpoint(self):
        response = self.client.post("/captures", json=self.capture())
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["state"], "parsed")

    def test_capture_list_endpoint(self):
        self.client.post("/captures", json=self.capture())
        self.assertEqual(len(self.client.get("/captures?state=parsed").json()), 1)

    def test_capture_detail_endpoint(self):
        capture_id = self.client.post("/captures", json=self.capture()).json()["capture_id"]
        self.assertEqual(len(self.client.get(f"/captures/{capture_id}").json()["claims"]), 1)

    def test_capture_batch_endpoint(self):
        first = self.capture()
        second = self.capture()
        second["external_id"] = "article-2"
        result = self.client.post("/captures/batch", json={"captures": [first, second]}).json()
        self.assertEqual(result["processed"], 2)

    def test_claim_queue_endpoint(self):
        self.client.post("/captures", json=self.capture())
        self.assertEqual(len(self.client.get("/claims").json()), 1)

    def test_review_endpoint(self):
        claim_id = self.client.post("/captures", json=self.capture()).json()["claim_ids"][0]
        body = {"decision": "accepted", "reviewer": "analyst", "reason": "Checked evidence",
                "expected_version": 0}
        result = self.client.post(f"/claims/{claim_id}/reviews", json=body)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["scope"]["selected_claim"], claim_id)

    def test_snapshot_endpoint(self):
        claim_id = self.client.post("/captures", json=self.capture()).json()["claim_ids"][0]
        self.client.post(f"/claims/{claim_id}/reviews", json={"decision": "accepted", "reviewer": "analyst",
                                                                "reason": "Checked evidence", "expected_version": 0})
        result = self.client.post("/snapshots", json={"name": "release-1"})
        self.assertEqual(result.status_code, 201)
        self.assertEqual(result.json()["scope_count"], 1)

    def test_report_endpoint(self):
        claim_id = self.client.post("/captures", json=self.capture()).json()["claim_ids"][0]
        self.client.post(f"/claims/{claim_id}/reviews", json={"decision": "accepted", "reviewer": "analyst",
                                                                "reason": "Checked evidence", "expected_version": 0})
        report = self.client.get("/reports/current?project_key=rail-a").json()
        self.assertEqual(report["series"][0]["latest"], "100")

    def test_quality_endpoint(self):
        self.client.post("/captures", json=self.capture())
        self.assertEqual(self.client.get("/quality/sources/news").json()["captures"], 1)

    def test_error_status_and_code(self):
        response = self.client.get("/sources/missing")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"], "unknown_source")

    def test_html_visible_text(self):
        item = self.capture("<html><script>ignore</script><p>Actual railway evidence.</p></html>", "text/html")
        self.assertIn("Actual railway evidence", Extractor().visible_text(Capture.parse(item)))
        self.assertNotIn("ignore", Extractor().visible_text(Capture.parse(item)))

    def test_plain_text_without_marker_is_quarantined(self):
        item = self.capture("Freight reached 100 tonnes today.", "text/plain")
        self.assertEqual(self.client.post("/captures", json=item).json()["state"], "quarantined")

    def test_json_duplicate_claim_is_quarantined(self):
        claim = self.claim()
        item = self.capture(json.dumps({"text": claim["quote"], "claims": [claim, claim]}))
        result = self.client.post("/captures", json=item).json()
        self.assertEqual(result["state"], "quarantined")
        self.assertEqual(result["issues"][0]["code"], "duplicate_claim")

    def test_json_bad_item_is_quarantined(self):
        item = self.capture(json.dumps({"text": "Evidence", "claims": [42]}))
        self.assertEqual(self.client.post("/captures", json=item).json()["issues"][0]["code"], "invalid_item")

    def test_json_missing_claims_is_error(self):
        item = self.capture(json.dumps({"text": "Evidence"}))
        response = self.client.post("/captures", json=item)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "missing_claims")

    def test_review_conflict_blocks_snapshot_api(self):
        first = self.client.post("/captures", json=self.capture()).json()["claim_ids"][0]
        self.client.post(f"/claims/{first}/reviews", json={"decision": "accepted", "reviewer": "analyst",
                                                             "reason": "Checked evidence", "expected_version": 0})
        second_item = self.capture()
        second_item["external_id"] = "article-2"
        other = self.claim("120")
        second_item["body"] = json.dumps({"text": other["quote"], "claims": [other]})
        second = self.client.post("/captures", json=second_item).json()["claim_ids"][0]
        self.client.post(f"/claims/{second}/reviews", json={"decision": "accepted", "reviewer": "analyst",
                                                              "reason": "Checked evidence", "expected_version": 1})
        self.assertEqual(self.client.post("/snapshots", json={"name": "release-1"}).status_code, 409)


if __name__ == "__main__":
    unittest.main()
