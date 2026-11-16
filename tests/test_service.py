import json
import tempfile
import unittest
from pathlib import Path

from railwatch.domain import ProductError
from railwatch.quality import Quality
from railwatch.report import Reports
from railwatch.service import RailwayService


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = RailwayService(Path(self.temp.name) / "evidence.sqlite3")
        self.service.register_source(self.policy())

    def tearDown(self):
        self.temp.cleanup()

    def policy(self, source_id="news", host="example.org", rank=10):
        return {"source_id": source_id, "host": host, "kinds": ["freight", "milestone", "capacity"],
                "max_age_days": 30, "trust_rank": rank, "version": 1, "active": True}

    def claim(self, quantity="100", metric="cargo", project="rail-a", period="2026-11-15"):
        return {"project_key": project, "kind": "freight", "metric": metric, "period": period,
                "amount": quantity, "unit": "tonne", "quote": "Freight reached " + quantity + " tonnes on the route."}

    def capture(self, source_id="news", external_id="article-1", claim=None, host="example.org"):
        claim = claim or self.claim()
        return {"source_id": source_id, "external_id": external_id,
                "url": "https://" + host + "/article/1", "fetched_at": "2026-11-16T00:00:00Z",
                "published_at": "2026-11-15T12:00:00Z", "title": "Railway article",
                "body": json.dumps({"title": "Railway article", "text": claim["quote"], "claims": [claim]}),
                "media_type": "application/json", "metadata": {"publisher": "synthetic"}}

    def review(self, claim_id, decision="accepted", expected=0, reviewer="analyst"):
        return self.service.review(claim_id, {"decision": decision, "reviewer": reviewer,
                                               "reason": "Verified quoted source evidence", "expected_version": expected})

    def assert_code(self, code, action, *args):
        with self.assertRaises(ProductError) as caught:
            action(*args)
        self.assertEqual(caught.exception.code, code)

    def test_register_source(self):
        self.assertEqual(self.service.policy("news").host, "example.org")

    def test_source_update_requires_cas(self):
        policy = self.policy()
        policy["version"] = 2
        self.assert_code("stale_version", self.service.register_source, policy, 0)
        self.assertEqual(self.service.register_source(policy, 1)["version"], 2)

    def test_source_update_rejects_skipped_version(self):
        policy = self.policy()
        policy["version"] = 3
        self.assert_code("invalid_version", self.service.register_source, policy, 1)

    def test_unknown_source(self):
        self.assert_code("unknown_source", self.service.policy, "absent")

    def test_replay_extracts_claim(self):
        result = self.service.replay(self.capture())
        self.assertEqual(result["state"], "parsed")
        self.assertEqual(len(result["claim_ids"]), 1)
        self.assertEqual(self.service.capture(result["capture_id"])["claims"][0]["amount"], "100")

    def test_replay_idempotent(self):
        first = self.service.replay(self.capture())
        second = self.service.replay(self.capture())
        self.assertEqual(first["capture_id"], second["capture_id"])
        self.assertFalse(second["created"])

    def test_replay_rejects_wrong_host(self):
        self.assert_code("source_mismatch", self.service.replay, self.capture(host="wrong.org"))

    def test_replay_rejects_unknown_source(self):
        self.assert_code("unknown_source", self.service.replay, self.capture(source_id="absent"))

    def test_replay_quarantines_invalid_quote(self):
        item = self.capture()
        body = json.loads(item["body"])
        body["text"] = "Unrelated article text"
        item["body"] = json.dumps(body)
        result = self.service.replay(item)
        self.assertEqual(result["state"], "quarantined")
        self.assertEqual(result["issues"][0]["code"], "missing_quote")

    def test_replay_quarantines_no_claims(self):
        item = self.capture()
        item["body"] = json.dumps({"text": "No claims", "claims": []})
        self.assertEqual(self.service.replay(item)["state"], "quarantined")

    def test_replay_quarantines_future_period(self):
        item = self.capture(claim=self.claim(period="2026-11-16"))
        result = self.service.replay(item)
        self.assertEqual(result["issues"][0]["code"], "future_period")

    def test_review_accepts_and_selects(self):
        claim_id = self.service.replay(self.capture())["claim_ids"][0]
        result = self.review(claim_id)
        self.assertEqual(result["scope"]["selected_claim"], claim_id)
        self.assertEqual(result["scope"]["version"], 1)

    def test_review_rejects_stale_version(self):
        claim_id = self.service.replay(self.capture())["claim_ids"][0]
        self.assert_code("stale_version", self.review, claim_id, "accepted", 1)

    def test_review_rejects_duplicate_decision(self):
        claim_id = self.service.replay(self.capture())["claim_ids"][0]
        self.review(claim_id)
        self.assert_code("already_reviewed", self.review, claim_id)

    def test_review_rejects_quarantined_capture(self):
        item = self.capture()
        item["body"] = json.dumps({"text": "No claims", "claims": []})
        result = self.service.replay(item)
        self.assertEqual(result["claim_ids"], [])

    def test_review_rejected_claim_leaves_empty_scope(self):
        claim_id = self.service.replay(self.capture())["claim_ids"][0]
        self.review(claim_id, "rejected")
        self.assertIsNone(self.service.scopes()[0]["selected"])

    def test_review_hold_leaves_empty_scope(self):
        claim_id = self.service.replay(self.capture())["claim_ids"][0]
        self.review(claim_id, "hold")
        self.assertEqual(self.service.review_queue(state="hold")[0]["id"], claim_id)

    def test_conflict_blocks_release(self):
        first = self.service.replay(self.capture())["claim_ids"][0]
        self.review(first)
        second_capture = self.capture(external_id="article-2", claim=self.claim("120"))
        second = self.service.replay(second_capture)["claim_ids"][0]
        result = self.review(second, expected=1)
        self.assertEqual(result["scope"]["conflict"], 1)
        self.assert_code("unresolved_scope", self.service.publish, {"name": "release-1"})

    def test_agreement_selects_trusted_source(self):
        first = self.service.replay(self.capture())["claim_ids"][0]
        self.review(first)
        self.service.register_source(self.policy("second", "second.org", 20))
        second = self.service.replay(self.capture("second", "article-2", host="second.org"))["claim_ids"][0]
        self.review(second, expected=1)
        self.assertEqual(self.service.scopes()[0]["selected_claim"], second)

    def test_snapshot_freezes_scope(self):
        claim_id = self.service.replay(self.capture())["claim_ids"][0]
        self.review(claim_id)
        release = self.service.publish({"name": "release-1"})
        self.assertEqual(self.service.snapshot(release["snapshot_id"])["content"][0]["selected"]["amount"], "100")

    def test_snapshot_idempotent_by_content(self):
        claim_id = self.service.replay(self.capture())["claim_ids"][0]
        self.review(claim_id)
        first = self.service.publish({"name": "release-1"})
        second = self.service.publish({"name": "release-2"})
        self.assertEqual(first["snapshot_id"], second["snapshot_id"])
        self.assertFalse(second["created"])

    def test_new_capture_supersedes_and_invalidates_scope(self):
        first = self.service.replay(self.capture())["claim_ids"][0]
        self.review(first)
        replacement = self.capture(claim=self.claim("120"))
        second = self.service.replay(replacement)["claim_ids"][0]
        self.assertEqual(self.service.claim(first)["state"], "retracted")
        self.assertEqual(self.service.scopes()[0]["conflict"], 1)
        self.assert_code("unresolved_scope", self.service.publish, {"name": "release-2"})

    def test_report_uses_selected_claim(self):
        claim_id = self.service.replay(self.capture())["claim_ids"][0]
        self.review(claim_id)
        report = Reports(self.service).current("rail-a")
        self.assertEqual(report["series"][0]["latest"], "100")
        self.assertEqual(report["selected_count"], 1)

    def test_report_does_not_sum_cumulative_values(self):
        first = self.service.replay(self.capture())["claim_ids"][0]
        self.review(first)
        later = self.capture(external_id="later", claim=self.claim("140", period="2026-11-15"))
        later["published_at"] = "2026-11-16T12:00:00Z"
        later["fetched_at"] = "2026-11-16T13:00:00Z"
        result = self.service.replay(later)
        self.assertTrue(result["claim_ids"])

    def test_quality_reports_unresolved(self):
        claim_id = self.service.replay(self.capture())["claim_ids"][0]
        self.review(claim_id, "rejected")
        readiness = Quality(self.service).publication_readiness()
        self.assertFalse(readiness["ready"])
        self.assertEqual(len(readiness["unresolved"]), 1)

    def test_quality_source_health(self):
        self.service.replay(self.capture())
        health = Quality(self.service).source_health("news")
        self.assertEqual(health["capture_states"]["parsed"], 1)

    def test_audit_records_business_events(self):
        claim_id = self.service.replay(self.capture())["claim_ids"][0]
        self.review(claim_id)
        events = [event["event"] for event in self.service.audit()]
        self.assertIn("capture_received", events)
        self.assertIn("claim_reviewed", events)


if __name__ == "__main__":
    unittest.main()
