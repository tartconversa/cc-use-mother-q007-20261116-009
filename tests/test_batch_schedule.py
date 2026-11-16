import json
import tempfile
import unittest
from pathlib import Path

from railwatch.batch import BatchRunner
from railwatch.domain import ProductError
from railwatch.schedule import Schedule
from railwatch.service import RailwayService


class BatchScheduleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.service = RailwayService(self.root / "db.sqlite3")
        self.service.register_source({"source_id": "news", "host": "example.org",
                                      "kinds": ["freight"], "max_age_days": 30,
                                      "trust_rank": 10, "version": 1, "active": True})
        self.batch = BatchRunner(self.service)
        self.schedule = Schedule(self.service.store)

    def tearDown(self):
        self.temp.cleanup()

    def claim(self, amount="100"):
        return {"project_key": "rail-a", "kind": "freight", "metric": "cargo",
                "period": "2026-11-15", "amount": amount, "unit": "tonne",
                "quote": "Freight reached " + amount + " tonnes today."}

    def capture(self, external_id="article-1", amount="100"):
        claim = self.claim(amount)
        return {"source_id": "news", "external_id": external_id,
                "url": "https://example.org/rail", "fetched_at": "2026-11-16T00:00:00Z",
                "published_at": "2026-11-15T12:00:00Z", "title": "Rail report",
                "body": json.dumps({"text": claim["quote"], "claims": [claim]}),
                "media_type": "application/json"}

    def assert_code(self, code, action, *args):
        with self.assertRaises(ProductError) as caught:
            action(*args)
        self.assertEqual(caught.exception.code, code)

    def test_batch_processes_multiple_captures(self):
        result = self.batch.run({"captures": [self.capture("a"), self.capture("b")]})
        self.assertEqual(result["next_offset"], 2)
        self.assertEqual([x["status"] for x in result["outcomes"]], ["parsed", "parsed"])

    def test_batch_is_idempotent_on_replay(self):
        manifest = {"captures": [self.capture()]}
        self.batch.run(manifest)
        result = self.batch.run(manifest)
        self.assertEqual(result["outcomes"][0]["status"], "duplicate")

    def test_batch_keeps_per_item_error(self):
        manifest = {"captures": [self.capture(), {"external_id": "broken"}, self.capture("next")]}
        result = self.batch.run(manifest)
        self.assertEqual([x["status"] for x in result["outcomes"]], ["parsed", "error", "parsed"])

    def test_batch_stop_on_error(self):
        result = self.batch.run({"captures": [{"bad": True}, self.capture()]}, stop_on_error=True)
        self.assertEqual(result["processed"] if "processed" in result else len(result["outcomes"]), 1)
        self.assertEqual(result["next_offset"], 1)

    def test_batch_offset_and_limit(self):
        manifest = {"captures": [self.capture("a"), self.capture("b"), self.capture("c")]}
        result = self.batch.run(manifest, offset=1, limit=1)
        self.assertEqual(result["outcomes"][0]["index"], 1)
        self.assertEqual(result["next_offset"], 2)

    def test_batch_rejects_negative_offset(self):
        self.assert_code("invalid_offset", self.batch.run, {"captures": []}, -1)

    def test_batch_rejects_bad_limit(self):
        self.assert_code("invalid_limit", self.batch.run, {"captures": []}, 0, 0)

    def test_manifest_loads_from_disk(self):
        path = self.root / "manifest.json"
        path.write_text(json.dumps({"captures": [self.capture()]}), encoding="utf-8")
        self.assertEqual(self.batch.load_manifest(path)["captures"][0]["external_id"], "article-1")

    def test_manifest_rejects_invalid_json(self):
        path = self.root / "manifest.json"
        path.write_text("{", encoding="utf-8")
        self.assert_code("manifest_json", self.batch.load_manifest, path)

    def test_checkpoint_resume(self):
        path, checkpoint = self.root / "manifest.json", self.root / "checkpoint.json"
        path.write_text(json.dumps({"captures": [self.capture("a"), self.capture("b")]}), encoding="utf-8")
        first = self.batch.resume(path, checkpoint, limit=1)
        second = self.batch.resume(path, checkpoint, limit=1)
        self.assertEqual(first["next_offset"], 1)
        self.assertEqual(second["next_offset"], 2)
        self.assertTrue(second["complete"])

    def test_checkpoint_rejects_manifest_change(self):
        path, checkpoint = self.root / "manifest.json", self.root / "checkpoint.json"
        path.write_text(json.dumps({"captures": [self.capture("a")]}), encoding="utf-8")
        self.batch.resume(path, checkpoint)
        path.write_text(json.dumps({"captures": [self.capture("b")]}), encoding="utf-8")
        self.assert_code("checkpoint_mismatch", self.batch.resume, path, checkpoint)

    def test_schedule_create_job(self):
        job = self.schedule.upsert("news", "article-1", "https://example.org/rail", "2026-11-16T00:00:00Z", 3600)
        self.assertEqual(job["state"], "ready")
        self.assertEqual(job["interval_seconds"], 3600)

    def test_schedule_rejects_wrong_host(self):
        self.assert_code("source_mismatch", self.schedule.upsert, "news", "article-1",
                         "https://wrong.org/rail", "2026-11-16T00:00:00Z", 3600)

    def test_schedule_rejects_short_interval(self):
        self.assert_code("invalid_interval", self.schedule.upsert, "news", "article-1",
                         "https://example.org/rail", "2026-11-16T00:00:00Z", 1)

    def test_lease_due_job(self):
        self.schedule.upsert("news", "article-1", "https://example.org/rail", "2026-11-16T00:00:00Z", 3600)
        job = self.schedule.lease("worker-a", "2026-11-16T00:00:00Z")
        self.assertEqual(job["lease_owner"], "worker-a")
        self.assertEqual(job["attempts"], 1)

    def test_lease_none_before_due(self):
        self.schedule.upsert("news", "article-1", "https://example.org/rail", "2026-11-17T00:00:00Z", 3600)
        self.assertIsNone(self.schedule.lease("worker-a", "2026-11-16T00:00:00Z"))

    def test_lease_excludes_second_worker(self):
        self.schedule.upsert("news", "article-1", "https://example.org/rail", "2026-11-16T00:00:00Z", 3600)
        self.schedule.lease("worker-a", "2026-11-16T00:00:00Z")
        self.assertIsNone(self.schedule.lease("worker-b", "2026-11-16T00:00:01Z"))

    def test_lease_recovers_after_expiry(self):
        self.schedule.upsert("news", "article-1", "https://example.org/rail", "2026-11-16T00:00:00Z", 3600)
        self.schedule.lease("worker-a", "2026-11-16T00:00:00Z", 10)
        result = self.schedule.lease("worker-b", "2026-11-16T00:00:11Z")
        self.assertEqual(result["lease_owner"], "worker-b")

    def test_complete_rejects_wrong_owner(self):
        self.schedule.upsert("news", "article-1", "https://example.org/rail", "2026-11-16T00:00:00Z", 3600)
        job = self.schedule.lease("worker-a", "2026-11-16T00:00:00Z")
        self.assert_code("lease_lost", self.schedule.complete, job["id"], "worker-b", None, None, None, "2026-11-16T00:00:01Z")

    def test_complete_schedules_next_run(self):
        self.schedule.upsert("news", "article-1", "https://example.org/rail", "2026-11-16T00:00:00Z", 3600)
        job = self.schedule.lease("worker-a", "2026-11-16T00:00:00Z")
        done = self.schedule.complete(job["id"], "worker-a", at="2026-11-16T00:00:01Z")
        self.assertEqual(done["next_run"], "2026-11-16T01:00:01+00:00")

    def test_failure_sets_retry(self):
        self.schedule.upsert("news", "article-1", "https://example.org/rail", "2026-11-16T00:00:00Z", 3600)
        job = self.schedule.lease("worker-a", "2026-11-16T00:00:00Z")
        failed = self.schedule.fail(job["id"], "worker-a", "fetch_network", "2026-11-16T00:00:01Z")
        self.assertEqual(failed["state"], "ready")
        self.assertEqual(failed["last_error"], "fetch_network")

    def test_pause_and_resume(self):
        job = self.schedule.upsert("news", "article-1", "https://example.org/rail", "2026-11-16T00:00:00Z", 3600)
        self.assertEqual(self.schedule.pause(job["id"])["state"], "paused")
        self.assertEqual(self.schedule.resume(job["id"])["state"], "ready")

    def test_pause_leased_job_rejected(self):
        self.schedule.upsert("news", "article-1", "https://example.org/rail", "2026-11-16T00:00:00Z", 3600)
        job = self.schedule.lease("worker-a", "2026-11-16T00:00:00Z")
        self.assert_code("job_leased", self.schedule.pause, job["id"])


if __name__ == "__main__":
    unittest.main()
