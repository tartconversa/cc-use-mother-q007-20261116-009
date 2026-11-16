"""铁路新闻证据系统：Offline, deterministic walkthrough of the evidence publication workflow."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from .quality import Quality
from .report import Reports
from .service import RailwayService


def main():
    with tempfile.TemporaryDirectory() as temporary:
        service = RailwayService(Path(temporary) / "rail.sqlite3")
        service.register_source({"source_id": "demo_publisher", "host": "example.org",
                                 "kinds": ["freight", "milestone"], "max_age_days": 30,
                                 "trust_rank": 10, "version": 1, "active": True})
        quote = "The synthetic route carried 342 tonnes through October."
        claim = {"project_key": "route_demo", "kind": "freight", "metric": "cumulative_cargo",
                 "period": "2026-10-31", "amount": "342", "unit": "tonne", "quote": quote}
        capture = {"source_id": "demo_publisher", "external_id": "story-1",
                   "url": "https://example.org/news/route-1",
                   "fetched_at": "2026-11-16T08:00:00Z",
                   "published_at": "2026-11-15T08:00:00Z",
                   "title": "Synthetic railway operations example",
                   "body": json.dumps({"title": "Synthetic railway operations example",
                                       "text": quote, "claims": [claim]}),
                   "media_type": "application/json", "metadata": {"origin": "local-synthetic"}}
        first = service.replay(capture)
        repeat = service.replay(capture)
        if not first["created"] or repeat["created"] or first["claim_ids"] != repeat["claim_ids"]:
            raise AssertionError("capture replay must be idempotent")
        review = service.review(first["claim_ids"][0], {"decision": "accepted",
                                                       "reviewer": "demo_reviewer",
                                                       "reason": "Quote matches the synthetic publisher text",
                                                       "expected_version": 0})
        if review["scope"]["selected_claim"] != first["claim_ids"][0]:
            raise AssertionError("accepted evidence was not selected")
        released = service.publish({"name": "route_demo_release", "project_key": "route_demo"})
        report = Reports(service).snapshot(released["snapshot_id"], "route_demo")
        quality = Quality(service).publication_readiness("route_demo")
        if report["selected_count"] != 1 or report["series"][0]["latest"] != "342":
            raise AssertionError("snapshot report lost selected evidence")
        if quality["ready"] is not True:
            raise AssertionError("reviewed route must be publishable")
        print(json.dumps({"capture_id": first["capture_id"], "idempotent_replay": True,
                          "claim_id": first["claim_ids"][0], "scope_version": review["scope"]["version"],
                          "snapshot_id": released["snapshot_id"], "digest": released["digest"],
                          "latest_tonnes": report["series"][0]["latest"],
                          "provenance_count": len(report["provenance"]), "ready": quality["ready"]},
                         ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
