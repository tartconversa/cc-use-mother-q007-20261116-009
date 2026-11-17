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
                                 "kinds": ["freight", "milestone"], "max_age_days": 3650,
                                 "trust_rank": 10, "version": 1, "active": True})
        # 量级跳变阈值按 (项目, 指标) 版本化配置。
        config = service.anomalies.put_config(
            {"project_key": "route_demo", "metric": "cumulative_cargo", "ratio": "3"})

        def freight_capture(external_id, amount, period, published):
            quote = f"The synthetic route carried {amount} tonnes through the period ending {period}."
            claim = {"project_key": "route_demo", "kind": "freight", "metric": "cumulative_cargo",
                     "period": period, "amount": str(amount), "unit": "tonne", "quote": quote}
            return {"source_id": "demo_publisher", "external_id": external_id,
                    "url": f"https://example.org/news/{external_id}",
                    "fetched_at": published, "published_at": published,
                    "title": "Synthetic railway operations example",
                    "body": json.dumps({"title": "Synthetic railway operations example",
                                        "text": quote, "claims": [claim]}),
                    "media_type": "application/json", "metadata": {"origin": "local-synthetic"}}

        def accept(capture, expected_version=0):
            result = service.replay(capture)
            review = service.review(result["claim_ids"][0], {
                "decision": "accepted", "reviewer": "demo_reviewer",
                "reason": "Quote matches the synthetic publisher text",
                "expected_version": expected_version})
            return result, review

        first, review = accept(freight_capture("story-1", 342, "2026-09-30", "2026-10-15T08:00:00Z"))
        repeat = service.replay(freight_capture("story-1", 342, "2026-09-30", "2026-10-15T08:00:00Z"))
        if not first["created"] or repeat["created"] or first["claim_ids"] != repeat["claim_ids"]:
            raise AssertionError("capture replay must be idempotent")
        if review["scope"]["selected_claim"] != first["claim_ids"][0]:
            raise AssertionError("accepted evidence was not selected")

        # 新期间出现 10 倍量级跳变：自动立案，且阻止当前指标发布。
        accept(freight_capture("story-2", 3420, "2026-10-31", "2026-11-15T08:00:00Z"))
        queue = service.anomalies.queue("route_demo", "cumulative_cargo", "open")
        if len(queue) != 1 or not queue[0]["blocks_publication"]:
            raise AssertionError("order-of-magnitude jump must open a blocking review item")
        anomaly_id = queue[0]["anomaly_id"]
        try:
            service.publish({"name": "blocked_release", "project_key": "route_demo"})
        except Exception as exc:  # noqa: BLE001 - demo asserts the business error code
            if getattr(exc, "code", None) != "anomaly_unresolved":
                raise
        else:
            raise AssertionError("publication must be blocked while the anomaly is open")

        # 复核人确认这是新线投运带来的合理跳变。
        decided = service.anomalies.review(anomaly_id, {
            "action": "confirm", "reviewer": "demo_editor",
            "reason": "New high-speed line opened; the tenfold jump is documented",
            "expected_version": queue[0]["version"]})
        if decided["state"] != "confirmed" or decided["blocks_publication"]:
            raise AssertionError("confirmed anomaly must release the publication block")

        released = service.publish({"name": "route_demo_release", "project_key": "route_demo"})
        report = Reports(service).snapshot(released["snapshot_id"], "route_demo")
        quality = Quality(service).publication_readiness("route_demo")
        if report["selected_count"] != 2 or report["series"][0]["latest"] != "3420":
            raise AssertionError("snapshot report lost selected evidence")
        if len(report["anomalies"]) != 1 or report["anomalies"][0]["state"] != "confirmed":
            raise AssertionError("snapshot report must carry the frozen anomaly explanation")
        if quality["ready"] is not True:
            raise AssertionError("reviewed route must be publishable after the anomaly is resolved")
        print(json.dumps({"capture_id": first["capture_id"], "idempotent_replay": True,
                          "claim_id": first["claim_ids"][0], "scope_version": review["scope"]["version"],
                          "anomaly_config_version": config["version"], "anomaly_id": anomaly_id,
                          "observed_ratio": queue[0]["observed_ratio"],
                          "anomaly_state": decided["state"],
                          "snapshot_id": released["snapshot_id"], "digest": released["digest"],
                          "latest_tonnes": report["series"][0]["latest"],
                          "frozen_anomaly_states": [item["state"] for item in report["anomalies"]],
                          "provenance_count": len(report["provenance"]), "ready": quality["ready"]},
                         ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
