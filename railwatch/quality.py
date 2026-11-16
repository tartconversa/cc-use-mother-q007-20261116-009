"""铁路新闻证据系统：Source quality diagnostics before operators approve evidence."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal

from .domain import ProductError, token
from .service import RailwayService


class Quality:
    def __init__(self, service: RailwayService):
        self.service = service

    def source_health(self, source_id):
        source = self.service.store.source(source_id)
        captures = self.service.store.all("SELECT id,external_id,state,created_at,published_at,policy_version FROM captures WHERE source_id=? ORDER BY id", (source_id,))
        jobs = self.service.store.all("SELECT id,state,attempts,last_error,last_capture_id FROM fetch_jobs WHERE source_id=? ORDER BY id", (source_id,))
        states = Counter(row["state"] for row in captures)
        failures = Counter(row["last_error"] for row in jobs if row["last_error"])
        stale_policy = [row["id"] for row in captures if row["policy_version"] != source["version"]]
        return {"source_id": source_id, "active": source["active"], "policy_version": source["version"],
                "captures": len(captures), "capture_states": dict(states), "jobs": len(jobs),
                "job_failures": dict(failures), "stale_policy_capture_ids": stale_policy,
                "latest_capture_id": captures[-1]["id"] if captures else None}

    def claim_quality(self, project_key=None):
        if project_key:
            project_key = token(project_key, "project_key")
            claims = self.service.store.all("SELECT c.*,a.source_id,a.state AS capture_state FROM claims c JOIN captures a ON a.id=c.capture_id WHERE c.project_key=? ORDER BY c.id", (project_key,))
        else:
            claims = self.service.store.all("SELECT c.*,a.source_id,a.state AS capture_state FROM claims c JOIN captures a ON a.id=c.capture_id ORDER BY c.id")
        states = Counter(row["state"] for row in claims)
        by_source = defaultdict(lambda: Counter())
        for claim in claims:
            by_source[claim["source_id"]][claim["state"]] += 1
        scopes = self.service.scopes(project_key)
        conflicts = [self._scope_key(scope) for scope in scopes if scope["conflict"]]
        missing = [self._scope_key(scope) for scope in scopes if not scope["selected"]]
        retracted = [row["id"] for row in claims if row["state"] == "retracted"]
        return {"project_key": project_key, "total_claims": len(claims), "states": dict(states),
                "by_source": {key: dict(value) for key, value in sorted(by_source.items())},
                "scopes": len(scopes), "conflicts": conflicts, "unselected": missing,
                "retracted_claim_ids": retracted}

    def stale_reviews(self, project_key=None):
        scopes = self.service.scopes(project_key)
        rows = []
        for scope in scopes:
            selected = scope["selected"]
            if not selected:
                continue
            claim = self.service.store.claim(selected["claim_id"])
            capture = self.service.store.capture(claim["capture_id"])
            policy = self.service.store.source(capture["source_id"])
            reasons = []
            if not policy["active"]:
                reasons.append("source_disabled")
            if capture["policy_version"] != policy["version"]:
                reasons.append("policy_changed")
            if capture["state"] != "parsed":
                reasons.append("capture_not_current")
            if claim["state"] != "accepted":
                reasons.append("claim_not_accepted")
            if reasons:
                rows.append({"claim_id": claim["id"], "scope": self._scope_key(scope),
                             "reasons": reasons, "policy_version": policy["version"],
                             "capture_policy_version": capture["policy_version"]})
        return rows

    def publication_readiness(self, project_key=None):
        scopes = self.service.scopes(project_key)
        unresolved = [self._scope_key(scope) for scope in scopes if scope["conflict"] or not scope["selected"]]
        stale = self.stale_reviews(project_key)
        return {"ready": bool(scopes) and not unresolved and not stale,
                "scope_count": len(scopes), "unresolved": unresolved, "stale_reviews": stale}

    def duplicate_external_ids(self, source_id):
        self.service.store.source(source_id)
        captures = self.service.store.all("SELECT id,external_id,content_hash,state FROM captures WHERE source_id=? ORDER BY external_id,id", (source_id,))
        grouped = defaultdict(list)
        for row in captures:
            grouped[row["external_id"]].append(row)
        return [{"external_id": key, "versions": value} for key, value in sorted(grouped.items()) if len(value) > 1]

    def recent_audit(self, source_id, limit=100):
        self.service.store.source(source_id)
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ProductError("invalid_limit", "limit must be 1..500")
        ids = {str(row["id"]) for row in self.service.store.all("SELECT id FROM captures WHERE source_id=?", (source_id,))}
        events = self.service.audit(0, 500)
        return [event for event in events if event["subject"] in ids][-limit:]

    def outliers(self, project_key, metric, ratio="3"):
        project_key, metric = token(project_key, "project_key"), token(metric, "metric")
        try:
            ratio = Decimal(str(ratio))
        except Exception as exc:
            raise ProductError("invalid_ratio", "ratio must be numeric") from exc
        if not ratio.is_finite() or ratio <= 1 or ratio > 100:
            raise ProductError("invalid_ratio", "ratio must be within (1,100]")
        scopes = [scope for scope in self.service.scopes(project_key, False) if scope["metric"] == metric]
        grouped = defaultdict(list)
        for scope in scopes:
            if scope["kind"] == "milestone":
                continue
            key = (scope["kind"], scope["selected"]["unit"])
            grouped[key].append(scope)
        result = []
        for key, rows in grouped.items():
            rows.sort(key=lambda row: row["period"])
            for previous, current in zip(rows, rows[1:]):
                before, after = Decimal(previous["selected"]["amount"]), Decimal(current["selected"]["amount"])
                if before == 0:
                    flagged = after > 0
                else:
                    flagged = after / before > ratio or before / max(after, Decimal("0.000000001")) > ratio
                if flagged:
                    result.append({"project_key": project_key, "metric": metric, "kind": key[0],
                                   "unit": key[1], "before_period": previous["period"],
                                   "after_period": current["period"], "before_amount": str(before),
                                   "after_amount": str(after), "claim_ids": [previous["selected"]["claim_id"], current["selected"]["claim_id"]]})
        return result

    @staticmethod
    def _scope_key(scope):
        return {key: scope[key] for key in ("project_key", "kind", "metric", "period")}
