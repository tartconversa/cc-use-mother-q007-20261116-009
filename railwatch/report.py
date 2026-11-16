"""铁路新闻证据系统：Derive auditable operational reports from selected claims and frozen snapshots."""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from .domain import ProductError, day, token
from .service import RailwayService


class Reports:
    def __init__(self, service: RailwayService):
        self.service = service

    def current(self, project_key=None, kinds=None, start=None, end=None):
        if project_key:
            project_key = token(project_key, "project_key")
        if start:
            start = day(start)
        if end:
            end = day(end)
        if start and end and start > end:
            raise ProductError("invalid_period", "start follows end")
        kinds = self._kinds(kinds)
        scopes = self.service.scopes(project_key)
        selected, unresolved = self._filter(scopes, kinds, start, end)
        return self._summarize(selected, unresolved, "current")

    def snapshot(self, snapshot_id, project_key=None, kinds=None, start=None, end=None):
        snapshot = self.service.snapshot(snapshot_id)
        if project_key:
            project_key = token(project_key, "project_key")
        if start:
            start = day(start)
        if end:
            end = day(end)
        if start and end and start > end:
            raise ProductError("invalid_period", "start follows end")
        kinds = self._kinds(kinds)
        scopes = snapshot["content"]
        if project_key:
            scopes = [row for row in scopes if row["project_key"] == project_key]
        selected, unresolved = self._filter(scopes, kinds, start, end)
        report = self._summarize(selected, unresolved, "snapshot")
        report["snapshot_id"] = snapshot_id
        report["snapshot_digest"] = snapshot["digest"]
        return report

    @staticmethod
    def _kinds(kinds):
        if kinds is None:
            return None
        if isinstance(kinds, str):
            kinds = kinds.split(",")
        allowed = {"freight", "train", "container", "milestone", "capacity"}
        if not isinstance(kinds, (list, tuple)) or not kinds or not set(kinds) <= allowed:
            raise ProductError("invalid_kinds", "unsupported report kind")
        return set(kinds)

    @staticmethod
    def _filter(scopes, kinds, start, end):
        selected, unresolved = [], []
        for scope in scopes:
            if kinds and scope["kind"] not in kinds:
                continue
            if start and scope["period"] < start:
                continue
            if end and scope["period"] > end:
                continue
            if scope["conflict"] or not scope.get("selected"):
                unresolved.append({"project_key": scope["project_key"], "kind": scope["kind"],
                                   "metric": scope["metric"], "period": scope["period"],
                                   "version": scope["version"], "conflict": bool(scope["conflict"])})
            else:
                selected.append(scope)
        return selected, unresolved

    def _summarize(self, selected, unresolved, origin):
        numeric = defaultdict(list)
        milestones = defaultdict(list)
        provenance = []
        for scope in selected:
            claim = scope["selected"]
            key = (scope["project_key"], scope["kind"], scope["metric"], claim["unit"])
            if scope["kind"] == "milestone":
                milestones[(scope["project_key"], scope["metric"])].append({
                    "period": scope["period"], "status": claim["milestone"],
                    "claim_id": claim["claim_id"]})
            else:
                numeric[key].append({"period": scope["period"], "amount": Decimal(claim["amount"]),
                                     "claim_id": claim["claim_id"]})
            provenance.append({"claim_id": claim["claim_id"], "project_key": scope["project_key"],
                               "period": scope["period"], "source_id": claim["source_id"],
                               "url": claim["url"], "quote": claim["quote"]})
        series = []
        for (project, kind, metric, unit), values in sorted(numeric.items()):
            values.sort(key=lambda value: value["period"])
            series.append({"project_key": project, "kind": kind, "metric": metric, "unit": unit,
                           "points": [{"period": value["period"], "amount": format(value["amount"], "f"),
                                       "claim_id": value["claim_id"]} for value in values],
                           "latest": format(values[-1]["amount"], "f")})
        milestone_rows = []
        for (project, metric), values in sorted(milestones.items()):
            values.sort(key=lambda value: value["period"])
            milestone_rows.append({"project_key": project, "metric": metric,
                                   "history": values, "latest": values[-1]})
        return {"origin": origin, "selected_count": len(selected),
                "unresolved_count": len(unresolved), "series": series,
                "milestones": milestone_rows, "unresolved": unresolved,
                "provenance": sorted(provenance, key=lambda row: (row["project_key"], row["period"], row["claim_id"]))}

    def compare_snapshots(self, left_id, right_id):
        left = self.service.snapshot(left_id)
        right = self.service.snapshot(right_id)
        def keyed(snapshot):
            return {(s["project_key"], s["kind"], s["metric"], s["period"]): s for s in snapshot["content"]}
        before, after = keyed(left), keyed(right)
        keys = sorted(set(before) | set(after))
        changes = []
        for key in keys:
            old, new = before.get(key), after.get(key)
            if old == new:
                continue
            changes.append({"key": key,
                            "before": old["selected"] if old else None,
                            "after": new["selected"] if new else None,
                            "before_version": old["version"] if old else None,
                            "after_version": new["version"] if new else None})
        return {"left_id": left_id, "right_id": right_id,
                "left_digest": left["digest"], "right_digest": right["digest"],
                "changed_scopes": changes, "change_count": len(changes)}
