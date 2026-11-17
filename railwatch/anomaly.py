"""铁路新闻证据系统：把量级跳变检测变成可追踪、可复核、可失效的核查事项。

设计要点：
- 阈值按 (项目, 指标) 版本化存储，检测结果冻结触发时的阈值与观测倍数；
- 每个核查事项锚定跳变前后两条 claim，保留采集、引文、URL 等完整上下文；
- 复核人可确认合理跳变、判定误报，或要求来源补证；补证文章到达后自动关联；
- 未解决事项阻止相应当前指标发布；撤回、阈值变化按明确规则使旧结论失效；
- explain() 是质量视图、复核队列、当前报告与发布快照共用的唯一解释来源。
"""
from __future__ import annotations

from decimal import Decimal

from .domain import ProductError, fingerprint, token
from .service import RailwayService

# 未配置阈值时使用的保守默认值（与历史只读 outliers 接口保持一致）。
DEFAULT_RATIO = Decimal("3")
MAX_RATIO = Decimal("100")

# 仍未解决、会阻止发布的状态。
BLOCKING_STATES = ("open", "evidence_requested", "evidence_received")
# 系统或人工给出的终态。
RESOLVED_STATES = ("confirmed", "dismissed", "supplemented", "config_cleared", "void")
ALL_STATES = BLOCKING_STATES + RESOLVED_STATES

# 复核动作 -> 允许的来源状态 -> 目标状态/结论。
TRANSITIONS = {
    "confirm": (("open", "evidence_requested", "evidence_received"), "confirmed", "confirmed"),
    "dismiss": (("open", "evidence_requested", "evidence_received"), "dismissed", "dismissed"),
    "request_evidence": (("open",), "evidence_requested", "evidence_requested"),
    "accept_evidence": (("evidence_received",), "supplemented", "supplemented"),
}


class AnomalyService:
    def __init__(self, service: RailwayService):
        self.service = service
        self.store = service.store

    # 阈值配置 -----------------------------------------------------------
    def put_config(self, data, expected_version=None):
        project_key = token(data.get("project_key"), "project_key")
        metric = token(data.get("metric"), "metric")
        try:
            ratio = Decimal(str(data.get("ratio", DEFAULT_RATIO)))
        except Exception as exc:
            raise ProductError("invalid_ratio", "ratio must be numeric") from exc
        if not ratio.is_finite() or ratio <= 1 or ratio > MAX_RATIO:
            raise ProductError("invalid_ratio", "ratio must be within (1,100]")
        active = data.get("active", True)
        if type(active) is not bool:
            raise ProductError("invalid_config", "active must be boolean")
        config = self.store.upsert_anomaly_config(project_key, metric, ratio, active, expected_version)
        self.reevaluate(project_key, metric)
        return config

    def get_config(self, project_key, metric):
        return self.store.anomaly_config(token(project_key, "project_key"), token(metric, "metric"))

    def list_configs(self, project_key=None):
        return self.store.anomaly_configs(token(project_key, "project_key") if project_key else None)

    def _resolve_config(self, project_key, metric):
        """返回 (ratio, version, active)；无配置时给默认阈值、版本 0。"""
        row = self.store.anomaly_config(project_key, metric)
        if row is None:
            return DEFAULT_RATIO, 0, True
        return Decimal(row["ratio"]), row["version"], row["active"]

    # 检测 ---------------------------------------------------------------
    def scan(self, project_key=None, metric=None):
        """对当前已选定指标做相邻期间量级跳变扫描，结果幂等。"""
        self.void_stale_anchors()
        if project_key:
            project_key = token(project_key, "project_key")
        if metric:
            metric = token(metric, "metric")
        scopes = self.service.scopes(project_key, include_empty=False)
        groups = {}
        for scope in scopes:
            if scope["kind"] == "milestone":
                continue
            if metric and scope["metric"] != metric:
                continue
            groups.setdefault((scope["project_key"], scope["kind"], scope["metric"],
                               scope["selected"]["unit"]), []).append(scope)
        opened, tracked = [], 0
        for (project, kind, group_metric, unit), rows in sorted(groups.items()):
            ratio, config_version, active = self._resolve_config(project, group_metric)
            if not active:
                continue
            rows.sort(key=lambda row: row["period"])
            for before, after in zip(rows, rows[1:]):
                observed, exceeds, direction = self._exceeds(
                    Decimal(before["selected"]["amount"]), Decimal(after["selected"]["amount"]), ratio)
                if not exceeds:
                    continue
                existing = self.store.anomaly_ident(before["selected"]["claim_id"], after["selected"]["claim_id"])
                if existing is None:
                    anomaly_id = self.store.insert_anomaly(self._new_row(
                        project, kind, group_metric, unit, before, after, observed, ratio, config_version))
                    opened.append(anomaly_id)
                else:
                    tracked += 1
                    if existing["state"] in BLOCKING_STATES and (
                            existing["config_version"] != config_version
                            or Decimal(existing["threshold_ratio"]) != ratio):
                        # 待决事项跟随当前阈值，人工终态不被改写。
                        self.store.update_anomaly(
                            existing["id"], config_version=config_version, threshold_ratio=str(ratio),
                            event="threshold_refreshed",
                            event_detail={"config_version": config_version, "threshold_ratio": str(ratio)})
        return {"scanned_groups": len(groups), "opened": opened, "opened_count": len(opened),
                "tracked_count": tracked, "open_count": len(self.store.anomalies(project_key, metric, "open"))}

    @staticmethod
    def _exceeds(before: Decimal, after: Decimal, ratio: Decimal):
        if before == 0:
            return None, after > 0, "increase"
        if after == 0:
            return None, True, "decrease"
        observed = max(after / before, before / after)
        return format(observed.normalize(), "f"), observed > ratio, (
            "increase" if after > before else "decrease")

    def _new_row(self, project, kind, group_metric, unit, before, after, observed, ratio, config_version):
        context = self._context(before["selected"]["claim_id"], after["selected"]["claim_id"])
        return {"project_key": project, "kind": kind, "metric": group_metric, "unit": unit,
                "before_claim": before["selected"]["claim_id"], "after_claim": after["selected"]["claim_id"],
                "before_period": before["period"], "after_period": after["period"],
                "before_amount": before["selected"]["amount"], "after_amount": after["selected"]["amount"],
                "observed_ratio": observed, "threshold_ratio": str(ratio), "config_version": config_version,
                "state": "open", "context_hash": fingerprint(context)}

    # 采集与撤回钩子 ------------------------------------------------------
    def after_claims_ingested(self, claim_ids):
        """新 claim 入库后：关联补证、清理失效锚点，并对受影响序列补扫。

        补证关联只看序列归属与期间，不要求 claim 已通过审核——复核人需要在
        候选仍处于 proposed 时就能看到来源补交了什么。
        """
        self.void_stale_anchors()
        affected = set()
        for claim_id in claim_ids:
            claim = self.store.claim(claim_id)
            affected.add((claim["project_key"], claim["metric"]))
            for anomaly in self.store.anomalies(claim["project_key"], claim["metric"]):
                if anomaly["state"] not in BLOCKING_STATES:
                    continue
                if not self._same_series(anomaly, claim) or claim["period"] < anomaly["after_period"]:
                    continue
                if anomaly["evidence_claim"] is None:
                    self.store.update_anomaly(
                        anomaly["id"], evidence_claim=claim_id,
                        event="evidence_attached",
                        event_detail={"claim_id": claim_id, "prior_state": anomaly["state"],
                                      "claim_state": claim["state"]})
                    if anomaly["state"] == "evidence_requested":
                        self.store.update_anomaly(
                            anomaly["id"], state="evidence_received",
                            event="evidence_arrived", event_detail={"claim_id": claim_id})
        for project, metric in affected:
            self.scan(project, metric)

    def after_review(self, claim_id):
        """复核落定后：撤回/拒绝的锚点立即作废，新选定的当前指标立即参与检测。"""
        claim = self.store.claim(claim_id)
        self.void_stale_anchors()
        self.scan(claim["project_key"], claim["metric"])

    def void_stale_anchors(self):
        """锚点 claim 被撤回或拒绝后，核查事项自动作废，不再阻止发布。"""
        voided = []
        for anomaly in self.store.anomalies():
            if anomaly["state"] == "void":
                continue
            before = self.store.claim(anomaly["before_claim"])
            after = self.store.claim(anomaly["after_claim"])
            dead = [c["id"] for c in (before, after) if c["state"] in ("retracted", "rejected")]
            if dead:
                self.store.update_anomaly(
                    anomaly["id"], state="void", resolution="void",
                    event="anchor_invalidated", event_detail={"claim_ids": dead})
                voided.append(anomaly["id"])
        return voided

    @staticmethod
    def _same_series(anomaly, claim):
        return (claim["project_key"] == anomaly["project_key"] and claim["kind"] == anomaly["kind"]
                and claim["metric"] == anomaly["metric"] and claim["unit"] == anomaly["unit"])

    # 阈值变化重算规则 ----------------------------------------------------
    def reevaluate(self, project_key, metric):
        """阈值配置变化后：待决且不再超阈的事项自动解除；其余保留，人工结论只标记不覆盖。"""
        ratio, config_version, _active = self._resolve_config(project_key, metric)
        cleared, refreshed = [], []
        for anomaly in self.store.anomalies(project_key, metric):
            if anomaly["state"] == "void":
                continue
            before, after = Decimal(anomaly["before_amount"]), Decimal(anomaly["after_amount"])
            _observed, exceeds, _direction = self._exceeds(before, after, ratio)
            if not exceeds and anomaly["state"] in BLOCKING_STATES:
                self.store.update_anomaly(
                    anomaly["id"], state="config_cleared", resolution="config_cleared",
                    config_version=config_version, threshold_ratio=str(ratio),
                    event="config_cleared",
                    event_detail={"new_ratio": str(ratio), "config_version": config_version})
                cleared.append(anomaly["id"])
            elif exceeds and anomaly["state"] in BLOCKING_STATES:
                self.store.update_anomaly(
                    anomaly["id"], config_version=config_version, threshold_ratio=str(ratio),
                    event="threshold_refreshed",
                    event_detail={"config_version": config_version, "threshold_ratio": str(ratio)})
                refreshed.append(anomaly["id"])
            # confirmed/dismissed/supplemented/config_cleared 保持原结论，读取时给 config_stale 标记。
        self.scan(project_key, metric)
        return {"project_key": project_key, "metric": metric, "config_version": config_version,
                "cleared": cleared, "refreshed": refreshed}

    # 复核 ---------------------------------------------------------------
    def review(self, anomaly_id, data):
        anomaly = self.store.anomaly(anomaly_id)
        action = data.get("action")
        if action not in TRANSITIONS:
            raise ProductError("invalid_action", "action must be confirm, dismiss, request_evidence or accept_evidence")
        allowed, target, resolution = TRANSITIONS[action]
        if anomaly["state"] not in allowed:
            raise ProductError("invalid_transition",
                               f"action {action} not allowed in state {anomaly['state']}", 409)
        reviewer = token(data.get("reviewer"), "reviewer")
        reason = data.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 1000:
            raise ProductError("invalid_reason", "review requires a short reason")
        expected = data.get("expected_version")
        if type(expected) is not int or expected < 0:
            raise ProductError("invalid_version", "expected_version must be nonnegative")
        if expected != anomaly["version"]:
            raise ProductError("stale_version", "anomaly changed during review", 409)
        if action == "accept_evidence" and not anomaly["evidence_claim"]:
            raise ProductError("missing_evidence", "no supporting evidence is attached to this anomaly", 409)
        updated = self.store.update_anomaly(
            anomaly_id, state=target, resolution=resolution, reviewer=reviewer,
            reason=reason.strip(), event="review_" + action,
            event_detail={"reviewer": reviewer, "from": anomaly["state"], "to": target})
        return self.explain(updated)

    def queue(self, project_key=None, metric=None, state=None):
        if state is not None and state not in ALL_STATES:
            raise ProductError("invalid_state", "unsupported anomaly state")
        self.void_stale_anchors()
        rows = self.store.anomalies(project_key, metric, state)
        return [self.explain(row) for row in rows]

    def get(self, anomaly_id):
        return self.explain(self.store.anomaly(anomaly_id))

    # 发布拦截与冻结 ------------------------------------------------------
    def blocking(self, project_key=None):
        """返回当前确实阻止发布的核查事项（未解决且锚点仍是当前选定 claim）。"""
        self.void_stale_anchors()
        result = []
        for row in self.store.anomalies(project_key):
            if row["state"] not in BLOCKING_STATES:
                continue
            explanation = self.explain(row)
            if explanation["blocks_publication"]:
                result.append(explanation)
        return result

    def freeze(self, project_key=None):
        """发布时冻结的全部非作废事项（含已人工结论），供历史报告长期解释。"""
        return [self.explain(row) for row in self.store.anomalies(project_key) if row["state"] != "void"]

    # 统一解释 ------------------------------------------------------------
    def explain(self, anomaly):
        current_ratio, current_version, _active = self._resolve_config(
            anomaly["project_key"], anomaly["metric"])
        before_ctx = self._claim_context(anomaly["before_claim"])
        after_ctx = self._claim_context(anomaly["after_claim"])
        evidence = None
        if anomaly["evidence_claim"]:
            evidence = self._claim_context(anomaly["evidence_claim"])
        blocks = self._blocks_current(anomaly, after_ctx)
        return {
            "anomaly_id": anomaly["id"],
            "project_key": anomaly["project_key"], "kind": anomaly["kind"],
            "metric": anomaly["metric"], "unit": anomaly["unit"],
            "state": anomaly["state"], "resolution": anomaly["resolution"],
            "blocking": anomaly["state"] in BLOCKING_STATES, "blocks_publication": blocks,
            "direction": self._direction(anomaly),
            "observed_ratio": anomaly["observed_ratio"],
            "threshold_ratio": anomaly["threshold_ratio"],
            "config_version": anomaly["config_version"],
            "current_config_version": current_version,
            "config_stale": anomaly["state"] != "void" and anomaly["config_version"] != current_version,
            "before": before_ctx, "after": after_ctx, "evidence": evidence,
            "reviewer": anomaly["reviewer"], "reason": anomaly["reason"],
            "context_hash": anomaly["context_hash"], "version": anomaly["version"],
            "created_at": anomaly["created_at"], "updated_at": anomaly["updated_at"],
            "events": self.store.anomaly_events(anomaly["id"]),
        }

    @staticmethod
    def _direction(anomaly):
        before, after = Decimal(anomaly["before_amount"]), Decimal(anomaly["after_amount"])
        if before == after:
            return "flat"
        return "increase" if after > before else "decrease"

    def _blocks_current(self, anomaly, after_ctx):
        if anomaly["state"] not in BLOCKING_STATES:
            return False
        scope = self.store.scope((anomaly["project_key"], anomaly["kind"],
                                  anomaly["metric"], anomaly["after_period"]))
        if scope.get("selected_claim") != anomaly["after_claim"]:
            return False
        return not bool(scope.get("conflict")) and after_ctx["capture_state"] == "parsed"

    def _claim_context(self, claim_id):
        claim = self.store.claim(claim_id)
        capture = self.store.capture(claim["capture_id"])
        return {"claim_id": claim["id"], "period": claim["period"], "amount": claim["amount"],
                "unit": claim["unit"], "milestone": claim["milestone"], "quote": claim["quote"],
                "claim_state": claim["state"], "capture_id": capture["id"],
                "source_id": capture["source_id"], "external_id": capture["external_id"],
                "url": capture["url"], "title": capture["title"],
                "published_at": capture["published_at"], "fetched_at": capture["fetched_at"],
                "capture_state": capture["state"], "content_hash": capture["content_hash"]}

    def _context(self, before_claim, after_claim):
        return {"before": self._claim_context(before_claim), "after": self._claim_context(after_claim)}

    # 供报告使用的紧凑解释 ------------------------------------------------
    def compact(self, explanation):
        return {key: explanation[key] for key in (
            "anomaly_id", "project_key", "kind", "metric", "unit", "state", "resolution",
            "blocking", "blocks_publication", "direction", "observed_ratio", "threshold_ratio",
            "config_version", "current_config_version", "config_stale", "reviewer", "reason",
            "context_hash", "version", "updated_at")} | {
            "before_period": explanation["before"]["period"],
            "after_period": explanation["after"]["period"],
            "before_amount": explanation["before"]["amount"],
            "after_amount": explanation["after"]["amount"],
            "before_claim_id": explanation["before"]["claim_id"],
            "after_claim_id": explanation["after"]["claim_id"],
            "evidence_claim_id": explanation["evidence"]["claim_id"] if explanation["evidence"] else None,
        }
