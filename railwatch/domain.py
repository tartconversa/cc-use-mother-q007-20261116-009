"""铁路新闻证据系统：Validated identities, timestamps and measurements for railway evidence."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re
from urllib.parse import urlsplit, urlunsplit

KINDS = {"freight", "train", "container", "milestone", "capacity"}
UNITS = {"tonne", "wagon", "train", "teu", "km", "platform", "track"}


class ProductError(Exception):
    def __init__(self, code, detail, status=400):
        self.code, self.detail, self.status = code, detail, status
        super().__init__(detail)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fingerprint(value):
    if isinstance(value, str):
        value = value.encode("utf-8")
    elif not isinstance(value, bytes):
        value = canonical(value).encode("utf-8")
    return sha256(value).hexdigest()


def token(value, label, maximum=120):
    if not isinstance(value, str) or len(value) > maximum or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]*", value):
        raise ProductError("invalid_token", label + " must be a short identifier")
    return value


def text(value, label, maximum=200000):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum or chr(0) in value:
        raise ProductError("invalid_text", label + " must contain readable text")
    return value.strip()


def url(value):
    parsed = urlsplit(text(value, "url", 2048))
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
        raise ProductError("invalid_url", "source URL must be HTTP(S) without credentials")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ProductError("invalid_url", "invalid URL port") from exc
    host = parsed.hostname.lower()
    if port and port not in (80, 443):
        host += ":" + str(port)
    return urlunsplit((parsed.scheme.lower(), host, parsed.path or "/", parsed.query, ""))


def day(value):
    if not isinstance(value, str):
        raise ProductError("invalid_date", "date must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ProductError("invalid_date", "date must be YYYY-MM-DD") from exc


def instant(value):
    if not isinstance(value, str):
        raise ProductError("invalid_time", "timestamp must include timezone")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProductError("invalid_time", "timestamp must be ISO format") from exc
    if parsed.tzinfo is None:
        raise ProductError("invalid_time", "timestamp must include timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


def amount(value):
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise ProductError("invalid_amount", "amount must be numeric")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ProductError("invalid_amount", "amount must be numeric") from exc
    if not result.is_finite() or result < 0 or result > Decimal("1000000000000"):
        raise ProductError("invalid_amount", "amount outside supported range")
    return format(result.normalize(), "f")


@dataclass(frozen=True)
class SourcePolicy:
    source_id: str
    host: str
    kinds: tuple[str, ...]
    max_age_days: int
    trust_rank: int
    version: int
    active: bool

    @classmethod
    def parse(cls, value):
        source_id = token(value.get("source_id"), "source_id")
        host = text(value.get("host"), "host", 253).lower()
        if not re.fullmatch(r"[a-z0-9.-]+", host) or host.startswith(".") or host.endswith("."):
            raise ProductError("invalid_host", "host must be a DNS name")
        kinds = value.get("kinds")
        if not isinstance(kinds, list) or not kinds or len(set(kinds)) != len(kinds) or not set(kinds) <= KINDS:
            raise ProductError("invalid_kinds", "kinds must be distinct supported values")
        age, rank, version = (value.get(k) for k in ("max_age_days", "trust_rank", "version"))
        if any(type(v) is not int or v < 1 for v in (age, rank, version)) or age > 3650 or rank > 100:
            raise ProductError("invalid_policy", "policy numeric fields outside range")
        active = value.get("active", True)
        if type(active) is not bool:
            raise ProductError("invalid_policy", "active must be boolean")
        return cls(source_id, host, tuple(kinds), age, rank, version, active)


@dataclass(frozen=True)
class Capture:
    source_id: str
    external_id: str
    url: str
    fetched_at: str
    published_at: str
    title: str
    body: str
    media_type: str
    metadata: dict

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict):
            raise ProductError("invalid_capture", "capture must be an object")
        metadata = value.get("metadata", {})
        if not isinstance(metadata, dict) or len(canonical(metadata)) > 10000:
            raise ProductError("invalid_capture", "metadata must be a small object")
        media_type = value.get("media_type", "text/html")
        if media_type not in ("text/html", "text/plain", "application/json"):
            raise ProductError("invalid_media", "unsupported media type")
        return cls(token(value.get("source_id"), "source_id"), token(value.get("external_id"), "external_id"),
                   url(value.get("url")), instant(value.get("fetched_at")), instant(value.get("published_at")),
                   text(value.get("title"), "title", 500), text(value.get("body"), "body"), media_type, metadata)

    def check(self, policy):
        if self.source_id != policy.source_id or urlsplit(self.url).hostname != policy.host:
            raise ProductError("source_mismatch", "capture host differs from policy")
        if not policy.active:
            raise ProductError("source_disabled", "source is disabled", 409)
        fetched, published = datetime.fromisoformat(self.fetched_at), datetime.fromisoformat(self.published_at)
        if published > fetched:
            raise ProductError("future_publication", "publication follows capture")
        if (fetched - published).days > policy.max_age_days:
            raise ProductError("stale_capture", "capture exceeds age policy")

    def identity(self):
        return self.source_id + ":" + self.external_id

    def content_hash(self):
        return fingerprint({"title": self.title, "body": self.body, "media_type": self.media_type})


@dataclass(frozen=True)
class Claim:
    project_key: str
    kind: str
    metric: str
    period: str
    amount: str | None
    unit: str | None
    milestone: str | None
    quote: str

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict):
            raise ProductError("invalid_claim", "claim must be an object")
        project_key, kind = token(value.get("project_key"), "project_key"), value.get("kind")
        if kind not in KINDS:
            raise ProductError("invalid_claim", "unsupported claim kind")
        metric = token(value.get("metric"), "metric")
        period = day(value.get("period"))
        quote = text(value.get("quote"), "quote", 1500)
        if kind == "milestone":
            milestone = token(value.get("milestone"), "milestone")
            if value.get("amount") is not None or value.get("unit") is not None:
                raise ProductError("invalid_claim", "milestone cannot have quantity")
            return cls(project_key, kind, metric, period, None, None, milestone, quote)
        unit = value.get("unit")
        if unit not in UNITS or value.get("milestone") is not None:
            raise ProductError("invalid_claim", "numeric claim requires a supported unit")
        return cls(project_key, kind, metric, period, amount(value.get("amount")), unit, None, quote)

    def key(self):
        return (self.project_key, self.kind, self.metric, self.period)

    def value(self):
        return self.milestone if self.kind == "milestone" else (self.amount, self.unit)

    def hash(self):
        return fingerprint(self.__dict__)
