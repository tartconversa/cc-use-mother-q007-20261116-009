"""铁路新闻证据系统：Parse replayed publisher payloads into source-quoted railway claims."""
from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import json
import re

from .domain import Claim, ProductError


class VisibleText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden = 0
        self.block = {"p", "div", "section", "article", "h1", "h2", "h3", "li", "br"}

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style", "nav", "footer", "header"}:
            self.hidden += 1
        if tag in self.block and not self.hidden:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in {"script", "style", "nav", "footer", "header"}:
            self.hidden = max(0, self.hidden - 1)
        if tag in self.block and not self.hidden:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)

    def text(self):
        return re.sub(r"[ \t]+", " ", "".join(self.parts)).strip()


@dataclass(frozen=True)
class ExtractionIssue:
    code: str
    location: str
    detail: str

    def as_dict(self):
        return {"code": self.code, "location": self.location, "detail": self.detail}


class Extractor:
    """铁路新闻证据系统：Strict replay parser; it never opens arbitrary network addresses."""

    def visible_text(self, capture):
        if capture.media_type == "text/plain":
            return capture.body
        if capture.media_type == "text/html":
            parser = VisibleText()
            parser.feed(capture.body)
            parser.close()
            return parser.text()
        try:
            data = json.loads(capture.body)
        except (ValueError, TypeError) as exc:
            raise ProductError("invalid_json", "publisher JSON is malformed") from exc
        if not isinstance(data, dict):
            raise ProductError("invalid_json", "publisher JSON must be an object")
        value = data.get("text", "")
        if not isinstance(value, str):
            raise ProductError("invalid_json", "publisher text must be a string")
        return value

    def parse(self, capture, policy):
        if capture.media_type == "application/json":
            return self._structured(capture, policy)
        return self._marked(capture, policy)

    def _structured(self, capture, policy):
        data = json.loads(capture.body)
        claims = data.get("claims")
        if not isinstance(claims, list):
            raise ProductError("missing_claims", "publisher JSON requires claims array")
        result, issues, seen = [], [], set()
        for index, item in enumerate(claims):
            location = f"claims[{index}]"
            if not isinstance(item, dict):
                issues.append(ExtractionIssue("invalid_item", location, "claim is not an object"))
                continue
            try:
                claim = Claim.parse(item)
                self._validate_claim(claim, capture, policy)
            except ProductError as exc:
                issues.append(ExtractionIssue(exc.code, location, exc.detail))
                continue
            if claim.hash() in seen:
                issues.append(ExtractionIssue("duplicate_claim", location, "same claim repeated"))
                continue
            seen.add(claim.hash())
            result.append(claim)
        return result, issues

    def _marked(self, capture, policy):
        """铁路新闻证据系统：Read explicit local evidence markers without inferring dates or amounts from prose."""
        body = self.visible_text(capture)
        lines = [line.strip() for line in body.splitlines() if line.strip()]
        result, issues, seen = [], [], set()
        for line_no, line in enumerate(lines, 1):
            if not line.startswith("[RAILCLAIM "):
                continue
            match = re.fullmatch(r"\[RAILCLAIM (\{.*\})\]", line)
            if not match:
                issues.append(ExtractionIssue("bad_marker", f"line:{line_no}", "claim marker syntax is invalid"))
                continue
            try:
                data = json.loads(match.group(1))
                if not isinstance(data, dict):
                    raise ProductError("invalid_item", "marker claim must be object")
                quote = data.get("quote")
                if not quote or quote not in body:
                    raise ProductError("missing_quote", "quote is absent from captured body")
                claim = Claim.parse(data)
                self._validate_claim(claim, capture, policy)
            except (ValueError, ProductError) as exc:
                code = exc.code if isinstance(exc, ProductError) else "bad_marker"
                issues.append(ExtractionIssue(code, f"line:{line_no}", str(exc)))
                continue
            if claim.hash() in seen:
                issues.append(ExtractionIssue("duplicate_claim", f"line:{line_no}", "same claim repeated"))
                continue
            seen.add(claim.hash())
            result.append(claim)
        return result, issues

    def _validate_claim(self, claim, capture, policy):
        if claim.kind not in policy.kinds:
            raise ProductError("kind_not_allowed", "source policy rejects this claim kind")
        if capture.media_type == "application/json":
            data = json.loads(capture.body)
            if claim.quote not in data.get("text", ""):
                raise ProductError("missing_quote", "quote is absent from publisher text")
        if claim.period > capture.published_at[:10]:
            raise ProductError("future_period", "claim period follows publication")
        if len(claim.quote) < 8:
            raise ProductError("short_quote", "source quote is too short to audit")

    def inspect(self, capture, policy):
        claims, issues = self.parse(capture, policy)
        if not claims and not issues:
            issues.append(ExtractionIssue("no_claims", "body", "no explicit claim evidence found"))
        if len(claims) > 1000:
            raise ProductError("too_many_claims", "capture exceeds claim limit")
        return claims, [issue.as_dict() for issue in issues]
