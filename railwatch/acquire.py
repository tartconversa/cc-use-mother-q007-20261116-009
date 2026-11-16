"""铁路新闻证据系统：Bounded HTTP acquisition for explicitly registered publisher endpoints."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import socket
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .domain import ProductError, url


MAX_BODY = 2_000_000
USER_AGENT = "RailwayEvidenceLedger/1.0 (+offline-replay-supported)"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


@dataclass(frozen=True)
class FetchResult:
    status: int
    url: str
    body: str | None
    media_type: str | None
    etag: str | None
    last_modified: str | None
    fetched_at: str
    published_at: str | None
    redirect_count: int

    def as_dict(self):
        return self.__dict__.copy()


class Acquisition:
    def __init__(self, opener=None, timeout=8.0):
        self.opener = opener or build_opener(NoRedirect)
        self.timeout = timeout

    def _check_target(self, target, policy):
        target = url(target)
        parts = urlsplit(target)
        if parts.hostname != policy.host:
            raise ProductError("source_mismatch", "request host differs from registered source")
        if parts.username or parts.password:
            raise ProductError("invalid_url", "credentials in source URL are not allowed")
        return target

    def fetch(self, target, policy, etag=None, last_modified=None, max_redirects=2):
        if not policy.active:
            raise ProductError("source_disabled", "source is disabled", 409)
        current = self._check_target(target, policy)
        if type(max_redirects) is not int or not 0 <= max_redirects <= 3:
            raise ProductError("redirect_limit", "redirect limit outside supported range")
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html, text/plain, application/json"}
        if etag:
            if not isinstance(etag, str) or len(etag) > 300 or any(ch in etag for ch in "\r\n"):
                raise ProductError("invalid_etag", "ETag contains invalid characters")
            headers["If-None-Match"] = etag
        if last_modified:
            if not isinstance(last_modified, str) or len(last_modified) > 120 or any(ch in last_modified for ch in "\r\n"):
                raise ProductError("invalid_last_modified", "Last-Modified contains invalid characters")
            headers["If-Modified-Since"] = last_modified
        for redirects in range(max_redirects + 1):
            try:
                response = self.opener.open(Request(current, headers=headers), timeout=self.timeout)
            except HTTPError as error:
                if error.code == 304:
                    return FetchResult(304, current, None, None, etag, last_modified,
                                       datetime.now(timezone.utc).isoformat(timespec="seconds"), None, redirects)
                if error.code in {301, 302, 303, 307, 308}:
                    location = error.headers.get("Location")
                    if not location:
                        raise ProductError("redirect_missing", "redirect lacks Location") from error
                    if redirects >= max_redirects:
                        raise ProductError("redirect_limit", "publisher redirect limit exceeded") from error
                    current = self._check_target(urljoin(current, location), policy)
                    continue
                raise ProductError("fetch_http", f"publisher responded HTTP {error.code}", 502) from error
            except (URLError, TimeoutError, socket.timeout, OSError) as error:
                raise ProductError("fetch_network", f"publisher request failed: {error.__class__.__name__}", 502) from error
            with response:
                status = response.status
                if status != 200:
                    raise ProductError("fetch_http", f"publisher responded HTTP {status}", 502)
                declared = response.headers.get("Content-Length")
                if declared:
                    try:
                        size = int(declared)
                    except ValueError:
                        raise ProductError("body_size", "publisher Content-Length is invalid")
                    if size > MAX_BODY:
                        raise ProductError("body_size", "publisher body exceeds size limit")
                raw = response.read(MAX_BODY + 1)
                if len(raw) > MAX_BODY:
                    raise ProductError("body_size", "publisher body exceeds size limit")
                media_type = response.headers.get_content_type()
                if media_type not in {"text/html", "text/plain", "application/json"}:
                    raise ProductError("media_type", "publisher content type unsupported")
                charset = response.headers.get_content_charset() or "utf-8"
                if charset.lower() not in {"utf-8", "utf8"}:
                    raise ProductError("charset", "publisher text must be UTF-8")
                try:
                    body = raw.decode("utf-8")
                except UnicodeDecodeError as error:
                    raise ProductError("charset", "publisher returned invalid UTF-8") from error
                stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
                published = self._published(response.headers)
                return FetchResult(200, current, body, media_type,
                                   response.headers.get("ETag"), response.headers.get("Last-Modified"),
                                   stamp, published, redirects)
        raise ProductError("redirect_limit", "publisher redirect limit exceeded")

    @staticmethod
    def _published(headers):
        value = headers.get("Last-Modified") or headers.get("Date")
        if not value:
            return None
        try:
            instant = parsedate_to_datetime(value)
        except (ValueError, TypeError):
            return None
        if instant.tzinfo is None:
            return None
        return instant.astimezone(timezone.utc).isoformat(timespec="seconds")
