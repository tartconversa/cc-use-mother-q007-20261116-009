"""铁路新闻证据系统：Execute one scheduled acquisition while retaining lease and replay semantics."""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256

from .acquire import Acquisition
from .domain import ProductError
from .schedule import Schedule
from .service import RailwayService


class Worker:
    def __init__(self, service: RailwayService, acquisition=None):
        self.service = service
        self.schedule = Schedule(service.store)
        self.acquisition = acquisition or Acquisition()

    def run_once(self, owner, at=None):
        job = self.schedule.lease(owner, at=at)
        if job is None:
            return {"status": "idle"}
        policy = self.service.policy(job["source_id"])
        try:
            result = self.acquisition.fetch(job["url"], policy, job["etag"], job["last_modified"])
            if result.status == 304:
                self.schedule.complete(job["id"], owner, etag=result.etag,
                                       last_modified=result.last_modified, at=at)
                return {"status": "unchanged", "job_id": job["id"]}
            if result.body is None:
                raise ProductError("empty_body", "publisher returned no body")
            capture = {
                "source_id": job["source_id"], "external_id": job["external_id"],
                "url": result.url, "fetched_at": result.fetched_at,
                "published_at": result.published_at or result.fetched_at,
                "title": self._title(result.body, result.media_type),
                "body": result.body, "media_type": result.media_type,
                "metadata": {"etag": result.etag, "last_modified": result.last_modified,
                             "redirect_count": result.redirect_count},
            }
            replay = self.service.replay(capture)
            self.schedule.complete(job["id"], owner, replay["capture_id"], result.etag,
                                   result.last_modified, at=at)
            return {"status": replay["state"], "job_id": job["id"],
                    "capture_id": replay["capture_id"], "claim_ids": replay["claim_ids"],
                    "issues": replay["issues"]}
        except ProductError as error:
            self.schedule.fail(job["id"], owner, error.code, at=at)
            return {"status": "failed", "job_id": job["id"],
                    "error": error.code, "detail": error.detail}

    @staticmethod
    def _title(body, media_type):
        if media_type == "text/html":
            import re
            from html import unescape
            match = re.search(r"<title[^>]*>(.*?)</title>", body, re.I | re.S)
            if match:
                title = unescape(re.sub(r"\s+", " ", match.group(1))).strip()
                if title:
                    return title[:500]
        if media_type == "application/json":
            import json
            try:
                title = json.loads(body).get("title")
            except (ValueError, AttributeError):
                title = None
            if isinstance(title, str) and title.strip():
                return title.strip()[:500]
        first = next((line.strip() for line in body.splitlines() if line.strip()), "")
        return first[:500] or "Untitled publisher capture"
