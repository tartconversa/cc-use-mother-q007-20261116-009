"""铁路新闻证据系统：Deterministic manifest replay with local checkpoints and per-record outcomes."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

from .domain import ProductError, fingerprint, token
from .service import RailwayService


@dataclass(frozen=True)
class BatchOutcome:
    index: int
    external_id: str | None
    status: str
    capture_id: int | None
    issues: list

    def as_dict(self):
        return {"index": self.index, "external_id": self.external_id,
                "status": self.status, "capture_id": self.capture_id, "issues": self.issues}


class BatchRunner:
    def __init__(self, service: RailwayService):
        self.service = service

    def load_manifest(self, path):
        path = Path(path)
        try:
            content = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ProductError("manifest_read", "manifest cannot be read") from exc
        if len(content) > 10_000_000:
            raise ProductError("manifest_size", "manifest is too large")
        try:
            manifest = json.loads(content)
        except ValueError as exc:
            raise ProductError("manifest_json", "manifest is not valid JSON") from exc
        if not isinstance(manifest, dict) or not isinstance(manifest.get("captures"), list):
            raise ProductError("manifest_shape", "manifest requires captures array")
        if len(manifest["captures"]) > 10000:
            raise ProductError("manifest_size", "too many captures")
        return manifest

    def run(self, manifest, offset=0, limit=None, stop_on_error=False):
        if type(offset) is not int or offset < 0:
            raise ProductError("invalid_offset", "offset must be nonnegative")
        if limit is not None and (type(limit) is not int or not 1 <= limit <= 10000):
            raise ProductError("invalid_limit", "limit must be 1..10000")
        captures = manifest.get("captures")
        if not isinstance(captures, list):
            raise ProductError("manifest_shape", "manifest requires captures array")
        start = min(offset, len(captures))
        end = min(len(captures), start + limit) if limit else len(captures)
        outcomes = []
        for index in range(start, end):
            item = captures[index]
            external_id = item.get("external_id") if isinstance(item, dict) else None
            try:
                result = self.service.replay(item)
                status = "duplicate" if not result["created"] else result["state"]
                outcomes.append(BatchOutcome(index, external_id, status, result["capture_id"], result["issues"]))
            except ProductError as exc:
                outcomes.append(BatchOutcome(index, external_id, "error", None,
                                             [{"code": exc.code, "detail": exc.detail}]))
                if stop_on_error:
                    break
        next_offset = outcomes[-1].index + 1 if outcomes else start
        return {"manifest_digest": fingerprint(manifest), "total": len(captures),
                "offset": start, "next_offset": next_offset, "complete": next_offset >= len(captures),
                "outcomes": [outcome.as_dict() for outcome in outcomes]}

    def run_path(self, path, offset=0, limit=None, stop_on_error=False):
        return self.run(self.load_manifest(path), offset, limit, stop_on_error)

    def checkpoint(self, path, result):
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".tmp")
        temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(target)
        return target

    def resume(self, manifest_path, checkpoint_path, limit=None, stop_on_error=False):
        manifest = self.load_manifest(manifest_path)
        checkpoint_path = Path(checkpoint_path)
        if checkpoint_path.exists():
            try:
                previous = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            except ValueError as exc:
                raise ProductError("checkpoint_json", "checkpoint is invalid JSON") from exc
            if previous.get("manifest_digest") != fingerprint(manifest):
                raise ProductError("checkpoint_mismatch", "manifest changed since checkpoint", 409)
            offset = previous.get("next_offset")
            if type(offset) is not int or offset < 0:
                raise ProductError("checkpoint_offset", "checkpoint offset invalid")
        else:
            offset = 0
        result = self.run(manifest, offset, limit, stop_on_error)
        self.checkpoint(checkpoint_path, result)
        return result
