"""铁路新闻证据系统：FastAPI boundary for railway capture and evidence releases."""
from __future__ import annotations

from contextlib import asynccontextmanager
import os
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from .domain import ProductError
from .service import RailwayService
from .schedule import Schedule
from .worker import Worker
from .report import Reports
from .quality import Quality


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceInput(StrictModel):
    source_id: str
    host: str
    kinds: list[str]
    max_age_days: int
    trust_rank: int
    version: int
    active: bool = True
    expected_version: int | None = None


class CaptureInput(StrictModel):
    source_id: str
    external_id: str
    url: str
    fetched_at: str
    published_at: str
    title: str
    body: str
    media_type: str = "text/html"
    metadata: dict = Field(default_factory=dict)


class ReviewInput(StrictModel):
    decision: str
    reviewer: str
    reason: str
    expected_version: int


class PublishInput(StrictModel):
    name: str
    project_key: str | None = None


class BatchInput(StrictModel):
    captures: list[CaptureInput]
    stop_on_error: bool = False


def create_app(db_path=None):
    db_path = db_path or os.environ.get("RAILWATCH_DB", "/tmp/railwatch.sqlite3")
    service = RailwayService(db_path)
    schedule = Schedule(service.store)
    app = FastAPI(title="Railway Evidence Ledger", version="1.0.0")
    app.state.service = service

    @app.exception_handler(ProductError)
    async def product_error(_request: Request, error: ProductError):
        return JSONResponse(status_code=error.status, content={"error": error.code, "detail": error.detail})

    @app.get("/health")
    def health():
        return {"status": "ok", "database": "ready"}

    @app.put("/sources/{source_id}")
    def put_source(source_id: str, body: SourceInput):
        value = body.model_dump()
        expected = value.pop("expected_version")
        if source_id != value["source_id"]:
            raise ProductError("source_mismatch", "source path and body disagree")
        return service.register_source(value, expected)

    @app.get("/sources/{source_id}")
    def get_source(source_id: str):
        return service.store.source(source_id)

    @app.get("/sources")
    def list_sources():
        rows = service.store.all("SELECT source_id FROM sources ORDER BY source_id")
        return [service.store.source(row["source_id"]) for row in rows]

    @app.post("/captures", status_code=201)
    def post_capture(body: CaptureInput):
        return service.replay(body.model_dump())

    @app.post("/captures/batch")
    def post_batch(body: BatchInput):
        results = []
        for index, capture in enumerate(body.captures):
            try:
                results.append({"index": index, "result": service.replay(capture.model_dump())})
            except ProductError as error:
                results.append({"index": index, "error": error.code, "detail": error.detail})
                if body.stop_on_error:
                    break
        return {"received": len(body.captures), "processed": len(results), "results": results}

    @app.get("/captures/{capture_id}")
    def get_capture(capture_id: int):
        return service.capture(capture_id)

    @app.get("/captures")
    def list_captures(source_id: str | None = None, state: str | None = None,
                      limit: int = Query(100, ge=1, le=500)):
        clauses, args = [], []
        if source_id:
            clauses.append("source_id=?")
            args.append(source_id)
        if state:
            if state not in {"received", "parsed", "quarantined", "superseded"}:
                raise ProductError("invalid_state", "unsupported capture state")
            clauses.append("state=?")
            args.append(state)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = service.store.all("SELECT id FROM captures" + where + " ORDER BY id DESC LIMIT ?", (*args, limit))
        return [service.capture(row["id"]) for row in rows]

    @app.get("/claims")
    def list_claims(project_key: str | None = None, state: str = "proposed",
                    limit: int = Query(100, ge=1, le=500)):
        return service.review_queue(project_key, state, limit)

    @app.get("/claims/{claim_id}")
    def get_claim(claim_id: int):
        return service.claim(claim_id)

    @app.get("/claims/{claim_id}/comparison")
    def compare_claim(claim_id: int):
        return service.compare(claim_id)

    @app.post("/claims/{claim_id}/reviews")
    def review_claim(claim_id: int, body: ReviewInput):
        return service.review(claim_id, body.model_dump())

    @app.get("/scopes")
    def get_scopes(project_key: str | None = None, include_empty: bool = True):
        return service.scopes(project_key, include_empty)

    @app.post("/snapshots", status_code=201)
    def post_snapshot(body: PublishInput):
        return service.publish(body.model_dump())

    @app.get("/snapshots/{snapshot_id}")
    def get_snapshot(snapshot_id: int):
        return service.snapshot(snapshot_id)

    @app.get("/audit")
    def get_audit(since: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500)):
        return service.audit(since, limit)

    @app.get("/reports/current")
    def current_report(project_key: str | None = None, kinds: str | None = None,
                       start: str | None = None, end: str | None = None):
        return Reports(service).current(project_key, kinds, start, end)

    @app.get("/reports/snapshots/{snapshot_id}")
    def snapshot_report(snapshot_id: int, project_key: str | None = None,
                        kinds: str | None = None, start: str | None = None, end: str | None = None):
        return Reports(service).snapshot(snapshot_id, project_key, kinds, start, end)

    @app.get("/reports/diff/{left_id}/{right_id}")
    def report_diff(left_id: int, right_id: int):
        return Reports(service).compare_snapshots(left_id, right_id)

    @app.get("/quality/sources/{source_id}")
    def source_quality(source_id: str):
        return Quality(service).source_health(source_id)

    @app.get("/quality/claims")
    def claim_quality(project_key: str | None = None):
        return Quality(service).claim_quality(project_key)

    @app.get("/quality/readiness")
    def publication_readiness(project_key: str | None = None):
        return Quality(service).publication_readiness(project_key)

    @app.get("/quality/outliers/{project_key}/{metric}")
    def outliers(project_key: str, metric: str, ratio: str = "3"):
        return Quality(service).outliers(project_key, metric, ratio)

    @app.post("/jobs")
    def post_job(body: dict):
        return schedule.upsert(body.get("source_id"), body.get("external_id"), body.get("url"),
                               body.get("next_run"), body.get("interval_seconds"))

    @app.get("/jobs/due")
    def get_due(at: str | None = None, limit: int = Query(100, ge=1, le=500)):
        return schedule.due(at, limit)

    @app.get("/jobs/{job_id}")
    def get_job(job_id: int):
        return schedule.job(job_id)

    @app.post("/jobs/{job_id}/pause")
    def pause_job(job_id: int):
        return schedule.pause(job_id)

    @app.post("/jobs/{job_id}/resume")
    def resume_job(job_id: int):
        return schedule.resume(job_id)

    @app.post("/workers/{owner}/once")
    def worker_once(owner: str):
        return Worker(service).run_once(owner)

    return app


app = create_app()
