"""铁路新闻证据系统：SQLite-backed acquisition schedule with leases and conditional fetch metadata."""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timedelta, timezone
import sqlite3

from .domain import ProductError, instant, token, url
from .store import Store, now


SCHEDULE_SCHEMA = """
CREATE TABLE IF NOT EXISTS fetch_jobs (
 id INTEGER PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(source_id),
 external_id TEXT NOT NULL, url TEXT NOT NULL, next_run TEXT NOT NULL,
 interval_seconds INTEGER NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
 lease_owner TEXT, lease_until TEXT, etag TEXT, last_modified TEXT,
 last_capture_id INTEGER REFERENCES captures(id), last_error TEXT,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(source_id,external_id)
);
CREATE INDEX IF NOT EXISTS fetch_jobs_due ON fetch_jobs(state,next_run,lease_until);
"""


class Schedule:
    def __init__(self, store: Store):
        self.store = store
        with closing(self.store.connect()) as db:
            db.executescript(SCHEDULE_SCHEMA)

    def job(self, job_id):
        row = self.store.one("SELECT * FROM fetch_jobs WHERE id=?", (job_id,))
        if row is None:
            raise ProductError("unknown_job", "fetch job does not exist", 404)
        return row

    def upsert(self, source_id, external_id, target, next_run, interval_seconds):
        source_id, external_id = token(source_id, "source_id"), token(external_id, "external_id")
        policy = self.store.source(source_id)
        target, next_run = url(target), instant(next_run)
        from urllib.parse import urlsplit
        if urlsplit(target).hostname != policy["host"]:
            raise ProductError("source_mismatch", "job host differs from registered source")
        if type(interval_seconds) is not int or not 60 <= interval_seconds <= 86400 * 30:
            raise ProductError("invalid_interval", "interval must be 60 seconds to 30 days")
        with self.store.transaction() as db:
            old = db.execute("SELECT * FROM fetch_jobs WHERE source_id=? AND external_id=?",
                             (source_id, external_id)).fetchone()
            if old:
                if old["state"] == "leased":
                    raise ProductError("job_leased", "cannot edit a leased job", 409)
                db.execute("UPDATE fetch_jobs SET url=?,next_run=?,interval_seconds=?,state='ready',updated_at=? WHERE id=?",
                           (target, next_run, interval_seconds, now(), old["id"]))
                job_id = old["id"]
            else:
                cursor = db.execute("INSERT INTO fetch_jobs(source_id,external_id,url,next_run,interval_seconds,state,created_at,updated_at) VALUES(?,?,?,?,?,'ready',?,?)",
                                    (source_id, external_id, target, next_run, interval_seconds, now(), now()))
                job_id = cursor.lastrowid
            self.store.audit(db, "job_scheduled", str(job_id), {"source_id": source_id, "external_id": external_id})
        return self.job(job_id)

    def due(self, at=None, limit=100):
        at = instant(at) if at else now()
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ProductError("invalid_limit", "limit must be 1..500")
        return self.store.all("SELECT * FROM fetch_jobs WHERE state='ready' AND next_run<=? OR state='leased' AND lease_until<=? ORDER BY next_run,id LIMIT ?",
                              (at, at, limit))

    def lease(self, owner, at=None, seconds=120):
        owner = token(owner, "owner")
        at = instant(at) if at else now()
        if type(seconds) is not int or not 10 <= seconds <= 3600:
            raise ProductError("invalid_lease", "lease seconds must be 10..3600")
        until = (datetime.fromisoformat(at) + timedelta(seconds=seconds)).isoformat(timespec="seconds")
        with self.store.transaction() as db:
            row = db.execute("SELECT * FROM fetch_jobs WHERE (state='ready' AND next_run<=?) OR (state='leased' AND lease_until<=?) ORDER BY next_run,id LIMIT 1",
                             (at, at)).fetchone()
            if row is None:
                return None
            db.execute("UPDATE fetch_jobs SET state='leased',lease_owner=?,lease_until=?,attempts=attempts+1,updated_at=? WHERE id=?",
                       (owner, until, now(), row["id"]))
            self.store.audit(db, "job_leased", str(row["id"]), {"owner": owner, "lease_until": until})
            job_id = row["id"]
        return self.job(job_id)

    def complete(self, job_id, owner, capture_id=None, etag=None, last_modified=None, at=None):
        owner = token(owner, "owner")
        at = instant(at) if at else now()
        with self.store.transaction() as db:
            job = db.execute("SELECT * FROM fetch_jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                raise ProductError("unknown_job", "fetch job does not exist", 404)
            if job["state"] != "leased" or job["lease_owner"] != owner or job["lease_until"] < at:
                raise ProductError("lease_lost", "job lease no longer belongs to worker", 409)
            if capture_id is not None:
                capture = db.execute("SELECT source_id,external_id FROM captures WHERE id=?", (capture_id,)).fetchone()
                if capture is None or (capture["source_id"], capture["external_id"]) != (job["source_id"], job["external_id"]):
                    raise ProductError("capture_mismatch", "capture does not belong to this job")
            next_run = (datetime.fromisoformat(at) + timedelta(seconds=job["interval_seconds"])).isoformat(timespec="seconds")
            db.execute("UPDATE fetch_jobs SET state='ready',lease_owner=NULL,lease_until=NULL,next_run=?,last_capture_id=COALESCE(?,last_capture_id),etag=COALESCE(?,etag),last_modified=COALESCE(?,last_modified),last_error=NULL,updated_at=? WHERE id=?",
                       (next_run, capture_id, etag, last_modified, now(), job_id))
            self.store.audit(db, "job_completed", str(job_id), {"capture_id": capture_id, "next_run": next_run})
        return self.job(job_id)

    def fail(self, job_id, owner, code, at=None):
        owner = token(owner, "owner")
        code = token(code, "code")
        at = instant(at) if at else now()
        with self.store.transaction() as db:
            job = db.execute("SELECT * FROM fetch_jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                raise ProductError("unknown_job", "fetch job does not exist", 404)
            if job["state"] != "leased" or job["lease_owner"] != owner or job["lease_until"] < at:
                raise ProductError("lease_lost", "job lease no longer belongs to worker", 409)
            backoff = min(job["interval_seconds"], 60 * 2 ** min(job["attempts"], 8))
            next_run = (datetime.fromisoformat(at) + timedelta(seconds=backoff)).isoformat(timespec="seconds")
            db.execute("UPDATE fetch_jobs SET state='ready',lease_owner=NULL,lease_until=NULL,next_run=?,last_error=?,updated_at=? WHERE id=?",
                       (next_run, code, now(), job_id))
            self.store.audit(db, "job_failed", str(job_id), {"code": code, "next_run": next_run})
        return self.job(job_id)

    def pause(self, job_id):
        with self.store.transaction() as db:
            job = db.execute("SELECT state FROM fetch_jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                raise ProductError("unknown_job", "fetch job does not exist", 404)
            if job["state"] == "leased":
                raise ProductError("job_leased", "cannot pause leased job", 409)
            db.execute("UPDATE fetch_jobs SET state='paused',updated_at=? WHERE id=?", (now(), job_id))
            self.store.audit(db, "job_paused", str(job_id), {})
        return self.job(job_id)

    def resume(self, job_id):
        with self.store.transaction() as db:
            job = db.execute("SELECT state FROM fetch_jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                raise ProductError("unknown_job", "fetch job does not exist", 404)
            if job["state"] != "paused":
                raise ProductError("job_state", "job is not paused", 409)
            db.execute("UPDATE fetch_jobs SET state='ready',updated_at=? WHERE id=?", (now(), job_id))
            self.store.audit(db, "job_resumed", str(job_id), {})
        return self.job(job_id)
