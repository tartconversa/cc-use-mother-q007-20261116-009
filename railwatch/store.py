"""铁路新闻证据系统：SQLite ledger for sources, captures, claims, review and publishable snapshots."""
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import datetime, timezone
import json
import sqlite3
from pathlib import Path

from .domain import ProductError, canonical, fingerprint


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS sources (
 source_id TEXT PRIMARY KEY, host TEXT NOT NULL UNIQUE, kinds_json TEXT NOT NULL,
 max_age_days INTEGER NOT NULL, trust_rank INTEGER NOT NULL, version INTEGER NOT NULL,
 active INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS captures (
 id INTEGER PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(source_id),
 external_id TEXT NOT NULL, url TEXT NOT NULL, fetched_at TEXT NOT NULL,
 published_at TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL,
 media_type TEXT NOT NULL, metadata_json TEXT NOT NULL, content_hash TEXT NOT NULL,
 state TEXT NOT NULL, policy_version INTEGER NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(source_id,external_id,content_hash)
);
CREATE INDEX IF NOT EXISTS captures_identity ON captures(source_id,external_id,id);
CREATE TABLE IF NOT EXISTS claims (
 id INTEGER PRIMARY KEY, capture_id INTEGER NOT NULL REFERENCES captures(id),
 project_key TEXT NOT NULL, kind TEXT NOT NULL, metric TEXT NOT NULL,
 period TEXT NOT NULL, amount TEXT, unit TEXT, milestone TEXT, quote TEXT NOT NULL,
 claim_hash TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(capture_id,claim_hash)
);
CREATE INDEX IF NOT EXISTS claims_scope ON claims(project_key,kind,metric,period);
CREATE TABLE IF NOT EXISTS reviews (
 id INTEGER PRIMARY KEY, claim_id INTEGER NOT NULL REFERENCES claims(id),
 decision TEXT NOT NULL, reviewer TEXT NOT NULL, reason TEXT NOT NULL,
 expected_version INTEGER NOT NULL, resulting_version INTEGER NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS scopes (
 project_key TEXT NOT NULL, kind TEXT NOT NULL, metric TEXT NOT NULL,
 period TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 0,
 selected_claim INTEGER REFERENCES claims(id), conflict INTEGER NOT NULL DEFAULT 0,
 updated_at TEXT NOT NULL, PRIMARY KEY(project_key,kind,metric,period)
);
CREATE TABLE IF NOT EXISTS snapshots (
 id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL,
 digest TEXT NOT NULL UNIQUE, scope_count INTEGER NOT NULL, claim_count INTEGER NOT NULL,
 content_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit (
 id INTEGER PRIMARY KEY, event TEXT NOT NULL, subject TEXT NOT NULL,
 detail_json TEXT NOT NULL, created_at TEXT NOT NULL
);
"""


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Store:
    def __init__(self, path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        return connection

    def initialize(self):
        with closing(self.connect()) as db:
            db.executescript(SCHEMA)

    @contextmanager
    def transaction(self):
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def one(self, query, args=()):
        with closing(self.connect()) as db:
            row = db.execute(query, args).fetchone()
        return dict(row) if row else None

    def all(self, query, args=()):
        with closing(self.connect()) as db:
            rows = db.execute(query, args).fetchall()
        return [dict(row) for row in rows]

    def audit(self, db, event, subject, detail):
        db.execute("INSERT INTO audit(event,subject,detail_json,created_at) VALUES(?,?,?,?)",
                   (event, subject, canonical(detail), now()))

    def source(self, source_id):
        row = self.one("SELECT * FROM sources WHERE source_id=?", (source_id,))
        if row is None:
            raise ProductError("unknown_source", "source is not registered", 404)
        row["kinds"] = json.loads(row.pop("kinds_json"))
        row["active"] = bool(row["active"])
        return row

    def upsert_source(self, policy, expected_version=None):
        with self.transaction() as db:
            old = db.execute("SELECT * FROM sources WHERE source_id=?", (policy.source_id,)).fetchone()
            stamp = now()
            if old:
                if expected_version != old["version"]:
                    raise ProductError("stale_version", "source policy version changed", 409)
                if policy.version != old["version"] + 1:
                    raise ProductError("invalid_version", "policy version must increase by one", 409)
                db.execute("UPDATE sources SET host=?,kinds_json=?,max_age_days=?,trust_rank=?,version=?,active=?,updated_at=? WHERE source_id=?",
                           (policy.host, canonical(policy.kinds), policy.max_age_days, policy.trust_rank,
                            policy.version, int(policy.active), stamp, policy.source_id))
                self.audit(db, "source_updated", policy.source_id, {"old_version": old["version"], "new_version": policy.version})
            else:
                if expected_version is not None or policy.version != 1:
                    raise ProductError("invalid_version", "new source starts at version one", 409)
                db.execute("INSERT INTO sources VALUES(?,?,?,?,?,?,?,?,?)",
                           (policy.source_id, policy.host, canonical(policy.kinds), policy.max_age_days,
                            policy.trust_rank, policy.version, int(policy.active), stamp, stamp))
                self.audit(db, "source_created", policy.source_id, {"version": policy.version})
        return self.source(policy.source_id)

    def capture(self, capture_id):
        row = self.one("SELECT * FROM captures WHERE id=?", (capture_id,))
        if row is None:
            raise ProductError("unknown_capture", "capture does not exist", 404)
        row["metadata"] = json.loads(row.pop("metadata_json"))
        return row

    def capture_identity(self, source_id, external_id):
        return self.all("SELECT id,content_hash,state,policy_version FROM captures WHERE source_id=? AND external_id=? ORDER BY id",
                        (source_id, external_id))

    def put_capture(self, item, policy):
        with self.transaction() as db:
            duplicate = db.execute("SELECT id FROM captures WHERE source_id=? AND external_id=? AND content_hash=?",
                                   (item.source_id, item.external_id, item.content_hash())).fetchone()
            if duplicate:
                return int(duplicate["id"]), False
            old_ids = [row["id"] for row in db.execute(
                "SELECT id FROM captures WHERE source_id=? AND external_id=? AND state!='superseded'",
                (item.source_id, item.external_id)).fetchall()]
            for old_id in old_ids:
                affected = db.execute("SELECT id FROM claims WHERE capture_id=?", (old_id,)).fetchall()
                for claim in affected:
                    db.execute("UPDATE scopes SET selected_claim=NULL,conflict=1,version=version+1,updated_at=? WHERE selected_claim=?",
                               (now(), claim["id"]))
                db.execute("UPDATE claims SET state='retracted' WHERE capture_id=? AND state='accepted'", (old_id,))
            db.execute("UPDATE captures SET state='superseded' WHERE source_id=? AND external_id=? AND state!='superseded'",
                       (item.source_id, item.external_id))
            cursor = db.execute("INSERT INTO captures(source_id,external_id,url,fetched_at,published_at,title,body,media_type,metadata_json,content_hash,state,policy_version,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (item.source_id, item.external_id, item.url, item.fetched_at, item.published_at,
                                 item.title, item.body, item.media_type, canonical(item.metadata), item.content_hash(),
                                 "received", policy.version, now()))
            capture_id = cursor.lastrowid
            self.audit(db, "capture_received", str(capture_id), {"identity": item.identity(), "hash": item.content_hash()})
            if old_ids:
                self.audit(db, "capture_superseded", str(capture_id), {"old_capture_ids": old_ids})
            return capture_id, True

    def set_capture_state(self, capture_id, expected, target, detail):
        with self.transaction() as db:
            result = db.execute("UPDATE captures SET state=? WHERE id=? AND state=?", (target, capture_id, expected))
            if result.rowcount != 1:
                raise ProductError("capture_state", "capture state changed", 409)
            self.audit(db, "capture_" + target, str(capture_id), detail)

    def insert_claims(self, capture_id, claims):
        inserted = []
        with self.transaction() as db:
            row = db.execute("SELECT state FROM captures WHERE id=?", (capture_id,)).fetchone()
            if row is None or row["state"] != "received":
                raise ProductError("capture_state", "capture is not ready for extraction", 409)
            for claim in claims:
                cursor = db.execute("INSERT INTO claims(capture_id,project_key,kind,metric,period,amount,unit,milestone,quote,claim_hash,state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (capture_id, claim.project_key, claim.kind, claim.metric, claim.period,
                                     claim.amount, claim.unit, claim.milestone, claim.quote, claim.hash(), "proposed", now()))
                inserted.append(cursor.lastrowid)
            db.execute("UPDATE captures SET state='parsed' WHERE id=?", (capture_id,))
            self.audit(db, "capture_parsed", str(capture_id), {"claim_ids": inserted})
        return inserted

    def claim(self, claim_id):
        row = self.one("SELECT * FROM claims WHERE id=?", (claim_id,))
        if row is None:
            raise ProductError("unknown_claim", "claim does not exist", 404)
        return row

    def claims_for_capture(self, capture_id):
        return self.all("SELECT * FROM claims WHERE capture_id=? ORDER BY id", (capture_id,))

    def scope(self, key):
        row = self.one("SELECT * FROM scopes WHERE project_key=? AND kind=? AND metric=? AND period=?", key)
        if row is None:
            return {"project_key": key[0], "kind": key[1], "metric": key[2], "period": key[3],
                    "version": 0, "selected_claim": None, "conflict": 0}
        return row

    def candidates(self, key):
        return self.all("SELECT c.*,s.trust_rank,a.published_at,a.state AS capture_state FROM claims c JOIN captures a ON a.id=c.capture_id JOIN sources s ON s.source_id=a.source_id WHERE c.project_key=? AND c.kind=? AND c.metric=? AND c.period=? ORDER BY c.id", key)

    def record_review(self, claim_id, decision, reviewer, reason, expected_version, selected, conflict):
        claim = self.claim(claim_id)
        key = (claim["project_key"], claim["kind"], claim["metric"], claim["period"])
        with self.transaction() as db:
            current = db.execute("SELECT * FROM scopes WHERE project_key=? AND kind=? AND metric=? AND period=?", key).fetchone()
            version = current["version"] if current else 0
            if version != expected_version:
                raise ProductError("stale_version", "scope changed during review", 409)
            if current:
                db.execute("UPDATE scopes SET version=?,selected_claim=?,conflict=?,updated_at=? WHERE project_key=? AND kind=? AND metric=? AND period=?",
                           (version + 1, selected, int(conflict), now(), *key))
            else:
                db.execute("INSERT INTO scopes VALUES(?,?,?,?,?,?,?,?)",
                           (*key, 1, selected, int(conflict), now()))
            db.execute("UPDATE claims SET state=? WHERE id=?", (decision, claim_id))
            db.execute("INSERT INTO reviews(claim_id,decision,reviewer,reason,expected_version,resulting_version,created_at) VALUES(?,?,?,?,?,?,?)",
                       (claim_id, decision, reviewer, reason, version, version + 1, now()))
            self.audit(db, "claim_reviewed", str(claim_id), {"decision": decision, "scope_version": version + 1})
        return self.scope(key)

    def save_snapshot(self, name, scopes):
        content = canonical(scopes)
        value = fingerprint(content)
        with self.transaction() as db:
            old = db.execute("SELECT * FROM snapshots WHERE digest=?", (value,)).fetchone()
            if old:
                return dict(old), False
            cursor = db.execute("INSERT INTO snapshots(name,created_at,digest,scope_count,claim_count,content_json) VALUES(?,?,?,?,?,?)",
                                (name, now(), value, len(scopes), sum(bool(s["selected_claim"]) for s in scopes), content))
            self.audit(db, "snapshot_created", str(cursor.lastrowid), {"digest": value, "name": name})
            return dict(db.execute("SELECT * FROM snapshots WHERE id=?", (cursor.lastrowid,)).fetchone()), True

    def snapshot(self, snapshot_id):
        row = self.one("SELECT * FROM snapshots WHERE id=?", (snapshot_id,))
        if row is None:
            raise ProductError("unknown_snapshot", "snapshot does not exist", 404)
        row["content"] = json.loads(row.pop("content_json"))
        return row

    def audit_events(self, since=0, limit=100):
        rows = self.all("SELECT * FROM audit WHERE id>? ORDER BY id LIMIT ?", (since, limit))
        for row in rows:
            row["detail"] = json.loads(row.pop("detail_json"))
        return rows
