"""sloperator bridge relay: a small job queue on muse_slop.

The agent on muse_slop (sloperator) cannot reach corporate services; a worker inside the
corporate network can, but accepts no inbound connections. The relay sits between them:

  agent  ── POST /v1/call ──►  relay  ◄── GET /v1/worker/lease (long-poll) ──  worker
  agent  ◄─ result ──────────  relay  ◄── POST /v1/worker/jobs/<id>/result ──  worker

The relay stores and forwards; it holds no service credentials and does not decide what is
allowed — the worker enforces the host/method policy on its side.

Jobs are never re-queued: a lease that expires becomes `worker_lost`, because re-running a
POST (a Jenkins build) twice is worse than reporting a failure the agent can retry.
"""
from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import os
import sqlite3
import time
import uuid
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse

AGENT_TOKEN = os.environ.get("RELAY_AGENT_TOKEN", "")
WORKER_TOKEN = os.environ.get("RELAY_WORKER_TOKEN", "")
DB_PATH = os.environ.get("RELAY_DB", "/data/relay.db")
QUEUE_TTL = float(os.environ.get("RELAY_QUEUE_TTL_SECONDS") or 90)     # unleased -> worker_offline
LEASE_GRACE = float(os.environ.get("RELAY_LEASE_GRACE_SECONDS") or 60)  # on top of the job timeout
RETENTION = float(os.environ.get("RELAY_RETENTION_HOURS") or 24) * 3600
MAX_WAIT = 55.0                    # below nginx/uvicorn idle timeouts
MAX_REQUEST_BYTES = 2 * 1024 * 1024
MAX_JOB_TIMEOUT = 300
WORKER_ONLINE_SECONDS = 90



# --------------------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------------------

class Store:
    def __init__(self, path: str):
        if path != ":memory:":
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                state TEXT NOT NULL,              -- queued | leased | done
                request TEXT NOT NULL,
                created REAL NOT NULL,
                lease TEXT,
                leased_at REAL,
                deadline REAL,
                result TEXT,
                finished REAL
            )""")
        self.db.execute("CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state, created)")
        self.new_job = asyncio.Event()
        self.waiters: dict[str, asyncio.Event] = {}
        self.worker_seen = 0.0

    def create(self, request: dict) -> str:
        job_id = uuid.uuid4().hex
        self.db.execute("INSERT INTO jobs(id, state, request, created) VALUES (?, 'queued', ?, ?)",
                        (job_id, json.dumps(request), time.time()))
        self.new_job.set()
        return job_id

    def get(self, job_id: str) -> sqlite3.Row | None:
        return self.db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    def claim(self) -> dict | None:
        row = self.db.execute("SELECT id, request FROM jobs WHERE state = 'queued' "
                              "ORDER BY created LIMIT 1").fetchone()
        if row is None:
            return None
        req = json.loads(row["request"])
        lease = uuid.uuid4().hex
        now = time.time()
        timeout = min(float(req.get("timeout") or 60), MAX_JOB_TIMEOUT)
        cur = self.db.execute("UPDATE jobs SET state = 'leased', lease = ?, leased_at = ?, "
                              "deadline = ? WHERE id = ? AND state = 'queued'",
                              (lease, now, now + timeout + LEASE_GRACE, row["id"]))
        if cur.rowcount != 1:
            return None
        return {"id": row["id"], "lease": lease, "request": req}

    def finish(self, job_id: str, result: dict, lease: str | None = None) -> bool:
        sql = "UPDATE jobs SET state = 'done', result = ?, finished = ? WHERE id = ? AND state != 'done'"
        args: list[Any] = [json.dumps(result), time.time(), job_id]
        if lease is not None:
            sql += " AND state = 'leased' AND lease = ?"
            args.append(lease)
        ok = self.db.execute(sql, args).rowcount == 1
        if ok and job_id in self.waiters:
            self.waiters[job_id].set()
        return ok

    def sweep(self) -> None:
        now = time.time()
        for row in self.db.execute("SELECT id FROM jobs WHERE state = 'queued' AND created < ?",
                                   (now - QUEUE_TTL,)).fetchall():
            self.finish(row["id"], _error("worker_offline",
                                          f"no worker picked the job up within {QUEUE_TTL:.0f}s"))
        for row in self.db.execute("SELECT id FROM jobs WHERE state = 'leased' AND deadline < ?",
                                   (now,)).fetchall():
            self.finish(row["id"], _error("worker_lost",
                                          "the worker took the job but never reported back"))
        self.db.execute("DELETE FROM jobs WHERE state = 'done' AND finished < ?", (now - RETENTION,))

    def stats(self) -> dict:
        counts = {r["state"]: r["n"] for r in
                  self.db.execute("SELECT state, count(*) n FROM jobs GROUP BY state")}
        return {"worker_online": time.time() - self.worker_seen < WORKER_ONLINE_SECONDS,
                "worker_last_seen": self.worker_seen or None, "jobs": counts}


def _error(code: str, message: str) -> dict:
    return {"ok": False, "status": None, "headers": {}, "body_b64": "", "body_bytes": 0,
            "elapsed_ms": 0, "error": {"code": code, "message": message}}


def _view(row: sqlite3.Row) -> dict:
    out = {"id": row["id"], "state": row["state"], "created": row["created"]}
    if row["state"] == "done":
        out["result"] = json.loads(row["result"])
        out["finished"] = row["finished"]
    return out


store: Store | None = None


def get_store() -> Store:
    assert store is not None
    return store


@contextlib.asynccontextmanager
async def _lifespan(_app: FastAPI):
    global store
    if not AGENT_TOKEN or not WORKER_TOKEN or AGENT_TOKEN == WORKER_TOKEN:
        raise RuntimeError("RELAY_AGENT_TOKEN and RELAY_WORKER_TOKEN must be set and differ")
    if store is None:
        store = Store(DB_PATH)
    janitor = asyncio.get_running_loop().create_task(_janitor())
    yield
    janitor.cancel()


app = FastAPI(title="sloperator-bridge-relay", docs_url=None, redoc_url=None, openapi_url=None,
              lifespan=_lifespan)


async def _janitor() -> None:
    while True:
        with contextlib.suppress(Exception):
            get_store().sweep()
        await asyncio.sleep(2)


# --------------------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------------------

def _bearer(authorization: str | None, expected: str) -> None:
    token = (authorization or "").removeprefix("Bearer ").strip()
    if not expected or not hmac.compare_digest(token.encode(), expected.encode()):
        raise HTTPException(401, "bad token")


def agent(authorization: str | None = Header(None)) -> None:
    _bearer(authorization, AGENT_TOKEN)


def worker(authorization: str | None = Header(None)) -> None:
    _bearer(authorization, WORKER_TOKEN)


# --------------------------------------------------------------------------------------
# Agent API (local only: nginx does not expose it)
# --------------------------------------------------------------------------------------

async def _read_request(request: Request) -> dict:
    raw = await request.body()
    if len(raw) > MAX_REQUEST_BYTES:
        raise HTTPException(413, f"request larger than {MAX_REQUEST_BYTES} bytes")
    try:
        req = json.loads(raw)
    except ValueError:
        raise HTTPException(400, "body must be JSON")
    if not isinstance(req, dict) or not isinstance(req.get("host"), str) \
            or not isinstance(req.get("path"), str):
        raise HTTPException(400, "body must be an object with string 'host' and 'path'")
    return req


async def _wait_done(s: Store, job_id: str, wait: float) -> sqlite3.Row:
    row = s.get(job_id)
    if row is None or row["state"] == "done" or wait <= 0:
        return row
    ev = s.waiters.setdefault(job_id, asyncio.Event())
    try:
        await asyncio.wait_for(ev.wait(), timeout=min(wait, MAX_WAIT))
    except asyncio.TimeoutError:
        pass
    finally:
        s.waiters.pop(job_id, None)
    return s.get(job_id)


def _job_response(row: sqlite3.Row) -> JSONResponse:
    return JSONResponse(_view(row), status_code=200 if row["state"] == "done" else 202)


@app.post("/v1/jobs", dependencies=[Depends(agent)])
async def create_job(request: Request) -> JSONResponse:
    s = get_store()
    job_id = s.create(await _read_request(request))
    return _job_response(s.get(job_id))


@app.post("/v1/call", dependencies=[Depends(agent)])
async def call(request: Request, wait: float = MAX_WAIT) -> JSONResponse:
    """Create a job and wait for it: 200 with the result, or 202 to keep polling GET /v1/jobs/<id>."""
    s = get_store()
    job_id = s.create(await _read_request(request))
    return _job_response(await _wait_done(s, job_id, wait))


@app.get("/v1/jobs/{job_id}", dependencies=[Depends(agent)])
async def get_job(job_id: str, wait: float = 0) -> JSONResponse:
    row = await _wait_done(get_store(), job_id, wait)
    if row is None:
        raise HTTPException(404, "no such job (results are kept for a limited time)")
    return _job_response(row)


@app.get("/v1/status", dependencies=[Depends(agent)])
async def status() -> dict:
    return get_store().stats()


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


# --------------------------------------------------------------------------------------
# Worker API (exposed through nginx under /bridge/v1/worker/)
# --------------------------------------------------------------------------------------

@app.get("/v1/worker/lease", dependencies=[Depends(worker)])
async def lease(wait: float = 25) -> Response:
    s = get_store()
    deadline = time.monotonic() + max(0.0, min(wait, MAX_WAIT))
    while True:
        s.worker_seen = time.time()
        s.new_job.clear()            # before claim(): a job created after this point re-sets it
        job = s.claim()
        if job is not None:
            return JSONResponse(job)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return Response(status_code=204)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(s.new_job.wait(), timeout=remaining)


@app.post("/v1/worker/jobs/{job_id}/result", dependencies=[Depends(worker)])
async def submit(job_id: str, request: Request) -> dict:
    s = get_store()
    s.worker_seen = time.time()
    try:
        payload = json.loads(await request.body())
        lease_id, result = payload["lease"], payload["result"]
        assert isinstance(result, dict)
    except Exception:
        raise HTTPException(400, "body must be {'lease': str, 'result': object}")
    if not s.finish(job_id, result, lease=lease_id):
        raise HTTPException(409, "unknown job, already finished, or lease expired")
    return {"ok": True}
