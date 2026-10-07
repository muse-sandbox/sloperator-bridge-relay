"""Relay tests against a real uvicorn server. Run: python -m pytest -q tests

The end-to-end test also runs the real worker (analytics-tools/muse-slop-bridge-worker)
when BRIDGE_WORKER_DIR points at it; it is skipped otherwise.
"""
from __future__ import annotations

import base64
import json
import os
import socket
import sys
import threading
import time
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "relay"))
sys.path.insert(0, str(ROOT / "client"))

os.environ.update(RELAY_AGENT_TOKEN="agent-tok", RELAY_WORKER_TOKEN="worker-tok",
                  RELAY_DB=":memory:", RELAY_QUEUE_TTL_SECONDS="3", RELAY_LEASE_GRACE_SECONDS="1")

import uvicorn  # noqa: E402

import app as relay_app  # noqa: E402
import bridge_client  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def relay(monkeypatch):
    relay_app.store = None
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(relay_app.app, host="127.0.0.1", port=port,
                                           log_level="warning"))
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    base = f"http://127.0.0.1:{port}"
    monkeypatch.setenv("BRIDGE_RELAY_URL", base)
    monkeypatch.setenv("BRIDGE_AGENT_TOKEN", "agent-tok")
    yield base
    server.should_exit = True
    t.join(5)


def http(method, url, token, body=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {token}",
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, None


def fake_worker(base, stop, handler, token="worker-tok"):
    while not stop.is_set():
        code, job = http("GET", f"{base}/v1/worker/lease?wait=1", token)
        if code == 204:
            continue
        result = handler(job["request"])
        if result is not None:
            http("POST", f"{base}/v1/worker/jobs/{job['id']}/result", token,
                 {"lease": job["lease"], "result": result})


def ok_result(body: bytes, status=200):
    return {"ok": status < 400, "status": status, "headers": {"content-type": "application/json"},
            "body_b64": base64.b64encode(body).decode(), "body_bytes": len(body),
            "elapsed_ms": 5, "error": None}


def run_worker(base, handler):
    stop = threading.Event()
    t = threading.Thread(target=fake_worker, args=(base, stop, handler), daemon=True)
    t.start()
    return stop


def test_round_trip(relay):
    seen = []
    stop = run_worker(relay, lambda req: (seen.append(req), ok_result(b'{"a": 1}'))[1])
    try:
        r = bridge_client.call("jenkins.mu.se", "/api/json", query={"tree": "jobs[name]"})
    finally:
        stop.set()
    assert r.status == 200 and r.json() == {"a": 1}
    assert seen[0]["host"] == "jenkins.mu.se" and seen[0]["query"] == {"tree": "jobs[name]"}


def test_upstream_error_is_a_response(relay):
    stop = run_worker(relay, lambda req: ok_result(b"nope", status=404))
    try:
        r = bridge_client.call("jenkins.mu.se", "/missing")
    finally:
        stop.set()
    assert r.status == 404 and not r.ok
    with pytest.raises(bridge_client.UpstreamHTTPError):
        r.raise_for_status()


def test_policy_error_raises(relay):
    stop = run_worker(relay, lambda req: {**ok_result(b""), "ok": False, "status": None,
                                          "error": {"code": "host_not_allowed", "message": "x"}})
    try:
        with pytest.raises(bridge_client.BridgeError) as e:
            bridge_client.call("evil.com", "/")
    finally:
        stop.set()
    assert e.value.code == "host_not_allowed"


def test_tokens_are_separate(relay):
    assert http("GET", f"{relay}/v1/worker/lease?wait=0", "agent-tok")[0] == 401
    assert http("POST", f"{relay}/v1/jobs", "worker-tok", {"host": "a", "path": "/"})[0] == 401
    assert http("GET", f"{relay}/v1/status", "")[0] == 401
    assert http("POST", f"{relay}/v1/jobs", "agent-tok", {"host": "a"})[0] == 400


def test_worker_offline(relay):
    t0 = time.monotonic()
    with pytest.raises(bridge_client.BridgeError) as e:
        bridge_client.call("jenkins.mu.se", "/api/json")
    assert e.value.code == "worker_offline"
    assert time.monotonic() - t0 < 15


def test_lease_expiry_is_not_requeued(relay):
    calls = []
    # A worker that takes the job and dies: never reports.
    stop = run_worker(relay, lambda req: calls.append(req))
    try:
        with pytest.raises(bridge_client.BridgeError) as e:
            bridge_client.call("jenkins.mu.se", "/job/x/build", method="POST", timeout=1)
    finally:
        stop.set()
    assert e.value.code == "worker_lost"
    assert len(calls) == 1, "a lost POST must never be re-run"


def test_stale_lease_rejected(relay):
    code, job = http("POST", f"{relay}/v1/jobs", "agent-tok", {"host": "a", "path": "/"})
    code, leased = http("GET", f"{relay}/v1/worker/lease?wait=1", "worker-tok")
    assert http("POST", f"{relay}/v1/worker/jobs/{leased['id']}/result", "worker-tok",
                {"lease": "wrong", "result": ok_result(b"")})[0] == 409
    assert http("POST", f"{relay}/v1/worker/jobs/{leased['id']}/result", "worker-tok",
                {"lease": leased["lease"], "result": ok_result(b"")})[0] == 200
    assert http("POST", f"{relay}/v1/worker/jobs/{leased['id']}/result", "worker-tok",
                {"lease": leased["lease"], "result": ok_result(b"")})[0] == 409


def test_long_poll_wakes_immediately(relay):
    got = {}

    def lease():
        t0 = time.monotonic()
        got["code"], got["job"] = http("GET", f"{relay}/v1/worker/lease?wait=20", "worker-tok")
        got["dt"] = time.monotonic() - t0

    t = threading.Thread(target=lease)
    t.start()
    time.sleep(0.5)
    http("POST", f"{relay}/v1/jobs", "agent-tok", {"host": "a", "path": "/"})
    t.join(25)
    assert got["code"] == 200 and got["dt"] < 3


def test_status(relay):
    http("GET", f"{relay}/v1/worker/lease?wait=0", "worker-tok")
    st = bridge_client.status()
    assert st["worker_online"] is True


@pytest.mark.skipif(not os.environ.get("BRIDGE_WORKER_DIR"), reason="BRIDGE_WORKER_DIR not set")
def test_end_to_end_with_real_worker(relay, monkeypatch):
    sys.path.insert(0, os.environ["BRIDGE_WORKER_DIR"])
    import worker as real_worker
    monkeypatch.setattr(real_worker, "LEASE_WAIT", 1)
    stop = threading.Event()
    w = real_worker.Relay(relay, "worker-tok")
    t = threading.Thread(target=real_worker.loop, args=(w, stop), daemon=True)
    t.start()
    try:
        with pytest.raises(bridge_client.BridgeError) as e:
            bridge_client.call("evil.example", "/")
        assert e.value.code == "host_not_allowed"
        with pytest.raises(bridge_client.BridgeError) as e:
            bridge_client.call("git.wsmgroup.ru", "/api/v4/projects", method="POST")
        assert e.value.code == "method_not_allowed"
    finally:
        stop.set()
