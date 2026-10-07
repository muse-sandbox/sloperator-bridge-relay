#!/usr/bin/env python3
"""Client for the sloperator bridge relay — stdlib only, so any agent process can import it.

    from bridge_client import call
    r = call("jenkins.mu.se", "/job/X/api/json")
    r.raise_for_status(); data = r.json()

    r = call("jenkins.mu.se", "/job/X/buildWithParameters", method="POST", query={"P": "1"})
    r = call("greenlog.ultimate-guitar.com", "/api/views/search/sync", method="POST", json={...})
    r = call("git.wsmgroup.ru", "/api/v4/projects/123/repository/files/README.md/raw",
             query={"ref": "master"})

CLI:
    bridge_client.py GET jenkins.mu.se /api/json
    bridge_client.py POST jenkins.mu.se /job/X/build --query delay=0sec
    bridge_client.py status

Env: BRIDGE_RELAY_URL (default http://127.0.0.1:8095), BRIDGE_AGENT_TOKEN (required).
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

DEFAULT_URL = "http://127.0.0.1:8095"
QUEUE_TTL = 90          # the relay fails a job no worker took within this many seconds


class BridgeError(Exception):
    """The bridge could not get an upstream answer (policy refusal, worker offline, timeout…)."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


class UpstreamHTTPError(Exception):
    def __init__(self, response: "Response"):
        super().__init__(f"HTTP {response.status}: {response.text[:300]}")
        self.response = response


@dataclass
class Response:
    status: int
    headers: dict = field(default_factory=dict)
    content: bytes = b""
    elapsed_ms: int = 0

    @property
    def ok(self) -> bool:
        return self.status < 400

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self):
        return json.loads(self.content)

    def raise_for_status(self) -> "Response":
        if not self.ok:
            raise UpstreamHTTPError(self)
        return self


def _relay(method: str, path: str, body: dict | None = None, timeout: float = 70) -> tuple[int, dict]:
    base = os.environ.get("BRIDGE_RELAY_URL", DEFAULT_URL).rstrip("/")
    token = os.environ.get("BRIDGE_AGENT_TOKEN", "")
    if not token:
        raise BridgeError("not_configured", "BRIDGE_AGENT_TOKEN is not set")
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{base}{path}", data=data, method=method, headers={
        "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")[:300]
        raise BridgeError("relay_http_" + str(e.code), detail) from None
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise BridgeError("relay_unreachable", str(e)) from None


def call(host: str, path: str, method: str = "GET", *, query: dict | None = None,
         json: object = None, body: str | bytes | None = None, headers: dict | None = None,
         timeout: float = 60) -> Response:
    """Run one HTTP request through the corporate worker and return the upstream response.

    Raises BridgeError when there is no upstream answer at all. An upstream 4xx/5xx is a
    normal Response (check `.ok` or call `.raise_for_status()`).
    """
    req: dict = {"host": host, "method": method.upper(), "path": path, "timeout": timeout}
    if query:
        req["query"] = query
    if headers:
        req["headers"] = headers
    if json is not None:
        req["json"] = json
    elif isinstance(body, bytes):
        req["body_b64"] = base64.b64encode(body).decode()
    elif body is not None:
        req["body"] = body

    deadline = time.monotonic() + QUEUE_TTL + timeout + 90
    code, job = _relay("POST", "/v1/call?wait=50", req)
    while code == 202:
        if time.monotonic() > deadline:
            raise BridgeError("client_timeout", f"job {job.get('id')} still {job.get('state')}")
        code, job = _relay("GET", f"/v1/jobs/{job['id']}?wait=50")
    res = job["result"]
    if res.get("error"):
        raise BridgeError(res["error"]["code"], res["error"]["message"])
    return Response(status=res["status"], headers=res.get("headers") or {},
                    content=base64.b64decode(res.get("body_b64") or ""),
                    elapsed_ms=res.get("elapsed_ms") or 0)


def status() -> dict:
    return _relay("GET", "/v1/status")[1]


def _main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("method", help="GET / HEAD / POST, or 'status'")
    ap.add_argument("host", nargs="?")
    ap.add_argument("path", nargs="?")
    ap.add_argument("--query", action="append", default=[], metavar="K=V")
    ap.add_argument("--json", dest="json_body", help="JSON request body")
    ap.add_argument("--data", help="raw request body")
    ap.add_argument("--header", action="append", default=[], metavar="K: V")
    ap.add_argument("--timeout", type=float, default=60)
    ap.add_argument("--out", help="write the response body to this file")
    a = ap.parse_args()

    try:
        if a.method == "status":
            print(json.dumps(status(), indent=2))
            return 0
        if not a.host or not a.path:
            ap.error("host and path are required")
        query: dict = {}
        for kv in a.query:
            k, _, v = kv.partition("=")
            query.setdefault(k, []).append(v)
        hdrs = dict(h.split(":", 1) for h in a.header)
        r = call(a.host, a.path, a.method, query=query or None,
                 json=json.loads(a.json_body) if a.json_body else None, body=a.data,
                 headers={k.strip(): v.strip() for k, v in hdrs.items()} or None,
                 timeout=a.timeout)
    except BridgeError as e:
        print(f"bridge error: {e}", file=sys.stderr)
        return 2
    print(f"HTTP {r.status} ({r.elapsed_ms} ms) {r.headers}", file=sys.stderr)
    if a.out:
        with open(a.out, "wb") as f:
            f.write(r.content)
    else:
        sys.stdout.write(r.text)
    return 0 if r.ok else 1


if __name__ == "__main__":
    raise SystemExit(_main())
