# sloperator-bridge-relay

Lets the agent on **muse_slop** (sloperator, no corporate VPN) call corporate services —
`git.wsmgroup.ru`, `jenkins.mu.se`, `greenlog.ultimate-guitar.com` — through a worker on a
VPS inside the corporate network. The VPS accepts no inbound connections, so the worker
**pulls** jobs from this relay over outbound HTTPS.

```
sloperator ──POST /v1/call──► relay (127.0.0.1:8095) ◄──GET /bridge/v1/worker/lease── worker on the VPS
sloperator ◄────result─────── relay                  ◄──POST …/jobs/<id>/result─────  (long-poll, outbound)
```

- **This repo** — the relay (FastAPI + SQLite, one container) and the agent client.
- **Worker** — `muse-slop-bridge-worker/` in `git.wsmgroup.ru/d.stoletov/analytics-tools`.
  The worker holds every service credential and enforces what is allowed (hosts, methods,
  POST paths); the relay only stores and forwards and never sees a credential.

## Using it from the agent

`client/bridge_client.py` is stdlib-only. On the server it is installed for sloperator at
`~/.config/sloperator-bridge-client.py`, with its settings in `~/.config/sloperator-bridge.env`
(`BRIDGE_RELAY_URL`, `BRIDGE_AGENT_TOKEN`).

```python
from bridge_client import call, BridgeError

r = call("jenkins.mu.se", "/job/X/lastBuild/api/json")
r.raise_for_status(); build = r.json()

call("jenkins.mu.se", "/job/X/buildWithParameters", method="POST", query={"PARAM": "v"})
call("greenlog.ultimate-guitar.com", "/api/views/search/sync", method="POST", json={...})
call("git.wsmgroup.ru", "/api/v4/projects/<id>/repository/files/<path>/raw", query={"ref": "master"})
```

```bash
set -a; . ~/.config/sloperator-bridge.env; set +a
python3 ~/.config/sloperator-bridge-client.py status
python3 ~/.config/sloperator-bridge-client.py GET jenkins.mu.se /api/json --query tree='jobs[name]'
```

- An upstream 4xx/5xx is a normal `Response` (`.ok`, `.status`, `.raise_for_status()`).
- `BridgeError.code` means there was no upstream answer: `host_not_allowed`, `bad_path`,
  `path_not_allowed`, `method_not_allowed`, `not_configured` (worker policy);
  `upstream_timeout`, `upstream_error`, `response_too_large` (worker → service);
  `worker_offline` (no worker took the job within 90 s), `worker_lost` (the worker took it
  and never answered), `relay_unreachable`, `relay_http_*`, `client_timeout`.
- **Jobs are never re-run automatically.** A lost POST could already have started a build,
  so `worker_lost` is reported, not retried. Retry deliberately if the call is safe.
- Limits: upstream timeout ≤ 300 s, response ≤ 20 MB, request body ≤ 2 MB.

## API

Agent (local only, `Authorization: Bearer $RELAY_AGENT_TOKEN`):

| Call | |
|---|---|
| `POST /v1/call?wait=55` | create a job and wait; `200` with `result`, or `202` → keep polling |
| `POST /v1/jobs` | create without waiting (`202`) |
| `GET /v1/jobs/<id>?wait=55` | `200` done / `202` pending / `404` unknown or expired (kept 24 h) |
| `GET /v1/status` | `worker_online`, `worker_last_seen`, job counts by state |

Worker (public as `https://<muse_slop>/bridge/v1/worker/…`, `Bearer $RELAY_WORKER_TOKEN`):

| Call | |
|---|---|
| `GET /v1/worker/lease?wait=25` | `200 {"id", "lease", "request"}` or `204` |
| `POST /v1/worker/jobs/<id>/result` | `{"lease", "result"}`; `409` if the lease expired |

Request / result schemas: the worker's README.

## Deploy

```bash
deploy/deploy.sh            # deploys the committed HEAD to the `muse_slop` ssh host
```

Ships `git archive HEAD` to `/opt/sloperator-bridge-relay`, generates `.env` with two random
tokens on the first run (kept on later runs), rebuilds the container, installs
`deploy/nginx-bridge.conf` as `/etc/nginx/snippets/sloperator-bridge.conf` (included from the
`muse-slop` site next to the interview snippet), reloads nginx and smoke-checks that the
worker path answers `401` without a token and the agent API is not public.

Worker token for the GitLab variable `RELAY_WORKER_TOKEN`:

```bash
ssh muse_slop "grep ^RELAY_WORKER_TOKEN= /opt/sloperator-bridge-relay/.env | cut -d= -f2"
```

Rotate a token: edit `/opt/sloperator-bridge-relay/.env`, `docker compose up -d`, update the
GitLab variable (worker) or rerun `deploy/deploy.sh` (agent side).

Operations: `docker compose -f /opt/sloperator-bridge-relay/compose.yml logs -f`;
public access log `/var/log/nginx/sloperator-bridge.access.log`.

## Tests

```bash
python3.13 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python -m pytest -q tests
# plus the end-to-end test with the real worker:
BRIDGE_WORKER_DIR=<analytics-tools checkout>/muse-slop-bridge-worker .venv/bin/python -m pytest -q tests
```
