#!/usr/bin/env bash
# Deploy the committed HEAD to muse_slop: ship the tree, (first run) generate tokens,
# rebuild and restart the container, install the nginx snippet, smoke-check.
#   deploy/deploy.sh [ssh-host]        (default: muse_slop)
set -euo pipefail

HOST="${1:-muse_slop}"
DIR=/opt/sloperator-bridge-relay
cd "$(git rev-parse --show-toplevel)"
REV="$(git rev-parse --short HEAD)"
git diff --quiet HEAD -- . || { echo "uncommitted changes — commit first, deploys are from HEAD"; exit 1; }

git archive --format=tar HEAD | ssh "$HOST" "mkdir -p $DIR && tar -x -C $DIR && echo $REV > $DIR/REVISION"

ssh "$HOST" bash -s <<REMOTE
set -euo pipefail
cd $DIR
if [ ! -f .env ]; then
  umask 077
  printf 'RELAY_AGENT_TOKEN=%s\nRELAY_WORKER_TOKEN=%s\n' "\$(openssl rand -hex 32)" "\$(openssl rand -hex 32)" > .env
  echo "generated .env with fresh tokens"
fi
chmod 600 .env

# Hand the agent token to sloperator (runs on the host as \$SLOPERATOR_USER), readable only by it.
SLOPERATOR_USER="${SLOPERATOR_USER:-egor}"
if id "\$SLOPERATOR_USER" >/dev/null 2>&1; then
  HOME_DIR="\$(getent passwd "\$SLOPERATOR_USER" | cut -d: -f6)"
  install -d -m 700 -o "\$SLOPERATOR_USER" "\$HOME_DIR/.config"
  { echo "BRIDGE_RELAY_URL=http://127.0.0.1:8095"; grep '^RELAY_AGENT_TOKEN=' .env | sed 's/^RELAY_AGENT_TOKEN=/BRIDGE_AGENT_TOKEN=/'; } \
    | install -m 600 -o "\$SLOPERATOR_USER" /dev/stdin "\$HOME_DIR/.config/sloperator-bridge.env"
  install -m 644 -o "\$SLOPERATOR_USER" client/bridge_client.py "\$HOME_DIR/.config/sloperator-bridge-client.py"
fi
mkdir -p data && chown 10095:10095 data && chmod 700 data
docker compose up -d --build --remove-orphans

install -m 644 deploy/nginx-bridge.conf /etc/nginx/snippets/sloperator-bridge.conf
SITE=/etc/nginx/sites-available/muse-slop
if ! grep -q 'snippets/sloperator-bridge.conf' \$SITE; then
  cp \$SITE \$SITE.bak.\$(date +%Y%m%d%H%M%S)
  sed -i 's|^\(\s*\)include /etc/nginx/snippets/muse-slop-interview.conf;|&\n\1include /etc/nginx/snippets/sloperator-bridge.conf;|' \$SITE
fi
nginx -t && systemctl reload nginx

for i in \$(seq 1 30); do
  curl -fsS http://127.0.0.1:8095/healthz >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS http://127.0.0.1:8095/healthz && echo
echo "public worker endpoint without token (expect 401):"
curl -s -o /dev/null -w '%{http_code}\n' https://127.0.0.1/bridge/v1/worker/lease -k
echo "agent API must NOT be public (expect 404):"
curl -s -o /dev/null -w '%{http_code}\n' https://127.0.0.1/bridge/v1/status -k
echo "deployed \$(cat REVISION)"
REMOTE
