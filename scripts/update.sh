#!/usr/bin/env bash
# Updates the FediTexter server and makes sure the DB is set up correctly for
# the next version, repairing it if required:
#
#   1. git pull
#   2. build the release server
#   3. run `feditexter-server migrate` (applies any pending migrations, creates
#      the _sqlx_migrations table if missing, and reports the schema state) —
#      this is the repair step: it runs even if the running server is down.
#   4. install the new binary
#   5. restart the systemd unit (if present) and verify /healthz
#
# Usage: scripts/update.sh
# Env:   FEDITEXTER_BIN_DIR  install dir (default ~/.local/bin)
#        FEDITEXTER_SERVICE  systemd unit name (default feditexter)
#        FEDITEXTER_ENV      path to the env file (default:
#                            $XDG_CONFIG_HOME/feditexter/server.env, else .env)
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="${FEDITEXTER_BIN_DIR:-$HOME/.local/bin}"
SERVICE="${FEDITEXTER_SERVICE:-feditexter}"
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/feditexter"
ENV_FILE="${FEDITEXTER_ENV:-}"

cd "$REPO"

echo "==> [1/5] Pulling latest code…"
git pull --ff-only

echo "==> [2/5] Building release server…"
cargo build --release -p feditexter-server

if [ -z "$ENV_FILE" ]; then
  if [ -f "$CONFIG_DIR/server.env" ]; then
    ENV_FILE="$CONFIG_DIR/server.env"
  elif [ -f ".env" ]; then
    ENV_FILE=".env"
  fi
fi

echo "==> [3/5] Applying migrations / repairing database setup…"
if [ -n "$ENV_FILE" ]; then
  target/release/feditexter-server --env "$ENV_FILE" migrate
else
  target/release/feditexter-server migrate
fi

echo "==> [4/5] Installing binary…"
mkdir -p "$BIN_DIR"
install -m755 target/release/feditexter-server "$BIN_DIR/feditexter-server"

echo "==> [5/5] Restarting service…"
RESTARTED=0
if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files --no-legend 2>/dev/null | grep -q "^${SERVICE}"; then
  systemctl restart "$SERVICE"
  RESTARTED=1
  sleep 2
else
  echo "    No systemd unit '$SERVICE' found — restart your server manually (e.g. 'feditexter-tui')."
fi

# Verify with healthz.
BIND_ADDR="$(grep -E '^BIND_ADDR=' "$ENV_FILE" 2>/dev/null | cut -d= -f2 || echo 127.0.0.1)"
PORT="$(grep -E '^PORT=' "$ENV_FILE" 2>/dev/null | cut -d= -f2 || echo 3000)"
if [ "$RESTARTED" = "1" ]; then
  HZ=$(curl -fsS -m 5 "http://${BIND_ADDR}:${PORT}/healthz" 2>/dev/null || echo "unreachable")
  echo "    healthz: $HZ"
fi

echo "==> Update complete."
