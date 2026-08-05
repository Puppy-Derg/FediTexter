#!/usr/bin/env bash
#
# FediTexter server — update script.
#
# Compares the latest GitHub release tag against the locally recorded
# version (.current-tag), and if different: downloads the new binary,
# sanity-checks it, swaps it into place and restarts the service.
#
set -euo pipefail

REPO="Puppy-Derg/FediTexter"
ASSET="feditexter-server"
GH_BASE="https://github.com/$REPO/releases/latest/download/$ASSET"

INSTALL_DIR="${FEDITEXTER_INSTALL_DIR:-/srv/feditexter}"
SERVICE_NAME="${FEDITEXTER_SERVICE:-feditexter}"
PORT="${FEDITEXTER_PORT:-3100}"
BIND="${FEDITEXTER_BIND:-127.0.0.1}"

log() { echo "[update] $*"; }
die() { echo "[update] ERROR: $*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "run as root (sudo ./update.sh)"

[[ -d "$INSTALL_DIR" ]] || die "install dir $INSTALL_DIR does not exist — run install.sh first"
[[ -x "$INSTALL_DIR/$ASSET" ]] || die "binary missing — run install.sh first"

is_elf() { [[ "$(head -c4 "$1" 2>/dev/null)" == $'\x7fELF' ]]; }

latest_tag() {
    curl -sI "$GH_BASE" | grep -i '^location:.*releases/download/' | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1
}

CURRENT_TAG_FILE="$INSTALL_DIR/.current-tag"
CURRENT="$(cat "$CURRENT_TAG_FILE" 2>/dev/null || echo none)"
LATEST="$(latest_tag)"

log "current: $CURRENT"
log "latest:  $LATEST"

if [[ -n "$LATEST" && "$LATEST" == "$CURRENT" ]]; then
    log "already up to date ($LATEST)"
    exit 0
fi

log "downloading $LATEST ..."
TMP="$INSTALL_DIR/$ASSET.new"
curl -fsSL "$GH_BASE" -o "$TMP"
is_elf "$TMP" || die "downloaded file is not an ELF binary"
chmod 755 "$TMP"
chown "$(stat -c '%U:%G' "$INSTALL_DIR/$ASSET")" "$TMP"

mv -f "$TMP" "$INSTALL_DIR/$ASSET"
printf '%s\n' "$LATEST" > "$CURRENT_TAG_FILE"
log "installed $LATEST"

systemctl restart "$SERVICE_NAME"
systemctl is-active --quiet "$SERVICE_NAME" || die "service failed to restart"

sleep 1
HEALTH="$(curl -fsS "http://$BIND:$PORT/healthz" || true)"
if [[ "$HEALTH" == ok* ]]; then
    log "healthz OK: $HEALTH"
else
    die "healthz failed after update (got: '${HEALTH:-<empty>}'). Check: journalctl -u $SERVICE_NAME"
fi

log "done"
