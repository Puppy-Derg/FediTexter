#!/usr/bin/env bash
#
# FediTexter server — install / self-heal script.
#
# Idempotent: safe to re-run at any time. It repairs broken installs:
#   - missing dedicated service user          -> created
#   - missing/misowned /srv/feditexter        -> created/fixed
#   - missing .env or bad DATABASE_URL        -> regenerated + DB user/grants fixed
#   - missing/corrupt binary                  -> redownloaded from latest release
#   - missing/stale systemd unit              -> rewritten hardened unit
#
set -euo pipefail

REPO="Puppy-Derg/FediTexter"
ASSET="feditexter-server"
GH_BASE="https://github.com/$REPO/releases/latest/download/$ASSET"

INSTALL_DIR="${FEDITEXTER_INSTALL_DIR:-/srv/feditexter}"
SERVICE_USER="${FEDITEXTER_USER:-feditexter}"
SERVICE_NAME="${FEDITEXTER_SERVICE:-feditexter}"
PORT="${FEDITEXTER_PORT:-3100}"
BIND="${FEDITEXTER_BIND:-127.0.0.1}"
DOMAIN="${FEDITEXTER_DOMAIN:-dergdungeon.com.au}"
DB_NAME="${FEDITEXTER_DB:-feditexter}"
DB_USER="${FEDITEXTER_DB_USER:-feditexter}"

# Email verification / SMTP (configure via FEDITEXTER_SMTP_* or the prompts below)
SMTP_HOST="${FEDITEXTER_SMTP_HOST:-}"
SMTP_PORT="${FEDITEXTER_SMTP_PORT:-587}"
SMTP_USER="${FEDITEXTER_SMTP_USER:-}"
SMTP_PASS="${FEDITEXTER_SMTP_PASS:-}"
SMTP_FROM="${FEDITEXTER_SMTP_FROM:-}"
SMTP_REPLY_TO="${FEDITEXTER_SMTP_REPLY_TO:-}"

log()  { echo "[install] $*"; }
die()  { echo "[install] ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

is_elf() { [[ "$(head -c4 "$1" 2>/dev/null)" == $'\x7fELF' ]]; }

latest_tag() {
    curl -sI "$GH_BASE" | grep -i '^location:.*releases/download/' | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1
}

gen_password() { openssl rand -hex 24; }

# Add or update a KEY=VALUE line in .env, escaping & and | for sed.
set_env() {
    local key="$1" val="$2"
    local esc
    esc="$(printf '%s' "$val" | sed 's/[&|]/\\&/g')"
    if grep -q "^$key=" "$ENV_FILE"; then
        sed -i "s|^$key=.*|$key=$esc|" "$ENV_FILE"
    else
        printf '%s=%s\n' "$key" "$val" >> "$ENV_FILE"
    fi
}

# Ask for a value unless one was already supplied via env var.
ask() { # var_name prompt
    local var="$1" prompt="$2"
    if [[ -n "${!var}" ]]; then
        return
    fi
    read -r -p "$prompt" val
    eval "$var=\$val"
}

# Install and configure Postfix as a loopback-only outbound mail relay so the
# server can send verification emails without an external SMTP provider.
# "loopback-only" means it never accepts mail from the internet (ignore inbound).
setup_postfix() {
    log "installing Postfix (loopback-only, outbound)"
    DEBIAN_FRONTEND=noninteractive apt-get install -y postfix >/dev/null 2>&1 || \
        DEBIAN_FRONTEND=noninteractive apt-get install -y postfix
    postconf -e "inet_interfaces = loopback-only"
    postconf -e "myhostname = $DOMAIN"
    postconf -e "mydomain = $DOMAIN"
    postconf -e "myorigin = \$mydomain"
    postconf -e "mydestination = \$myhostname, localhost"
    postconf -e "mynetworks = 127.0.0.0/8"
    postconf -e "inet_protocols = ipv4"
    systemctl enable --now postfix >/dev/null 2>&1 || true
    systemctl restart postfix
    log "Postfix ready on 127.0.0.1:25 (external mail is ignored)"
}

# Configure email verification + SMTP in .env (prompts when not provided).
setup_email() {
    if [[ -z "$SMTP_HOST" ]]; then
        if [[ "${FEDITEXTER_SMTP:-postfix}" == "none" ]]; then
            log "email verification ON but SMTP skipped; codes will be logged to journalctl -u $SERVICE_NAME"
            set_env REQUIRE_EMAIL_VERIFICATION 1
            return
        fi
        setup_postfix
        SMTP_HOST="127.0.0.1"
        SMTP_PORT="25"
        SMTP_USER=""
        SMTP_PASS=""
        SMTP_FROM="noreply@$DOMAIN"
    elif [[ -t 0 ]]; then
        echo ""
        echo "Configuring external SMTP for email verification."
        ask SMTP_PORT "SMTP port [$SMTP_PORT]: "
        ask SMTP_USER "SMTP username: "
        ask SMTP_PASS "SMTP password: "
        ask SMTP_FROM "From email (no-reply preferred) [noreply@$DOMAIN]: "
        [[ -n "$SMTP_FROM" ]] || SMTP_FROM="noreply@$DOMAIN"
    fi
    set_env REQUIRE_EMAIL_VERIFICATION 1
    if [[ -n "$SMTP_HOST" ]]; then
        set_env SMTP_HOST "$SMTP_HOST"
        set_env SMTP_PORT "$SMTP_PORT"
        set_env SMTP_USERNAME "$SMTP_USER"
        set_env SMTP_PASSWORD "$SMTP_PASS"
        set_env SMTP_FROM "$SMTP_FROM"
        [[ -n "$SMTP_REPLY_TO" ]] || SMTP_REPLY_TO="noreply@${SMTP_FROM##*@}"
        set_env SMTP_REPLY_TO "$SMTP_REPLY_TO"
        log "email verification enabled (SMTP $SMTP_HOST:$SMTP_PORT)"
    else
        log "WARNING: email verification is ON but SMTP is not configured; codes will be logged to journalctl -u $SERVICE_NAME"
    fi
}

# DATABASE_URL=mysql://user:pass@host:port/db  -> fills DB_URL_* globals
parse_db_url() {
    if [[ "$1" =~ ^mysql://([^:]+):([^@]+)@([^:/]+):([0-9]+)/([^/]+)$ ]]; then
        DB_URL_USER="${BASH_REMATCH[1]}"
        DB_URL_PASS="${BASH_REMATCH[2]}"
        DB_URL_HOST="${BASH_REMATCH[3]}"
        DB_URL_PORT="${BASH_REMATCH[4]}"
        DB_URL_DB="${BASH_REMATCH[5]}"
        return 0
    fi
    return 1
}

# ---------------------------------------------------------------------------
# 0. must be root
# ---------------------------------------------------------------------------
[[ $EUID -eq 0 ]] || die "run as root (sudo ./install.sh)"

# ---------------------------------------------------------------------------
# 1. dedicated system user
# ---------------------------------------------------------------------------
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd -r -s /usr/sbin/nologin "$SERVICE_USER"
    log "created system user $SERVICE_USER"
else
    log "system user $SERVICE_USER already exists"
fi

# ---------------------------------------------------------------------------
# 2. install dir
# ---------------------------------------------------------------------------
mkdir -p "$INSTALL_DIR"

# ---------------------------------------------------------------------------
# 3. base packages
# ---------------------------------------------------------------------------
command -v curl >/dev/null 2>&1 || { apt-get update -y; apt-get install -y curl; }
if ! command -v mariadb >/dev/null 2>&1; then
    log "installing MariaDB server + client"
    apt-get update -y
    apt-get install -y mariadb-server mariadb-client
fi
systemctl enable --now mariadb >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# 4. .env + database self-heal
# ---------------------------------------------------------------------------
ENV_FILE="$INSTALL_DIR/.env"
DB_PASSWORD=""
FIXED_DB=0

if [[ -f "$ENV_FILE" ]] && grep -q '^DATABASE_URL=mysql://' "$ENV_FILE"; then
    DB_URL="$(grep '^DATABASE_URL=' "$ENV_FILE" | head -1 | cut -d= -f2-)"
    parse_db_url "$DB_URL"
    DB_PASSWORD="$DB_URL_PASS"
    log "found existing DATABASE_URL; testing connectivity..."
    if MYSQL_PWD="$DB_PASSWORD" mariadb -h "$DB_URL_HOST" -P "$DB_URL_PORT" -u "$DB_URL_USER" \
        "$DB_URL_DB" -e "SELECT 1" &>/dev/null; then
        log "database credentials are valid"
    else
        log "database credentials invalid -> regenerating"
        FIXED_DB=1
        DB_PASSWORD="$(gen_password)"
    fi
else
    log "no valid .env -> generating fresh database credentials"
    FIXED_DB=1
    DB_PASSWORD="$(gen_password)"
fi

if [[ $FIXED_DB -eq 1 ]]; then
    log "ensuring database '$DB_NAME' exists"
    mariadb -e "CREATE DATABASE IF NOT EXISTS \`$DB_NAME\` CHARACTER SET utf8mb4;"
    log "creating/updating DB user '$DB_USER' (localhost + 127.0.0.1)"
    mariadb -e "CREATE USER IF NOT EXISTS '$DB_USER'@'localhost' IDENTIFIED BY '$DB_PASSWORD';"
    mariadb -e "ALTER USER '$DB_USER'@'localhost' IDENTIFIED BY '$DB_PASSWORD';"
    mariadb -e "CREATE USER IF NOT EXISTS '$DB_USER'@'127.0.0.1' IDENTIFIED BY '$DB_PASSWORD';"
    mariadb -e "ALTER USER '$DB_USER'@'127.0.0.1' IDENTIFIED BY '$DB_PASSWORD';"
    mariadb -e "GRANT SELECT,INSERT,UPDATE,DELETE,CREATE,ALTER,DROP,INDEX,REFERENCES ON \`$DB_NAME\`.* TO '$DB_USER'@'localhost';"
    mariadb -e "GRANT SELECT,INSERT,UPDATE,DELETE,CREATE,ALTER,DROP,INDEX,REFERENCES ON \`$DB_NAME\`.* TO '$DB_USER'@'127.0.0.1';"
    mariadb -e "FLUSH PRIVILEGES;"
fi

# write .env (root-only) — only when credentials changed; otherwise just
# ensure the non-secret keys are present in the existing working file.
if [[ $FIXED_DB -eq 1 ]]; then
    umask 077
    cat > "$ENV_FILE" <<EOF
DATABASE_URL=mysql://$DB_USER:$DB_PASSWORD@127.0.0.1:3306/$DB_NAME
BIND_ADDR=$BIND
PORT=$PORT
PUBLIC_DOMAIN=$DOMAIN
EOF
else
    grep -q '^BIND_ADDR=' "$ENV_FILE" || printf 'BIND_ADDR=%s\n' "$BIND" >> "$ENV_FILE"
    grep -q '^PORT=' "$ENV_FILE" || printf 'PORT=%s\n' "$PORT" >> "$ENV_FILE"
    grep -q '^PUBLIC_DOMAIN=' "$ENV_FILE" || printf 'PUBLIC_DOMAIN=%s\n' "$DOMAIN" >> "$ENV_FILE"
fi
chown root:root "$ENV_FILE"
chmod 600 "$ENV_FILE"
log "wrote $ENV_FILE (chmod 600, root-owned)"

setup_email
chown root:root "$ENV_FILE"
chmod 600 "$ENV_FILE"

# ---------------------------------------------------------------------------
# 5. binary download (latest release) + sanity check
# ---------------------------------------------------------------------------
TAG="$(latest_tag)"
log "latest release tag: $TAG"
TMP="$INSTALL_DIR/$ASSET.new"
curl -fsSL "$GH_BASE" -o "$TMP"
is_elf "$TMP" || die "downloaded file is not an ELF binary"
chmod 755 "$TMP"
chown "$SERVICE_USER:$SERVICE_USER" "$TMP"
mv -f "$TMP" "$INSTALL_DIR/$ASSET"
printf '%s\n' "$TAG" > "$INSTALL_DIR/.current-tag"
log "installed $ASSET ($TAG)"

# ---------------------------------------------------------------------------
# 6. hardened systemd unit
# ---------------------------------------------------------------------------
UNIT="/etc/systemd/system/$SERVICE_NAME.service"
cat > "$UNIT" <<EOF
[Unit]
Description=FediTexter server
After=network.target mariadb.service

[Service]
User=$SERVICE_USER
EnvironmentFile=$INSTALL_DIR/.env
ExecStart=$INSTALL_DIR/$ASSET
Restart=always
RestartSec=3
MemoryMax=512M
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectControlGroups=true
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
RestrictSUIDSGID=true
LockPersonality=true
CapabilityBoundingSet=

[Install]
WantedBy=multi-user.target
EOF
log "wrote $UNIT"

systemctl daemon-reload
systemctl enable "$SERVICE_NAME" >/dev/null 2>&1 || true
systemctl restart "$SERVICE_NAME"
systemctl is-active --quiet "$SERVICE_NAME" || die "service failed to start"

# ---------------------------------------------------------------------------
# 7. verify
# ---------------------------------------------------------------------------
sleep 1
HEALTH="$(curl -fsS "http://$BIND:$PORT/healthz" || true)"
if [[ "$HEALTH" == ok* ]]; then
    log "healthz OK: $HEALTH"
else
    die "healthz failed after install (got: '${HEALTH:-<empty>}'). Check: journalctl -u $SERVICE_NAME"
fi

log "done. nginx/HTTPS config is out of scope for this script."
