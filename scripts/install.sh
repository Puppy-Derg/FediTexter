#!/usr/bin/env bash
# Installs FediTexter so it can be launched from anywhere on the system:
#
#   - builds the release server
#   - copies it to ~/.local/bin/feditexter-server
#   - creates a `feditexter-tui` launcher (runs `feditexter-server --tui`)
#   - scaffolds ~/.config/feditexter/server.env if it doesn't exist
#
# After this, `feditexter-tui` works from any directory.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="${FEDITEXTER_BIN_DIR:-$HOME/.local/bin}"
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/feditexter"

cd "$REPO"
echo "==> Building release server…"
cargo build --release -p feditexter-server

mkdir -p "$BIN_DIR" "$CONFIG_DIR"
install -m755 "target/release/feditexter-server" "$BIN_DIR/feditexter-server"

cat > "$BIN_DIR/feditexter-tui" <<'EOF'
#!/usr/bin/env bash
exec feditexter-server --tui "$@"
EOF
chmod +x "$BIN_DIR/feditexter-tui"

if [ ! -f "$CONFIG_DIR/server.env" ]; then
  cat > "$CONFIG_DIR/server.env" <<'EOF'
# FediTexter server configuration. feditexter-server reads this file when it is
# launched from outside the repo (no .env in the working directory).
DATABASE_URL=mysql://user:password@localhost:3306/feditexter
BIND_ADDR=127.0.0.1
PORT=3000
PUBLIC_DOMAIN=localhost
# Set to 0 to auto-verify new accounts (dev only).
REQUIRE_EMAIL_VERIFICATION=1
EOF
  echo "==> Created $CONFIG_DIR/server.env — edit it with your real database credentials."
else
  echo "==> $CONFIG_DIR/server.env already exists (left unchanged)."
fi

case ":$PATH:" in
  *":$BIN_DIR:"*) : ;;
  *) echo "==> NOTE: $BIN_DIR is not on your PATH; add 'export PATH=\"\$HOME/.local/bin:\$PATH\"' to ~/.zshrc (or ~/.bashrc)." ;;
esac

echo "==> Done. Launch the dashboard from anywhere with: feditexter-tui"
