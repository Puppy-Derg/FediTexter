# Project: FediTexter

Rust + Slint federated messaging app. Server: axum + MySQL. Client: Slint (material style) with a winit backend.

## Workflow rules

- **Wait for explicit user approval before any `git push`** (including tag pushes / force-pushes that trigger the CI release workflow). Make commits locally freely, but always ask before pushing.

## Build & verify

- Server binary: `cargo build -p feditexter-server` (dev) or `--release`.
- Client binary: `cargo build -p feditexter-client`; Slint UI in `crates/feditexter-client/ui/app.slint` (build.rs compiles it with `with_style("material")`).
- Local dev server: `./target/debug/feditexter-server` from repo root (reads `.env`); logs to `/tmp/feditexter-server.log`.
- Releases: CI workflow `.github/workflows/release.yml` runs on tags `v*`. After a change, retag with `git tag -f vX.Y.Z` and force-push the tag (requires approval).
- macOS ships ONLY as the universal `.app` zip (`feditexter-client-macos-universal.zip`); raw mac binaries are intermediates only.

## Gotchas

- Client `build.rs` uses the material style.
- ListView/ScrollView `viewport-y` is NEGATIVE (0 at top, `-(viewport-height - visible-height)` at bottom).
- `scraper::Html` is not `Sync` — keep it out of async futures.
- TOTP secrets are base32 (hand-rolled `base32_encode`), not hex.
- MySQL `COUNT(*)`/`SELECT 1` decode as signed → bind to `i64`, not `u64`.
- 2FA is mandatory: server rejects authenticated endpoints with 403 unless `totp_enabled`; a lax extractor allows `2fa/setup`, `2fa/enable`, `verify`, `verify/resend`, `logout`.
- Slint `Window` cannot specify both `width` and `min-width`; use `preferred-width/height` + `min-width/height`.
