# Changelog

## [Unreleased]

### Added

- **`wa-mcp daemon`**: owns the WhatsApp session (bridge, account locks, database)
  and listens on `run/serve.sock`, a `0700` directory in the data directory. One
  daemon per data directory (`run/daemon.lock`). Started by hand it stays until
  Ctrl-C or SIGTERM; started by `serve` (`--idle-exit`) it stops once the last
  client has been gone for `WA_IDLE_GRACE` seconds (300 by default). Log in
  `run/daemon.log`; `doctor` reports whether it runs.

### Changed

- **`wa-mcp serve` is a thin stdio client of the daemon** on macOS and Linux: it
  connects to `run/serve.sock`, starting the daemon (detached, own process group) if
  none listens. Several hosts, or one host starting several instances (Claude
  Desktop for its chats and for Cowork/Code sessions), now share one WhatsApp
  session instead of the second failing on the account lock, and stopping one host
  no longer affects the others. Each client keeps its own `--read-only`; the send
  limit (20/min per account) and the locks of accounts paired through MCP belong to
  the daemon. If the daemon cannot start (account held by `pair`/`run`), `serve`
  says why at once. Windows keeps the previous in-process `serve`.

### Fixed

- The `resources/subscribe` notification task outlived its connection; it now stops
  with it.

## [0.1.1] - 2026-09-30

### Fixed

- Output schemas declared as required some fields that are omitted when empty or
  false (`edited`, `deleted`, `has_media`, `reactions` on messages, `archived` and
  `pinned` on chats, `groups` on contacts, `truncated` on `list_recent`). Strict MCP
  hosts rejected the output as not matching its schema. These fields are now
  optional in the schema; responses are unchanged.

## [0.1.0] - 2026-09-30

First public release.

### Added

- **Bridge** (Go, whatsmeow): one process for any number of accounts, driven over
  stdin/stdout with length-prefixed protobuf frames (`proto/bridge.proto`). Pairing by
  QR code or 8-character phone code, three-level reconnection (whatsmeow, backoff,
  watchdog), at-least-once delivery (`SynchronousAck`, decrypted event buffer).
- **Ingestion**: live messages and history sync into one SQLite database per account,
  raw protobuf kept for every message, phone-number / LID identity unification with
  retroactive merge, replies, reactions, edits, deletions for everyone, receipts,
  contacts, groups and participants, chat settings synced from other devices,
  FTS5 accent-insensitive search.
- **MCP server** (Rust, rmcp, stdio): 39 tools (reading, search, contacts, media,
  sending, message actions, chat settings, groups and communities, profile, blocklist,
  privacy, multi-account pairing), output schemas and risk annotations on every tool,
  resources and URI templates, 5 prompts, argument completion, live resource
  notifications (`resources/subscribe` and `subscriptions/listen`).
- **Media**: raw files returned to the host model (inline image or audio when small),
  expired media re-requested from the phone, on-demand older history.
- **Safety**: per-account lock, 20 sends per minute rate limit, edit and delete windows
  enforced, `--read-only` mode.
- **Tooling**: `wa-mcp doctor`, `install.sh`, prebuilt binaries for macOS and Linux
  (arm64, x86_64).
