# Changelog

All notable changes to this project. Versions follow SemVer (`0.x`).

## 0.7.0 — 2026-11-15

### Added

- **Incremental re-import of re-activated sessions.** When a session id already exists on the destination, import now appends the unseen tail if — and only if — the local log is a strict prefix of the bundled one. Diverged histories (both sides resumed) and sessions held by a live writer are skipped with an explicit reason instead of being silently dropped; a log-format mismatch and event types this DSH build would refuse to read are skipped too.
- Import report fields: `sessionsUpdated`, `eventsAppended`, `sessionsUpToDate`, `updatedSessions`, `seeded`.
- Export update report fields: `sessionsUpdated`, `eventsAdded`.
- Import UI: “Update existing sessions (fast-forward only)” checkbox (default on) plus an explicit note that appended events cannot be undone.
- `CHANGELOG.md`, minimal CI (`.github/workflows/ci.yml`), `.gitattributes`.

### Changed

- Same-id sessions are no longer reported as “本机已存在同名会话”. Skip reasons now distinguish up-to-date, local-newer and diverged cases; the message says “同 id 会话”.
- Seeded (fork) sessions are imported with DSH’s native structure — inherited prefix + `session/end-seed` marker + own events, created with `inheritedEventCount`. This fixes a pre-existing bug where the importer prepended the parent prefix again, duplicated events and made the batch fail the backend’s seq-contiguity check.

### Safety

- Appends go through `sessionPersistence.open(id, 'write')`, so the backend keeps its single-writer and contiguous-`seq` guarantees; the first appended event must start at the local next-seq.
- Lineage is proven with `header.createdAt` plus the `seq`/`type`/`time` tuple, so path rewriting can never fake a match or a mismatch across re-imports to a different root.
- The event vocabulary of the running DSH build is checked before an append; unknown non-`ignorable` types are refused instead of poisoning an existing session.
- The candidate log (local prefix + tail) is constructed once through DSH's own `Session` constructor via `ctx.sessions.prepare()` — no store entry, no announce — so malformed payloads of otherwise-known types are refused before anything is written.
- The plugin never rewrites or deletes existing local events, and updated sessions are excluded from “Undo this import”.

## 0.6.1 — 2026-11-13

- Remember the last export location; the placeholder shows the effective default directory.

## 0.6.0 — 2026-11-11

- Display name “备份与迁移”; remember import targets; browser `.zip` upload; zip transport envelope hardening.

## 0.5.3-local.7 — 2026-11-11

- Dialog pickers for the bundle and the import root; remember the last import root.
