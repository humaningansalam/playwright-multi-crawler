# Changelog

All notable changes to this project are documented in this file.

## [0.3.0] - 2026-07-19

### Added

- Added job cancellation for queued and running work, including terminal cancellation results and safe job-name reuse.
- Added persisted stdout/stderr logs and live Server-Sent Events streaming while jobs are running.
- Added structured health, metrics, result, error, and download contracts for operators and API clients.

### Changed

- Made browser and worker readiness explicit, validated the shared Chromium CDP endpoint, and completed startup, browser-loss, timeout, and shutdown cleanup.
- Made worker results an explicit JSON contract: tuples become arrays, invalid or lossy values fail with `WORKER_RESULT_INVALID`, and result files are written atomically.
- Streamed uploads and worker logs without blocking the event loop, bounded retained log tails, and preserved terminal log whitespace.
- Aligned README examples, bundled clients, CI, systemd deployment, package metadata, and wheel contents with the `uv` and `src.main` runtime.

### Fixed

- Fixed cancellation races, duplicate job-name ownership, terminal-state handling, interrupted results, and subprocess-group cleanup.
- Fixed upload rollback, reserved filename handling, helper-module imports, job working directories, and unavailable-worker submissions.
- Fixed result parsing and error preservation across worker exit, cleanup failure, cancellation, and invalid result paths.
- Fixed download URL encoding, traversal and symlink protection, inode replacement races, invalid-range descriptor leaks, and cancellation-time descriptor leaks.
- Fixed malformed multipart errors so clients consistently receive the documented structured API error response.
