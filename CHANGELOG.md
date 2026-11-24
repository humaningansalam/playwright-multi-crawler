# Changelog

All notable changes to this project are documented in this file.

## [0.4.3] - 2026-11-24

### Added

- Advertise the private-job contract and protect job status, cancellation, logs, results, and downloads with a per-job access token; persist only its digest.
- Store private multipart inputs separately from result artifacts, restrict their filesystem permissions, and purge them on terminal transitions or interrupted recovery.

### Fixed

- Reject legacy public Tistory credential submissions and hide old Tistory jobs from the API; discard known legacy credential files during recovery.
- Disable job-response caching to prevent shared caches from retaining sensitive results.

## [0.4.2] - 2026-11-24

### Changed

- Split job submission, download validation, worker execution, persisted-state recovery, and application lifecycle into explicit feature-local steps with domain-specific names.
- Separated runtime and development dependencies, added Ruff as a locked development tool, and aligned local validation with CI.
- Replaced generated repository boilerplate with concise contributor, repository, ignore, and deployment guidance.
- Pinned CI and deployment actions, installed locked runtime dependencies for production, and made the systemd unit run the project virtual environment directly. Deployments use the exact validated commit.

### Fixed

- Removed import-time configuration output and made cleanup-boundary failures consistently observable through module loggers.
- Made both bundled clients reject unsafe server-controlled filenames and stream downloads through atomically replaced temporary files.
- Updated vulnerable locked dependencies and added regression checks for dynamic script imports and isolated test storage.
- Removed the duplicate `dotenv` dependency and the unused `FileInfo` model.

## [0.4.1] - 2026-08-15

### Changed

- Simplified job lifecycle state coordination and removed redundant recovery/result I/O.

### Fixed

- Made the crawler CLI show structured API error codes and messages instead of generic HTTP status failures.

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
