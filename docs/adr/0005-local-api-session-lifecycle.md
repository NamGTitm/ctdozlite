# ADR-0005: Local API Session Ownership and Lifecycle

- **Status:** proposed
- **Date:** 2026-11-16
- **Deciders:** project maintainers
- **Related:** Issue #35, ADR-0004

## Context

The GUI Worker owns the active `Session`, while the previous REST startup path retained a clone and could start a second listener. That allowed API calls to keep using credentials from an account that had been switched or logged out, and startup errors could be discarded. `Session` is exported by the library with public fields; adding private generation state means downstream code that constructs `Session` with a struct literal must migrate to `Session::new`.

## Decision

Use a shared generation counter to revoke every cloned `Session` snapshot when the active session changes, logs out or expires. API operations take a snapshot without holding the session mutex over network I/O and validate the generation immediately before request dispatch and before returning results. The GUI Worker owns the GUI listener; CLI/API-only and TUI own at most one listener in their mode. CLI `--api-port` overrides `CTDOZLITE_API_PORT`. Listener shutdown is joined, bind/configuration failures are reported, and data operations without an active session return an unauthenticated response. ADR-0004's Bearer authentication and CORS policy remains required.

## Consequences

- Positive: account transitions revoke stale snapshots, duplicate startup paths are removed, and listener failures are surfaced.
- Negative: downstream Rust callers that use `Session` struct literals must move to `Session::new`; API data routes now return `401` when the session is absent, expired or transitioning.
- Limit: a Zalo request that passed its final generation check and was dispatched may complete after a transition because `reqwest` cannot recall it. Later dispatches from that snapshot fail closed.
- Persistence: generation state is in memory only; `session.json` format and Zalo request protocol remain unchanged.
- Rollback: stop the local API before reverting this change; restore the previous session behavior only with ADR-0004's authentication controls intact.

## Evidence

No Zalo protocol or wire behavior changed, so no network capture was used. Local regression coverage is recorded in [`re-notes/39_api_session_lifecycle.md`](../../re-notes/39_api_session_lifecycle.md).

## Alternatives Considered

| Option | Why rejected |
| :--- | :--- |
| Keep the detached cloned session | Account changes would leave stale credentials usable by API requests. |
| Hold the session mutex for the network request | Account changes and logout would wait on network I/O, blocking the Worker. |
| Start another listener from each mode | Duplicate ownership can cause bind conflicts and silent task failures. |
