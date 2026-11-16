# 39 — Local API Session Lifecycle

Date: 2026-11-16
Scope: local REST/MCP session ownership and listener lifecycle

No Zalo protocol or request-signing behavior changed. No live Zalo account or network request was used for this fix.

Automated evidence in `src/api_mcp.rs` covers account replacement, logout, expiry, a concurrent stale snapshot, an API listener tracking account A → B → logout without restart, duplicate bind rejection and shutdown. `src/session.rs` verifies cloned snapshots are revoked and cannot dispatch after the generation changes. `src/main.rs` verifies CLI port precedence over `CTDOZLITE_API_PORT`, environment fallback and invalid values.

The final generation check is the authorization point for dispatch. A request that has already passed that check and entered `reqwest` may complete after a session transition; later sends from that stale snapshot are rejected.
