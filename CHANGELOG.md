# Changelog

## 2026-10-15 — Correct failed-probe classification

Fixed WAN disconnections being reported as UNKNOWN when the operating system
returned a socket error outside the previous error-number allowlist. Every
unsuccessful socket connection now counts as a failed target probe. A round
succeeds when at least one configured Internet target is reached.

Consecutive-failure and recovery thresholds, confirmation-time transition
timestamps, SQLite persistence, offline delivery retries, and the Home Assistant
Daily Activity ingestion contract are unchanged. Previously recorded UNKNOWN
intervals are not rewritten. The README explains ON, OFF, UNKNOWN, and remaining
reliability limitations.

Validation:

- All 18 tests passed locally and on the deployment host with Python 3.9.
- Regression coverage includes refusal, timeout, unreachable/down networks,
  unrecognized socket errors, mixed target outcomes, interrupted failure and
  recovery streaks, and offline delivery across a database reopen.
- Deployed code commit `2d919c0` with installed configuration and the existing
  database preserved; the service restarted and confirmed ON.
- A controlled WAN disconnection produced one outage and 4m 57s OFF in Home
  Assistant, matching the reporter's OFF-to-ON interval. No UNKNOWN transition
  occurred during the test. Delivery failures during disconnection were followed
  by successful reporting after reconnection.

Existing daily UNKNOWN totals can include earlier observations and the initial
confirmation period after a service restart; they do not reset when a fix is
deployed. Host timezone settings affect local log display, while event timestamps
use UTC. No host timezone change was performed as part of this deployment.
