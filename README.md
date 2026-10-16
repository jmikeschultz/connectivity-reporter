# Connectivity Reporter

A standalone client project for observing connectivity and reporting timestamped state intervals to Daily Activity in Home Assistant.

**Status:** implemented standalone Python reporter with a durable SQLite queue and systemd service.

## Behavior

- Observe connectivity from the machine running the reporter.
- Record ON, OFF, and UNKNOWN intervals using timestamps with explicit UTC offsets.
- Persist observations locally when Home Assistant is unreachable.
- Send closed intervals to `POST /api/daily_activity/events` using a source-specific ingestion token.
- Use stable event IDs for safe retries and process partial-batch acknowledgements.
- Keep credentials and runtime state outside version control.

Daily Activity remains the generic receiver. This repository owns connectivity observation, buffering, and delivery. Home Assistant's own Ping integration is not required.

## Receiver contract

Each request covers one configured reported-interval source:

```json
{
  "events": [
    {
      "source": "example_source",
      "event_id": "142",
      "state": "off",
      "start": "2026-10-12T10:00:00Z",
      "end": "2026-10-12T11:30:00Z"
    }
  ]
}
```

Use `Authorization: Bearer <source-specific-token>` and `Content-Type: application/json`. The receiver accepts at most 100 events and 64 KiB per request. Intervals are half-open, must not overlap, and cannot extend into the future. Exactly adjacent known intervals establish transitions. Retries must retain their original event IDs and contents.

The receiver returns HTTP 200 for successful/duplicate-only batches, 207 for per-event failures, 401 for invalid credentials, 403 for a source mismatch, and 503 when persistence cannot be confirmed. The reporter inspects individual results and avoid treating an HTTP 207 as complete delivery.

See the [Daily Activity producer contract](https://github.com/jmikeschultz/ha_daily_activity#http-producer-contract) for the complete schema, retention rules, and error responses.

## Development

Python 3.9 or newer is required. There are no runtime dependencies beyond the standard library.

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Install as a systemd service

On lakehouse, edit your local `config.json` with the HA endpoint, registered source ID, and ingestion token. Set:

```json
"database_path": "/var/lib/connectivity-reporter/connectivity.sqlite3"
```

For the private Tailscale connection to HA's IP, set `ha_verify_certificate` to `false` if its certificate does not match that IP. Then, from the cloned repository:

```bash
git pull
sudo ./install.sh
```

The installer validates configuration, creates the service account and private state directory, installs Python into an isolated environment without downloading dependencies, protects the configuration, installs the systemd unit, enables it at boot, and starts it. Python 3.9+ and systemd are required. It only controls the reporter service.

For subsequent code updates, use the same two commands. The installer preserves the installed configuration and database. To replace installed configuration with your edited local file explicitly:

```bash
sudo ./install.sh --config config.json
```

View logs with `sudo journalctl -u connectivity-reporter -f`. The installed configuration is `/etc/connectivity-reporter/config.local.json`.

Use `sudo systemctl restart connectivity-reporter` after editing configuration and `sudo systemctl stop connectivity-reporter` to stop it. Configure the reported source in Daily Activity before starting. Token issuance happens in the integration's configuration UI; a general HA access token is not accepted by this endpoint.

Checks start immediately and repeat every 60 seconds by default. With the sample thresholds, three failed rounds confirm OFF and two successful rounds confirm ON. Transitions are timestamped at confirmation, so shorter failures may be filtered and recorded outage duration has this detection delay. The initial period is UNKNOWN until a state is confirmed. A successful TCP connection to either public target indicates connectivity; this checks public TCP reachability, not DNS response correctness or every Internet service.

Open observations are durably saved every round. Restart recovery closes the previous interval at its last saved observation, omits the unobserved restart gap, and starts UNKNOWN. A long observation gap similarly ends old coverage at the last observation. Periodic checkpoints close sustained states for reporting. The delivery worker drains available batches independently of the probe loop and retries transient failures with capped exponential backoff and jitter. Invalid/conflicting/expired events remain in SQLite with an `error` and are excluded from retries; their IDs appear in the journal for diagnosis. Capacity failures and future timestamps are retried. Authentication failures retain the queue until configuration is corrected.

## Reporter configuration

The flat configuration is in `config.example.json`. Copy it to `config.local.json` when deploying. Local configuration is ignored by Git; only the sample belongs in version control.

| JSON setting | Purpose |
| --- | --- |
| `internet_probe_ips` | Public Internet targets: `1.1.1.1` and `8.8.8.8`. A successful TCP connection to either establishes reachability for a round. These are not home-network probes. |
| `internet_probe_tcp_port` | TCP port checked on each Internet target; 53 checks reachability of their DNS service without requiring hostname resolution. |
| `internet_check_seconds` | Time between Internet check rounds. |
| `internet_timeout_seconds` | Timeout per target. |
| `failed_checks_to_off` | Consecutive rounds where all targets fail before confirming OFF. |
| `successful_checks_to_on` | Consecutive successful rounds before confirming ON. |
| `checkpoint_seconds` | Close an interval during a sustained state; 900 seconds yields about 96 records/day before extra transitions. Size against receiver retention and its 10,000-record cap. |
| `database_path` | Durable ledger and delivery queue, relative to the configuration file's directory. |
| `ha_events_url` | Full Daily Activity ingestion endpoint, preferably reached over the private network. Certificate verification is controlled by `ha_verify_certificate`. |
| `ha_verify_certificate` | Boolean, default `true`. Set `false` to allow HA's HTTPS certificate to mismatch its private IP over the trusted Tailscale route. HTTPS remains encrypted, but server identity is not verified. |
| `ha_source_id` | Stable source ID registered in Daily Activity. |
| `ha_ingestion_token` | Source-specific secret token; keep only in the local configuration with restricted file access. |
| `report_seconds` | Normal delivery cadence. |
| `retry_max_seconds` | Maximum exponential retry delay. |

Fixed implementation defaults: accept either successful Internet target, use a 15-second HTTP timeout, cap batches at 100 events and 65,536 encoded bytes, retry from 5 seconds with jitter, and log at INFO without secrets. Treat observation gaps longer than the greater of 60 seconds or three check intervals as lost continuity.

The reported states mean:

- **ON:** At least one configured Internet target accepted a TCP connection for the configured number of consecutive successful rounds.
- **OFF:** Every configured target failed to connect for the configured number of consecutive failed rounds. Every socket connection error counts as failure, including refusal, timeout, unreachable/down networks, permission errors, and errors without a recognized error number. A failed target does not override another target's successful connection.
- **UNKNOWN:** Connectivity has not yet been confirmed, such as at startup or after a lost observation gap. A failed connection attempt never produces UNKNOWN. While a new state awaits confirmation, the previous state remains in effect; an opposite result resets the consecutive-round count. Unexpected programming errors propagate rather than becoming a connectivity observation.

Reporter downtime is omitted coverage. Confirmation thresholds delay state changes; transitions use the confirmation timestamp.

### Known reliability limitations

Outage timestamps remain confirmation times, rather than the first failed or recovered probe. Short outages can be filtered entirely, and both outage start and recovery are delayed by polling and their respective thresholds. This patch does not reconstruct previously reported UNKNOWN intervals.

An unexpected delivery-worker exception currently stops the reporter through its stop flag without propagating a failing exit status. With the supplied systemd `Restart=on-failure` policy, that path may leave reporting stopped until manually restarted. Also, the SQLite queue has no local retention or size limit; prolonged delivery failure or permanently rejected events can grow disk usage. These are follow-up concerns; this patch preserves delivery and persistence behavior.

Idempotency is mandatory behavior, not a setting: generate a unique event ID and durably persist the immutable closed interval before sending it. Every retry uses that same ID and content, including after a restart. Remove pending delivery only after the individual event is acknowledged as `accepted` or `duplicate`; inspect partial responses and retain rejected events with their error for diagnosis. Open intervals must be checkpointed into distinct, exactly adjacent closed intervals rather than modifying an already submitted event. Reporter downtime must not be extrapolated as known connectivity.

Configure the receiver's time zone, retention days, and ON → OFF counting direction in Home Assistant. The reporter sends explicit UTC timestamps; it does not need a separate calendar time zone or outage counter setting.

## Privacy and deployment

Use generic examples in tracked files. Never commit real ingestion tokens, private hostnames, personal information, or runtime observations. Store local credentials and configuration outside the repository or in ignored local files; `.gitignore` is a safeguard, not a substitute for reviewing staged changes.

Plan to reach Home Assistant over a trusted private network, such as Tailscale. No public router port forwarding is required. TLS verification defaults to enabled; the local configuration may disable it for the private Tailscale connection.
