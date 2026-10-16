"""Persistent connectivity observations and scoped HTTP delivery."""
import argparse
import concurrent.futures
from datetime import datetime, timezone
import ipaddress
import json
import logging
from pathlib import Path
import random
import signal
import socket
import ssl
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

LOG = logging.getLogger(__name__)
DEFAULTS = dict(internet_probe_ips=['1.1.1.1', '8.8.8.8'], internet_probe_tcp_port=53,
                internet_check_seconds=60, internet_timeout_seconds=3,
                failed_checks_to_off=3, successful_checks_to_on=2,
                checkpoint_seconds=900, database_path='state/connectivity.sqlite3',
                report_seconds=60, retry_max_seconds=300, ha_verify_certificate=True)


def load_config(path):
    path = Path(path).resolve()
    raw = json.loads(path.read_text())
    allowed = set(DEFAULTS) | {'ha_events_url', 'ha_source_id', 'ha_ingestion_token'}
    if not isinstance(raw, dict) or set(raw) - allowed:
        raise ValueError('Configuration must be a flat object with supported settings')
    config = DEFAULTS | raw
    if type(config['ha_verify_certificate']) is not bool:
        raise ValueError('ha_verify_certificate must be a JSON boolean')
    for key in ('ha_events_url', 'ha_source_id', 'ha_ingestion_token', 'database_path'):
        if not isinstance(config.get(key), str) or not config[key].strip():
            raise ValueError(f'{key} is required')
    import re
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}', config['ha_source_id']):
        raise ValueError('Invalid ha_source_id')
    if config['ha_ingestion_token'] == 'REPLACE_ME' or not 32 <= len(config['ha_ingestion_token']) <= 256 or any(c.isspace() for c in config['ha_ingestion_token']):
        raise ValueError('Set a valid source-specific ha_ingestion_token')
    url = urllib.parse.urlsplit(config['ha_events_url'])
    if url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password or url.query or url.fragment or url.path != '/api/daily_activity/events':
        raise ValueError('ha_events_url must be the HTTP(S) events endpoint without credentials')
    for key in DEFAULTS:
        if key.endswith('_seconds') or key in ('internet_probe_tcp_port', 'failed_checks_to_off', 'successful_checks_to_on'):
            value = config[key]
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f'{key} must be a positive integer')
    if config['internet_probe_tcp_port'] > 65535 or config['checkpoint_seconds'] > 31 * 86400:
        raise ValueError('Port or checkpoint exceeds supported limits')
    targets = config['internet_probe_ips']
    if not isinstance(targets, list) or not targets:
        raise ValueError('internet_probe_ips must be a nonempty list of public IP addresses')
    for target in targets:
        if not isinstance(target, str) or not ipaddress.ip_address(target).is_global:
            raise ValueError('Internet probe targets must be public IP addresses')
    db = Path(config['database_path'])
    config['database_path'] = str(db if db.is_absolute() else path.parent / db)
    return config


def stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec='microseconds')


class Ledger:
    def __init__(self, path, source):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY, payload TEXT NOT NULL, error TEXT);
        ''')
        saved = self.db.execute("SELECT value FROM metadata WHERE key='source'").fetchone()
        if saved and saved[0] != source:
            raise ValueError('Database belongs to another source; use a separate database')
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO metadata VALUES('source', ?)", (source,))
        self.source = source
        self.state = 'unknown'
        self.start = None
        self.last = None
        self.candidate = None
        self.count = 0

    def _close(self, end):
        if self.start is not None and end > self.start:
            event_id = uuid.uuid4().hex
            event = dict(source=self.source, event_id=event_id, state=self.state,
                         start=stamp(self.start), end=stamp(end))
            self.db.execute('INSERT INTO events VALUES (?, ?, NULL)', (event_id, json.dumps(event)))
        self.start = end

    def observe(self, result, now, config):
        if self.last is not None and now < self.last:
            LOG.warning('Clock moved backwards; waiting for last observation timestamp')
            return
        gap_limit = max(60, 3 * config['internet_check_seconds'])
        with self.db:
            if self.last is not None and (now < self.last or now - self.last > gap_limit):
                # Stop at the last observation; never extrapolate through a lost observation gap.
                self._close(self.last)
                self.state, self.start, self.candidate, self.count = 'unknown', now, None, 0
            if self.start is None:
                self.start = now
            desired = 'unknown' if result is None else ('on' if result else 'off')
            if desired == self.candidate:
                self.count += 1
            else:
                self.candidate, self.count = desired, 1
            threshold = 1 if desired == 'unknown' else config['successful_checks_to_on' if desired == 'on' else 'failed_checks_to_off']
            # Timestamp transitions at confirmation, not retroactively at first failure.
            if desired != self.state and self.count >= threshold:
                self._close(now)
                self.state = desired
                LOG.info('Internet state: %s', desired)
            elif now - self.start >= config['checkpoint_seconds']:
                self._close(now)
            # Persist observed coverage on every round for abrupt restart recovery.
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES('open', ?)",
                            (json.dumps(dict(state=self.state, start=self.start, last=now)),))
        self.last = now

    def recover(self):
        row = self.db.execute("SELECT value FROM metadata WHERE key='open'").fetchone()
        if row:
            saved = json.loads(row[0])
            self.state, self.start = saved['state'], saved['start']
            with self.db:
                self._close(saved['last'])
                self.db.execute("DELETE FROM metadata WHERE key='open'")
            self.state, self.start = 'unknown', None

    def finish(self):
        with self.db:
            if self.last is not None:
                self._close(self.last)
            self.db.execute("DELETE FROM metadata WHERE key='open'")

    def batch(self):
        events = []
        for payload, in self.db.execute('SELECT payload FROM events WHERE error IS NULL ORDER BY rowid LIMIT 100'):
            candidate = events + [json.loads(payload)]
            if len(json.dumps({'events': candidate}).encode()) > 65536:
                break
            events = candidate
        return events

    def acknowledge(self, events, body):
        results = body.get('results') if isinstance(body, dict) else None
        if not isinstance(results, list) or len(results) != len(events):
            raise ValueError('Incomplete acknowledgement')
        seen = set()
        for result in results:
            if not isinstance(result, dict):
                raise ValueError('Invalid acknowledgement')
            index = result.get('index')
            if type(index) is not int or index in seen or not 0 <= index < len(events) or result.get('event_id') != events[index]['event_id']:
                raise ValueError('Mismatched acknowledgement')
            status, code = result.get('status'), result.get('http_status')
            if (status, code) not in [('accepted', 201), ('duplicate', 200)] and not (status == 'rejected' and type(code) is int and 400 <= code < 500 and isinstance(result.get('error'), str)):
                raise ValueError('Invalid result status')
            seen.add(index)
        retry = False
        with self.db:
            for result in results:
                event = events[result['index']]
                if result['status'] in ('accepted', 'duplicate'):
                    self.db.execute('DELETE FROM events WHERE id=?', (event['event_id'],))
                elif result['http_status'] == 429 or result['error'] == 'future_interval':
                    retry = True
                else:
                    self.db.execute('UPDATE events SET error=? WHERE id=?', (result['error'], event['event_id']))
                    LOG.error('Event %s rejected (%s); retained for diagnosis', event['event_id'], result['error'])
        return not retry


def check_target(ip, port, timeout):
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except OSError:
        # Any unsuccessful socket connection is evidence of failed reachability,
        # regardless of the platform-specific error (including unknown errno).
        return False


def probe(config, pool):
    futures = [pool.submit(check_target, ip, config['internet_probe_tcp_port'], config['internet_timeout_seconds']) for ip in config['internet_probe_ips']]
    results = [future.result() for future in futures]
    return any(results)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None  # Never forward the scoped token to a redirected host.


def deliver(config, events, ledger):
    request = urllib.request.Request(config['ha_events_url'], data=json.dumps({'events': events}).encode(),
                headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + config['ha_ingestion_token']}, method='POST')
    try:
        # Avoid inheriting proxy settings that could route private HA traffic elsewhere.
        context = ssl.create_default_context()
        if not config.get('ha_verify_certificate', True):
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(),
                                             urllib.request.HTTPSHandler(context=context))
        with opener.open(request, timeout=15) as response:
            if response.status not in (200, 207):
                return False
            body = response.read(65537)
            if len(body) > 65536:
                return False
            return ledger.acknowledge(events, json.loads(body))
    except urllib.error.HTTPError as error:
        LOG.warning('Delivery HTTP status %s; queued events retained', error.code)
    except (OSError, urllib.error.URLError, ValueError):
        LOG.warning('Delivery failed; queued events retained')
    return False


def run(config):
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    ledger = Ledger(config['database_path'], config['ha_source_id'])
    # One service process per database; also prevents accidental overlapping observations.
    import fcntl
    lock = open(config['database_path'] + '.lock', 'a')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise ValueError('Another reporter is using this database') from None
    ledger.recover()
    # Delivery runs in its own thread so HTTP delays cannot disrupt observation cadence.
    def sender():
        queue = Ledger(config['database_path'], config['ha_source_id'])
        delay = min(5, config['retry_max_seconds'])
        try:
            while not stop.is_set():
                events = queue.batch()
                success = not events or deliver(config, events, queue)
                wait = config['report_seconds'] if success else random.uniform(delay / 2, delay)
                delay = min(5, config['retry_max_seconds']) if success else min(config['retry_max_seconds'], delay * 2)
                if success and events and queue.batch():
                    continue
                stop.wait(wait)
        except Exception:
            LOG.error('Delivery worker stopped unexpectedly')
            stop.set()
        finally:
            queue.db.close()
    worker = threading.Thread(target=sender, name='delivery')
    worker.start()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(config['internet_probe_ips'])) as pool:
            while not stop.is_set():
                started = time.monotonic()
                result = probe(config, pool)
                ledger.observe(result, time.time(), config)
                stop.wait(max(0, config['internet_check_seconds'] - (time.monotonic() - started)))
    finally:
        stop.set()
        worker.join()
        ledger.finish()
        ledger.db.close()
        lock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--check-config', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        config = load_config(args.config)
        if args.check_config:
            print('Configuration valid')
        else:
            run(config)
    except (ValueError, OSError, json.JSONDecodeError):
        parser.exit(1, 'Reporter failed: check configuration, database access, and running instances.\n')
