import json
import concurrent.futures
import errno
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from connectivity_reporter.reporter import DEFAULTS, Ledger, check_target, deliver, load_config, probe, stamp


class ProbeTests(unittest.TestCase):
    def test_successful_connection_is_closed(self):
        with patch('socket.create_connection') as connect:
            self.assertIs(check_target('1.1.1.1', 53, 3), True)
            connect.assert_called_once_with(('1.1.1.1', 53), timeout=3)
            connect.return_value.__exit__.assert_called_once()

    def test_every_socket_error_is_a_failed_probe(self):
        errors = [ConnectionRefusedError(errno.ECONNREFUSED, 'refused'),
                  TimeoutError('timed out'),
                  OSError(errno.ENETUNREACH, 'network unreachable'),
                  OSError(errno.EHOSTUNREACH, 'host unreachable'),
                  OSError(errno.ENETDOWN, 'network down'),
                  OSError(errno.EACCES, 'permission denied'),
                  OSError(123456, 'unrecognized socket error'), OSError()]
        for error in errors:
            with self.subTest(error=repr(error)), patch('socket.create_connection', side_effect=error):
                self.assertIs(check_target('1.1.1.1', 53, 3), False)

    def test_any_reached_target_makes_round_successful(self):
        for outcomes in [(True, True), (True, False), (False, True), (False, False)]:
            def connect(address, timeout):
                if outcomes[DEFAULTS['internet_probe_ips'].index(address[0])]:
                    return MagicMock()
                raise OSError(errno.ENETDOWN, 'network down')
            with self.subTest(outcomes=outcomes), patch('socket.create_connection', side_effect=connect):
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    self.assertIs(probe(DEFAULTS, pool), any(outcomes))

    def test_unexpected_programming_error_is_not_a_connectivity_result(self):
        with patch('socket.create_connection', side_effect=RuntimeError('unexpected')):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                with self.assertRaises(RuntimeError):
                    probe(DEFAULTS, pool)


class ReporterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'queue.sqlite3')
        self.ledger = Ledger(self.path, 'lakehouse_internet')
        self.config = DEFAULTS | dict(checkpoint_seconds=120, failed_checks_to_off=1, successful_checks_to_on=1)

    def tearDown(self):
        self.ledger.db.close()
        self.tmp.cleanup()

    def test_outage_contiguity_and_checkpoint(self):
        for result, now in [(True, 1000), (True, 1060), (False, 1120), (False, 1180), (True, 1240), (True, 1300), (True, 1360)]:
            self.ledger.observe(result, now, self.config)
        events = self.ledger.batch()
        self.assertEqual([e['state'] for e in events], ['on', 'off', 'on'])
        self.assertEqual(events[0]['end'], events[1]['start'])
        self.assertEqual(events[1]['end'], events[2]['start'])
        self.assertEqual(self.ledger.batch(), events)

    def test_partial_ack_and_idempotent_retry(self):
        for result, now in [(True, 1000), (False, 1060), (True, 1120)]:
            self.ledger.observe(result, now, self.config)
        events = self.ledger.batch()
        body = {'results': [dict(index=0, event_id=events[0]['event_id'], status='duplicate', http_status=200),
                            dict(index=1, event_id=events[1]['event_id'], status='rejected', http_status=429, error='source_capacity')]}
        self.assertFalse(self.ledger.acknowledge(events, body))
        self.assertEqual(self.ledger.batch(), [events[1]])

    def test_malformed_ack_removes_nothing(self):
        self.ledger.observe(True, 1000, self.config)
        self.ledger.observe(False, 1060, self.config)
        events = self.ledger.batch()
        with self.assertRaises(ValueError):
            self.ledger.acknowledge(events, {'results': []})
        self.assertEqual(self.ledger.batch(), events)

    def test_restart_closes_only_observed_time(self):
        self.ledger.observe(True, 1000, self.config)
        self.ledger.observe(True, 1060, self.config)
        self.ledger.db.close()
        self.ledger = Ledger(self.path, 'lakehouse_internet')
        self.ledger.recover()
        events = self.ledger.batch()
        self.ledger.recover()
        self.assertEqual(self.ledger.batch(), events)
        self.ledger.observe(True, 2000, self.config)
        self.assertEqual(events[0]['end'], '1970-01-01T00:17:40.000000+00:00')
        self.assertEqual(self.ledger.start, 2000)

    def test_gap_does_not_extend_old_state(self):
        self.ledger.observe(True, 1000, self.config)
        self.ledger.observe(True, 1060, self.config)
        self.ledger.observe(False, 2000, self.config)
        self.assertEqual(self.ledger.batch()[0]['end'], '1970-01-01T00:17:40.000000+00:00')

    def test_lost_response_retains_identical_event(self):
        import urllib.error
        self.ledger.observe(True, 1000, self.config)
        self.ledger.observe(False, 1060, self.config)
        events = self.ledger.batch()
        config = self.config | dict(ha_events_url='https://ha.example.com/api/daily_activity/events', ha_ingestion_token='a' * 43)
        with patch('urllib.request.OpenerDirector.open', side_effect=urllib.error.URLError('lost response')):
            self.assertFalse(deliver(config, events, self.ledger))
        self.assertEqual(self.ledger.batch(), events)

    def test_backward_clock_does_not_overlap(self):
        self.ledger.observe(True, 1000, self.config)
        self.ledger.observe(True, 1060, self.config)
        self.ledger.observe(False, 900, self.config)
        self.assertEqual(self.ledger.last, 1060)
        self.ledger.observe(False, 1120, self.config)
        self.assertEqual(len(self.ledger.batch()), 1)

    def test_confirmation_threshold(self):
        config = self.config | dict(successful_checks_to_on=2, failed_checks_to_off=3)
        self.ledger.observe(True, 1000, config)
        self.assertEqual(self.ledger.state, 'unknown')
        self.ledger.observe(True, 1060, config)
        self.assertEqual(self.ledger.state, 'on')
        self.ledger.observe(False, 1120, config)
        self.ledger.observe(True, 1180, config)
        self.assertEqual(self.ledger.state, 'on')

    def test_failed_connections_confirm_outage_and_consecutive_recovery(self):
        config = self.config | dict(successful_checks_to_on=2, failed_checks_to_off=3,
                                    checkpoint_seconds=900)
        # Interrupted failure and recovery streaks must restart their counts.
        rounds = [True, True, False, False, True, False, False, False,
                  True, False, True, True]
        expected = ['unknown', 'on', 'on', 'on', 'on', 'on', 'on', 'off',
                    'off', 'off', 'off', 'on']
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            for i, (reachable, state) in enumerate(zip(rounds, expected)):
                with patch('socket.create_connection') as connect:
                    if not reachable:
                        connect.side_effect = OSError(errno.ENETDOWN, 'network down')
                    self.ledger.observe(probe(config, pool), 1000 + i * 60, config)
                self.assertEqual(self.ledger.state, state)
        events = self.ledger.batch()
        self.assertEqual([e['state'] for e in events], ['unknown', 'on', 'off'])
        self.assertEqual(events[1]['end'], stamp(1420))
        self.assertEqual(events[2]['start'], stamp(1420))
        self.assertEqual(events[2]['end'], stamp(1660))

    def test_unknown_observation_requires_fresh_confirmation(self):
        config = self.config | dict(successful_checks_to_on=2)
        for result, now in [(True, 1000), (None, 1060), (True, 1120)]:
            self.ledger.observe(result, now, config)
            self.assertEqual(self.ledger.state, 'unknown')
        self.ledger.observe(True, 1180, config)
        self.assertEqual(self.ledger.state, 'on')

    def test_offline_outage_survives_restart_and_delivery_retry(self):
        config = self.config | dict(ha_events_url='https://ha.example.com/api/daily_activity/events',
                                   ha_ingestion_token='a' * 43)
        self.ledger.observe(True, 1000, config)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            with patch('socket.create_connection', side_effect=OSError(errno.ENETDOWN, 'network down')):
                self.ledger.observe(probe(config, pool), 1060, config)
        self.ledger.observe(False, 1120, config)
        self.ledger.observe(True, 1180, config)
        events = self.ledger.batch()
        self.assertEqual([e['state'] for e in events], ['on', 'off'])
        with patch('urllib.request.OpenerDirector.open', side_effect=OSError('offline')):
            self.assertFalse(deliver(config, events, self.ledger))
        self.ledger.db.close()
        self.ledger = Ledger(self.path, 'lakehouse_internet')
        self.ledger.recover()
        self.assertEqual(self.ledger.batch(), events)
        with patch('urllib.request.OpenerDirector.open') as opened:
            response = opened.return_value.__enter__.return_value
            response.status = 200
            response.read.return_value = json.dumps({'results': [
                dict(index=i, event_id=e['event_id'], status='accepted', http_status=201)
                for i, e in enumerate(events)]}).encode()
            self.assertTrue(deliver(config, self.ledger.batch(), self.ledger))
            self.assertEqual(json.loads(opened.call_args.args[0].data), {'events': events})
        self.assertEqual(self.ledger.batch(), [])

    def test_real_http_delivery_and_auth(self):
        from http.server import BaseHTTPRequestHandler, HTTPServer
        import threading
        captured = []
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                captured.append((self.headers['Authorization'], body))
                response = json.dumps({'results': [dict(index=i, event_id=e['event_id'], status='accepted', http_status=201) for i, e in enumerate(body['events'])]}).encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(response)
            def log_message(self, *args):
                pass
        server = HTTPServer(('127.0.0.1', 0), Handler)
        worker = threading.Thread(target=server.handle_request)
        worker.start()
        self.ledger.observe(True, 1000, self.config)
        self.ledger.observe(False, 1060, self.config)
        config = self.config | dict(ha_events_url=f'http://127.0.0.1:{server.server_port}/api/daily_activity/events', ha_ingestion_token='test-token')
        try:
            self.assertTrue(deliver(config, self.ledger.batch(), self.ledger))
            self.assertEqual(captured[0][0], 'Bearer test-token')
            self.assertEqual(self.ledger.batch(), [])
        finally:
            worker.join(timeout=5)
            server.server_close()

    def test_tls_verification_modes(self):
        import ssl
        self.ledger.observe(True, 1000, self.config)
        self.ledger.observe(False, 1060, self.config)
        for verify in (True, False):
            config = self.config | dict(ha_events_url='https://192.0.2.30:8123/api/daily_activity/events', ha_ingestion_token='a' * 43, ha_verify_certificate=verify)
            with patch('urllib.request.build_opener') as build:
                build.return_value.open.side_effect = OSError('test connection failure')
                self.assertFalse(deliver(config, self.ledger.batch(), self.ledger))
                handler = build.call_args.args[2]
                self.assertEqual(handler._context.check_hostname, verify)
                self.assertEqual(handler._context.verify_mode, ssl.CERT_REQUIRED if verify else ssl.CERT_NONE)

    def test_config_defaults_and_private_targets_rejected(self):
        path = Path(self.tmp.name) / 'config.json'
        raw = dict(ha_events_url='https://ha.example.com/api/daily_activity/events', ha_source_id='lakehouse', ha_ingestion_token='a' * 43)
        path.write_text(json.dumps(raw))
        self.assertEqual(load_config(path)['internet_check_seconds'], 60)
        self.assertTrue(load_config(path)['ha_verify_certificate'])
        path.write_text(json.dumps(raw | dict(ha_verify_certificate='false')))
        with self.assertRaises(ValueError):
            load_config(path)
        path.write_text(json.dumps(raw | dict(internet_probe_ips=['192.168.1.1'])))
        with self.assertRaises(ValueError):
            load_config(path)


if __name__ == '__main__':
    unittest.main()
