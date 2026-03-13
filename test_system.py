import socket
import threading
import time
import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import server as srv
from client import NodeClient


def find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_test_server(port: int, timeout: float = 15.0) -> threading.Thread:
    def _run():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ssock:
            ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            ssock.bind(("127.0.0.1", port))
            ssock.listen()
            ssock.settimeout(timeout)
            try:
                while True:
                    try:
                        conn, addr = ssock.accept()
                    except socket.timeout:
                        break
                    t = threading.Thread(
                        target=srv.handle_client,
                        args=(conn, addr),
                        daemon=True,
                    )
                    t.start()
            except Exception:
                pass

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    time.sleep(0.05) 
    return t

class TestProtocol(unittest.TestCase):

    def setUp(self):
        srv.nodes.clear()

    def test_register_ok(self):
        resp = srv.process_message("REGISTER:alpha")
        self.assertEqual(resp, "OK:REGISTERED")
        self.assertIn("alpha", srv.nodes)

    def test_register_stores_timestamp(self):
        before = time.time()
        srv.process_message("REGISTER:ts_node")
        after = time.time()
        self.assertGreaterEqual(srv.nodes["ts_node"], before)
        self.assertLessEqual(srv.nodes["ts_node"], after)

    def test_register_duplicate(self):
        srv.process_message("REGISTER:alpha")
        resp = srv.process_message("REGISTER:alpha")
        self.assertEqual(resp, "ERROR:ALREADY_REGISTERED")

    def test_register_missing_id(self):
        self.assertEqual(srv.process_message("REGISTER:"), "ERROR:MISSING_NODE_ID")

    def test_heartbeat_ok(self):
        srv.process_message("REGISTER:beta")
        old_ts = srv.nodes["beta"]
        time.sleep(0.05)
        resp = srv.process_message("HEARTBEAT:beta")
        self.assertEqual(resp, "OK:HEARTBEAT")
        self.assertGreater(srv.nodes["beta"], old_ts)  # timestamp atualizado

    def test_heartbeat_not_registered(self):
        self.assertEqual(srv.process_message("HEARTBEAT:ghost"), "ERROR:NOT_REGISTERED")

    def test_heartbeat_missing_id(self):
        self.assertEqual(srv.process_message("HEARTBEAT:"), "ERROR:MISSING_NODE_ID")

    def test_list_empty(self):
        self.assertEqual(srv.process_message("LIST"), "NODES:")

    def test_list_active_nodes(self):
        srv.process_message("REGISTER:n1")
        srv.process_message("REGISTER:n2")
        resp = srv.process_message("LIST")
        self.assertIn("n1", resp)
        self.assertIn("n2", resp)
        self.assertTrue(resp.startswith("NODES:"))

    def test_list_excludes_expired(self):
        srv.process_message("REGISTER:old_node")
        srv.nodes["old_node"] = time.time() - (srv.NODE_TIMEOUT + 1)
        resp = srv.process_message("LIST")
        self.assertNotIn("old_node", resp)

    def test_list_mixed_active_and_expired(self):
        srv.process_message("REGISTER:active")
        srv.process_message("REGISTER:expired")
        srv.nodes["expired"] = time.time() - (srv.NODE_TIMEOUT + 1)
        resp = srv.process_message("LIST")
        self.assertIn("active", resp)
        self.assertNotIn("expired", resp)

    def test_quit_removes_node(self):
        srv.process_message("REGISTER:gamma")
        resp = srv.process_message("QUIT:gamma")
        self.assertEqual(resp, "OK:BYE")
        self.assertNotIn("gamma", srv.nodes)

    def test_quit_unknown_node_no_crash(self):
        resp = srv.process_message("QUIT:nobody")
        self.assertEqual(resp, "OK:BYE")

    def test_unknown_command(self):
        self.assertEqual(srv.process_message("PING"), "ERROR:UNKNOWN_COMMAND")

    def test_empty_message(self):
        self.assertEqual(srv.process_message(""), "ERROR:EMPTY_COMMAND")

    def test_whitespace_only(self):
        self.assertEqual(srv.process_message("   "), "ERROR:EMPTY_COMMAND")

    def test_case_sensitive_commands(self):
        self.assertEqual(srv.process_message("register:x"), "ERROR:UNKNOWN_COMMAND")

class TestIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        srv.nodes.clear()
        cls.port = find_free_port()
        start_test_server(cls.port)

    def setUp(self):
        srv.nodes.clear()

    def _client(self, node_id: str) -> NodeClient:
        c = NodeClient(node_id, "127.0.0.1", self.port)
        c.connect()
        return c

    def test_full_protocol_flow(self):
        c = self._client("flow_node")
        try:
            self.assertEqual(c.register(), "OK:REGISTERED")
            self.assertEqual(c.heartbeat(), "OK:HEARTBEAT")
            nodes = c.list_nodes()
            self.assertIn("flow_node", nodes)
            self.assertEqual(c.quit(), "OK:BYE")
        finally:
            c.close()

    def test_multiple_clients_visible_in_list(self):
        clients = [self._client(f"mc_{i}") for i in range(5)]
        for c in clients:
            c.register()

        nodes = clients[0].list_nodes()
        for i in range(5):
            self.assertIn(f"mc_{i}", nodes)

        for c in clients:
            c.quit()

    def test_expired_node_excluded_from_list(self):
        c = self._client("expiry_node")
        c.register()
        srv.nodes["expiry_node"] = time.time() - (srv.NODE_TIMEOUT + 1)

        nodes = c.list_nodes()
        self.assertNotIn("expiry_node", nodes)
        c.close()

    def test_heartbeat_prevents_expiry(self):
        c = self._client("hb_reset")
        c.register()
        srv.nodes["hb_reset"] = time.time() - (srv.NODE_TIMEOUT - 0.5)
        c.heartbeat()  
        nodes = c.list_nodes()
        self.assertIn("hb_reset", nodes)
        c.quit()

    def test_abrupt_disconnect_removes_node(self):
        c = self._client("crash_node")
        c.register()
        self.assertIn("crash_node", srv.nodes)

        c.close()
        time.sleep(0.2)  

        self.assertNotIn("crash_node", srv.nodes)


    def test_concurrent_heartbeat_and_list(self):

        c = self._client("ts_node")
        c.register()

        errors = []
        stop = threading.Event()

        def hb_loop():
            while not stop.wait(0.05):
                try:
                    c.heartbeat()
                except Exception as exc:
                    errors.append(exc)

        t = threading.Thread(target=hb_loop, daemon=True)
        t.start()

        for _ in range(10):
            try:
                c.list_nodes()
            except Exception as exc:
                errors.append(exc)
            time.sleep(0.02)

        stop.set()
        t.join(timeout=1)
        c.quit()

        self.assertEqual(errors, [], f"Erros de concorrência: {errors}")

    def test_concurrent_registrations(self):
        results = []
        lock = threading.Lock()

        def register_node(nid):
            try:
                c = self._client(nid)
                resp = c.register()
                with lock:
                    results.append((nid, resp))
                c.quit()
            except Exception as exc:
                with lock:
                    results.append((nid, f"EXCEPTION:{exc}"))

        threads = [threading.Thread(target=register_node, args=(f"con_{i}",))
                   for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertEqual(len(results), 10)
        for nid, resp in results:
            self.assertEqual(resp, "OK:REGISTERED", f"Falha em {nid}: {resp}")

    def test_duplicate_registration_rejected(self):
        c1 = self._client("dup_node")
        c2 = self._client("dup_node")
        try:
            self.assertEqual(c1.register(), "OK:REGISTERED")
            self.assertEqual(c2.register(), "ERROR:ALREADY_REGISTERED")
        finally:
            c1.quit()
            c2.close()

    def test_unknown_command_over_network(self):
        c = self._client("err_node")
        c.register()
        resp = c._send("INVALID_CMD")
        self.assertEqual(resp, "ERROR:UNKNOWN_COMMAND")
        nodes = c.list_nodes()
        self.assertIn("err_node", nodes)
        c.quit()

if __name__ == "__main__":
    unittest.main(verbosity=2)