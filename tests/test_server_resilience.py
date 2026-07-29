import json
import threading
import time
import unittest
import urllib.request
from unittest.mock import patch

from live_trader.server import bind_server


class ServerResilienceTest(unittest.TestCase):
    def test_slow_broker_refresh_does_not_block_snapshot_health(self) -> None:
        server = bind_server("127.0.0.1", 0)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        entered = threading.Event()
        release = threading.Event()
        post_errors = []

        def slow_poll(*_args, **_kwargs):
            entered.set()
            release.wait(2.0)
            return {"ok": True, "execution_events": {}}

        def post_refresh():
            request = urllib.request.Request(
                f"http://127.0.0.1:{server.server_port}/api/execution-events",
                data=json.dumps({"broker_id": "all"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=3.0) as response:
                    json.loads(response.read().decode("utf-8"))
            except Exception as exc:  # Assertion below reports the background failure.
                post_errors.append(exc)

        try:
            with patch(
                "live_trader.server.state.poll_execution_events",
                side_effect=slow_poll,
            ), patch(
                "live_trader.server.state.snapshot",
                return_value={"ok": True, "api_connected": True},
            ):
                post_thread = threading.Thread(target=post_refresh, daemon=True)
                post_thread.start()
                self.assertTrue(entered.wait(1.0))

                started = time.perf_counter()
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{server.server_port}/api/snapshot",
                    timeout=1.0,
                ) as response:
                    snapshot = json.loads(response.read().decode("utf-8"))
                elapsed = time.perf_counter() - started

                self.assertTrue(snapshot["api_connected"])
                self.assertLess(elapsed, 0.75)
                release.set()
                post_thread.join(3.0)
                self.assertFalse(post_thread.is_alive())
                self.assertEqual([], post_errors)
        finally:
            release.set()
            server.shutdown()
            server.server_close()
            server_thread.join(3.0)


if __name__ == "__main__":
    unittest.main()
