from __future__ import annotations

import time
import webbrowser

from .server import start_in_thread


def main() -> None:
    server, url = start_in_thread()
    try:
        import webview

        webview.create_window("Live Trader", url, width=1480, height=980, min_size=(1180, 760))
        webview.start()
    except Exception as exc:
        print(f"WebView unavailable ({exc}). Opening browser at {url}")
        webbrowser.open(url)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    finally:
        server.shutdown()
