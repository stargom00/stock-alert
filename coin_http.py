"""코인 신호 HTTP 엔드포인트 (v2.37) — 표준 라이브러리 http.server, 데몬 스레드.

  POST /tv-webhook       트레이딩뷰 알람 수신. 토큰 검증 후 큐에 넣고 즉시 200.
  GET  /api/coin-signals 저장된 신호·추적 결과(읽기 전용).
  GET  /health           200 ok

토큰: 트레이딩뷰 Webhook은 커스텀 HTTP 헤더를 못 보내므로 헤더
(X-TV-Token) 또는 URL 쿼리(?token=) 둘 다 받는다. COIN_WEBHOOK_TOKEN
미설정이면 모든 Webhook 요청 401(닫힌 상태로 시작).
"""
import hmac
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import signal_tracker as tr
from signal_levels import CONFIG

MAX_BODY = 64 * 1024


def _token_ok(expected, given):
    return bool(expected) and bool(given) and hmac.compare_digest(expected.encode(), given.encode())


class Handler(BaseHTTPRequestHandler):
    server_version = "coin-signals"

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):   # 쿼리의 토큰이 로그에 남지 않게 경로만
        print(f"[coin_http] {self.command} {urlparse(self.path).path} {args[1] if len(args) > 1 else ''}")

    def do_POST(self):
        url = urlparse(self.path)
        if url.path != "/tv-webhook" or not CONFIG["WEBHOOK_ENABLED"]:
            return self._send(404, {"error": "not found"})
        given = self.headers.get("X-TV-Token") or (parse_qs(url.query).get("token") or [""])[0]
        if not _token_ok(os.environ.get("COIN_WEBHOOK_TOKEN", ""), given):
            return self._send(401, {"error": "unauthorized"})
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            return self._send(400, {"error": "bad length"})
        if n > MAX_BODY:
            return self._send(413, {"error": "too large"})
        try:
            sig = tr.parse_webhook_payload(self.rfile.read(n))
        except ValueError as e:
            return self._send(400, {"error": str(e)})
        if not tr.enqueue_webhook(sig):
            return self._send(503, {"error": "queue full"})
        return self._send(200, {"ok": True, "queued": sig})

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/health":
            return self._send(200, {"ok": True})
        if url.path == "/api/coin-signals":
            need = os.environ.get("COIN_API_READ_TOKEN", "")
            if need and not _token_ok(need, self.headers.get("X-Api-Read-Token", "")):
                return self._send(401, {"error": "unauthorized"})
            sigs = tr.load_signals()
            return self._send(200, {"count": len(sigs), "signals": sigs})
        return self._send(404, {"error": "not found"})


def make_server(port=None, host="0.0.0.0"):
    port = int(port if port is not None else os.environ.get("PORT", "8080"))
    return ThreadingHTTPServer((host, port), Handler)


def start_server():
    if os.environ.get("COIN_HTTP_ENABLED", "1").strip().lower() in ("0", "false", "no", "off"):
        return None
    srv = make_server()
    threading.Thread(target=srv.serve_forever, daemon=True, name="coin_http").start()
    print(f"[coin_http] listening on :{srv.server_address[1]}")
    return srv
