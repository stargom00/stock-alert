"""코인 신호 HTTP 엔드포인트 (v2.37) — 표준 라이브러리 http.server, 데몬 스레드.

  POST /tv-webhook       트레이딩뷰 알람 수신. 토큰 검증 후 큐에 넣고 즉시 200.
  GET  /api/coin-signals 저장된 신호·추적 결과(읽기 전용).
  GET  /api/coin-positions 가상 포지션·이벤트(읽기 전용, v2.41).
  GET  /api/coin-track   발송 신호별 72h 추적 결과(읽기 전용, v2.46).
  GET  /api/coin-journal 눌림목 스캐너 코인 일지를 대신 읽어 그대로 돌려줌(읽기 전용, v2.46).
                         스캐너 /api/*는 로그인 게이트라 바깥에서 못 읽는다 — 봇이 가진
                         API_READ_TOKEN(X-Api-Read-Token)으로 SCANNER_URL에 GET 하고 본문을
                         그대로 전달한다. 예약 작업(Claude)이 이 주소 하나로 신호·일지를 본다.
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

import requests

import coin_positions as cp
import coin_signal_track as st
import signal_tracker as tr
from signal_levels import CONFIG

MAX_BODY = 64 * 1024
READ_PATHS = ("/api/coin-signals", "/api/coin-positions", "/api/coin-track", "/api/coin-journal")


def _scanner_get(url, **kw):                    # 테스트에서 이 이름만 바꿔치기(requests.get 자체는 손대지 않음)
    return requests.get(url, **kw)


def fetch_scanner_journal(get=None):
    """스캐너 코인 일지를 봇 토큰으로 읽는다. (status, body) — 실패는 (502, {...})."""
    get = get or _scanner_get
    base = os.environ.get("SCANNER_URL", "https://pullback2-production.up.railway.app").rstrip("/")
    token = os.environ.get("API_READ_TOKEN", "")
    if not token:
        return 502, {"error": "API_READ_TOKEN unset"}
    try:
        r = get(f"{base}/api/coin-journal", headers={"X-Api-Read-Token": token}, timeout=10)
    except Exception as e:                      # noqa: BLE001 — 바깥 호출 실패는 그대로 보고
        return 502, {"error": f"scanner unreachable: {type(e).__name__}"}
    if r.status_code != 200:
        return 502, {"error": f"scanner {r.status_code}"}
    try:
        return 200, r.json()
    except ValueError:
        return 502, {"error": "scanner non-json"}


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
        if url.path in READ_PATHS:
            need = os.environ.get("COIN_API_READ_TOKEN", "")
            if need and not _token_ok(need, self.headers.get("X-Api-Read-Token", "")):
                return self._send(401, {"error": "unauthorized"})
            if url.path == "/api/coin-signals":
                sigs = tr.load_signals()
                return self._send(200, {"count": len(sigs), "signals": sigs})
            if url.path == "/api/coin-track":
                tracks = st.load_tracks()
                return self._send(200, {"count": len(tracks), "tracks": tracks})
            if url.path == "/api/coin-journal":
                code, body = fetch_scanner_journal()
                return self._send(code, body)
            ps = cp.load_positions()
            return self._send(200, {"count": len(ps), "positions": ps})
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
