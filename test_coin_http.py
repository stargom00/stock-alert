"""v2.37 — coin_http: Webhook 인증·검증, 조회 API (실제 소켓으로)."""
import json
import queue
import threading

import pytest
import requests

import coin_http
import signal_tracker as tr

GOOD = {"symbol": "BTCUSDT.P", "tf": "5", "side": "short", "close": 81353.3,
        "time": "2026-10-15T07:55:00Z"}


@pytest.fixture
def base(monkeypatch, tmp_path):
    monkeypatch.setenv("COIN_WEBHOOK_TOKEN", "s3cret")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.delenv("COIN_API_READ_TOKEN", raising=False)
    monkeypatch.setattr(tr, "_webhook_q", queue.Queue(maxsize=200))
    srv = coin_http.make_server(port=0, host="127.0.0.1")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    srv.server_close()


def test_webhook_no_token_401(base):
    r = requests.post(base + "/tv-webhook", json=GOOD, timeout=5)
    assert r.status_code == 401
    assert tr._webhook_q.empty()


def test_webhook_wrong_token_401(base):
    r = requests.post(base + "/tv-webhook?token=nope", json=GOOD, timeout=5)
    assert r.status_code == 401


def test_webhook_token_unset_server_401(base, monkeypatch):
    monkeypatch.setenv("COIN_WEBHOOK_TOKEN", "")
    r = requests.post(base + "/tv-webhook?token=", json=GOOD, timeout=5)
    assert r.status_code == 401


def test_webhook_bad_json_400(base):
    r = requests.post(base + "/tv-webhook", data=b"{oops", headers={"X-TV-Token": "s3cret"}, timeout=5)
    assert r.status_code == 400
    assert tr._webhook_q.empty()


def test_webhook_ok_header_and_query(base):
    r = requests.post(base + "/tv-webhook", json=GOOD, headers={"X-TV-Token": "s3cret"}, timeout=5)
    assert r.status_code == 200
    r = requests.post(base + "/tv-webhook?token=s3cret", data=json.dumps(GOOD), timeout=5)
    assert r.status_code == 200
    w = tr._webhook_q.get_nowait()
    assert (w["symbol"], w["tf"], w["side"]) == ("BTCUSDT", "5m", "short")
    assert tr._webhook_q.qsize() == 1


def test_get_coin_signals_readonly(base, tmp_path, monkeypatch):
    tr.save_signals([{"id": 1, "symbol": "BTCUSDT"}], str(tmp_path / "coin_signals.json"))
    r = requests.get(base + "/api/coin-signals", timeout=5)
    assert r.status_code == 200 and r.json()["count"] == 1
    assert requests.post(base + "/api/coin-signals", timeout=5).status_code == 404
    monkeypatch.setenv("COIN_API_READ_TOKEN", "rt")
    assert requests.get(base + "/api/coin-signals", timeout=5).status_code == 401
    assert requests.get(base + "/api/coin-signals", headers={"X-Api-Read-Token": "rt"},
                        timeout=5).status_code == 200


def test_get_coin_positions_readonly(base, tmp_path):
    import coin_positions as cp
    cp.save_positions([{"id": 1, "symbol": "BTCUSDT"}], str(tmp_path / "positions.json"))
    r = requests.get(base + "/api/coin-positions", timeout=5)
    assert r.status_code == 200 and r.json()["count"] == 1
    assert requests.post(base + "/api/coin-positions", timeout=5).status_code == 404
