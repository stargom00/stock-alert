"""보낸 신호별 독립 추적 (v2.45) — 포지션 시뮬과 별개, 텔레그램 발송 없음.

sent=true 신호마다 신호 봉 마감부터 TRACK_HOURS(72h) 동안 마감된 1m 봉으로:
  - TP(신호에 저장된 고정 TP, 평일 +0.5% / 주말 +0.3%) 도달 여부와 걸린 분
  - TP 도달 전(미도달이면 72h 전체) 최대 역행 MAE %
  - −1% / −2% 역행 선행 터치(TP 전) 여부와 처음 닿은 시각
  - 되돌림 ✓ / ⚠️ 신저점(pullback_ok)
한 1m 봉이 TP와 역행선을 둘 다 닿으면 역행을 먼저 본다(보수적 — 포지션과 같음).

저장: $DATA_DIR/coin_signal_track.json (새 파일 — coin_signals.json·positions.json
스키마는 그대로). 바이낸스 공개 klines만 읽는다. 주문 기능 없음.
"""
import json
import os
import threading
import time

import signal_levels as lv

TRACK_HOURS = 72
TOUCH_PCTS = (1, 2)          # 역행 선행 터치 기록 (%)
MINUS = "−"

_lock = threading.RLock()


def track_path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "coin_signal_track.json")


def load_tracks(path=None):
    path = path or track_path()
    with _lock:
        if not os.path.exists(path):
            return []
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            ts = data.get("tracks", []) if isinstance(data, dict) else []
            return ts if isinstance(ts, list) else []
        except (ValueError, OSError):
            return []


def save_tracks(tracks, path=None):
    path = path or track_path()
    with _lock:
        try:
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"tracks": tracks}, f, ensure_ascii=False)
            os.replace(tmp, path)
        except OSError as e:
            print(f"[신호추적] 저장 실패: {e}")


def new_track(sig):
    return {
        "signal_id": sig["id"], "symbol": sig["symbol"], "tf": sig["tf"], "side": sig["side"],
        "entry": sig["entry"], "tp": sig["tp"], "tp_pct": sig.get("tp_pct"),
        "pullback_ok": sig.get("pullback_ok"),
        "start_ms": sig["bar_close_ms"] + 1,
        "checked_until_ms": sig["bar_close_ms"],
        "status": "open", "result": None,          # hit_tp | expired
        "tp_ms": None, "tp_minutes": None,
        "mae_pct": 0.0,
        "touch_1_ms": None, "touch_2_ms": None,    # −1% / −2% 처음 닿은 봉 마감(TP 전만)
    }


def register(sig, path=None):
    """발송된 v2.44+ 신호(고정 TP 있음)를 추적 목록에 추가. 이미 있으면 무시."""
    if not sig.get("sent") or sig.get("tp") is None:
        return None
    with _lock:
        tracks = load_tracks(path)
        if any(t["signal_id"] == sig["id"] for t in tracks):
            return None
        t = new_track(sig)
        tracks.append(t)
        save_tracks(tracks, path)
    return t


def _adverse_pct(t, price):
    p = (price - t["entry"]) / t["entry"] * 100
    return -p if t["side"] == "long" else p


def update_track(t, bars, now_ms):
    """마감된 1m 봉으로 t 갱신(제자리). 바뀌었으면 True."""
    if t["status"] != "open":
        return False
    long_ = t["side"] == "long"
    expire_ms = t["start_ms"] + TRACK_HOURS * 3_600_000
    changed = False
    for b in sorted(bars, key=lambda x: x["t"]):
        if b["T"] <= t["checked_until_ms"] or b["T"] >= now_ms:
            continue
        if b["t"] >= expire_ms:
            break
        changed = True
        adv = _adverse_pct(t, b["l"] if long_ else b["h"])
        t["mae_pct"] = max(t["mae_pct"], adv)
        for k in TOUCH_PCTS:
            if t[f"touch_{k}_ms"] is None and adv >= k:
                t[f"touch_{k}_ms"] = b["T"] + 1
        t["checked_until_ms"] = b["T"]
        if b["h"] >= t["tp"] if long_ else b["l"] <= t["tp"]:
            t.update(status="done", result="hit_tp", tp_ms=b["T"] + 1,
                     tp_minutes=round((b["T"] + 1 - t["start_ms"]) / 60_000))
            return True
    if t["checked_until_ms"] + 1 >= expire_ms:
        t.update(status="done", result="expired")
        changed = True
    return changed


def track_open(fetch=lv.fetch_klines, path=None, now_ms=None):
    """1분마다. 열린 추적을 심볼별로 1m 조회해서 갱신. 발송 없음."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    with _lock:
        open_ts = [t for t in load_tracks(path) if t["status"] == "open"]
    if not open_ts:
        return
    fetched = {}
    for sym in {t["symbol"] for t in open_ts}:
        start = min(t["checked_until_ms"] for t in open_ts if t["symbol"] == sym) + 1
        try:
            fetched[sym] = fetch(sym, "1m", limit=lv.SPOT_MAX_LIMIT, start_ms=start)
        except Exception as e:
            print(f"[신호추적] {sym} 1m 조회 실패: {e}")
    with _lock:
        tracks = load_tracks(path)    # 조회 사이 새 신호가 들어왔을 수 있어 다시 읽음
        changed = False
        for t in tracks:
            if t["status"] == "open" and t["symbol"] in fetched:
                changed = update_track(t, fetched[t["symbol"]], now_ms) or changed
        if changed:
            save_tracks(tracks, path)


# ── 요약 ──────────────────────────────────────────────────────────────

def summarize(tracks, now_ms, days, pullback_ok):
    """되돌림 그룹별(None=조회 실패는 제외). 비율·MAE는 끝난 추적만."""
    since = now_ms - days * 86_400_000
    w = [t for t in tracks if t["start_ms"] >= since and t.get("pullback_ok") is pullback_ok]
    done = [t for t in w if t["status"] == "done"]
    return {"n": len(w), "done": len(done),
            "tp": sum(1 for t in done if t["result"] == "hit_tp"),
            "mae_avg": (sum(t["mae_pct"] for t in done) / len(done)) if done else None,
            "touch2": sum(1 for t in w if t.get("touch_2_ms") is not None)}


def format_line(label, s):
    mae = f"{MINUS}{s['mae_avg']:.2f}%" if s["mae_avg"] is not None else "—"
    pend = s["n"] - s["done"]
    return (f"{label} 신호: TP {s['tp']}/{s['done']} · MAE 평균 {mae} · "
            f"{MINUS}2% 선행 {s['touch2']}건" + (f" (추적 중 {pend})" if pend else ""))


def format_lines(tracks, now_ms, days):
    return [format_line("되돌림 ✓", summarize(tracks, now_ms, days, True)),
            format_line("⚠️ 신저점", summarize(tracks, now_ms, days, False))]
