"""보낸 신호별 독립 추적 (v2.45) — 포지션 시뮬과 별개, 텔레그램 발송 없음.

sent=true 신호마다 신호 봉 마감부터 TRACK_HOURS(72h) 동안 마감된 1m 봉으로:
  - TP(신호에 저장된 고정 TP, 평일 +0.5% / 주말 +0.3%) 도달 여부와 걸린 분
  - TP 도달 전(미도달이면 72h 전체) 최대 역행 MAE %
  - −1% / −2% 역행 선행 터치(TP 전) 여부와 처음 닿은 시각
  - 되돌림 ✓ / ⚠️ 신저점(pullback_ok)
한 1m 봉이 TP와 역행선을 둘 다 닿으면 역행을 먼저 본다(보수적 — 포지션과 같음).

v2.52부터 같은 신호를 EXT_HOURS(14일)까지 따로 더 따라가 경로를 남긴다(ext_* / mae_at /
ext_touch). 72h 창 필드의 뜻은 그대로다 — 옛 집계와 계속 비교할 수 있어야 하므로.

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

# ── v2.52 경로 기록 ───────────────────────────────────────────────────
# 72h 창(위 TRACK_HOURS)은 그대로 두고, 같은 신호를 EXT_HOURS까지 따로 더 따라간다.
# 왜: 72h 만료 38건 중 13건이 −3~4.8%에 있던 채로 "결말 모름"으로 끝나, "포기선을 두는
# 게 나은가"를 데이터로 답할 수 없었다(2026-11-05 분석). 창을 늘리는 대신 경로를 적으면
# 나중에 아무 기간으로나 잘라서 다시 계산할 수 있다. 기존 status/result/mae_pct/
# tp_minutes/touch_1/touch_2의 뜻은 바뀌지 않는다 — 옛 숫자와 계속 비교 가능해야 한다.
EXT_HOURS = 336                      # 14일
CHECKPOINT_HOURS = (6, 24, 72, 168, 336)   # mae_at: 시작 후 N시간 안의 최대 역행(TP 여부 무관)
EXT_TOUCH_PCTS = (1, 2, 3, 4, 5)     # ext_touch: ext TP 전에 이 역행선에 처음 닿은 시각
                                     # 4·5는 20배 격리 청산선(약 4.8%)을 양쪽에서 감싼다

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
        **_ext_defaults(sig["bar_close_ms"]),
    }


def _ext_defaults(bar_close_ms):
    """v2.52 경로 기록 필드. 72h 창과 독립적으로 EXT_HOURS까지 따라간다."""
    return {
        "ext_checked_until_ms": bar_close_ms,
        "ext_status": "open", "ext_result": None,   # hit_tp | expired
        "ext_tp_ms": None, "ext_tp_minutes": None,
        "ext_mae_pct": 0.0,
        "mae_at": {}, "ext_touch": {},
    }


def ensure_ext(t):
    """옛 레코드에 경로 필드를 붙인다(제자리). 바뀌었으면 True.
    ext_checked_until_ms를 start−1로 두므로 과거 봉부터 다시 훑어 경로가 채워진다 —
    전부 같은 심볼이라 심볼당 한 줄기 조회로 전체가 따라잡힌다(1000봉/회)."""
    if "ext_status" in t:
        return False
    t.update(_ext_defaults(t["start_ms"] - 1))
    return True


def update_ext(t, bars, now_ms):
    """경로 기록 갱신(제자리). 72h 창 필드(status/result/mae_pct/touch_1·2)는 건드리지 않는다.
    TP를 만나도 멈추지 않고 체크포인트를 계속 채운다 — 그래야 "24h 창이었으면?"을
    나중에 계산할 수 있다. 바뀌었으면 True."""
    if t.get("ext_status") != "open":
        return False
    long_ = t["side"] == "long"
    expire_ms = t["start_ms"] + EXT_HOURS * 3_600_000
    changed = False
    for b in sorted(bars, key=lambda x: x["t"]):
        if b["T"] <= t["ext_checked_until_ms"] or b["T"] >= now_ms:
            continue
        if b["t"] >= expire_ms:
            break
        changed = True
        adv = _adverse_pct(t, b["l"] if long_ else b["h"])
        t["ext_mae_pct"] = max(t["ext_mae_pct"], adv)
        if t["ext_tp_ms"] is None:
            for k in EXT_TOUCH_PCTS:
                if str(k) not in t["ext_touch"] and adv >= k:
                    t["ext_touch"][str(k)] = b["T"] + 1
            if (b["h"] >= t["tp"]) if long_ else (b["l"] <= t["tp"]):
                t["ext_tp_ms"] = b["T"] + 1
                t["ext_tp_minutes"] = round((b["T"] + 1 - t["start_ms"]) / 60_000)
        t["ext_checked_until_ms"] = b["T"]
        for h in CHECKPOINT_HOURS:
            if str(h) not in t["mae_at"] and b["T"] + 1 >= t["start_ms"] + h * 3_600_000:
                t["mae_at"][str(h)] = round(t["ext_mae_pct"], 4)
    if t["ext_checked_until_ms"] + 1 >= expire_ms:
        for h in CHECKPOINT_HOURS:                      # 조회 공백으로 건너뛴 체크포인트 메우기
            t["mae_at"].setdefault(str(h), round(t["ext_mae_pct"], 4))
        t.update(ext_status="done",
                 ext_result="hit_tp" if t["ext_tp_ms"] else "expired")
        changed = True
    return changed


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


def _pointer(t):
    """이 추적이 아직 봉을 필요로 하면 가장 뒤처진 지점, 아니면 None.
    72h 창과 경로 기록이 각자 포인터를 갖되 조회는 한 줄기로 합친다."""
    ptrs = ([t["checked_until_ms"]] if t["status"] == "open" else []) + \
           ([t["ext_checked_until_ms"]] if t.get("ext_status") == "open" else [])
    return min(ptrs) if ptrs else None


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
        tracks = load_tracks(path)
        if any([ensure_ext(t) for t in tracks]):     # 옛 레코드에 경로 필드 부여(한 번)
            save_tracks(tracks, path)
        open_ts = [t for t in tracks if _pointer(t) is not None]
    if not open_ts:
        return
    fetched = {}
    for sym in {t["symbol"] for t in open_ts}:
        start = min(_pointer(t) for t in open_ts if t["symbol"] == sym) + 1
        try:
            fetched[sym] = fetch(sym, "1m", limit=lv.SPOT_MAX_LIMIT, start_ms=start)
        except Exception as e:
            print(f"[신호추적] {sym} 1m 조회 실패: {e}")
    with _lock:
        tracks = load_tracks(path)    # 조회 사이 새 신호가 들어왔을 수 있어 다시 읽음
        changed = False
        for t in tracks:
            ensure_ext(t)
            if t["symbol"] not in fetched:
                continue
            bars = fetched[t["symbol"]]
            if t["status"] == "open":
                changed = update_track(t, bars, now_ms) or changed
            changed = update_ext(t, bars, now_ms) or changed
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
