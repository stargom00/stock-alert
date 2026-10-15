"""코인 선물 RSI 신호 — 수신(Webhook/자체 판정)·발송·결과 추적·요약 (v2.37).

흐름
  A. 트레이딩뷰 Webhook → coin_http가 검증 후 enqueue_webhook() → 메인
     스케줄 스레드의 drain_webhook_queue()가 처리(HTTP 응답은 즉시 200 —
     트레이딩뷰 Webhook 타임아웃이 짧아서 바이낸스 조회를 응답 경로에서 안 함).
  B. self_detect(): 설정된 심볼·주기의 봉 마감마다 같은 규칙으로 판정.
  둘 다 ingest_signal()로 모여 중복 제거 → 레벨 계산 → 저장 → 발송.
  track_open_signals(): 1분마다 1m 봉으로 목표/손절 도달 확인.
  daily_summary(): 매일 09:00 NZ.

저장: $DATA_DIR/coin_signals.json (기존 sent_log.json과 같은 DATA_DIR 규칙).
바이낸스 공개 klines만 읽는다(기본 현물 data-api.binance.vision, v2.39). 주문 기능 없음.
"""
import json
import os
import queue
import threading
import time
from datetime import datetime, timezone, timedelta

import pytz

import signal_levels as lv
from signal_levels import CONFIG, TF_MS

KST = timezone(timedelta(hours=9))
NZ_TZ = pytz.timezone("Pacific/Auckland")
MINUS = "−"
MIN_MS = 60_000

_lock = threading.RLock()
_webhook_q = queue.Queue(maxsize=200)
_self_next_due = {}    # {(symbol, tf): 다음 판정 시각 ms} — 봉 마감 전엔 조회 생략
_self_last_bar = {}    # {(symbol, tf): 마지막으로 판정한 봉 시작 ms}


# ── 저장소 ────────────────────────────────────────────────────────────

def store_path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "coin_signals.json")


def load_signals(path=None):
    path = path or store_path()
    with _lock:
        if not os.path.exists(path):
            return []
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            sigs = data.get("signals", []) if isinstance(data, dict) else []
            return sigs if isinstance(sigs, list) else []
        except (ValueError, OSError):
            return []


def save_signals(signals, path=None):
    path = path or store_path()
    with _lock:
        try:
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"signals": signals}, f, ensure_ascii=False)
            os.replace(tmp, path)
        except OSError as e:
            print(f"[코인신호] 저장 실패: {e}")


# ── 표기 ──────────────────────────────────────────────────────────────

def fmt_px(p):
    if p is None:
        return "—"
    if p >= 10000:
        return f"{p:,.0f}"
    if p >= 100:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:,.3f}"
    return f"{p:.5f}"


def fmt_pct(x, digits=2):
    """부호 있는 % (음수는 유니코드 마이너스)."""
    if x is None:
        return "—"
    s = f"{abs(x):.{digits}f}%"
    return (MINUS + s) if round(x, digits) < 0 else ("+" + s)


def _pct_from(entry, price):
    return (price - entry) / entry * 100.0


def fmt_signal_time(bar_close_ms):
    """신호 시각 = ②확인봉 마감 시각(= ③진입봉 시작, 알람이 울리는 시각). UTC (KST / NZ)."""
    t = datetime.fromtimestamp((bar_close_ms + 1) / 1000, timezone.utc)
    nz = t.astimezone(NZ_TZ)
    return (f"{t.strftime('%H:%M')} UTC ({t.astimezone(KST).strftime('%H:%M')} KST / "
            f"{nz.strftime('%H:%M')} NZ)")


_SIDE_KO = {"long": "롱", "short": "숏"}
_POS_KO = {"above": "위", "inside": "안", "below": "아래"}


def format_signal_message(sig):
    entry = sig["entry"]
    side_ko = _SIDE_KO[sig["side"]]
    head = "🔺" if sig["side"] == "long" else "🔻"
    rr = sig.get("rr")
    rr_warn = rr is not None and rr < 1
    errs = sig.get("errors") or {}
    lines = [f"{head} {sig['symbol']} {sig['tf']} {side_ko} 신호 {fmt_signal_time(sig['bar_close_ms'])}",
             f"진입 참고 {fmt_px(entry)}"]
    if "levels" in errs:
        lines.append("목표/손절 —")   # 조회 실패를 "없음"(스윙 0개)과 구분
    else:
        for key, label in (("target1", "목표1"), ("target2", "목표2"), ("stop", "손절")):
            v = sig.get(key)
            if v is None:
                lines.append(f"{label} 없음")
                continue
            line = f"{label} {fmt_px(v)} ({fmt_pct(_pct_from(entry, v))})"
            if (sig.get("level_src") or {}).get(key) == "extreme":
                line += " 구간 극값"
            if key == "target1" and rr_warn:
                line += " ⚠️"
            lines.append(line)
        lines.append(f"손익비 {rr:.1f}" + (" ⚠️" if rr_warn else "") if rr is not None else "손익비 —")
    c = sig.get("cloud")
    if c:
        tail = "중립" if c["bias"] == "중립" else f"{side_ko} {c['bias']}"
        lines.append(f"{CONFIG['CLOUD_TF']} 구름: {_POS_KO[c['pos']]}, 두께 {c['thick_pct']:.1f}% → {tail}")
    else:
        lines.append(f"{CONFIG['CLOUD_TF']} 구름 —")
    h = sig.get("htf_rsi")
    if h:
        lines.append(f"{CONFIG['HTF_RSI_TF']} RSI {h['value']:.0f} {h['dir']}")
    else:
        lines.append(f"{CONFIG['HTF_RSI_TF']} RSI —")
    # 실패 사유: 같은 사유는 한 줄로 묶음 (보통 전부 같은 HTTP 오류)
    labels = {"levels": "목표/손절", "cloud": f"{CONFIG['CLOUD_TF']} 구름", "htf_rsi": f"{CONFIG['HTF_RSI_TF']} RSI"}
    by_reason = {}
    for k in ("levels", "cloud", "htf_rsi"):
        if k in errs:
            by_reason.setdefault(errs[k], []).append(labels[k])
    for reason, what in by_reason.items():
        lines.append(f"⚠️ {'·'.join(what)} 조회 실패: {reason}")
    lines.append(f"신호ID #{sig['id']:04d}")
    lines.append(f"가격 기준: {sig.get('price_basis') or lv.price_basis()}")
    return "\n".join(lines)


# ── 수신·중복 제거·발송 ──────────────────────────────────────────────

def is_duplicate(signals, symbol, tf, side, bar_open_ms, n_bars=None):
    """같은 심볼·주기·방향의 저장된(=발송된) 신호와 N봉 이내면 중복.
    저장된 신호 기준이라 재시작 후에도 유지되고, Webhook과 자체 판정이 같은
    봉을 둘 다 내도 하나만 나간다."""
    n_bars = lv.dedup_bars(tf) if n_bars is None else n_bars
    span = n_bars * TF_MS[tf]
    return any(s["symbol"] == symbol and s["tf"] == tf and s["side"] == side
               and abs(bar_open_ms - s["bar_open_ms"]) < span for s in signals)


def suppress_reason(entry, target1, rr):
    """v2.40 알림 억제 사유(없으면 None). 값이 없는 항목(None)은 판정하지 않음."""
    reasons = []
    if target1 is not None and abs(target1 - entry) / entry * 100.0 < CONFIG["SUPPRESS_MIN_T1_PCT"]:
        reasons.append(f"목표1 거리 < {CONFIG['SUPPRESS_MIN_T1_PCT']:g}%")
    if rr is not None and rr < CONFIG["SUPPRESS_MIN_RR"]:
        reasons.append(f"손익비 < {CONFIG['SUPPRESS_MIN_RR']:g}")
    return ", ".join(reasons) or None


def cross_tf_duplicate(signals, symbol, tf, side, bar_close_ms):
    """v2.40 다른 주기·같은 심볼·같은 방향으로 실제 발송된 신호가 신호 시각 기준
    CROSS_DUP_MIN분 이내(앞뒤)에 있으면 그 신호 id, 없으면 None."""
    win = CONFIG["CROSS_DUP_MIN"] * MIN_MS
    for s in reversed(signals):
        if (s["symbol"] == symbol and s["side"] == side and s["tf"] != tf
                and s.get("sent", True) and abs(bar_close_ms - s["bar_close_ms"]) <= win):
            return s["id"]
    return None


def ingest_signal(symbol, tf, side, entry, bar_open_ms, source, send,
                  fetch=lv.fetch_klines, path=None, now_ms=None):
    """신호 1건 처리. 발송했으면 저장된 레코드, 중복이면 None."""
    bar_close_ms = bar_open_ms + TF_MS[tf] - 1
    with _lock:
        signals = load_signals(path)
        if is_duplicate(signals, symbol, tf, side, bar_open_ms):
            print(f"[코인신호] 중복 무시 {symbol} {tf} {side} {bar_open_ms} ({source})")
            return None
    ctx = lv.build_signal_context(symbol, tf, side, entry, bar_close_ms, fetch=fetch)
    for k, e in ctx["errors"].items():
        print(f"[코인신호] {symbol} {tf} {k} 실패: {e}")
    with _lock:
        signals = load_signals(path)
        if is_duplicate(signals, symbol, tf, side, bar_open_ms):
            return None
        sup = suppress_reason(entry, ctx["target1"], ctx["rr"])
        dup_of = cross_tf_duplicate(signals, symbol, tf, side, bar_close_ms)
        sig = {
            "id": max((s["id"] for s in signals), default=0) + 1,
            "time": datetime.fromtimestamp((bar_close_ms + 1) / 1000, timezone.utc).isoformat(),
            "received_ms": now_ms if now_ms is not None else int(time.time() * 1000),
            "bar_open_ms": bar_open_ms, "bar_close_ms": bar_close_ms,
            "symbol": symbol, "tf": tf, "side": side, "entry": entry,
            "target1": ctx["target1"], "target2": ctx["target2"], "stop": ctx["stop"],
            "rr": ctx["rr"],
            "cloud": ({k: ctx["cloud"][k] for k in ("pos", "thick_pct", "bias")}
                      if ctx["cloud"] else None),
            "htf_rsi": ctx["htf_rsi"],
            "source": source,
            "errors": ctx["errors"], "price_basis": lv.price_basis(),
            "level_src": ctx.get("src") or {},
            # v2.40: 억제·교차 중복은 발송만 안 하고 저장·추적은 그대로
            "suppressed": sup is not None, "suppress_reason": sup,
            "duplicate": dup_of is not None, "dup_of": dup_of,
            "sent": sup is None and dup_of is None,
            # 추적 상태
            "status": "open", "phase": "pre_t1", "result": None,
            "checked_until_ms": bar_close_ms, "bars": 0,
            "max_high": None, "min_low": None, "last_close": None,
            "mfe_pct": 0.0, "mae_pct": 0.0,
            "t1_hit_ms": None, "t1_bars": None,
            "result_ms": None, "result_bars": None, "result_pct": None, "after_t1": None,
        }
        signals.append(sig)
        save_signals(signals, path)
    if sig["sent"]:
        send(format_signal_message(sig))
    else:
        print(f"[코인신호] #{sig['id']:04d} {symbol} {tf} {side} 발송 안 함 — "
              f"{'억제: ' + sup if sup else ''}{' / ' if sup and dup_of else ''}"
              f"{'중복: #%04d' % dup_of if dup_of else ''}")
    return sig


# ── B. 자체 판정 ─────────────────────────────────────────────────────

def detect_last_closed(bars, now_ms):
    """마감된 봉 중 마지막 봉의 신호. (side, bar) 또는 (None, bar|None)."""
    closed = lv.closed_only(bars, now_ms)
    if len(closed) < CONFIG["RSI_LEN"] + 2:
        return None, None
    rsi = lv.rsi_wilder([b["c"] for b in closed], CONFIG["RSI_LEN"])
    i = len(closed) - 1
    return lv.signal_at(closed, i, rsi), closed[i]


def self_detect(send, fetch=lv.fetch_klines, path=None, now_ms=None):
    if not CONFIG["SELF_ENABLED"]:
        return
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    for symbol, tf in CONFIG["SELF_TARGETS"]:
        if tf not in TF_MS:
            continue
        key = (symbol, tf)
        if now_ms < _self_next_due.get(key, 0):
            continue
        try:
            bars = fetch(symbol, tf, limit=500)
        except Exception as e:
            print(f"[코인신호] {symbol} {tf} 봉 조회 실패: {e}")
            continue
        side, bar = detect_last_closed(bars, now_ms)
        if bar is None:
            continue
        # 다음 봉 마감 + 2초 뒤까지 조회 생략
        _self_next_due[key] = bar["T"] + 1 + TF_MS[tf] + 2000
        if _self_last_bar.get(key) == bar["t"]:
            continue
        _self_last_bar[key] = bar["t"]
        # 재시작 직후 오래된 봉으로 발송하지 않게 — 마감 후 2봉 이내만
        if now_ms - (bar["T"] + 1) > 2 * TF_MS[tf]:
            continue
        if side:
            ingest_signal(symbol, tf, side, bar["c"], bar["t"], "self", send,
                          fetch=fetch, path=path, now_ms=now_ms)


# ── A. Webhook ───────────────────────────────────────────────────────

_TV_TF = {"1": "1m", "3": "3m", "5": "5m", "15": "15m", "30": "30m", "60": "1h",
          "120": "2h", "240": "4h", "D": "1d", "1D": "1d"}
_SIDE_ALIASES = {"long": "long", "buy": "long", "롱": "long",
                 "short": "short", "sell": "short", "숏": "short"}


def _parse_time_ms(v):
    if isinstance(v, bool):
        raise ValueError("time")
    if isinstance(v, (int, float)):
        return int(v if v > 1e11 else v * 1000)   # 초 단위도 허용
    s = str(v).strip()
    if s.isdigit():
        return _parse_time_ms(int(s))
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def parse_webhook_payload(body):
    """트레이딩뷰 알람 메시지 → 정규화 dict. 잘못되면 ValueError.
    symbol: "BINANCE:BTCUSDT.P"/"BTCUSDT.P" → "BTCUSDT"
    tf: "5"({{interval}}) 또는 "5m"
    time: 신호 봉의 시작 시각({{time}}, ISO 또는 ms). 주기 경계로 내림."""
    try:
        d = json.loads(body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else body)
    except (ValueError, UnicodeDecodeError):
        raise ValueError("invalid JSON")
    if not isinstance(d, dict):
        raise ValueError("JSON object required")
    sym = str(d.get("symbol", "")).strip().upper()
    sym = sym.split(":")[-1]
    if sym.endswith(".P"):
        sym = sym[:-2]
    if not sym.isalnum():
        raise ValueError("symbol")
    tf_raw = str(d.get("tf", "")).strip()
    tf = _TV_TF.get(tf_raw.upper() if tf_raw.upper() in ("D", "1D") else tf_raw, tf_raw.lower())
    if tf not in TF_MS:
        raise ValueError("tf")
    side = _SIDE_ALIASES.get(str(d.get("side", "")).strip().lower())
    if not side:
        raise ValueError("side")
    try:
        close = float(d.get("close"))
    except (TypeError, ValueError):
        raise ValueError("close")
    if not close > 0:
        raise ValueError("close")
    try:
        t = _parse_time_ms(d.get("time"))
    except (TypeError, ValueError):
        raise ValueError("time")
    bar_open = t - (t % TF_MS[tf])
    return {"symbol": sym, "tf": tf, "side": side, "close": close, "bar_open_ms": bar_open}


def enqueue_webhook(sig):
    try:
        _webhook_q.put_nowait(sig)
        return True
    except queue.Full:
        return False


def drain_webhook_queue(send, fetch=lv.fetch_klines, path=None):
    while True:
        try:
            w = _webhook_q.get_nowait()
        except queue.Empty:
            return
        ingest_signal(w["symbol"], w["tf"], w["side"], w["close"], w["bar_open_ms"],
                      "webhook", send, fetch=fetch, path=path)


# ── 추적 ──────────────────────────────────────────────────────────────

def _fav_pct(sig, price):
    """신호 방향 기준 유리한 쪽 +%."""
    p = _pct_from(sig["entry"], price)
    return p if sig["side"] == "long" else -p


def update_signal(sig, bars, now_ms):
    """마감된 1m 봉으로 sig 상태 갱신(제자리 수정). 발송할 한 줄 메시지 리스트 반환.
    한 봉에서 손절과 목표가 같이 닿으면 손절(보수적). 목표1 도달 후에도 목표2·
    손절·만료 중 하나까지 계속 보며, 목표2에 닿으면 결과를 hit_target2로 올림."""
    if sig["status"] != "open":
        return []
    msgs = []
    entry, side = sig["entry"], sig["side"]
    t1, t2, stop = sig.get("target1"), sig.get("target2"), sig.get("stop")
    expire_ms = sig["bar_close_ms"] + 1 + int(CONFIG["EXPIRE_HOURS"] * 3_600_000)
    side_ko = _SIDE_KO[side]

    def touched(level, b, toward_profit):
        if level is None:
            return False
        up = (side == "long") == toward_profit   # 가격이 위로 가야 닿는가
        return b["h"] >= level if up else b["l"] <= level

    def finish():
        sig["status"] = "done"

    for b in sorted(bars, key=lambda x: x["t"]):
        if b["T"] <= sig["checked_until_ms"] or b["T"] >= now_ms:
            continue
        if b["t"] >= expire_ms:
            break
        sig["bars"] += 1
        sig["max_high"] = b["h"] if sig["max_high"] is None else max(sig["max_high"], b["h"])
        sig["min_low"] = b["l"] if sig["min_low"] is None else min(sig["min_low"], b["l"])
        best, worst = (sig["max_high"], sig["min_low"]) if side == "long" else (sig["min_low"], sig["max_high"])
        sig["mfe_pct"] = max(0.0, _fav_pct(sig, best))
        sig["mae_pct"] = max(0.0, -_fav_pct(sig, worst))
        sig["checked_until_ms"] = b["T"]
        sig["last_close"] = b["c"]
        hit_stop = touched(stop, b, False)
        hit_t1 = touched(t1, b, True)
        hit_t2 = touched(t2, b, True)
        mins = sig["bars"]
        if sig["phase"] == "pre_t1":
            if hit_stop:
                pct = _fav_pct(sig, stop)
                sig.update(result="hit_stop", result_ms=b["T"] + 1, result_bars=mins, result_pct=pct)
                msgs.append(f"#{sig['id']:04d} {side_ko} → 손절 ({fmt_pct(pct)}, {mins}분)")
                finish()
                break
            if hit_t1:
                sig.update(t1_hit_ms=b["T"] + 1, t1_bars=mins)
                if hit_t2:
                    pct = _fav_pct(sig, t2)
                    sig.update(result="hit_target2", result_ms=b["T"] + 1, result_bars=mins, result_pct=pct)
                    msgs.append(f"#{sig['id']:04d} {side_ko} → 목표2 도달 ({fmt_pct(pct)}, {mins}분)")
                    finish()
                    break
                pct = _fav_pct(sig, t1)
                sig.update(result="hit_target1", result_ms=b["T"] + 1, result_bars=mins,
                           result_pct=pct, phase="post_t1")
                msgs.append(f"#{sig['id']:04d} {side_ko} → 목표1 도달 ({fmt_pct(pct)}, {mins}분)")
                if t2 is None:
                    finish()
                    break
        else:  # post_t1: 목표2 승격 또는 손절로 관찰 종료 (결과는 hit_target1 유지)
            if hit_stop:
                sig["after_t1"] = "stop"
                finish()
                break
            if hit_t2:
                pct = _fav_pct(sig, t2)
                sig.update(result="hit_target2", result_ms=b["T"] + 1, result_bars=mins, result_pct=pct)
                msgs.append(f"#{sig['id']:04d} {side_ko} → 목표2 도달 ({fmt_pct(pct)}, {mins}분)")
                finish()
                break

    if sig["status"] == "open" and sig["checked_until_ms"] + 1 >= expire_ms:
        if sig["phase"] == "pre_t1":
            pct = _fav_pct(sig, sig["last_close"]) if sig["last_close"] is not None else None
            sig.update(result="expired", result_ms=expire_ms, result_bars=sig["bars"], result_pct=pct)
            msgs.append(f"#{sig['id']:04d} {side_ko} → 만료 {CONFIG['EXPIRE_HOURS']:g}시간 미도달 "
                        f"(종가 {fmt_pct(pct)})")
        else:
            sig["after_t1"] = "expired"
        finish()
    return msgs


def track_open_signals(send, fetch=lv.fetch_klines, path=None, now_ms=None):
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    with _lock:
        signals = load_signals(path)
        open_sigs = [s for s in signals if s.get("status") == "open"]
        if not open_sigs:
            return
        by_sym = {}
        for s in open_sigs:
            by_sym.setdefault(s["symbol"], []).append(s)
    msgs = []
    fetched = {}
    for sym, sigs in by_sym.items():
        start = min(s["checked_until_ms"] for s in sigs) + 1
        try:
            fetched[sym] = fetch(sym, "1m", limit=lv.SPOT_MAX_LIMIT, start_ms=start)
        except Exception as e:
            print(f"[코인추적] {sym} 1m 조회 실패: {e}")
    with _lock:
        signals = load_signals(path)   # 조회 사이 새 신호가 들어왔을 수 있어 다시 읽음
        changed = False
        for s in signals:
            if s.get("status") == "open" and s["symbol"] in fetched:
                before = (s["checked_until_ms"], s["status"])
                out = update_signal(s, fetched[s["symbol"]], now_ms)
                if s.get("sent", True):      # 억제·중복 신호는 결과도 조용히 기록만
                    msgs += out
                changed = changed or before != (s["checked_until_ms"], s["status"])
        if changed:
            save_signals(signals, path)
    for m in msgs:
        send(m)


# ── 요약 ──────────────────────────────────────────────────────────────

def _rate(sub):
    """판정된 신호 중 목표1(이상) 도달률 %, 건수."""
    dec = [s for s in sub if s.get("result")]
    if not dec:
        return None, 0
    hit = sum(1 for s in dec if s["result"] in ("hit_target1", "hit_target2"))
    return hit / len(dec) * 100.0, len(dec)


def summarize(signals, now_ms, days):
    """주 통계는 발송된 신호만. 억제·중복은 건수와 억제 신호 도달률을 따로."""
    since = now_ms - days * 86_400_000
    window = [s for s in signals if s["bar_close_ms"] >= since]
    sub = [s for s in window if s.get("sent", True)]
    supp = [s for s in window if s.get("suppressed")]
    supp_dec = [s for s in supp if s.get("result")]
    dec = [s for s in sub if s.get("result")]
    n_dec = len(dec)

    def share(res):
        return sum(1 for s in dec if s["result"] in res) / n_dec * 100.0 if n_dec else None

    done = [s for s in sub if s.get("status") == "done"]
    return {
        "days": days, "n": len(sub), "decided": n_dec, "pending": len(sub) - n_dec,
        "t1_rate": share(("hit_target1", "hit_target2")),
        "stop_rate": share(("hit_stop",)),
        "expired_rate": share(("expired",)),
        "avg_mfe": sum(s["mfe_pct"] for s in done) / len(done) if done else None,
        "avg_mae": sum(s["mae_pct"] for s in done) / len(done) if done else None,
        "rr_ge1": _rate([s for s in sub if s.get("rr") is not None and s["rr"] >= 1]),
        "rr_lt1": _rate([s for s in sub if s.get("rr") is not None and s["rr"] < 1]),
        "cloud_with": _rate([s for s in sub if (s.get("cloud") or {}).get("bias") == "편"]),
        "cloud_against": _rate([s for s in sub if (s.get("cloud") or {}).get("bias") == "역"]),
        "n_suppressed": len(supp),
        "n_duplicate": sum(1 for s in window if s.get("duplicate")),
        "supp_decided": len(supp_dec),
        "supp_t1_rate": (sum(1 for s in supp_dec if s["result"] in ("hit_target1", "hit_target2"))
                         / len(supp_dec) * 100.0) if supp_dec else None,
        "supp_stop_rate": (sum(1 for s in supp_dec if s["result"] == "hit_stop")
                           / len(supp_dec) * 100.0) if supp_dec else None,
    }


def _r(x):
    return "—" if x is None else f"{x:.0f}%"


def format_summary(sums, now_ms):
    nz = datetime.fromtimestamp(now_ms / 1000, timezone.utc).astimezone(NZ_TZ)
    lines = [f"📊 코인 신호 요약 ({nz.strftime('%m/%d %H:%M')} NZ)"]
    for s in sums:
        lines.append("")
        lines.append(f"[최근 {s['days']}일] 신호 {s['n']} (판정 {s['decided']}, 진행 {s['pending']})")
        lines.append(f"억제 {s['n_suppressed']}건 / 중복 {s['n_duplicate']}건")
        if s["supp_decided"]:
            lines.append(f"억제 신호: 목표1 {_r(s['supp_t1_rate'])} · 손절 {_r(s['supp_stop_rate'])} "
                         f"(판정 {s['supp_decided']}건)")
        if not s["decided"]:
            continue
        lines.append(f"목표1 도달 {_r(s['t1_rate'])} · 손절 {_r(s['stop_rate'])} · 만료 {_r(s['expired_rate'])}")
        if s["avg_mfe"] is not None:
            lines.append(f"평균 MFE {fmt_pct(s['avg_mfe'])} / MAE {fmt_pct(-s['avg_mae'])}")
        lines.append(f"손익비≥1: 목표1 {_r(s['rr_ge1'][0])} ({s['rr_ge1'][1]}건) · "
                     f"<1: {_r(s['rr_lt1'][0])} ({s['rr_lt1'][1]}건)")
        lines.append(f"{CONFIG['CLOUD_TF']} 구름 편: 목표1 {_r(s['cloud_with'][0])} ({s['cloud_with'][1]}건) · "
                     f"역: {_r(s['cloud_against'][0])} ({s['cloud_against'][1]}건)")
    return "\n".join(lines)


def daily_summary(send, path=None, now_ms=None):
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    signals = load_signals(path)
    send(format_summary([summarize(signals, now_ms, 7), summarize(signals, now_ms, 30)], now_ms))


# ── 등록 ──────────────────────────────────────────────────────────────

def _safe(fn, name):
    """스케줄 잡 예외가 메인 루프(기존 알림 전체)를 죽이지 않게."""
    def run():
        try:
            fn()
        except Exception as e:
            print(f"[코인신호] {name} 오류: {e}")
    return run


def register(sched, send_telegram):
    """main.py가 호출. 스케줄 등록 + HTTP 서버(Webhook·조회 API) 기동."""
    chat_id = os.environ.get("COIN_CHAT_ID") or None

    def send(msg):
        send_telegram(msg, chat_id=chat_id)

    sched.every(15).seconds.do(_safe(lambda: self_detect(send), "자체판정"))
    sched.every(5).seconds.do(_safe(lambda: drain_webhook_queue(send), "webhook"))
    sched.every(1).minutes.do(_safe(lambda: track_open_signals(send), "추적"))
    sched.every().day.at(CONFIG["SUMMARY_TIME_NZ"], "Pacific/Auckland").do(
        _safe(lambda: daily_summary(send), "요약"))
    try:
        import coin_http
        coin_http.start_server()
    except Exception as e:
        print(f"[코인신호] HTTP 서버 기동 실패: {e}")
