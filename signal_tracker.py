"""코인 선물 RSI 신호 — 수신(Webhook/자체 판정)·발송·결과 추적·요약 (v2.37).

흐름
  A. 트레이딩뷰 Webhook → coin_http가 검증 후 enqueue_webhook() → 메인
     스케줄 스레드의 drain_webhook_queue()가 처리(HTTP 응답은 즉시 200 —
     트레이딩뷰 Webhook 타임아웃이 짧아서 바이낸스 조회를 응답 경로에서 안 함).
  B. self_detect(): 설정된 심볼·주기의 봉 마감마다 같은 규칙으로 판정.
  둘 다 ingest_signal()로 모여 중복 제거 → 레벨 계산 → 저장 → 발송.
  coin_positions: 발송 신호로 가상 포지션(채널 방식, 손절 없음) 신규/추가,
     1분마다 덜어내기·목표·포기선·만료 추적 (v2.41).
  track_open_signals(): 신호 단위 목표 도달 기록(발송 없음).
  coin_signal_track: 발송 신호별 72h 독립 추적(TP·MAE·−1/−2% 선행, v2.45).
  daily_summary(): 매일 09:00 NZ, 포지션 기준.

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

import coin_positions as cp
import coin_signal_track as st
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


def _window_label():
    mins = CONFIG["PULLBACK_BARS"] * TF_MS[CONFIG["PULLBACK_TF"]] // MIN_MS
    return f"{mins // 60}h" if mins % 60 == 0 else f"{mins}m"


def htf_signal_line(sig):
    """v2.49 큰 주기 반대 신호 경고. 필드 없거나(옛 신호·HTF 이상 주기) None이면 줄 없음."""
    h = sig.get("htf_signal")
    if not h:
        return None
    side_ko = _SIDE_KO[sig["side"]]
    opp_ko = _SIDE_KO["short" if sig["side"] == "long" else "long"]
    if not h["opposite"]:
        return f"{h['tf']} 반대 신호 없음 ✓ (최근 {h['n']}봉)"
    when = datetime.fromtimestamp((h["at_ms"] + 1) / 1000, timezone.utc).strftime("%H:%M")
    move = "반등" if sig["side"] == "short" else "되돌림"
    return f"⚠️ {h['tf']} 최근 {h['n']}봉에 {opp_ko} 신호 ({when} UTC) — {move} 중 {side_ko}"


def pullback_line(sig):
    """v2.43 되돌림 ✓ / ⚠️ 신저점·신고점 구간. v2.43 이전 신호(필드 없음)는 None."""
    if "pullback_ok" not in sig:
        return None
    if sig["pullback_ok"] is None:
        return "되돌림 —"
    if sig["pullback_ok"]:
        return "되돌림 ✓"
    long_ = sig["side"] == "long"
    shown = sig["dist_pct"] if long_ else -sig["dist_pct"]     # 진입가 − 기준 (기준 대비 %)
    return (f"⚠️ {'신저점' if long_ else '신고점'} 구간 ({_window_label()} {'최저' if long_ else '최고'} "
            f"{fmt_px(sig['ref_extreme'])} 대비 {fmt_pct(shown)})")


def tp_line(sig):
    """v2.44 TP +0.5% → 82,460 (20배 ROI +10%)"""
    pct = sig["tp_pct"]
    sign = "+" if sig["side"] == "long" else "−"
    return (f"TP {fmt_px(sig['tp'])} ({sign}{pct:g}% · "
            f"{CONFIG['TP_ROI_LEV']:g}배 ROI +{round(pct * CONFIG['TP_ROI_LEV'], 2):g}%)")


def ref_line(sig):
    """v2.44 참고 1m 직전 고점(롱)/저점(숏) — 목표 아님."""
    what = "고점" if sig["side"] == "long" else "저점"
    v = sig.get("ref_swing")
    if v is None:
        return f"참고 1m 직전 {what} 없음"
    return f"참고 1m 직전 {what} {fmt_px(v)} ({fmt_pct(_pct_from(sig['entry'], v))})"


def format_signal_message(sig):
    """v2.49(사용자 지시 2026-10-20 "한눈에 안 들어와, 정리해줘"): 3덩어리 — 머리(주기·방향·가격·시각),
    숫자(TP·청산·참고), 판정(되돌림·15m 반대 신호·구름·RSI). 꼬리에 ID·가격 기준·가상 포지션."""
    entry = sig["entry"]
    side_ko = _SIDE_KO[sig["side"]]
    head = "🔺" if sig["side"] == "long" else "🔻"
    errs = sig.get("errors") or {}
    t = datetime.fromtimestamp((sig["bar_close_ms"] + 1) / 1000, timezone.utc)
    when = f"{t.strftime('%H:%M')} UTC ({t.astimezone(NZ_TZ).strftime('%H:%M')} NZ)"
    lines = [f"{head} {sig['symbol']} <b>{sig['tf']} {side_ko}</b> {fmt_px(entry)} · {when}",
             "🎯 " + tp_line(sig)]
    liq = lv.liquidation_price(sig["side"], entry)
    liq_s = f"💀 청산 {fmt_px(liq)} ({fmt_pct(_pct_from(entry, liq))})"
    if "ref" not in errs:
        liq_s += " · " + ref_line(sig)
    lines.append(liq_s)
    lines.append("")
    lines.append("<b>판정</b>")
    pb_line = pullback_line(sig)
    if pb_line:
        lines.append(pb_line)
    hs_line = htf_signal_line(sig)
    if hs_line:
        lines.append(hs_line)
    c = sig.get("cloud")
    if c:
        tail = "중립" if c["bias"] == "중립" else f"{side_ko} {c['bias']}"
        cloud_s = f"{CONFIG['CLOUD_TF']} 구름 {_POS_KO[c['pos']]}·{tail} (두께 {c['thick_pct']:.1f}%)"
    else:
        cloud_s = f"{CONFIG['CLOUD_TF']} 구름 —"
    h = sig.get("htf_rsi")
    rsi_s = f"{CONFIG['HTF_RSI_TF']} RSI {h['value']:.0f}{h['dir']}" if h else f"{CONFIG['HTF_RSI_TF']} RSI —"
    lines.append(f"{cloud_s} · {rsi_s}")
    # 실패 사유: 같은 사유는 한 줄로 묶음 (보통 전부 같은 HTTP 오류)
    labels = {"bar": "확인봉", "ref": "1m 참고", "cloud": f"{CONFIG['CLOUD_TF']} 구름",
              "htf_rsi": f"{CONFIG['HTF_RSI_TF']} RSI", "pullback": "되돌림",
              "htf_signal": f"{CONFIG['HTF_SIGNAL_TF']} 반대 신호"}
    by_reason = {}
    for k in ("bar", "ref", "cloud", "htf_rsi", "pullback", "htf_signal"):
        if k in errs:
            by_reason.setdefault(errs[k], []).append(labels[k])
    for reason, what in by_reason.items():
        lines.append(f"⚠️ {'·'.join(what)} 조회 실패: {reason}")
    if sig.get("confirm_note"):
        lines.append(sig["confirm_note"])
    if sig.get("position_note"):
        lines.append(sig["position_note"])
    lines.append("")
    lines.append(f"#{sig['id']:04d} · {sig.get('price_basis') or lv.price_basis()} 기준")
    return "\n".join(lines)


# ── 수신·중복 제거·발송 ──────────────────────────────────────────────

def is_duplicate(signals, symbol, tf, side, bar_open_ms, n_bars=None):
    """같은 심볼·주기·방향의 발송된 신호와 N봉 이내면 중복.
    저장된 신호 기준이라 재시작 후에도 유지되고, Webhook과 자체 판정이 같은
    봉을 둘 다 내도 하나만 나간다.
    v2.44.2: N봉 기준은 실제 발송(sent)된 신호만 — 억제·교차 중복·반대방향 보류
    신호가 뒤 정상 신호를 막지 않게(교차 중복과 동일). 단 같은 봉(같은 bar_open_ms)은
    발송 여부와 상관없이 중복 — Webhook·자체 판정이 같은 봉을 두 번 저장하지 않게."""
    n_bars = lv.dedup_bars(tf) if n_bars is None else n_bars
    span = n_bars * TF_MS[tf]
    for s in signals:
        if s["symbol"] != symbol or s["tf"] != tf or s["side"] != side:
            continue
        if s["bar_open_ms"] == bar_open_ms:
            return True
        if s.get("sent", True) and abs(bar_open_ms - s["bar_open_ms"]) < span:
            return True
    return False


def suppress_reason(confirm_range_pct):
    """알림 억제 사유(없으면 None). v2.44: ② 확인봉 변동폭(고가−저가)/시가가
    STILL_BAR_PCT 미만이면 "미동 확인봉". 봉을 못 받았으면 판정하지 않음.
    (v2.41 목표1 거리 억제는 고정 TP라 삭제)"""
    if confirm_range_pct is not None and confirm_range_pct < CONFIG["STILL_BAR_PCT"]:
        return "미동 확인봉"
    return None


def _cross_tf_refs(signals, symbol, tf, side, bar_close_ms):
    win = CONFIG["CROSS_DUP_MIN"] * MIN_MS
    return [s for s in reversed(signals)
            if s["symbol"] == symbol and s["side"] == side and s["tf"] != tf
            and s.get("sent", True) and abs(bar_close_ms - s["bar_close_ms"]) <= win]


def _is_confirm(ref, tf, bar_close_ms):
    """짧은 주기(1m) 발송 뒤 긴 주기(5m)가 같은 방향으로 뜬 경우."""
    return TF_MS[tf] > TF_MS[ref["tf"]] and ref["bar_close_ms"] <= bar_close_ms


def cross_tf_duplicate(signals, symbol, tf, side, bar_close_ms):
    """v2.40 다른 주기·같은 심볼·같은 방향으로 실제 발송된 신호가 신호 시각 기준
    CROSS_DUP_MIN분 이내(앞뒤)에 있으면 그 신호 id, 없으면 None.
    v2.42: 짧은 주기 신호 뒤의 긴 주기 신호(1m → 5m)는 중복이 아니라 확인
    (cross_tf_confirm). 반대 순서(5m → 1m)는 그대로 중복."""
    for s in _cross_tf_refs(signals, symbol, tf, side, bar_close_ms):
        if not _is_confirm(s, tf, bar_close_ms):
            return s["id"]
    return None


def cross_tf_confirm(signals, symbol, tf, side, bar_close_ms):
    """v2.42 확인 대상인 짧은 주기 발송 신호(가장 최근) 또는 None."""
    return next((s for s in _cross_tf_refs(signals, symbol, tf, side, bar_close_ms)
                 if _is_confirm(s, tf, bar_close_ms)), None)


def ingest_signal(symbol, tf, side, entry, bar_open_ms, source, send,
                  fetch=lv.fetch_klines, path=None, now_ms=None, pos_path=None,
                  track_path=None):
    """신호 1건 처리. 저장된 레코드(발송 여부는 sig["sent"]), 같은 주기 중복이면 None.
    발송 대상이면 포지션에 반영(신규/추가/보유), 반대 방향 포지션이 열려 있으면
    opposite_of·reason="opposite"를 달고 참고 한 줄과 함께 그대로 발송(v2.47; v2.45~46은 미발송).
    발송 신호는 coin_signal_track에 72h 독립 추적 등록(track_path=None이면 기본 파일)."""
    bar_close_ms = bar_open_ms + TF_MS[tf] - 1
    with _lock:
        signals = load_signals(path)
        if is_duplicate(signals, symbol, tf, side, bar_open_ms):
            print(f"[코인신호] 중복 무시 {symbol} {tf} {side} {bar_open_ms} ({source})")
            return None
    ctx = lv.build_signal_context(symbol, tf, side, entry, bar_close_ms, fetch=fetch,
                                  spot_entry=(source == "webhook"))
    for k, e in ctx["errors"].items():
        print(f"[코인신호] {symbol} {tf} {k} 실패: {e}")
    with _lock:
        signals = load_signals(path)
        if is_duplicate(signals, symbol, tf, side, bar_open_ms):
            return None
        sup = suppress_reason(ctx.get("confirm_range_pct"))
        dup_of = cross_tf_duplicate(signals, symbol, tf, side, bar_close_ms)
        ref = cross_tf_confirm(signals, symbol, tf, side, bar_close_ms) if dup_of is None else None
        sig = {
            "id": max((s["id"] for s in signals), default=0) + 1,
            "time": datetime.fromtimestamp((bar_close_ms + 1) / 1000, timezone.utc).isoformat(),
            "received_ms": now_ms if now_ms is not None else int(time.time() * 1000),
            "bar_open_ms": bar_open_ms, "bar_close_ms": bar_close_ms,
            "symbol": symbol, "tf": tf, "side": side, "entry": entry,
            "tp": ctx["tp"], "tp_pct": ctx["tp_pct"],              # v2.44 고정 TP
            "ref_swing": ctx.get("ref_swing"),                     # 참고 1m 직전 고점/저점
            "confirm_range_pct": ctx.get("confirm_range_pct"),     # ② 확인봉 변동폭 %
            "cloud": ({k: ctx["cloud"][k] for k in ("pos", "thick_pct", "bias")}
                      if ctx["cloud"] else None),
            "htf_rsi": ctx["htf_rsi"],
            "htf_signal": ctx.get("htf_signal"),                   # v2.49 큰 주기 반대 신호(경고만)
            # v2.43 되돌림(통계용, 억제 안 함). 조회 실패면 셋 다 None
            "pullback_ok": (ctx.get("pullback") or {}).get("pullback_ok"),
            "ref_extreme": (ctx.get("pullback") or {}).get("ref_extreme"),
            "dist_pct": (ctx.get("pullback") or {}).get("dist_pct"),
            # v2.43.1 되돌림 비교에 쓴 현물 환산가(Webhook만, entry는 원래 선물가 그대로)
            "entry_spot": ctx.get("entry_spot"),
            "source": source,
            "errors": ctx["errors"], "price_basis": lv.price_basis(),
            # v2.40: 억제·교차 중복은 발송만 안 하고 저장·추적은 그대로
            "suppressed": sup is not None, "suppress_reason": sup,
            "duplicate": dup_of is not None, "dup_of": dup_of,
            "sent": sup is None and dup_of is None,
            "reason": None,                   # v2.45 반대방향 보류 = "opposite"
            "opposite_of": None, "position_id": None, "position_action": None, "position_note": None,
            "confirm_of": None, "confirm_note": None,
            # 추적 상태
            "status": "open", "result": None,
            "checked_until_ms": bar_close_ms, "bars": 0,
            "max_high": None, "min_low": None, "last_close": None,
            "mfe_pct": 0.0, "mae_pct": 0.0,
            "result_ms": None, "result_bars": None, "result_pct": None,
        }
        pre_msgs = []
        if sig["sent"]:
            bind = ref.get("position_id") if ref else None
            action, line, pre_msgs, pid = cp.on_signal(sig, fetch, path=pos_path, now_ms=now_ms,
                                                       bind_pid=bind)
            sig.update(position_id=pid, position_action=action, position_note=line)
            if ref is not None:
                sig["confirm_of"] = ref["id"]
                sig["confirm_note"] = f"{ref['tf']} #{ref['id']:04d} → {tf} 확인" + \
                    (f" (포지션 #P{pid} 유지)" if action == "confirm" else "")   # v2.44 고정 TP라 목표 갱신 없음
                if action == "confirm":
                    sig["position_note"] = None      # 확인 줄이 포지션 줄을 대신함
            if action == "opposite":
                # v2.47(사용자 지시 2026-10-19 "알림은 그래도 다 와야지"): 신호는 그대로 발송.
                # 봇의 가상 포지션은 슬기 실제 포지션과 따로 놀 수 있어(봇 롱 vs 실제 숏) 반대방향을
                # 숨기면 필요한 신호가 빠진다. 가상 포지션 상태는 참고 한 줄로만. sent=True라
                # dedup 기준·72h 추적에도 들어간다. v2.45~v2.46은 여기서 sent=False였다.
                sig.update(opposite_of=pid, reason="opposite",
                           position_note=f"봇 가상 #P{pid} 반대 보유 (참고만)")
                print(f"[코인신호] {line}")
        signals.append(sig)
        save_signals(signals, path)
    for m in pre_msgs:          # 신호 직전까지 따라잡기에서 생긴 포지션 이벤트
        send(m)
    if sig["sent"]:
        try:
            st.register(sig, track_path)
        except Exception as e:
            print(f"[신호추적] #{sig['id']:04d} 등록 실패: {e}")
        send(format_signal_message(sig))
    else:
        why = [x for x in (sup and f"억제: {sup}", dup_of and f"중복: #{dup_of:04d}",
                           sig["opposite_of"] and f"반대 방향 포지션 #P{sig['opposite_of']} 보유") if x]
        print(f"[코인신호] #{sig['id']:04d} {symbol} {tf} {side} 발송 안 함 — {' / '.join(why)}")
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


def sig_tp(sig):
    """v2.44 이전 신호(목표1/2)는 목표1을 TP로 봄."""
    return sig.get("tp", sig.get("target1"))


def update_signal(sig, bars, now_ms):
    """신호 단위 기록(텔레그램 발송 없음 — 포지션 이벤트가 대신함. 억제·중복
    신호의 "실제로 어땠는지"와 요약 통계용). 마감된 1m 봉으로 sig 갱신(제자리
    수정), 로그용 한 줄 리스트 반환. v2.44: 단일 TP 도달 / EXPIRE_HOURS 만료.
    MAE·MFE는 TP 도달 봉까지."""
    if sig["status"] != "open":
        return []
    msgs = []
    side = sig["side"]
    tp = sig_tp(sig)
    expire_ms = sig["bar_close_ms"] + 1 + int(CONFIG["EXPIRE_HOURS"] * 3_600_000)
    side_ko = _SIDE_KO[side]

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
        if tp is not None and (b["h"] >= tp if side == "long" else b["l"] <= tp):
            pct = _fav_pct(sig, tp)
            sig.update(result="hit_tp", result_ms=b["T"] + 1, result_bars=sig["bars"], result_pct=pct,
                       status="done")
            msgs.append(f"#{sig['id']:04d} {side_ko} → TP ({fmt_pct(pct)}, {sig['bars']}분)")
            break

    if sig["status"] == "open" and sig["checked_until_ms"] + 1 >= expire_ms:
        pct = _fav_pct(sig, sig["last_close"]) if sig["last_close"] is not None else None
        sig.update(result="expired", result_ms=expire_ms, result_bars=sig["bars"], result_pct=pct,
                   status="done")
        msgs.append(f"#{sig['id']:04d} {side_ko} → 만료 {CONFIG['EXPIRE_HOURS']:g}시간 미도달 "
                    f"(종가 {fmt_pct(pct)})")
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
        print(f"[코인신호기록] {m}")   # v2.41: 결과 알림은 포지션 이벤트로 — 신호 단위는 기록만


# ── 요약 (v2.41: 포지션 기준, 신호는 한 줄) ──────────────────────────

def _is_tp(s):
    return s.get("result") == "hit_tp"


STILL = "미동 확인봉"


def still_stats(signals, now_ms, days):
    """v2.44 미동 억제 신호 건수와 그 결과(신호 단위 기록 — 보냈다면 어땠는지)."""
    since = now_ms - days * 86_400_000
    w = [s for s in signals if s["bar_close_ms"] >= since and s.get("suppress_reason") == STILL]
    done = [s for s in w if s.get("status") == "done"]
    return {"n": len(w), "done": len(done), "tp": sum(1 for s in done if _is_tp(s))}


def format_still_line(st):
    if not st["n"]:
        return "미동 억제 0건"
    pend = st["n"] - st["done"]
    return (f"미동 억제 {st['n']}건 → TP {cp.fmt_rate(st['tp'], st['done'])}"
            + (f" (기록 중 {pend})" if pend else ""))


def signal_counts(signals, now_ms, days):
    since = now_ms - days * 86_400_000
    w = [s for s in signals if s["bar_close_ms"] >= since]
    return {"sent": sum(1 for s in w if s.get("sent", True)),
            "suppressed": sum(1 for s in w if s.get("suppressed")),
            "duplicate": sum(1 for s in w if s.get("duplicate")),
            "opposite": sum(1 for s in w if s.get("opposite_of"))}


def format_summary(signals, positions, now_ms, windows=(7, 30), tracks=()):
    """v2.44 요약: v2.44 포지션(고정 TP) 기준 + 미동 억제 + v2.44 이전 기록 분리."""
    nz = datetime.fromtimestamp(now_ms / 1000, timezone.utc).astimezone(NZ_TZ)
    lines = [f"📊 코인 요약 ({nz.strftime('%m/%d %H:%M')} NZ)"]
    for days in windows:
        lines.append("")
        lines += cp.format_position_summary(cp.summarize_positions(positions, now_ms, days))
        c = signal_counts(signals, now_ms, days)     # v2.44.2 복구
        lines.append(f"신호: 발송 {c['sent']} · 억제 {c['suppressed']} · 중복 {c['duplicate']} · "
                     f"반대방향 {c['opposite']}")
        lines += st.format_lines(tracks, now_ms, days)       # v2.45 신호별 독립 추적
        lines.append(format_still_line(still_stats(signals, now_ms, days)))
        legacy = cp.format_legacy_line(cp.summarize_legacy(positions, now_ms, days),
                                       sum(1 for s in signals if "tp" not in s and s.get("sent", True)
                                           and s["bar_close_ms"] >= now_ms - days * 86_400_000))
        if legacy:
            lines.append(legacy)
    return "\n".join(lines)


def daily_summary(send, path=None, pos_path=None, now_ms=None, track_path=None):
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    send(format_summary(load_signals(path), cp.load_positions(pos_path), now_ms,
                        tracks=st.load_tracks(track_path)))


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
    sched.every(1).minutes.do(_safe(lambda: track_open_signals(send), "신호기록"))
    sched.every(1).minutes.do(_safe(lambda: cp.track_positions(send, lv.fetch_klines), "포지션추적"))
    sched.every(1).minutes.do(_safe(lambda: st.track_open(lv.fetch_klines), "신호별추적"))
    sched.every().day.at(CONFIG["SUMMARY_TIME_NZ"], "Pacific/Auckland").do(
        _safe(lambda: daily_summary(send), "요약"))
    try:
        import coin_http
        coin_http.start_server()
    except Exception as e:
        print(f"[코인신호] HTTP 서버 기동 실패: {e}")
