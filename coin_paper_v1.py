"""설계 v1 종이 매매 (v2.69) — 주문 없음. 가상 진입·청산을 기록만 한다.

규칙 전문과 판정 기준은 프로젝트 문서 `코인-설계-v1`(2026-11-14 사전 등록). 요지:
  진입  발송된 신호 중 ① 전일 일봉과 반대 방향(prev_day.fade) ② 같은 방향 연속 RUN_MAX번째까지
        ③ 같은 방향 가상 포지션이 CAP_PER_SIDE개 미만일 때만.
  청산  TP(신호의 고정 TP) / 손절 −STOP_PCT% / HOLD_HOURS 시간 안에 TP 못 가면 그 시각 종가로 정리.
  한 1m 봉이 TP와 손절을 둘 다 닿으면 손절로 본다(보수적 — coin_signal_track과 같음).

**RULE의 숫자는 판정 기간(2026-11-14부터 4주) 동안 바꾸지 않는다.** 바꾸면 그날부터 4주를 다시 센다
(사전 등록 조건). 그래서 환경변수로 열어 두지 않았다 — 바꾸려면 코드와 version을 같이 바꿀 것.
숫자의 출처는 전부 2026-11-14 in-sample 분석(추적 705건)이고 검증 전이다.

저장: $DATA_DIR/coin_paper_v1.json. 바이낸스 공개 klines만 읽는다.
"""
import json
import os
import threading
import time

import signal_levels as lv

RULE = {
    "version": "v1",
    "registered": "2026-11-14",
    "stop_pct": 3.0,          # 진입가 대비 역행 손절
    "hold_hours": 24,         # 이 시간 안에 TP 못 가면 정리
    "run_max": 4,             # 같은 방향 연속 n번째까지 진입
    "cap_per_side": 2,        # 방향별 동시 보유
    "margin": 56.0,           # 1단위 증거금 USDT
    "leverage": 20,
    "fee_tp_pct": 0.07,       # 명목 대비: 진입 taker 0.05 + TP 지정가 maker 0.02
    "fee_other_pct": 0.10,    # 진입 taker 0.05 + 시장가 청산 taker 0.05
}
MIN_MS = 60_000
_lock = threading.RLock()


def paper_path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "coin_paper_v1.json")


def load(path=None):
    path = path or paper_path()
    with _lock:
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except (ValueError, OSError):
            data = {}
        if not isinstance(data, dict):
            data = {}
        return {"rule": RULE, "trades": list(data.get("trades") or []), "skips": list(data.get("skips") or [])}


def save(data, path=None):
    path = path or paper_path()
    with _lock:
        try:
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"rule": RULE, "trades": data["trades"], "skips": data["skips"]}, f, ensure_ascii=False)
            os.replace(tmp, path)
        except OSError as e:
            print(f"[종이v1] 저장 실패: {e}")


def decide(sig, trades):
    """진입이면 None, 아니면 건너뛴 사유. 사유는 판정 기간 뒤 "무엇이 걸렀나"를 세는 데 쓴다."""
    pd = sig.get("prev_day")
    if not pd or pd.get("fade") is None:
        return "no_prev_day"          # 일봉 조회 실패·보합 — 조건을 확인 못 하면 안 들어간다
    if not pd["fade"]:
        return "prev_day"
    if (sig.get("run_n") or 0) > RULE["run_max"]:
        return "run"
    same = [t for t in trades if t["status"] == "open" and t["symbol"] == sig["symbol"] and t["side"] == sig["side"]]
    if len(same) >= RULE["cap_per_side"]:
        return "cap"
    return None


def new_trade(sig, tid):
    long_ = sig["side"] == "long"
    entry = sig["entry"]
    start = sig["bar_close_ms"] + 1
    return {
        "id": tid, "signal_id": sig["id"], "symbol": sig["symbol"], "side": sig["side"], "tf": sig["tf"],
        "entry": entry, "tp": sig["tp"], "tp_pct": sig["tp_pct"],
        "stop": entry * (1 - RULE["stop_pct"] / 100) if long_ else entry * (1 + RULE["stop_pct"] / 100),
        "start_ms": start, "expire_ms": start + RULE["hold_hours"] * 3_600_000,
        "run_n": sig.get("run_n"), "prev_day": sig.get("prev_day"),
        "status": "open", "checked_until_ms": sig["bar_close_ms"], "last_close": None,
        "result": None,               # tp | stop | time
        "exit_px": None, "exit_ms": None, "pnl_pct": None, "pnl_usdt": None,
    }


def on_signal(sig, path=None):
    """발송된 신호 1건을 규칙에 대 본다. 진입했으면 거래 레코드, 아니면 None."""
    if not sig.get("sent"):
        return None
    with _lock:
        data = load(path)
        if any(t["signal_id"] == sig["id"] for t in data["trades"]) or \
           any(s["signal_id"] == sig["id"] for s in data["skips"]):
            return None
        why = decide(sig, data["trades"])
        if why:
            data["skips"].append({"signal_id": sig["id"], "side": sig["side"], "reason": why,
                                  "ms": sig["bar_close_ms"] + 1})
            save(data, path)
            return None
        t = new_trade(sig, max((x["id"] for x in data["trades"]), default=0) + 1)
        data["trades"].append(t)
        save(data, path)
        return t


def _close(t, kind, px, ms):
    sign = 1 if t["side"] == "long" else -1
    fee = RULE["fee_tp_pct"] if kind == "tp" else RULE["fee_other_pct"]
    pct = sign * (px - t["entry"]) / t["entry"] * 100 - fee
    t.update(status="done", result=kind, exit_px=px, exit_ms=ms, pnl_pct=round(pct, 4),
             pnl_usdt=round(pct / 100 * RULE["margin"] * RULE["leverage"], 2))


def update_trade(t, bars, now_ms):
    """마감된 1m 봉으로 거래 하나 갱신(제자리). 바뀌었으면 True."""
    if t["status"] != "open":
        return False
    long_ = t["side"] == "long"
    changed = False
    for b in sorted(bars, key=lambda x: x["t"]):
        if b["T"] <= t["checked_until_ms"] or b["T"] >= now_ms:
            continue
        if b["t"] >= t["expire_ms"]:          # 창을 지난 첫 봉 — 조회 공백이 있었어도 여기서 정리
            _close(t, "time", t["last_close"] if t["last_close"] is not None else b["o"], t["expire_ms"])
            return True
        changed = True
        t["checked_until_ms"] = b["T"]
        t["last_close"] = b["c"]
        if (b["l"] <= t["stop"]) if long_ else (b["h"] >= t["stop"]):
            _close(t, "stop", t["stop"], b["T"] + 1)
            return True
        if (b["h"] >= t["tp"]) if long_ else (b["l"] <= t["tp"]):
            _close(t, "tp", t["tp"], b["T"] + 1)
            return True
        if b["T"] + 1 >= t["expire_ms"]:      # 24시간째 봉이 마감 — 그 종가로 정리
            _close(t, "time", b["c"], b["T"] + 1)
            return True
    return changed


def track_open(fetch=lv.fetch_klines, path=None, now_ms=None):
    """스케줄러용: 열린 가상 거래 전부 갱신. 심볼당 한 번 조회."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    with _lock:
        open_ts = [t for t in load(path)["trades"] if t["status"] == "open"]
    if not open_ts:
        return
    fetched = {}
    for sym in {t["symbol"] for t in open_ts}:
        start = min(t["checked_until_ms"] for t in open_ts if t["symbol"] == sym) + 1
        try:
            fetched[sym] = fetch(sym, "1m", limit=lv.SPOT_MAX_LIMIT, start_ms=start)
        except Exception as e:
            print(f"[종이v1] {sym} 봉 조회 실패: {e}")
    with _lock:
        data = load(path)
        changed = False
        for t in data["trades"]:
            if t["status"] == "open" and t["symbol"] in fetched:
                changed = update_trade(t, fetched[t["symbol"]], now_ms) or changed
        if changed:
            save(data, path)


def stats(trades, now_ms, days=None):
    """끝난 거래 집계. days가 있으면 그 기간에 **끝난** 것만."""
    done = [t for t in trades if t["status"] == "done"
            and (days is None or t["exit_ms"] >= now_ms - days * 86_400_000)]
    return {"n": len(done), "usdt": round(sum(t["pnl_usdt"] for t in done), 2),
            **{k: sum(1 for t in done if t["result"] == k) for k in ("tp", "stop", "time")},
            "open": sum(1 for t in trades if t["status"] == "open")}


def _usd(v):
    return ("+" if v >= 0 else "−") + f"${abs(v):,.1f}"


def summary_line(trades, now_ms):
    """일일 요약용 한 줄. 거래가 한 건도 없으면 그 사실을 적는다(침묵 = 고장과 구분)."""
    d1, al = stats(trades, now_ms, 1), stats(trades, now_ms)
    if not trades:
        return "🧪 v1 가상(주문 없음): 아직 진입 없음"
    return (f"🧪 v1 가상(주문 없음): 24h {d1['n']}건 {_usd(d1['usdt'])} · "
            f"누적 {al['n']}건 {_usd(al['usdt'])} (TP {al['tp']} · 시간 {al['time']} · 손절 {al['stop']}) · "
            f"진행 {al['open']}")
