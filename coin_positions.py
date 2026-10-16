"""코인 신호 가상 포지션 추적 — 채널 방식, 손절 없음 (v2.41).

봇은 주문하지 않는다. "지금 뭘 할 차례인지"를 알리고 결과를 기록만 한다.

규칙
  - 발송된 신호의 심볼에 열린 포지션이 없으면 신규(1차 = ②확인봉 종가, 1단위).
    목표1/2는 1차 진입 때 값 고정.
  - 같은 방향 신호: 평단 대비 ADD_THRESHOLD_PCT 이상 역행 중이고 추가 < MAX_ADDS
    이고 아직 덜어내기 전이면 추가 진입(ADD_QTY), 평단 = 수량 가중 평균.
  - 반대 방향 신호: 포지션 동작 없음(신호 쪽에서 저장만).
  - 1m 봉 추적: 포기선(평단 대비 GIVEUP_PCT 역행) → 종료. 추가분이 있고 가격이
    평단에 닿으면 추가분만 덜어내기(평단 = 1차 진입가로 복귀, 1회). 목표1 → 절반
    청산. 목표2 → 종료. POS_EXPIRE_HOURS 미종료 → 만료.
  - 한 봉 안에서는 역행(포기선)을 먼저 본다(보수적) → 덜어내기 → 목표1 → 목표2.
  - 결과 % = 청산된 수량 가중 평균 수익률(각 청산의 기준가 대비).
  - MFE/MAE = 매 봉 그 시점 평단 대비 최대 유리/역행 폭(포지션 전체).
저장: $DATA_DIR/positions.json (신호 파일과 분리).
"""
import json
import os
import threading
import time

from signal_levels import CONFIG

MINUS = "−"
_lock = threading.RLock()


# ── 저장소 ────────────────────────────────────────────────────────────

def positions_path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "positions.json")


def load_positions(path=None):
    path = path or positions_path()
    with _lock:
        if not os.path.exists(path):
            return []
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            ps = data.get("positions", []) if isinstance(data, dict) else []
            return ps if isinstance(ps, list) else []
        except (ValueError, OSError):
            return []


def save_positions(positions, path=None):
    path = path or positions_path()
    with _lock:
        try:
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"positions": positions}, f, ensure_ascii=False)
            os.replace(tmp, path)
        except OSError as e:
            print(f"[코인포지션] 저장 실패: {e}")


# ── 계산 ──────────────────────────────────────────────────────────────

def fav_pct(side, ref, price):
    """ref 대비 price의 신호 방향 기준 유리한 쪽 +%."""
    p = (price - ref) / ref * 100.0
    return p if side == "long" else -p


def weighted_avg(avg, qty, price, add_qty):
    """수량 가중 평단."""
    return (avg * qty + price * add_qty) / (qty + add_qty)


def fmt_pct(x, digits=2):
    if x is None:
        return "—"
    s = f"{abs(x):.{digits}f}%"
    return (MINUS + s) if round(x, digits) < 0 else ("+" + s)


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


def fmt_dur(ms):
    m = int(ms // 60_000)
    h, m = divmod(m, 60)
    return f"{h}시간 {m}분" if h else f"{m}분"


def open_position(positions, symbol):
    for p in positions:
        if p["symbol"] == symbol and p["status"] == "open":
            return p
    return None


def _event(pos, ms, kind, price, pct=None):
    ev = {"ms": ms, "kind": kind, "price": price, "avg": pos["avg"], "qty": pos["qty"],
          "pct": pct if pct is not None else fav_pct(pos["side"], pos["avg"], price),
          "mfe": pos["mfe_pct"], "mae": pos["mae_pct"]}
    pos["events"].append(ev)
    return ev


def _realize(pos, kind, qty, price, basis):
    ret = fav_pct(pos["side"], basis, price)
    pos["realized"].append({"kind": kind, "qty": qty, "price": price, "basis": basis, "ret_pct": ret})
    return ret


def _close(pos, ms, result, price):
    _realize(pos, result, pos["qty"], price, pos["avg"])
    ev = _event(pos, ms, result, price)
    pos["qty"] = 0.0
    pos.update(status="closed", result=result, closed_ms=ms,
               result_pct=sum(r["qty"] * r["ret_pct"] for r in pos["realized"])
               / sum(r["qty"] for r in pos["realized"]))
    return ev


def new_position(positions, sig):
    pid = max((p["id"] for p in positions), default=0) + 1
    ms = sig["bar_close_ms"] + 1
    pos = {
        "id": pid, "symbol": sig["symbol"], "side": sig["side"], "tf": sig["tf"],
        "status": "open", "result": None, "result_pct": None,
        "opened_ms": ms, "closed_ms": None, "signal_ids": [sig["id"]],
        "entry1": sig["entry"], "target1": sig.get("target1"), "target2": sig.get("target2"),
        "cloud_bias": (sig.get("cloud") or {}).get("bias"),
        "qty": 1.0, "avg": sig["entry"], "adds": [], "trimmed": False, "t1_done": False,
        "realized": [], "events": [],
        "checked_until_ms": sig["bar_close_ms"], "last_close": None,
        "mfe_pct": 0.0, "mae_pct": 0.0,
    }
    _event(pos, ms, "open", sig["entry"])
    positions.append(pos)
    return pos


def try_add(pos, sig):
    """같은 방향 신호. 추가했으면 (True, 줄), 아니면 (False, 줄)."""
    price = sig["entry"]
    move = fav_pct(pos["side"], pos["avg"], price)        # 음수 = 역행
    reasons = []
    if len(pos["adds"]) >= CONFIG["MAX_ADDS"]:
        reasons.append(f"추가 {CONFIG['MAX_ADDS']}회 소진")
    if pos["trimmed"]:
        reasons.append("덜어내기 후")
    if -move < CONFIG["ADD_THRESHOLD_PCT"] or reasons:
        tail = ", ".join([f"평단 대비 {fmt_pct(move)}"] + reasons)
        return False, f"포지션 #P{pos['id']} 보유 중, 추가 조건 미달 ({tail})"
    old = pos["avg"]
    q = CONFIG["ADD_QTY"]
    pos["avg"] = weighted_avg(old, pos["qty"], price, q)
    pos["qty"] += q
    pos["adds"].append({"price": price, "qty": q, "signal_id": sig["id"], "ms": sig["bar_close_ms"] + 1})
    pos["signal_ids"].append(sig["id"])
    _event(pos, sig["bar_close_ms"] + 1, "add", price)
    n = len(pos["adds"]) + 1
    return True, f"포지션 #P{pos['id']} 추가 {n}회차 (평단 {fmt_px(old)} → {fmt_px(pos['avg'])})"


def update_position(pos, bars, now_ms, until_ms=None):
    """마감된 1m 봉으로 포지션 갱신(제자리 수정). 이벤트 한 줄 메시지 리스트."""
    if pos["status"] != "open":
        return []
    side, pid = pos["side"], pos["id"]
    expire_ms = pos["opened_ms"] + int(CONFIG["POS_EXPIRE_HOURS"] * 3_600_000)
    t1, t2 = pos.get("target1"), pos.get("target2")
    up = side == "long"
    msgs = []

    def reaches(level, b):          # 유리한 쪽으로 level 도달
        return b["h"] >= level if up else b["l"] <= level

    for b in sorted(bars, key=lambda x: x["t"]):
        if b["T"] <= pos["checked_until_ms"] or b["T"] >= now_ms:
            continue
        if until_ms is not None and b["T"] > until_ms:
            break
        if b["t"] >= expire_ms:
            break
        ms = b["T"] + 1
        adverse_px, fav_px = (b["l"], b["h"]) if up else (b["h"], b["l"])
        pos["mae_pct"] = max(pos["mae_pct"], -fav_pct(side, pos["avg"], adverse_px))
        pos["mfe_pct"] = max(pos["mfe_pct"], fav_pct(side, pos["avg"], fav_px))
        pos["checked_until_ms"] = b["T"]
        pos["last_close"] = b["c"]
        # 1) 포기선 (역행 먼저 — 보수적)
        if -fav_pct(side, pos["avg"], adverse_px) >= CONFIG["GIVEUP_PCT"]:
            g = CONFIG["GIVEUP_PCT"] / 100.0
            price = pos["avg"] * (1 - g) if up else pos["avg"] * (1 + g)
            ev = _close(pos, ms, "giveup", price)
            msgs.append(f"#P{pid} 포기선 ({fmt_pct(ev['pct'])})")
            break
        # 2) 추가분 덜어내기 — 평단 복귀
        if pos["adds"] and not pos["trimmed"] and reaches(pos["avg"], b):
            add_q = sum(a["qty"] for a in pos["adds"])
            add_basis = sum(a["price"] * a["qty"] for a in pos["adds"]) / add_q
            ret = _realize(pos, "trim", add_q, pos["avg"], add_basis)
            price = pos["avg"]
            pos["qty"] -= add_q
            pos["avg"] = pos["entry1"]
            pos["trimmed"] = True
            _event(pos, ms, "trim", price, pct=ret)
            msgs.append(f"#P{pid} 추가분 덜어내기 (평단 복귀, 추가분 {fmt_pct(ret)})")
        # 3) 목표1 — 절반 청산
        if t1 is not None and not pos["t1_done"] and reaches(t1, b):
            half = pos["qty"] / 2.0
            pct = _realize(pos, "target1", half, t1, pos["avg"])
            pos["qty"] -= half
            pos["t1_done"] = True
            _event(pos, ms, "target1", t1, pct=pct)
            msgs.append(f"#P{pid} 목표1 ({fmt_pct(pct)}, {fmt_dur(ms - pos['opened_ms'])})")
        # 4) 목표2 — 종료
        if t2 is not None and pos["t1_done"] and reaches(t2, b):
            ev = _close(pos, ms, "hit_target2", t2)
            msgs.append(f"#P{pid} 종료 목표2 ({fmt_pct(ev['pct'])})")
            break

    if pos["status"] == "open" and pos["checked_until_ms"] + 1 >= expire_ms and pos["last_close"] is not None:
        ev = _close(pos, expire_ms, "expired", pos["last_close"])
        msgs.append(f"#P{pid} 만료 {CONFIG['POS_EXPIRE_HOURS']:g}시간 (종가 {fmt_pct(ev['pct'])})")
    return msgs


def on_signal(sig, fetch, path=None, now_ms=None, bind_pid=None):
    """발송 대상 신호 1건을 포지션에 반영.
    반환 (action, line, msgs, pid): action = "new"/"add"/"hold"/"opposite"/"confirm",
    line = 신호 메시지에 넣을 포지션 줄(opposite면 따로 보낼 포지션 메시지 한 줄,
    confirm이면 None), msgs = 신호 직전까지 따라잡기 추적에서 생긴 이벤트 줄
    (신호보다 먼저 발송).
    v2.42 bind_pid: 1m → 5m 확인 신호. 그 포지션이 아직 열려 있으면 추가 없이
    묶고 목표1/2를 이 신호 값으로 갱신(값이 없는 쪽은 유지)."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    msgs = []
    with _lock:
        positions = load_positions(path)
        pos = open_position(positions, sig["symbol"])
        if pos is not None and pos["checked_until_ms"] < sig["bar_close_ms"]:
            # 추가 판단 전에 신호 시각까지 추적을 따라잡아야 평단·종료 여부가 맞음
            try:
                bars = fetch(sig["symbol"], "1m", limit=1000, start_ms=pos["checked_until_ms"] + 1)
                msgs += update_position(pos, bars, now_ms, until_ms=sig["bar_close_ms"])
            except Exception as e:
                print(f"[코인포지션] #P{pos['id']} 따라잡기 조회 실패: {e}")
            if pos["status"] != "open":
                pos = None
        if pos is None:
            pos = new_position(positions, sig)
            action, line = "new", f"포지션: 신규 1차 #P{pos['id']}"
        elif pos["side"] != sig["side"]:
            action = "opposite"
            line = f"#P{pos['id']} 반대 신호 #{sig['id']:04d} 발생 (보유 중)"
            _event(pos, sig["bar_close_ms"] + 1, "opposite_signal", sig["entry"])
            pos["events"][-1]["signal_id"] = sig["id"]
        elif bind_pid is not None and pos["id"] == bind_pid:
            action, line = "confirm", None
            old = (pos.get("target1"), pos.get("target2"))
            for key in ("target1", "target2"):
                if sig.get(key) is not None:
                    pos[key] = sig[key]
            pos["signal_ids"].append(sig["id"])
            _event(pos, sig["bar_close_ms"] + 1, "retarget", sig["entry"])
            pos["events"][-1].update(signal_id=sig["id"], old_targets=list(old),
                                     targets=[pos.get("target1"), pos.get("target2")])
        else:
            added, line = try_add(pos, sig)
            action = "add" if added else "hold"
            if not added:
                pos["signal_ids"].append(sig["id"])
        save_positions(positions, path)
        return action, line, msgs, pos["id"]


def track_positions(send, fetch, path=None, now_ms=None):
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    with _lock:
        open_ps = [p for p in load_positions(path) if p["status"] == "open"]
    if not open_ps:
        return
    fetched = {}
    for sym in {p["symbol"] for p in open_ps}:
        start = min(p["checked_until_ms"] for p in open_ps if p["symbol"] == sym) + 1
        try:
            fetched[sym] = fetch(sym, "1m", limit=1000, start_ms=start)
        except Exception as e:
            print(f"[코인포지션] {sym} 1m 조회 실패: {e}")
    msgs = []
    with _lock:
        positions = load_positions(path)
        for p in positions:
            if p["status"] == "open" and p["symbol"] in fetched:
                msgs += update_position(p, fetched[p["symbol"]], now_ms)
        save_positions(positions, path)
    for m in msgs:
        send(m)


# ── 요약 ──────────────────────────────────────────────────────────────

def _avg(xs):
    xs = list(xs)
    return sum(xs) / len(xs) if xs else None


def summarize_positions(positions, now_ms, days):
    since = now_ms - days * 86_400_000
    sub = [p for p in positions if p["opened_ms"] >= since]
    closed = [p for p in sub if p["status"] == "closed"]

    def res(ps):
        return _avg(p["result_pct"] for p in ps), len(ps)

    return {
        "days": days, "n": len(sub), "closed": len(closed), "open": len(sub) - len(closed),
        "by_result": {k: sum(1 for p in closed if p["result"] == k)
                      for k in ("hit_target2", "giveup", "expired")},
        "avg_result": _avg(p["result_pct"] for p in closed),
        "with_adds": res([p for p in closed if p["adds"]]),
        "no_adds": res([p for p in closed if not p["adds"]]),
        "trims": sum(1 for p in sub if p["trimmed"]),
        "t1_hits": sum(1 for p in sub if p["t1_done"]),
        "max_mae": max((p["mae_pct"] for p in sub), default=None),
        "cloud_with": res([p for p in closed if p.get("cloud_bias") == "편"]),
        "cloud_against": res([p for p in closed if p.get("cloud_bias") == "역"]),
    }


def format_position_summary(s, cloud_tf="30m"):
    br = s["by_result"]
    lines = [f"[최근 {s['days']}일] 포지션 {s['n']} (종료 {s['closed']}: 목표2 {br['hit_target2']} · "
             f"포기선 {br['giveup']} · 만료 {br['expired']} / 보유 {s['open']})"]
    if s["n"]:
        lines.append(f"평균 결과 {fmt_pct(s['avg_result'])} · 최대 MAE "
                     f"{fmt_pct(-s['max_mae']) if s['max_mae'] is not None else '—'}")
        lines.append(f"추가 있음: {fmt_pct(s['with_adds'][0])} ({s['with_adds'][1]}건) · "
                     f"없음: {fmt_pct(s['no_adds'][0])} ({s['no_adds'][1]}건)")
        lines.append(f"목표1 도달 {s['t1_hits']}건 · 덜어내기 {s['trims']}건")
        lines.append(f"{cloud_tf} 구름 편: {fmt_pct(s['cloud_with'][0])} ({s['cloud_with'][1]}건) · "
                     f"역: {fmt_pct(s['cloud_against'][0])} ({s['cloud_against'][1]}건)")
    return lines
