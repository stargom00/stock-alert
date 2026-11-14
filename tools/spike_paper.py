"""급등 숏 설계 v2 종이 매매 (v2.76) — 주문 없음. 하루 한 번 Actions가 전날 봉으로 가상 거래를 기록한다.

규칙 전문·근거는 프로젝트 문서 `코인-급등숏-설계-v2`(2026-11-14 사전 등록):
  신호  5분봉 고가 ≥ 직전 60분(12봉) 최저가 × (1 + SPIKE_PCT)
  필터  진입가 기준 일봉 볼린저(20, 2σ) 상단 대비 ≥ BB_MIN  또는  5일선 이격 ≥ DEV_MIN (둘 다 진행 중 가격 포함)
  진입  다음 5분봉 시가, 숏 1배.  같은 종목은 신호 봉부터 BLOCK_HOURS 동안 다시 안 들어간다
  청산  익절 −TP_PCT / 손절 +SL_PCT / HOLD_HOURS 뒤 종가. 한 봉에 둘 다면 손절(보수적)
**RULE 숫자는 판정 기간(2026-11-15부터 4주) 동안 바꾸지 않는다.** 출처는 전부 2026-11-14 백테스트
(USDT 무기한 918종목 2020~2026, 5분봉) — 2020~24로 고르고 2025·2026으로 확인한 값이며 실전 검증 전이다.

왜 봇(Railway)이 아니라 Actions인가: Railway 미국 리전은 선물 API(fapi)가 451이고, 대상 종목 다수가 선물 전용이라
현물 API로는 못 본다. data.binance.vision 일별 파일은 하루 늦지만 가상 기록에는 충분하다.
펀딩비: 백테스트에 없던 항목이라 따로 적는다(fund_pct, 숏 기준 +면 받음). 일별 파일이 없으면 None으로 두고
월별 파일이 나오면 채운다 — None인 동안은 "미확정"이다.
"""
import csv, io, json, os, sys, zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from alt_history import BASE, PFX, THREADS, get, rows_of, s3_list  # noqa: E402

RULE = {
    "version": "v2", "registered": "2026-11-14", "start": "2026-11-15",
    "spike_pct": 40.0, "lookback_bars": 12, "bb_min": 10.0, "dev_min": 30.0,
    "tp_pct": 20.0, "sl_pct": 100.0, "hold_hours": 48, "block_hours": 48, "fee_pct": 0.10,
}
M5, H, D = 300_000, 3_600_000, 86_400_000


def day_ms(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def indicators(prev20, px):
    """(볼린저 상단 대비 %, 5일선 이격 %). prev20 = 직전 20일 종가(오래된 것부터). 채널 표기와 같은 정의."""
    w = list(prev20[-19:]) + [px]
    m = sum(w) / 20
    sd = (sum((x - m) ** 2 for x in w) / 20) ** 0.5
    ma5 = (sum(prev20[-4:]) + px) / 5
    return (px / (m + 2 * sd) - 1) * 100, (px / ma5 - 1) * 100


def find_spikes(bars, from_ms, to_ms, blocked_until=0):
    """bars = [(t,o,h,l,c)] 시간순. 신호 봉 t가 [from_ms, to_ms)인 것만. 반환 [(신호 봉 t, 진입 봉 t, 진입가)].
    같은 종목 차단(block_hours)을 여기서 적용한다."""
    n, lb, out = len(bars), RULE["lookback_bars"], []
    for j in range(lb, n - 1):
        t = bars[j][0]
        if t < from_ms or t >= to_ms or t <= blocked_until:
            continue
        if t - bars[j - lb][0] != lb * M5 or bars[j + 1][0] - t != M5:      # 봉이 비면 판정하지 않는다
            continue
        lo = min(b[3] for b in bars[j - lb:j])
        if lo > 0 and bars[j][2] / lo - 1 >= RULE["spike_pct"] / 100:
            out.append((t, bars[j + 1][0], bars[j + 1][1]))
            blocked_until = t + RULE["block_hours"] * H
    return out


def new_trade(sym, sig_t, entry_t, entry, bb, dev):
    return {"sym": sym, "signal_ms": sig_t, "entry_ms": entry_t, "entry": entry, "bb": round(bb, 1), "dev": round(dev, 1),
            "status": "open", "checked_ms": entry_t - 1, "last_close": None,
            "result": None, "exit_ms": None, "exit_px": None, "pnl_pct": None, "max_up_pct": 0.0, "fund_pct": None}


def _close(t, kind, px, ms):
    t.update(status="done", result=kind, exit_px=px, exit_ms=ms,
             pnl_pct=round((1 - px / t["entry"]) * 100 - RULE["fee_pct"], 4))


def update_trade(t, bars):
    """진입 봉부터의 5분봉으로 갱신(제자리). 끝났으면 True."""
    if t["status"] != "open":
        return True
    e, end = t["entry"], t["entry_ms"] + RULE["hold_hours"] * H
    tp, sl = e * (1 - RULE["tp_pct"] / 100), e * (1 + RULE["sl_pct"] / 100)
    for b in bars:
        if b[0] <= t["checked_ms"] or b[0] < t["entry_ms"]:
            continue
        if b[0] > end:                                    # 창을 지난 첫 봉 — 직전 종가로 정리
            _close(t, "time", t["last_close"] if t["last_close"] is not None else b[1], end)
            return True
        t["checked_ms"], t["last_close"] = b[0], b[4]
        t["max_up_pct"] = round(max(t["max_up_pct"], (b[2] / e - 1) * 100), 2)
        if b[2] >= sl:
            _close(t, "stop", sl, b[0] + M5)
            return True
        if b[3] <= tp:
            _close(t, "tp", tp, b[0] + M5)
            return True
    return False


def funding_pct(rates, entry_ms, exit_ms):
    """rates = [(ms, rate)]. 숏 기준 %: 양수 펀딩이면 받는다. 구간 (entry, exit]."""
    return round(sum(r for ms, r in rates if entry_ms < ms <= exit_ms) * 100, 4)


def stats(trades):
    done = [t for t in trades if t["status"] == "done"]
    s = {"n": len(done), "open": sum(1 for t in trades if t["status"] == "open"),
         **{k: sum(1 for t in done if t["result"] == k) for k in ("tp", "stop", "time")}}
    if done:
        s["sum_pct"] = round(sum(t["pnl_pct"] for t in done), 2)
        s["mean_pct"] = round(s["sum_pct"] / len(done), 2)
        known = [t for t in done if t["fund_pct"] is not None]
        s["fund_known"] = len(known)
        s["mean_with_fund_pct"] = round(sum(t["pnl_pct"] + t["fund_pct"] for t in known) / len(known), 2) if known else None
    return s


# ── 원천 ──────────────────────────────────────────────────────────────
def fetch_5m(sym, d):
    b = get(f"{BASE}/{PFX}/daily/klines/{sym}/5m/{sym}-5m-{d.isoformat()}.zip")
    return [(r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows_of(b)] if b else []


def fetch_prev_closes(sym, d, n=20):
    """d 직전 n일 종가(연속일 때만). 모자라면 None — 상장 20일 미만은 백테스트에서도 제외였다."""
    out = []
    for k in range(n, 0, -1):
        b = get(f"{BASE}/{PFX}/daily/klines/{sym}/1d/{sym}-1d-{(d - timedelta(days=k)).isoformat()}.zip")
        rows = list(rows_of(b)) if b else []
        if len(rows) != 1:
            return None
        out.append(float(rows[0][4]))
    return out


def fetch_funding(sym, d0, d1):
    """[(ms, rate)] 또는 None(아직 없음). 일별 파일 → 월별 파일 순."""
    def parse(blob):
        z = zipfile.ZipFile(io.BytesIO(blob))
        with z.open(z.namelist()[0]) as f:
            return [(int(r[0]), float(r[2])) for r in csv.reader(io.TextIOWrapper(f, "utf-8")) if r and r[0].isdigit()]
    out, d = [], d0
    while d <= d1:
        b = get(f"{BASE}/{PFX}/daily/fundingRate/{sym}/{sym}-fundingRate-{d.isoformat()}.zip")
        if b is None:
            b = get(f"{BASE}/{PFX}/monthly/fundingRate/{sym}/{sym}-fundingRate-{d.isoformat()[:7]}.zip")
            if b is None:
                return None
        try:
            out += parse(b)
        except (zipfile.BadZipFile, IndexError, ValueError):
            return None
        d += timedelta(days=1)
    return sorted(set(out))


def run(ledger_path, today=None, start=None, until=None):
    """start/until은 점검용(과거 구간을 별도 원장에 돌려 백테스트와 대조). 실제 기록은 RULE["start"]부터."""
    today = today or datetime.now(timezone.utc).date()
    try:
        led = json.load(open(ledger_path, encoding="utf-8"))
    except (OSError, ValueError):
        led = {"last_day": None, "trades": [], "blocked": {}, "log": []}
    led["rule"] = RULE
    start = start or date.fromisoformat(RULE["start"])
    today = min(today, until) if until else today
    d = date.fromisoformat(led["last_day"]) + timedelta(days=1) if led["last_day"] else start
    prefixes, _ = s3_list(f"{PFX}/daily/klines/")
    syms = sorted(p.rstrip("/").split("/")[-1] for p in prefixes)
    syms = [s for s in syms if s.endswith("USDT") and "_" not in s]
    if len(syms) < 200:
        raise SystemExit(f"종목 목록이 너무 적다({len(syms)}) — 원천 문제. 기록하지 않는다.")
    while d < today:
        if not fetch_5m("BTCUSDT", d):                    # 그날 파일이 아직 안 올라왔다 — 다음 실행에서
            print(f"{d} 파일 미발행 — 중단", flush=True)
            break
        with ThreadPoolExecutor(THREADS) as ex:
            cur = dict(zip(syms, ex.map(lambda s: fetch_5m(s, d), syms)))
            need_prev = [s for s in syms if cur[s]]
            prev = dict(zip(need_prev, ex.map(lambda s: fetch_5m(s, d - timedelta(days=1)), need_prev)))
        got = sum(1 for s in syms if cur[s])
        if got < 200:
            raise SystemExit(f"{d} 5분봉 파일이 너무 적다({got}) — 기록하지 않는다.")
        new = 0
        for s in need_prev:
            bars = sorted(set(prev[s][-RULE["lookback_bars"] - 1:] + cur[s]))
            for t in led["trades"]:
                if t["sym"] == s and t["status"] == "open":
                    update_trade(t, bars)
            # 전날 마지막 봉(23:55)의 신호는 진입 봉이 오늘이라 오늘 처리한다(전날 실행에서는 다음 봉이 없어 건너뜀)
            spikes = find_spikes(bars, day_ms(d) - M5, day_ms(d) + D - M5, led["blocked"].get(s, 0))
            for sig_t, entry_t, entry in spikes:
                led["blocked"][s] = sig_t + RULE["block_hours"] * H     # 필터에서 걸러져도 차단은 건다(백테스트와 같음)
                closes = fetch_prev_closes(s, datetime.fromtimestamp(entry_t / 1000, timezone.utc).date())
                if closes is None:
                    led["log"].append({"sym": s, "signal_ms": sig_t, "skip": "no_20d"})
                    continue
                bb, dev = indicators(closes, entry)
                if bb < RULE["bb_min"] and dev < RULE["dev_min"]:
                    led["log"].append({"sym": s, "signal_ms": sig_t, "skip": "filter", "bb": round(bb, 1), "dev": round(dev, 1)})
                    continue
                t = new_trade(s, sig_t, entry_t, entry, bb, dev)
                update_trade(t, bars)
                led["trades"].append(t); new += 1
        led["last_day"] = d.isoformat()
        print(f"{d}: 종목 {got} · 새 가상 진입 {new} · 진행 {sum(1 for t in led['trades'] if t['status']=='open')}", flush=True)
        d += timedelta(days=1)
    for t in led["trades"]:                               # 펀딩비: 끝난 거래 중 아직 못 채운 것
        if t["status"] == "done" and t["fund_pct"] is None:
            f = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).date()
            rates = fetch_funding(t["sym"], f(t["entry_ms"]), f(t["exit_ms"]))
            if rates is not None:
                t["fund_pct"] = funding_pct(rates, t["entry_ms"], t["exit_ms"])
    led["stats"] = stats(led["trades"])
    led["updated_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
    os.makedirs(os.path.dirname(ledger_path) or ".", exist_ok=True)
    with open(ledger_path + ".tmp", "w", encoding="utf-8") as f:
        json.dump(led, f, ensure_ascii=False, indent=1)
    os.replace(ledger_path + ".tmp", ledger_path)
    print("요약", led["stats"], "| 처리한 마지막 날", led["last_day"], flush=True)


if __name__ == "__main__":
    st, un = os.environ.get("SPIKE_CHECK_START"), os.environ.get("SPIKE_CHECK_UNTIL")
    run(sys.argv[1] if len(sys.argv) > 1 else "paper/ledger.json",
        start=date.fromisoformat(st) if st else None, until=date.fromisoformat(un) if un else None)
