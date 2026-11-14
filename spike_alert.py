"""급등 숏 설계 v2 실시간 알림 (v2.78) — 주문 없음. 조건이 온 자리를 텔레그램으로 알려 준다.

사용자 지시 2026-11-14 "나한테 알림도 보내줘". 규칙(숫자·지표 계산)은 tools/spike_paper.py의 RULE·indicators를
그대로 가져다 쓴다(사본 금지). 성적 기록은 그쪽(바이낸스 봉, 하루 늦음)이 기준이고 이 모듈은 알림만 한다.

원천이 Bitget인 이유: 2026-11-14 실제로 찔러 본 결과(net_probe) Railway에서 binance 선물은 451, bybit 403,
bitget·okx·mexc·gate·kucoin은 200. Bitget USDT 선물은 채널 알림에 나온 바이낸스 종목 487개 중 390개(80%)를 덮는다 —
**나머지 20%는 알림이 안 온다**(가상 기록에는 잡힌다). 가격도 바이낸스와 조금 다를 수 있다.

흐름(1분마다): 전 종목 시세 1회 → 현재가가 24시간 저가보다 +40% 이상인 종목만(필요조건) 5분봉 14개 조회 →
진행 중 봉의 고가 ≥ 직전 12봉 최저가 × 1.40 이면 신호 → 일봉 20개로 필터(볼린저 +10% 또는 5일선 +30%) → 발송.
같은 종목은 신호부터 48시간 다시 안 본다(필터에서 걸러져도 — 백테스트와 같음). 그 기록은 파일에 남긴다(재시작 대비).
"""
import json
import os
import sys
import time

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools"))
from spike_paper import RULE, indicators  # noqa: E402

BASE = "https://api.bitget.com/api/v2/mix/market"
PRODUCT = "USDT-FUTURES"
SYMBOLS_URL = "https://raw.githubusercontent.com/stargom00/stock-alert/spike-paper/paper/symbols.json"
SYMBOLS_TTL_MS = 12 * 3_600_000
M5, H = 300_000, 3_600_000
MINUS = "−"
_sym_cache = {"at": 0, "set": None}
_beat = {"at": 0, "runs": 0, "cands": 0, "fails": 0}


def state_path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "spike_alert.json")


def _get(url, params=None):
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if isinstance(j, dict) and "code" in j and j["code"] != "00000":
        raise RuntimeError(f"bitget {j.get('code')} {j.get('msg')}")
    return j


def fetch_tickers(get=_get):
    return get(f"{BASE}/tickers", {"productType": PRODUCT})["data"]


def fetch_candles(sym, gran, limit, get=_get):
    """[(t, o, h, l, c)] 시간순. Bitget 행 = [ts, o, h, l, c, baseVol, quoteVol] 문자열."""
    rows = get(f"{BASE}/candles", {"symbol": sym, "productType": PRODUCT, "granularity": gran, "limit": limit})["data"]
    return sorted((int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows)


def binance_symbols(now_ms, get=_get):
    """바이낸스 USDT 무기한 목록(가상 기록 워크플로가 매일 써 둔다). 못 읽으면 None — 그때는 알림에 '미확인'을 붙인다."""
    if _sym_cache["set"] is not None and now_ms - _sym_cache["at"] < SYMBOLS_TTL_MS:
        return _sym_cache["set"]
    try:
        syms = set(get(SYMBOLS_URL)["symbols"])
        if len(syms) < 200:
            raise ValueError(f"목록이 너무 적다({len(syms)})")
        _sym_cache.update(at=now_ms, set=syms)
    except Exception as e:                      # noqa: BLE001
        print(f"[급등숏] 바이낸스 종목 목록 조회 실패: {type(e).__name__}: {e}")
    return _sym_cache["set"]


def candidates(tickers):
    """필요조건: 현재가 ≥ 24시간 저가 × (1+spike). (직전 60분 최저가는 24시간 저가보다 낮을 수 없다.)"""
    k = 1 + RULE["spike_pct"] / 100
    out = []
    for t in tickers:
        try:
            last, low = float(t["lastPr"]), float(t["low24h"])
        except (KeyError, TypeError, ValueError):
            continue
        if low > 0 and last >= low * k * 0.98:      # 시세·봉 시각 차이 여유 2%
            out.append(t)
    return out


def spike_now(bars, now_ms):
    """진행 중 봉이나 방금 마감된 봉이 신호인지. 반환 (신호 봉 t, 1시간 상승 %) 또는 None. 봉이 비면 판정 안 함."""
    lb = RULE["lookback_bars"]
    for end in (len(bars), len(bars) - 1):
        if end < lb + 1:
            continue
        cur, prev = bars[end - 1], bars[end - 1 - lb:end - 1]
        if now_ms - cur[0] >= 2 * M5 or cur[0] - prev[0][0] != lb * M5:
            continue
        lo = min(b[3] for b in prev)
        if lo > 0 and cur[2] / lo - 1 >= RULE["spike_pct"] / 100:
            return cur[0], (cur[2] / lo - 1) * 100
    return None


def prev20_closes(day_bars, now_ms):
    """마감된 UTC 일봉 20개 종가(연속일 때만). 모자라면 None."""
    D = 86_400_000
    closed = [b for b in day_bars if b[0] + D <= now_ms][-20:]
    if len(closed) < 20 or closed[-1][0] - closed[0][0] != 19 * D:
        return None
    return [b[4] for b in closed]


def fmt_px(v):
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:.4f}"
    return f"{v:.6f}".rstrip("0") if v >= 0.001 else f"{v:.8f}".rstrip("0")


def _pct(v, nd=0):
    return ("+" if v >= 0 else MINUS) + f"{abs(v):.{nd}f}%"


def format_message(sym, px, up_pct, bb, dev, funding, on_binance):
    tp, sl = px * (1 - RULE["tp_pct"] / 100), px * (1 + RULE["sl_pct"] / 100)
    tail = "" if on_binance else " · ⚠️ 바이낸스 상장 미확인"
    fund = f"펀딩 {_pct(funding * 100, 3)} · " if funding is not None else ""
    return (f"🚨 <b>급등 숏 자리 {sym}</b> {fmt_px(px)}\n"
            f"1시간 {_pct(up_pct)} · BB {_pct(bb)} · 5MA {_pct(dev)}\n"
            f"1배 숏 · 익절 {fmt_px(tp)} · 손절 {fmt_px(sl)} · 48시간\n"
            f"{fund}Bitget 가격 · 검증 전(가상 기록 중){tail}")


def load_state(path):
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except (OSError, ValueError):
        return {}


def save_state(st, path):
    try:
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            json.dump(st, f)
        os.replace(path + ".tmp", path)
    except OSError as e:
        print(f"[급등숏] 저장 실패: {e}")


def check(send, path=None, now_ms=None, tickers_f=fetch_tickers, candles_f=fetch_candles, symbols_f=binance_symbols):
    """스케줄러용(1분마다). 보낸 메시지 목록을 돌려준다."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    path = path or state_path()
    _beat["runs"] += 1
    try:
        tickers = tickers_f()
    except Exception as e:                      # noqa: BLE001
        _beat["fails"] += 1
        print(f"[급등숏] 시세 조회 실패: {type(e).__name__}: {e}")
        return []
    if len(tickers) < 100:
        _beat["fails"] += 1
        print(f"[급등숏] 시세 종목이 너무 적다({len(tickers)}) — 판정하지 않음")
        return []
    st = load_state(path)
    block = RULE["block_hours"] * H
    st = {s: v for s, v in st.items() if now_ms - v["signal_ms"] < block}      # 지난 차단은 버린다
    bn, sent, changed = None, [], False
    cands = [t for t in candidates(tickers) if t["symbol"] not in st]
    _beat["cands"] += len(cands)
    for t in cands:
        sym = t["symbol"]
        try:
            hit = spike_now(candles_f(sym, "5m", RULE["lookback_bars"] + 2), now_ms)
            if not hit:
                continue
            bn = bn if bn is not None else (symbols_f(now_ms) or set())
            listed = (sym in bn) if bn else None
            if listed is False:                                             # 바이낸스에 없는 종목 — 대상 아님
                st[sym] = {"signal_ms": hit[0], "why": "not_binance"}; changed = True
                continue
            px = float(t["lastPr"])
            closes = prev20_closes(candles_f(sym, "1Dutc", 25), now_ms)
            st[sym] = {"signal_ms": hit[0], "px": px, "up": round(hit[1], 1)}; changed = True
            if closes is None:
                st[sym]["why"] = "no_20d"
                continue
            bb, dev = indicators(closes, px)
            st[sym].update(bb=round(bb, 1), dev=round(dev, 1))
            if bb < RULE["bb_min"] and dev < RULE["dev_min"]:
                st[sym]["why"] = "filter"
                print(f"[급등숏] {sym} 1시간 +{hit[1]:.0f}% — 필터 탈락(BB {bb:.0f} / 5MA {dev:.0f})")
                continue
            try:
                funding = float(t.get("fundingRate"))
            except (TypeError, ValueError):
                funding = None
            msg = format_message(sym, px, hit[1], bb, dev, funding, bool(listed))
            send(msg); sent.append(msg)
            st[sym]["why"] = "sent"
            print(f"[급등숏] {sym} 발송 — 1시간 +{hit[1]:.0f}% BB {bb:.0f} 5MA {dev:.0f}")
        except Exception as e:                  # noqa: BLE001 — 한 종목 실패가 나머지를 막지 않게
            _beat["fails"] += 1
            if sym in st and "why" not in st[sym]:      # 판정을 못 끝냈으면 차단하지 않는다 — 다음 분에 다시
                del st[sym]
            print(f"[급등숏] {sym} 처리 실패: {type(e).__name__}: {e}")
    if changed or len(st) != len(load_state(path)):
        save_state(st, path)
    if now_ms - _beat["at"] >= H:               # 침묵 = 고장과 구분: 시간당 한 줄
        print(f"[급등숏] 최근 1시간: 실행 {_beat['runs']} · 후보 {_beat['cands']} · 실패 {_beat['fails']} · 차단 중 {len(st)}")
        _beat.update(at=now_ms, runs=0, cands=0, fails=0)
    return sent
