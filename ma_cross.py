"""120일선·240일선 상향 돌파 알림 — v2.58 (관심 종목·US) + v2.59 (KR 전체, 스캐너 /api/ma-cross).

사용자 지시(2026-11-12): "알림봇으로 120평, 240평을 아래에서 위로 뚫을때 알림오게할수있어?"

이평 = 일봉 종가 단순이동평균(120·240봉). 데이터는 봇이 직접 받는다 — KR naver siseJson(통합 시세, 스캐너 KR 기준과 같음),
US yahoo chart 일봉(분할 조정 종가). 스캐너 /api/ma는 10/20/50만 주고 캐시가 장중 봉일 수 있어 쓰지 않는다.

알림 두 가지:
  · 장중 돌파 시도(잠정): 어제 종가가 이평 아래였는데 지금가가 "오늘 봉을 지금가로 넣은 이평" 이상 → 종목·이평마다 하루 1회
  · 종가 돌파 확정: 어제 종가 < 어제 이평 AND 오늘 종가 ≥ 오늘 이평 (KR 20:20 KST · US 16:30 ET)
    장중 시도가 있었는데 종가가 다시 아래면 "종가 복귀(실패)"도 같이 보낸다. 아무 일 없으면 안 보낸다(로그만).
대상: DATA_DIR/ma_cross_list.json(텔레그램 `이평추가`/`이평삭제`로 바뀜), 없으면 저장소 ma_cross.json 기본 목록.
"""
import json
import os
import re
from datetime import date, datetime, timedelta, timezone

import pytz
import requests

from trendlines import market_of, market_open, session_date

KST = timezone(timedelta(hours=9))
ET = pytz.timezone("America/New_York")
WINDOWS = (120, 240)
DEFAULT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ma_cross.json")
UA = {"User-Agent": "Mozilla/5.0"}


# ── 순수 함수 ─────────────────────────────────────────────────────────
def sma(closes, w, end=None):
    """closes[:end]의 마지막 w개 평균. 모자라면 None."""
    seg = closes[:end] if end is not None else closes
    if len(seg) < w:
        return None
    return sum(seg[-w:]) / w


def close_cross_up(closes, w):
    """마지막 봉이 종가로 w일선을 아래→위로 넘었나. (넘었나, 오늘 이평, 어제 이평) — 봉이 w+1개 미만이면 (False, None, None)."""
    if len(closes) < w + 1:
        return False, None, None
    ma_t, ma_y = sma(closes, w), sma(closes, w, end=-1)
    return (closes[-2] < ma_y and closes[-1] >= ma_t), ma_t, ma_y


def intraday_cross_up(confirmed, price, w):
    """confirmed = 어제까지 확정 종가. 어제 종가가 이평 아래 + 지금가 ≥ (최근 w−1개 + 지금가)/w → (시도, 추정 이평)."""
    if len(confirmed) < w or not price:
        return False, None
    ma_y = sma(confirmed, w)
    est = (sum(confirmed[-(w - 1):]) + price) / w
    return (confirmed[-1] < ma_y and price >= est), est


def _fmt(p, market):
    return f"₩{p:,.0f}" if market == "kr" else f"${p:,.2f}"


def format_intraday(ticker, market, price, w, est):
    return (f"📈 <b>{w}일선 돌파 시도</b> (장중 · 잠정)\n"
            f"· <b>{ticker}</b> 지금 {_fmt(price, market)} ≥ {w}일선 {_fmt(est, market)} ({(price / est - 1) * 100:+.1f}%)\n"
            f"· 어제 종가는 {w}일선 아래였음\n"
            f"· 장중 값은 잠정 — 돌파는 종가로 판정, 그다음 며칠 거래량이 성패")


def format_close(rows, market, on):
    """rows: [(ticker, w, kind, close, ma)] kind = 'up' | 'fail'."""
    mk = "국내" if market == "kr" else "미국"
    out = [f"📈 <b>120·240일선 종가 점검 — {mk}</b> ({on})"]
    for t, w, kind, close, ma in rows:
        d = (close / ma - 1) * 100
        if kind == "up":
            out.append(f"✅ <b>{t}</b> 종가 {_fmt(close, market)} — {w}일선 {_fmt(ma, market)} <b>상향 돌파 확정</b> ({d:+.1f}%)")
        else:
            out.append(f"↩️ <b>{t}</b> 종가 {_fmt(close, market)} — 장중 {w}일선 돌파 시도 후 <b>종가 복귀</b> ({d:+.1f}%)")
    out.append("· 확정 = 어제 종가 < 어제 이평, 오늘 종가 ≥ 오늘 이평 · 성패는 다음 며칠 거래량으로")
    return "\n".join(out)


# ── 데이터 ───────────────────────────────────────────────────────────
def parse_sise(text):
    """naver siseJson → [(date, close)] 오름차순. 종가 0 행은 버린다."""
    cleaned = re.sub(r",\s*]", "]", text.strip().replace("'", '"'))
    rows = json.loads(cleaned)[1:]
    out = []
    for r in rows:
        if len(r) >= 5 and float(r[4]) > 0:
            out.append((date(int(str(r[0])[:4]), int(str(r[0])[4:6]), int(str(r[0])[6:8])), float(r[4])))
    return sorted(out)


def parse_yahoo(j):
    """yahoo chart 일봉 JSON → [(뉴욕 날짜, close)]. close None(미체결) 행은 버린다."""
    res = j["chart"]["result"][0]
    ts, cl = res.get("timestamp") or [], res["indicators"]["quote"][0].get("close") or []
    out = []
    for t, c in zip(ts, cl):
        if c is not None and c > 0:
            out.append((datetime.fromtimestamp(t, tz=pytz.utc).astimezone(ET).date(), float(c)))
    return out


def fetch_daily(ticker):
    """[(date, close)] 오름차순. 실패 None."""
    try:
        if market_of(ticker) == "kr":
            end = datetime.now(KST).date()
            r = requests.get("https://api.finance.naver.com/siseJson.naver",
                             params={"symbol": ticker[:6], "requestType": 1, "timeframe": "day",
                                     "startTime": (end - timedelta(days=520)).strftime("%Y%m%d"),
                                     "endTime": end.strftime("%Y%m%d")},
                             headers={**UA, "Referer": "https://finance.naver.com/"}, timeout=10)
            return parse_sise(r.text)
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=2y",
                         headers=UA, timeout=10)
        return parse_yahoo(r.json())
    except Exception as e:
        print(f"[이평돌파] {ticker} 일봉 조회 실패: {e}")
        return None


# ── 저장 ─────────────────────────────────────────────────────────────
def _data_dir():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return d


def _read(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def _write(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def list_path():
    return os.path.join(_data_dir(), "ma_cross_list.json")


def state_path():
    return os.path.join(_data_dir(), "ma_cross_state.json")


def load_list():
    lst = _read(list_path(), None)
    return lst if isinstance(lst, list) else _read(DEFAULT_PATH, [])


def edit_list(ticker, add):
    lst = load_list()
    t = ticker.upper()
    if add and t not in lst:
        lst.append(t)
    if not add and t in lst:
        lst.remove(t)
    _write(list_path(), lst)
    return lst


_confirmed_cache = {}


# ── 작업 ─────────────────────────────────────────────────────────────
def check_intraday(get_price, send, now_kst=None, tickers=None, state=None, fetch=fetch_daily, persist=True):
    now = now_kst or datetime.now(KST)
    st = state if state is not None else _read(state_path(), {})
    sent = 0
    for t in (tickers if tickers is not None else load_list()):
        mk = market_of(t)
        if not market_open(mk, now):
            continue
        on = session_date(mk, now)
        todo = [w for w in WINDOWS if st.get(f"{t}:{w}:intra") != str(on)]
        if not todo:
            continue
        price = (get_price(t) or {}).get("price")
        key = (t, str(on))
        if price and key not in _confirmed_cache:       # 어제까지 확정 종가는 하루 동안 안 바뀐다 — 하루 1번만 받는다
            bars = fetch(t)
            if bars:
                _confirmed_cache[key] = [c for d, c in bars if d < on]
        confirmed = _confirmed_cache.get(key)
        if not price or not confirmed:
            continue
        for w in todo:
            hit, est = intraday_cross_up(confirmed, price, w)
            if hit:
                st[f"{t}:{w}:intra"] = str(on)
                send(format_intraday(t, mk, price, w, est))
                sent += 1
    if persist:
        _write(state_path(), st)
    return sent


def close_check(market, send, now_kst=None, tickers=None, state=None, fetch=fetch_daily, persist=True):
    now = now_kst or datetime.now(KST)
    on = session_date(market, now)
    tickers = [t for t in (tickers if tickers is not None else load_list()) if market_of(t) == market]
    if on.weekday() >= 5 or not tickers:
        return None
    st = state if state is not None else _read(state_path(), {})
    rows, skipped = [], []
    for t in tickers:
        bars = fetch(t)
        if not bars or bars[-1][0] != on:       # 휴장·데이터 지연 — 오늘 봉이 없으면 판정 안 함
            skipped.append(t)
            continue
        closes = [c for _, c in bars]
        for w in WINDOWS:
            up, ma_t, _ = close_cross_up(closes, w)
            if ma_t is None or st.get(f"{t}:{w}:close") == str(on):
                continue
            if up:
                rows.append((t, w, "up", closes[-1], ma_t))
            elif st.get(f"{t}:{w}:intra") == str(on) and closes[-1] < ma_t:
                rows.append((t, w, "fail", closes[-1], ma_t))
            else:
                continue
            st[f"{t}:{w}:close"] = str(on)
    print(f"[이평돌파] {market} {on} 대상 {len(tickers)}종목 중 알림 {len(rows)}건 · 오늘 봉 없음 {skipped}")
    if rows:
        send(format_close(rows, market, on))
    if persist:
        _write(state_path(), st)
    return rows


def status_text(get_price, now_kst=None, tickers=None, fetch=fetch_daily):
    """텔레그램 `이평` — 대상 종목의 지금가와 120·240일선 거리(이평은 어제까지 확정 종가)."""
    now = now_kst or datetime.now(KST)
    out = ["📈 <b>120·240일선 돌파 감시</b>"]
    for t in (tickers if tickers is not None else load_list()):
        mk = market_of(t)
        on = session_date(mk, now)
        price = (get_price(t) or {}).get("price")
        bars = fetch(t)
        if not price or not bars:
            out.append(f"· {t} — 조회 실패")
            continue
        confirmed = [c for d, c in bars if d < on]
        parts = []
        for w in WINDOWS:
            m = sma(confirmed, w)
            parts.append(f"{w}일 {_fmt(m, mk)} ({(price / m - 1) * 100:+.1f}%)" if m else f"{w}일 봉 부족")
        out.append(f"· <b>{t}</b> {_fmt(price, mk)} · " + " · ".join(parts))
    out.append("추가: <code>이평추가 티커</code> · 빼기: <code>이평삭제 티커</code>")
    return "\n".join(out)


# ── v2.59 KR 전체 (스캐너) ────────────────────────────────────────────
# 사용자 지시(2026-11-12) "전종목은 무리인가? 코스피 코스닥". 봇이 2,000종목 일봉을 직접 받는 대신 스캐너 KR 캐시로 판정한
# GET /api/ma-cross?mode=intraday|close(v5.381)를 하루 두 번 읽는다. 대상 = 스캐너 KR 유니버스(시총 1000억 이상, 스팩 제외).
KR_SLOTS = (("kr-intra", "intraday", 14, 0), ("kr-close", "close", 20, 20))
KR_WINDOW_MIN = 30
KR_SHOW = 15


def kr_due(now_kst, fired):
    """지금 돌 슬롯 → [(키, 모드)]. 평일·시각~+30분·아직 안 보낸 것. 순수 함수."""
    if now_kst.weekday() >= 5:
        return []
    cur = now_kst.hour * 60 + now_kst.minute
    out = []
    for slot, mode, h, m in KR_SLOTS:
        key = f"{slot}:{now_kst.date()}"
        if h * 60 + m <= cur < h * 60 + m + KR_WINDOW_MIN and key not in fired:
            out.append((key, mode))
    return out


def _tv(ticker):
    t = str(ticker or "").upper()
    return f'<a href="https://www.tradingview.com/chart/?symbol=KRX:{t[:6]}">{t[:6]}</a>'


def digest_text(r):
    """v2.63 매물 소화 일수(스캐너 v5.382 overhang): 바로 위 +10% 가격대에서 지난 1년 거래된 물량 ÷ 평소(20일 평균) 거래량.
    AI 어림값 · 이미 판 물량을 못 빼서 실제보다 크게 나옴(상한). 표시 전용."""
    d, dt = r.get("digest_days"), r.get("digest_days_today")
    if d is None:
        return ""
    if d < 0.5:
        return " · 위 매물 거의 없음"
    return f" · 위 매물 소화 ~{d:.0f}일" + (f"(오늘 속도 {dt:.1f}일)" if dt is not None else "")


def format_kr(p):
    """스캐너 응답 → 텔레그램 본문 또는 None(장중 0건은 침묵). 마감은 0건도 보낸다."""
    hits, n = p.get("hits") or {}, p.get("n_hits") or {}
    total = sum(int(v or 0) for v in n.values())
    if p.get("mode") == "intraday":
        if not total:
            return None
        head = f"📈 <b>국내 전체 120·240일선 돌파 시도 — 장중</b> (장 {p.get('elapsed_pct')}% 경과 · 잠정)"
    else:
        head = f"📈 <b>국내 전체 120·240일선 상향 돌파 — 마감</b> ({p.get('session_date')} 확정)"
    out = [head, f"대상 {p.get('n_checked')}종목 (시총 1000억 이상 · 스팩 제외)"]
    for w in ("240", "120"):
        rows = hits.get(w) or []
        out += ["", f"<b>{w}일선</b> {int(n.get(w) or 0)}종목 (거래대금 순)"]
        for r in rows[:KR_SHOW]:
            chg = r.get("change_pct")
            vx = r.get("vol_x")
            out.append(f"· <b>{r.get('name')}</b> {_tv(r.get('ticker'))} {_fmt(r['close'], 'kr')}"
                       + (f" ({chg:+.1f}%)" if chg is not None else "")
                       + f" · 선 {r['dist_pct']:+.1f}%"
                       + (f" · 거래량 {vx:.1f}배" if vx is not None else "")
                       + digest_text(r))
        if int(n.get(w) or 0) > KR_SHOW:
            out.append(f"…외 {int(n[w]) - KR_SHOW}종목")
    out += ["", "· 거래량 = 직전 20일 평균 대비(장중은 경과 시간 환산)",
            "· 매물 소화 = 바로 위 +10% 가격대에서 지난 1년 거래된 물량 ÷ 평소 하루 거래량(어림값 · 실제보다 크게 나옴)",
            "· 장중은 잠정 — 종가로 판정, 돌파 성패는 다음 며칠 거래량",
            "· 측정 전 관심 신호"]
    return "\n".join(out)


def check_kr(send, scanner_url, headers, now_kst=None, state=None, get=requests.get, persist=True):
    now = now_kst or datetime.now(KST)
    st = state if state is not None else _read(state_path(), {})
    for key, mode in kr_due(now, st):
        try:
            p = get(f"{scanner_url}/api/ma-cross", params={"mode": mode}, timeout=180, headers=headers).json()
        except Exception as e:
            print(f"[이평돌파 KR] {key} 조회 실패: {e} — 다음 폴링에 재시도")
            continue
        if not p.get("ok"):
            print(f"[이평돌파 KR] {key} 응답 오류: {p}")
            continue
        st[key] = True
        if p.get("reason") == "no_session_bar":
            print(f"[이평돌파 KR] {key} 세션 봉 없음(휴장·캐시 낡음) — 발송 안 함")
            continue
        if mode == "close":
            skey = f"kr-close-session:{p.get('session_date')}"
            if skey in st:
                print(f"[이평돌파 KR] 세션 {p.get('session_date')} 이미 보냄 — 건너뜀")
                continue
            st[skey] = True
        msg = format_kr(p)
        if msg:
            send(msg)
        print(f"[이평돌파 KR] {key} 돌파 {p.get('n_hits')} · 대상 {p.get('n_checked')}")
    if persist:
        _write(state_path(), st)


def register(sched, get_price, send, scanner_url=None, scanner_headers=None):
    def safe(fn, name):
        def run():
            try:
                fn()
            except Exception as e:
                print(f"[이평돌파] {name} 오류: {e}")
        return run
    sched.every(5).minutes.do(safe(lambda: check_intraday(get_price, send), "장중"))
    sched.every().day.at("20:20", "Asia/Seoul").do(safe(lambda: close_check("kr", send), "KR 종가"))
    sched.every().day.at("16:30", "America/New_York").do(safe(lambda: close_check("us", send), "US 종가"))
    if scanner_url:
        sched.every(2).minutes.do(safe(lambda: check_kr(send, scanner_url, scanner_headers or {}), "KR 전체"))
