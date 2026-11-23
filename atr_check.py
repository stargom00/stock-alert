"""v2.96 ATR 조회 — 일지에 손절 쓸 때 "하루에 보통 얼마나 흔들리나"를 바로 본다.
사용자 2026-11-23 "99선을 손절로하면안돼겠다. ATR 를 어떻게 보지?" → "일지쓸때, 티커넣으면 ATR 자동으로 넣어줄수있어?"
(매매 훈련 홈 일지 화면은 시세를 직접 못 받는다 — claude.ai 페이지라 외부 주소에 접속이 안 됨. 그래서 봇이 계산하고, 일지는
"ATR 바이넥스 6861 6600" 글을 만들어 주고 봇 답의 숫자를 붙여 넣는다.)

명령: `ATR 티커|이름 [매수가] [손절가]`.
ATR = 고전 Wilder 14(첫 14개 TR 단순평균으로 시작, 이후 (이전×13 + 이번)/14 — 트레이딩뷰 ta.atr과 같은 정의). 키움 ATR 설정이
단순평균이면 값이 조금 다를 수 있다. **확정 봉만**(KR 20:10 KST 전·US 16:10 뉴욕 전 오늘 봉 제외) — 장중 값은 잠정치라서.
제안 손절(매수가 − 1.5·2 ATR)과 "손절폭 ÷ ATR"의 문턱 1·1.5·2는 AI(Claude) 어림값 — 스캐너 💎적격의 손절폭 적정(ATR×1.5)을
빌렸을 뿐 측정 없음. 손절이 맞다·틀리다가 아니라 하루 흔들림에 걸릴 거리인지 보는 점검용.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))
UA = {"User-Agent": "Mozilla/5.0"}
PERIOD = 14
KR_CONFIRM_HM = (20, 10)
US_CONFIRM_HM = (16, 10)


def market_of(ticker):
    t = str(ticker).upper().split(".")[0]
    return "kr" if re.match(r"^\d[0-9A-Z]{5}$", t) else "us"


def parse_sise_ohlc(text):
    """naver siseJson → [(date, high, low, close)] 오름차순. 고·저·종 중 0이 있는 행(무거래일 오염)은 버린다."""
    cleaned = re.sub(r",\s*]", "]", text.strip().replace("'", '"'))
    out = []
    for r in json.loads(cleaned)[1:]:
        if len(r) >= 5 and all(float(x) > 0 for x in r[2:5]):
            s = str(r[0])
            out.append((date(int(s[:4]), int(s[4:6]), int(s[6:8])), float(r[2]), float(r[3]), float(r[4])))
    return sorted(out)


def parse_yahoo_ohlc(j):
    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")
    res = j["chart"]["result"][0]
    q = res["indicators"]["quote"][0]
    out = []
    for t, h, l, c in zip(res.get("timestamp") or [], q.get("high") or [], q.get("low") or [], q.get("close") or []):
        if None not in (h, l, c) and min(h, l, c) > 0:
            out.append((datetime.fromtimestamp(t, tz=timezone.utc).astimezone(et).date(), float(h), float(l), float(c)))
    return out


def fetch_ohlc(ticker):
    try:
        if market_of(ticker) == "kr":
            end = datetime.now(KST).date()
            r = requests.get("https://api.finance.naver.com/siseJson.naver",
                             params={"symbol": ticker[:6], "requestType": 1, "timeframe": "day",
                                     "startTime": (end - timedelta(days=200)).strftime("%Y%m%d"), "endTime": end.strftime("%Y%m%d")},
                             headers={**UA, "Referer": "https://finance.naver.com/"}, timeout=10)
            return parse_sise_ohlc(r.text)
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=6mo", headers=UA, timeout=10)
        return parse_yahoo_ohlc(r.json())
    except Exception as e:
        print(f"[ATR] {ticker} 일봉 조회 실패: {e}")
        return None


def confirmed(rows, market, now):
    """오늘 봉이 아직 확정 전이면 뺀다."""
    if not rows:
        return rows
    if market == "kr":
        local, hm = now.astimezone(KST), KR_CONFIRM_HM
    else:
        from zoneinfo import ZoneInfo
        local, hm = now.astimezone(ZoneInfo("America/New_York")), US_CONFIRM_HM
    if rows[-1][0] >= local.date() and (local.hour, local.minute) < hm:
        return rows[:-1]
    return rows


def atr_wilder(rows, period=PERIOD):
    """[(date, high, low, close)] → 마지막 봉의 ATR(Wilder). 봉이 period+1개보다 적으면 None."""
    if len(rows) < period + 1:
        return None
    tr = [max(h - l, abs(h - rows[i - 1][3]), abs(l - rows[i - 1][3])) for i, (_, h, l, _) in enumerate(rows) if i > 0]
    a = sum(tr[:period]) / period
    for x in tr[period:]:
        a = (a * (period - 1) + x) / period
    return a


def _n(x):
    try:
        return float(str(x).replace(",", ""))
    except ValueError:
        return None


def _p(x, kr):
    return f"{x:,.0f}" if kr else f"{x:,.2f}"


def report(label, ticker, rows, buy=None, stop=None):
    kr = market_of(ticker) == "kr"
    a = atr_wilder(rows)
    if a is None:
        return f"❌ {label}: 일봉이 {PERIOD + 1}개보다 적어 ATR을 못 구해요."
    d, close = rows[-1][0], rows[-1][3]
    lines = [f"📏 <b>{label}</b> ATR(14) <b>{_p(a, kr)}</b> = 종가의 {a / close * 100:.1f}%",
             f"({d} 확정 종가 {_p(close, kr)} 기준 · 하루에 보통 이만큼 흔들림)"]
    if buy:
        lines.append(f"\n매수가 {_p(buy, kr)} 기준")
        if stop:
            gap = buy - stop
            k = gap / a
            tag = "⚠ 1 ATR 안 — 하루 흔들림에 걸려요" if k < 1 else ("· 1~1.5 ATR — 빠듯해요" if k < 1.5 else ("✓ 1.5~2 ATR" if k <= 2 else "· 2 ATR 넘음 — 넓은 편, 수량 줄이기"))
            lines.append(f"손절 {_p(stop, kr)} = −{gap / buy * 100:.1f}% = <b>{k:.1f} ATR</b> {tag}")
        lines += [f"1.5 ATR 손절 {_p(buy - 1.5 * a, kr)} (−{1.5 * a / buy * 100:.1f}%)",
                  f"2 ATR 손절 {_p(buy - 2 * a, kr)} (−{2 * a / buy * 100:.1f}%)"]
    lines.append("\n일지 ATR 칸에 붙여 넣기: " + (f"{a:.0f}" if kr else f"{a:.2f}"))
    lines.append("<i>Wilder 14 · 문턱은 어림값(측정 없음)</i>")
    return "\n".join(lines)


def parse(text):
    """'ATR 바이넥스 6861 6600' → (이름·티커, 매수가, 손절가). 형식이 아니면 None."""
    parts = text.split()
    if len(parts) < 2 or parts[0].upper() != "ATR":
        return None
    nums = [_n(x) for x in parts[2:]]
    if any(x is None for x in nums) or len(nums) > 2:
        return None
    return parts[1], (nums[0] if nums else None), (nums[1] if len(nums) > 1 else None)


def on_demand(text, resolve, name_search=None, fetch=fetch_ohlc, now=None):
    p = parse(text)
    if not p:
        return "형식: <code>ATR 티커|이름 [매수가] [손절가]</code>\n예: <code>ATR 바이넥스 6861 6600</code>"
    raw, buy, stop = p
    t = resolve(raw).upper()
    if not re.match(r"^(\d[0-9A-Z]{5}(\.(KS|KQ))?|[A-Z.\-]{1,10})$", t) and name_search and re.search(r"[가-힣]", raw):
        t = (name_search(raw) or t).upper()
    rows = fetch(t)
    if not rows:
        return f"❌ <b>{raw}</b> 일봉을 못 받았어요. 이름이나 코드를 확인해 주세요."
    rows = confirmed(rows, market_of(t), now or datetime.now(timezone.utc))
    label = raw if raw.upper() == t else f"{raw} {t.split('.')[0]}"
    return report(label, t, rows, buy, stop)
