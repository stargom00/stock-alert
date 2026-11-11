"""빗각(추세선) 추적 알림 — v2.56.

사용자 지시(2026-11-11): 빗각 600시간 공부 + 추적 종목을 정해 계속 추적, "엔비디아는 진입했는데, 문제는 또 파는자리를 모른다는거야.
그래서 추적이 필요함" → (선 근처·돌파·이탈을 봇이 대신 지켜보는 제안에) "응."

선 정의: trendlines.json (두 점 = [날짜, 가격]). 값 계산은 로그 차트 직선(두 점 사이를 달력 일수로 로그 보간·연장) — 수평선은 두 가격이 같다.
알림:
  · 접근: 장중 현재가가 선 ±NEAR_PCT 안 → 선마다 하루 1회
  · 종가 판정: 장 마감 뒤 하루 1번 요약(선 값·종가·거리) + 종가가 선 −BREAK_PCT 아래로 가면 "이탈", +BREAK_PCT 위로 가면 "돌파"
    (직전 상태와 달라졌을 때만 알림 줄에 표시). 3%는 사용자 공부 노트의 예시 규칙 — 측정 근거 없음, 바꿀 수 있다.
상태: DATA_DIR/trendline_state.json (선마다 마지막 판정 상태·알림 날짜).
"""
import json
import math
import os
from datetime import date, datetime, timedelta, timezone

import pytz

KST = timezone(timedelta(hours=9))
ET = pytz.timezone("America/New_York")
NEAR_PCT = 3.0
BREAK_PCT = 3.0
LINES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trendlines.json")
KIND_LABEL = {"rise": "상승 빗각", "fall": "하락 빗각", "flat": "수평선"}


# ── 순수 함수 ─────────────────────────────────────────────────────────
def _d(s):
    return s if isinstance(s, date) else date.fromisoformat(str(s)[:10])


def line_value(line, on):
    """로그 직선: p1 × (p2/p1)^((on − d1)/(d2 − d1)). 두 점 날짜가 같으면 오류."""
    (d1, p1), (d2, p2) = line["p1"], line["p2"]
    d1, d2, on = _d(d1), _d(d2), _d(on)
    span = (d2 - d1).days
    if span == 0:
        raise ValueError(f"{line.get('id')}: 두 점 날짜가 같다")
    return float(p1) * math.exp(math.log(float(p2) / float(p1)) * (on - d1).days / span)


def weekly_pct(line):
    (d1, p1), (d2, p2) = line["p1"], line["p2"]
    per_day = math.log(float(p2) / float(p1)) / (_d(d2) - _d(d1)).days
    return (math.exp(per_day * 7) - 1) * 100


def dist_pct(price, value):
    return (price / value - 1) * 100


def side_of(price, value):
    """'above'(선 +3% 위) / 'below'(선 −3% 아래) / 'on'(선 ±3% 안)."""
    d = dist_pct(price, value)
    if d > BREAK_PCT:
        return "above"
    if d < -BREAK_PCT:
        return "below"
    return "on"


def close_event(prev_side, new_side):
    """종가 기준 상태 변화 → '돌파' | '이탈' | None. 처음(prev None)은 알림 없음."""
    if prev_side is None or prev_side == new_side:
        return None
    if new_side == "below":
        return "이탈"
    if new_side == "above":
        return "돌파"
    return None


def market_of(ticker):
    t = str(ticker).upper()
    return "kr" if t.endswith((".KS", ".KQ")) or (len(t) == 6 and t[:6].isdigit()) else "us"


def market_open(market, now_kst):
    if market == "kr":
        if now_kst.weekday() >= 5:
            return False
        hm = now_kst.hour * 60 + now_kst.minute
        return 9 * 60 <= hm < 15 * 60 + 30
    et = now_kst.astimezone(ET)
    if et.weekday() >= 5:
        return False
    hm = et.hour * 60 + et.minute
    return 9 * 60 + 30 <= hm < 16 * 60


def session_date(market, now_kst):
    """그 시장의 오늘 날짜(선 값 계산 기준일)."""
    return (now_kst if market == "kr" else now_kst.astimezone(ET)).date()


def _fmt(p, market):
    return f"₩{p:,.0f}" if market == "kr" else f"${p:,.2f}"


def fmt_line_row(line, price, on, market):
    v = line_value(line, on)
    d = dist_pct(price, v)
    return (f"· <b>{line.get('name') or line['ticker']}</b> {KIND_LABEL.get(line.get('kind'), '선')} "
            f"{_fmt(v, market)} · 가격 {_fmt(price, market)} ({d:+.1f}%)")


def format_near(line, price, on, market):
    v = line_value(line, on)
    lines = [f"📐 <b>빗각 접근</b> (선 ±{NEAR_PCT:g}% 안 · 장중)",
             fmt_line_row(line, price, on, market),
             f"두 점: {line['p1'][0]} {line['p1'][1]} → {line['p2'][0]} {line['p2'][1]} · 주 {weekly_pct(line):+.2f}%"]
    if line.get("note"):
        lines.append(f"메모: {line['note']}")
    lines.append("· 장중 값은 잠정 — 지킴/뚫림은 종가로 판정")
    return "\n".join(lines)


def format_close_digest(rows, market, on):
    """rows: [(line, close, event|None)]."""
    mk = "국내" if market == "kr" else "미국"
    out = [f"📐 <b>빗각 종가 점검 — {mk}</b> ({on})"]
    for line, close, ev in rows:
        tag = f" ⚠️ <b>종가 {ev}</b>" if ev else ""
        out.append(fmt_line_row(line, close, on, market) + tag)
    out.append(f"· 이탈/돌파 = 종가가 선 ∓{BREAK_PCT:g}% 밖 (일봉 종가 · 금요일 요약 = 주봉 종가)")
    return "\n".join(out)


# ── 저장 ─────────────────────────────────────────────────────────────
def load_lines(path=LINES_PATH):
    with open(path, encoding="utf-8") as f:
        lines = json.load(f)
    ids = [x["id"] for x in lines]
    assert len(ids) == len(set(ids)), "trendlines.json: id 중복"
    for x in lines:
        line_value(x, x["p2"][0])          # 날짜·가격 형식 검증(잘못이면 기동 때 바로 실패)
    return lines


def state_path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "trendline_state.json")


def load_state(path=None):
    path = path or state_path()
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_state(st, path=None):
    path = path or state_path()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)
    os.replace(tmp, path)


# ── 작업 ─────────────────────────────────────────────────────────────
def check_near(get_price, send, now_kst=None, lines=None, state=None, persist=True):
    now = now_kst or datetime.now(KST)
    lines = lines if lines is not None else load_lines()
    st = state if state is not None else load_state()
    sent = 0
    for line in lines:
        mk = market_of(line["ticker"])
        if not market_open(mk, now):
            continue
        on = session_date(mk, now)
        rec = st.setdefault(line["id"], {})
        if rec.get("near_date") == str(on):
            continue
        q = get_price(line["ticker"])
        price = (q or {}).get("price")
        if not price:
            continue
        if abs(dist_pct(price, line_value(line, on))) <= NEAR_PCT:
            rec["near_date"] = str(on)
            send(format_near(line, price, on, mk))
            sent += 1
    if persist:
        save_state(st)
    return sent


def close_check(market, get_price, send, now_kst=None, lines=None, state=None, persist=True):
    """장 마감 뒤 하루 1번. 그 시장 선이 하나도 없으면 아무것도 안 보낸다(로그만)."""
    now = now_kst or datetime.now(KST)
    lines = [x for x in (lines if lines is not None else load_lines()) if market_of(x["ticker"]) == market]
    on = session_date(market, now)
    if on.weekday() >= 5 or not lines:
        print(f"[빗각] {market} 종가 점검 건너뜀 (주말 또는 선 {len(lines)}개)")
        return None
    st = state if state is not None else load_state()
    rows, missing = [], []
    for line in lines:
        q = get_price(line["ticker"])
        close = (q or {}).get("price")
        if not close:
            missing.append(line["id"])
            continue
        rec = st.setdefault(line["id"], {})
        new_side = side_of(close, line_value(line, on))
        rows.append((line, close, close_event(rec.get("side"), new_side)))
        rec["side"], rec["side_date"] = new_side, str(on)
    msg = format_close_digest(rows, market, on)
    if missing:
        msg += "\n· 가격 조회 실패: " + ", ".join(missing)
    send(msg)
    if persist:
        save_state(st)
    return msg


def status_text(get_price, now_kst=None, lines=None):
    """텔레그램 '빗각' 명령 — 지금 가격 기준 모든 선."""
    now = now_kst or datetime.now(KST)
    lines = lines if lines is not None else load_lines()
    out = ["📐 <b>빗각 추적 중</b>"]
    for line in lines:
        mk = market_of(line["ticker"])
        q = get_price(line["ticker"])
        p = (q or {}).get("price")
        on = session_date(mk, now)
        out.append(fmt_line_row(line, p, on, mk) if p else f"· {line.get('name')} — 가격 조회 실패")
    return "\n".join(out)


def register(sched, get_price, send):
    def safe(fn, name):
        def run():
            try:
                fn()
            except Exception as e:
                print(f"[빗각] {name} 오류: {e}")
        return run
    load_lines()     # 형식이 틀리면 기동 때 바로 실패
    sched.every(5).minutes.do(safe(lambda: check_near(get_price, send), "접근"))
    sched.every().day.at("20:20", "Asia/Seoul").do(safe(lambda: close_check("kr", get_price, send), "KR 종가"))
    sched.every().day.at("16:30", "America/New_York").do(safe(lambda: close_check("us", get_price, send), "US 종가"))
