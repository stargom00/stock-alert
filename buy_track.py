"""v2.94 매수 종목 6개월 추적 — 내 매수가 기준으로 의미 있는 상승·하락을 알린다(팔았어도 계속).
사용자 2026-11-21 "내가 매수한 종목을 추적해줘. 내가 매도해도 상관없이 약 6개월간 의미있는 상승과 하락일때 알림을 보내줘.
기준은 내 매수가에 맞춰서. 매매일지랑 연계시키면 될듯".

등록: 텔레그램 `매수추적 아남전자 008700 1352 2026-11-18`(이름·날짜는 생략 가능, 여러 줄 가능) — 매매 훈련 홈 일지의
"🔔 봇 추적" 버튼이 이 글을 만들어 준다(일지 화면은 봇에 직접 못 보낸다). `매수추적` = 현황, `매수추적삭제 008700` = 그만.
상태: DATA_DIR/buy_track.json.

판정: **확정 종가**만(KR 20:20 KST · US 16:30 뉴욕, 빗각 종가 점검과 같은 시각). 매수가 대비 등락률이 아래 단계를 처음 넘으면
한 번 알린다(같은 단계는 다시 안 알림, 여러 단계를 한 번에 넘으면 가장 먼 단계 하나로).
상수 출처 — 전부 AI 어림값(사용자는 "의미있는 상승과 하락"이라고만 했다. 측정·검색 없이 정함, 재검토 대상):
- UP_LEVELS +5·10·20·30·50·100% / DOWN_LEVELS −5·10·20·30·50%
- TRACK_DAYS 183("약 6개월") — 끝나는 날 최고·최저·마지막 등락률을 정리해 보내고 추적을 끝낸다.
**관찰용 · 측정 전.** 알림은 사실 통보일 뿐 매매 신호가 아니다.
"""
from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
ET = ZoneInfo("America/New_York")


def market_of(ticker):
    """숫자로 시작하는 6자리 = KR(0035S0처럼 알파벳이 섞인 새 코드 포함), 그 밖은 US."""
    t = str(ticker).upper().split(".")[0]
    return "kr" if len(t) == 6 and t[0].isdigit() else "us"


def session_date(market, now):
    return (now.astimezone(KST) if market == "kr" else now.astimezone(ET)).date()

UP_LEVELS = (5, 10, 20, 30, 50, 100)
DOWN_LEVELS = (-5, -10, -20, -30, -50)
TRACK_DAYS = 183
CMD = "매수추적"
ALIASES = ("매수추적", "매수 추적", "매수추적목록", "내매수")
DEL_ALIASES = ("매수추적삭제", "매수추적 삭제")


def state_path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "buy_track.json")


def load_state(path=None):
    try:
        with open(path or state_path(), encoding="utf-8") as f:
            st = json.load(f)
        return st if isinstance(st, dict) and isinstance(st.get("items"), list) else {"items": []}
    except (OSError, ValueError):
        return {"items": []}


def save_state(st, path=None):
    path = path or state_path()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)
    os.replace(tmp, path)


_TICKER = re.compile(r"^(\d[0-9A-Z]{5}(\.K[SQ])?|[A-Z][A-Z.\-]{0,6})$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_line(line, today):
    """한 줄 → (item, None) 또는 (None, 이유). 형식: [이름…] 티커 매수가 [날짜]. 뒤에서부터 읽는다."""
    parts = line.split()
    if not parts:
        return None, None
    d = today
    if _DATE.match(parts[-1]):
        try:
            d = date.fromisoformat(parts.pop())
        except ValueError:
            return None, f"날짜가 이상해: {line}"
    if len(parts) < 2:
        return None, f"티커와 매수가가 필요해: {line}"
    try:
        buy = float(parts[-1].replace(",", "").replace("원", "").replace("$", ""))
    except ValueError:
        return None, f"매수가를 못 읽었어: {line}"
    tk = parts[-2].upper()
    if buy <= 0 or not _TICKER.match(tk):
        return None, f"티커·매수가가 이상해: {line}"
    if d > today:
        return None, f"매수일이 미래야: {line}"
    tk = tk.split(".")[0] if tk[0].isdigit() else tk
    return {"id": f"{tk}|{d}", "ticker": tk, "name": " ".join(parts[:-2]) or tk, "buy": buy, "date": str(d),
            "until": str(d + timedelta(days=TRACK_DAYS)), "hit": [], "max": None, "min": None}, None


def pct_of(close, buy):
    return (close / buy - 1) * 100


def crossed(pct):
    """지금 등락률이 넘어 있는 단계 전부(+면 위 단계, −면 아래 단계)."""
    return [l for l in UP_LEVELS if pct >= l] + [l for l in DOWN_LEVELS if pct <= l]


def new_level(item, pct):
    """아직 알리지 않은 단계 중 가장 먼 것(없으면 None). item['hit']에 넘은 단계를 전부 적는다."""
    fresh = [l for l in crossed(pct) if l not in item["hit"]]
    if not fresh:
        return None
    item["hit"] = sorted(set(item["hit"]) | set(fresh))
    return max(fresh, key=abs)


def note_extreme(item, pct, on):
    if item.get("max") is None or pct > item["max"]["pct"]:
        item["max"] = {"pct": round(pct, 2), "date": str(on)}
    if item.get("min") is None or pct < item["min"]["pct"]:
        item["min"] = {"pct": round(pct, 2), "date": str(on)}


def _px(v, tk):
    return f"{v:,.0f}원" if tk[0].isdigit() else f"${v:,.2f}"


def _link(it):
    t = it["ticker"]
    return f'<a href="https://www.tradingview.com/chart/?symbol={"KRX:" + t if t[0].isdigit() else t}">{it["name"]}</a>'


def _p(x):
    return f"{x:+.1f}%"


def _ext(it):
    mx, mn = it.get("max"), it.get("min")
    return (f"최고 {_p(mx['pct'])}({mx['date'][5:]}) · 최저 {_p(mn['pct'])}({mn['date'][5:]})" if mx and mn else "")


def _days(it, on):
    return (on - date.fromisoformat(it["date"])).days


def alert_text(it, close, pct, level, on):
    up = level > 0
    return (f"{'📈' if up else '📉'} <b>매수 추적</b> · {_link(it)} <b>{'+' if up else ''}{level}%</b> 단계 {'도달' if up else '이탈'}\n"
            f"종가 {_px(close, it['ticker'])} · 매수가 {_px(it['buy'], it['ticker'])} 대비 <b>{_p(pct)}</b>\n"
            f"매수 {it['date'][5:]} · {_days(it, on)}일째 · {_ext(it)}\n"
            f"<i>확정 종가 기준 · 관찰용(매매 신호 아님)</i>")


def final_text(it, close, on):
    last = f"마지막 종가 {_px(close, it['ticker'])} <b>{_p(pct_of(close, it['buy']))}</b>" if close else "마지막 가격 조회 실패"
    return (f"🏁 <b>매수 추적 끝</b> · {_link(it)} · {TRACK_DAYS}일\n"
            f"매수가 {_px(it['buy'], it['ticker'])}({it['date']}) → {last}\n{_ext(it)}\n"
            f"<i>복기에 적어 둘 만한 게 있었는지 봐</i>")


def register_text(text, get_price, now=None, state=None, persist=True):
    """`매수추적 …` 등록(여러 줄). 이미 넘어 있는 단계는 알림 없이 지난 것으로 적는다(등록하자마자 알림이 쏟아지지 않게)."""
    now = now or datetime.now(KST)
    st = state if state is not None else load_state()
    out, n_ok = [], 0
    for raw in text.splitlines():
        line = raw.strip()
        for a in sorted(ALIASES, key=len, reverse=True):
            if line.startswith(a):
                line = line[len(a):].strip()
                break
        if not line:
            continue
        it, err = parse_line(line, now.date())
        if err:
            out.append("⚠ " + err)
            continue
        on = session_date(market_of(it["ticker"]), now)
        if date.fromisoformat(it["until"]) < on:
            out.append(f"⚠ {it['name']} — 매수일에서 {TRACK_DAYS}일이 이미 지났어")
            continue
        close = (get_price(it["ticker"]) or {}).get("price")
        if not close:
            out.append(f"⚠ {it['name']}({it['ticker']}) — 가격을 못 받았어. 티커를 확인해 줘")
            continue
        pct = pct_of(close, it["buy"])
        it["hit"] = sorted(crossed(pct))
        note_extreme(it, pct, on)
        old = next((x for x in st["items"] if x["id"] == it["id"]), None)
        if old:
            st["items"].remove(old)
        st["items"].append(it)
        n_ok += 1
        out.append(f"{'♻' if old else '✅'} {_link(it)} 매수가 {_px(it['buy'], it['ticker'])} · 지금 <b>{_p(pct)}</b> · {it['until']}까지")
    if not out:
        return status_text(get_price, now, st)
    if n_ok and persist:
        save_state(st)
    ups = "·".join(f"+{l}" for l in UP_LEVELS)
    dns = "·".join(str(l) for l in DOWN_LEVELS)
    return "\n".join(["🔔 <b>매수 추적 등록</b>"] + out + ["", f"<i>알림 단계 {ups}% / {dns}% (확정 종가, 단계마다 한 번) · 현황 <code>{CMD}</code></i>"])


def remove(arg, state=None, persist=True):
    st = state if state is not None else load_state()
    key = arg.strip().upper().split(".")[0]
    gone = [x for x in st["items"] if key and (x["ticker"] == key or x["name"].upper() == key)]
    if not gone:
        return f"🔔 '{arg.strip()}' 추적이 없어. <code>{CMD}</code>로 목록을 봐."
    st["items"] = [x for x in st["items"] if x not in gone]
    if persist:
        save_state(st)
    return "🔔 추적 그만: " + ", ".join(f"{x['name']}({x['date'][5:]})" for x in gone)


def status_text(get_price, now=None, state=None):
    now = now or datetime.now(KST)
    st = state if state is not None else load_state()
    if not st["items"]:
        return (f"🔔 추적 중인 매수가 없어.\n<code>{CMD} 아남전자 008700 1352 2026-11-18</code>처럼 보내거나, "
                "매매 훈련 홈 일지의 '🔔 봇 추적' 버튼을 눌러 줘.")
    rows = []
    for it in st["items"]:
        close = (get_price(it["ticker"]) or {}).get("price")
        on = session_date(market_of(it["ticker"]), now)
        left = (date.fromisoformat(it["until"]) - on).days
        rows.append((pct_of(close, it["buy"]) if close else None, it, close, left))
    rows.sort(key=lambda r: (r[0] is None, -(r[0] or 0)))
    out = [f"🔔 <b>매수 추적 중</b> · {len(rows)}개"]
    for pct, it, close, left in rows:
        out.append(f"{'⚪' if pct is None else '🔴' if pct >= 0 else '🔵'} {_link(it)} <b>{'가격 실패' if pct is None else _p(pct)}</b>"
                   f" · 매수 {_px(it['buy'], it['ticker'])}({it['date'][5:]}) · 남은 {left}일" + (f"\n   └ {_ext(it)}" if it.get("max") else ""))
    return "\n".join(out + ["", f"<i>지금 가격 기준 · 그만 보기 <code>매수추적삭제 티커</code></i>"])


def close_check(market, get_price, send, now=None, state=None, persist=True):
    """장 마감 뒤 하루 한 번 — 그 시장 종목의 확정 종가로 단계를 본다. 반환 = 보낸 알림 수."""
    now = now or datetime.now(KST)
    st = state if state is not None else load_state()
    items = [x for x in st["items"] if market_of(x["ticker"]) == market]
    on = session_date(market, now)
    if on.weekday() >= 5 or not items:
        print(f"[매수추적] {market} 건너뜀 (주말 또는 종목 {len(items)}개)", flush=True)
        return 0
    sent = failed = 0
    for it in items:
        close = (get_price(it["ticker"]) or {}).get("price")
        over = on >= date.fromisoformat(it["until"])
        if close:
            pct = pct_of(close, it["buy"])
            note_extreme(it, pct, on)
            lv = new_level(it, pct)
            if lv is not None and not over:
                send(alert_text(it, close, pct, lv, on))
                sent += 1
        else:
            failed += 1
        if over:
            send(final_text(it, close, on))
            st["items"].remove(it)
            sent += 1
    print(f"[매수추적] {market} {on} — 종목 {len(items)}개 · 알림 {sent}개 · 가격 실패 {failed}개", flush=True)
    if persist:
        save_state(st)
    return sent


def register(sched, get_price, send):
    def safe(mk):
        def run():
            try:
                close_check(mk, get_price, send)
            except Exception as e:
                print(f"[매수추적] {mk} 오류: {e}", flush=True)
        return run
    sched.every().day.at("20:22", "Asia/Seoul").do(safe("kr"))
    sched.every().day.at("16:32", "America/New_York").do(safe("us"))
