"""v2.92 테마 점화 알림 — 스캐너 테마 바구니(v5.393 GET /api/theme-baskets)가 점화하면 알린다. 텔레그램 `테마` = 지금 현황.
사용자 2026-11-19 "국장엔 테마가있어 … 종목 추적이 잘안돼" → 추적 설계 2단계 "이제 이거하자".

판정(강한 종목 수·점화)은 전부 스캐너 theme_baskets.py가 한다 — 여기는 받아서 보내기만. 바구니·날짜마다 한 번만 알린다
(상태: DATA_DIR/theme_ignite_state.json — 재배포로 같은 날 두 번 보내지 않게 파일에 둔다).
**관심 신호 · 측정 전.** 등락률은 애프터 포함 종가 기준이라 증권사 화면과 다를 수 있다(스캐너 theme_baskets.py docstring).

폴링 창(AI 어림값): KR 평일 09:00~20:10 KST(애프터마켓 종료 + 10분 — 2026-11-18 한한령은 애프터마켓에서 점화했다),
US 평일 09:30~16:10 뉴욕. 10분마다. 스캐너가 종목 가격을 5분 캐시한다.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

ALIASES = ("테마", "테마점화", "테마 점화", "바구니")
SHOW_TOP = 6
KEEP_DAYS = 30
_cfg = {"url": None, "headers": None}


def state_path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "theme_ignite_state.json")


def load_state(path=None):
    try:
        with open(path or state_path(), encoding="utf-8") as f:
            st = json.load(f)
        return st if isinstance(st, dict) else {}
    except (OSError, ValueError):
        return {}


def save_state(st, path=None):
    path = path or state_path()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)
    os.replace(tmp, path)


def window_open(now=None):
    """KR 평일 09:00~20:10 KST 또는 US 평일 09:30~16:10 뉴욕."""
    now = now or datetime.now(ZoneInfo("UTC"))
    k, n = now.astimezone(ZoneInfo("Asia/Seoul")), now.astimezone(ZoneInfo("America/New_York"))
    kr = k.weekday() < 5 and (9, 0) <= (k.hour, k.minute) <= (20, 10)
    us = n.weekday() < 5 and (9, 30) <= (n.hour, n.minute) <= (16, 10)
    return kr or us


def _link(r):
    t = str(r.get("ticker") or "").upper()
    kr = len(t) >= 6 and t[0].isdigit()
    return f'<a href="https://www.tradingview.com/chart/?symbol={"KRX:" + t[:6] if kr else t}">{r.get("name") or t}</a>'


def _p(x):
    return "—" if x is None else f"{x:+.1f}%"


def _has_line(t, lined):
    t = str(t).upper()
    return any(t == x or (t[:1].isdigit() and x[:6] == t[:6]) for x in lined)


def basket_text(b, lined=(), head="🔥 <b>테마 점화</b>"):
    sm = b.get("summary") or {}
    rows = [r for r in (b.get("rows") or []) if r.get("chg_pct") is not None][:SHOW_TOP]
    out = [f"{head} · <b>{b.get('name')}</b>",
           f"강한 종목 <b>{sm.get('strong')}/{sm.get('n_px')}</b> · 평균 <b>{_p(sm.get('avg_pct'))}</b> · {str(sm.get('date') or '')[5:]}"]
    out += [f"{i}. {_link(r)} <b>{_p(r['chg_pct'])}</b>" + (" 📐" if _has_line(r["ticker"], lined) else "") for i, r in enumerate(rows, 1)]
    if sm.get("n_failed"):
        out.append(f"⚠ 가격 실패 {sm['n_failed']}종목")
    return "\n".join(out)


FOOT = "<i>📐 장부에 선 있음 · 등락률 = 애프터 포함 종가 기준(증권사 화면과 다를 수 있음) · 관심 신호 · 측정 전</i>"


def status_line(b):
    sm = b.get("summary") or {}
    if not sm.get("n_px"):
        return f"⚪ <b>{b.get('name')}</b> 가격을 못 받았어"
    li = b.get("last_ignited")
    lead = sm.get("leader") or {}
    s = (f"{'🔥' if sm.get('ignited') else '⚪'} <b>{b.get('name')}</b> 강한 {sm['strong']}/{sm['n_px']} · 평균 {_p(sm.get('avg_pct'))}"
         + (f" · 대장 {lead.get('name')} {_p(lead.get('chg_pct'))}" if lead else ""))
    if li and not sm.get("ignited"):
        s += f"\n   └ 최근 점화 {li['date'][5:]} ({li['strong']}/{li['n_px']})"
    return s


def fetch(get=requests.get):
    p = get(f"{_cfg['url']}/api/theme-baskets", params={"prices": 1}, timeout=120, headers=_cfg["headers"]).json()
    if not p.get("ok"):
        raise RuntimeError(p.get("error") or "응답 오류")
    return p


def on_demand(get=requests.get):
    try:
        p = fetch(get)
    except Exception as e:
        return f"🔥 테마 조회 실패: {e}"
    act = [b for b in p.get("baskets") or [] if b.get("status") != "종료"]
    if not act:
        return "🔥 아직 테마 바구니가 없어 — 스캐너 추적 장부(/track)에서 '+ 테마'로 묶어 줘."
    lined = p.get("lined") or []
    out = ["🔥 <b>테마 현황</b>", ""] + [status_line(b) for b in act]
    for b in act:
        if (b.get("summary") or {}).get("ignited"):
            out += ["", basket_text(b, lined, head="🔥 <b>점화 중</b>")]
    r = p.get("rule") or {}
    out += ["", f"<i>점화 = +{r.get('up_pct', 5):g}% 이상 종목이 {r.get('min_count', 3)}개 이상이면서 {round((r.get('min_frac') or .3) * 100)}% 이상</i>", FOOT]
    return "\n".join(out)


def check(send, get=requests.get, state=None, now=None):
    """점화한 바구니를 날짜마다 한 번 알린다. 반환 = 보낸 바구니 id 목록."""
    if not window_open(now):
        return []
    p = fetch(get)
    st = state if state is not None else load_state()
    act = [b for b in p.get("baskets") or [] if b.get("status") != "종료"]
    sent = []
    for b in act:
        sm = b.get("summary") or {}
        if not (sm.get("ignited") and sm.get("is_today")):
            continue
        key = f"{b['id']}|{sm['date']}"
        if st.get(key):
            continue
        send(basket_text(b, p.get("lined") or []) + "\n\n" + FOOT)
        st[key] = sm["date"]
        sent.append(b["id"])
    print(f"[테마] 바구니 {len(act)}개 중 새 점화 {len(sent)}개", flush=True)
    if sent:
        for k in sorted(st, key=lambda k: st[k])[:-KEEP_DAYS * 8 or None]:   # 오래된 키 정리(바구니 8개 × 30일 어림)
            st.pop(k, None)
        if state is None:
            save_state(st)
    return sent


def register(sched, send, scanner_url, scanner_headers=None):
    _cfg["url"], _cfg["headers"] = scanner_url, scanner_headers

    def run():
        try:
            check(send)
        except Exception as e:
            print(f"[테마] 점화 확인 오류: {e}", flush=True)
    sched.every(10).minutes.do(run)
