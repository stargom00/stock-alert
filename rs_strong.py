"""v2.88 지수 하락 구간에서 덜 빠진 KR 종목 — 텔레그램 `덜빠진`(사용자 2026-11-19 "지수빠질때 덜빠지는 종목은 어떻게 선별해?" → "만들어줘").
계산은 스캐너 GET /api/rs-drawdown(v5.385, rs_drawdown.py)이 한다 — 여기는 받아서 보여 주기만.
**관심 신호 · 측정 전.** 덜 빠진 종목이 회복기에 먼저 오른다는 건 O'Neil·Minervini의 관찰이고 잰 적 없다. 방어주도 덜 빠진다 → 업종을 같이 본다.
"""
from __future__ import annotations

import requests

SHOW = 15
ALIASES = ("덜빠진", "덜빠진종목", "덜 빠진", "상대강도", "버틴종목")


def _tv(ticker):
    t = str(ticker or "").upper()
    return f'<a href="https://www.tradingview.com/chart/?symbol=KRX:{t[:6]}">{t[:6]}</a>'


def _p(x, d=1):
    return f"{x * 100:+.{d}f}%"


def _idx_line(name, w):
    s = (f"{name} {w['peak_date'][5:]} 고점 → {w['trough_date'][5:]} 저점 <b>{_p(w['dd'])}</b> · 이후 {_p(w['reb'])}")
    return s + ("" if w.get("meaningful") else " · ⚠ 하락이 얕음")


def row_text(r):
    flags = " · ".join(x for x in ("✓저점 유지" if r.get("low_ok") else "", "✓50일선 위" if r.get("ma50_ok") else "") if x)
    sec = r.get("sector") or ""
    l1 = f"<b>{r['name']}</b> {_tv(r['ticker'])}" + (f" · {sec}" if sec and sec != "기타" else "")
    l2 = f"하락 {_p(r['dd'])} (지수보다 {r['rel_dd'] * 100:+.1f}%p) · 이후 {_p(r['reb'], 0)}" + (f" · {flags}" if flags else "")
    return f"{l1}\n   └ {l2}"


def format_text(p):
    w = p.get("index") or {}
    out = [f"🛡 <b>지수 하락 구간 · 덜 빠진 종목</b> · {p.get('session_date', '')} 확정"]
    out += [_idx_line(k, w[k]) for k in ("KOSPI", "KOSDAQ") if k in w]
    rows = (p.get("top") or [])[:SHOW]
    out += ["", f"━ <b>상위 {len(rows)}</b> (지수보다 덜 빠진 순 · 대상 {p.get('n_checked')}종목)"] + [row_text(r) for r in rows]
    sec = p.get("sectors") or []
    if sec:
        out += ["", "━ <b>업종</b> (지수 대비 중앙값)", " · ".join(f"{s['sector']} {s['rel_dd_med'] * 100:+.1f}%p({s['n']})" for s in sec[:8])]
    out += ["", "<i>구간 = 각 시장 지수의 최근 60거래일 고점 → 그 뒤 저점 · 저점 유지 = 지수가 저점을 찍을 때 이 종목은 구간 신저가 안 감 · "
            "방어주도 덜 빠지니 업종·오를 이유를 같이 볼 것 · 측정 전 관심 신호</i>"]
    return "\n".join(out)


def on_demand(scanner_url, headers, get=requests.get):
    try:
        p = get(f"{scanner_url}/api/rs-drawdown", params={"top": SHOW}, timeout=180, headers=headers).json()
    except Exception as e:
        return f"🛡 덜 빠진 종목 조회 실패: {e}"
    if not p.get("ok"):
        return f"🛡 덜 빠진 종목 응답 오류: {p.get('error') or p}"
    if p.get("reason") == "no_index":
        return "🛡 지수 일봉을 못 받았어(naver). 잠시 뒤 다시 보내 줘."
    if p.get("reason") == "no_rows":
        return "🛡 계산할 종목이 없어(스캐너 캐시 갱신 중일 수 있어). 잠시 뒤 다시 보내 줘."
    return format_text(p)
