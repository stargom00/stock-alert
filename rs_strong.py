"""v2.88(v2.89 한 줄 형식) 지수 하락 구간에서 덜 빠진 KR 종목 — 텔레그램 `덜빠진`(사용자 2026-11-19 "지수빠질때 덜빠지는 종목은 어떻게 선별해?" → "만들어줘").
계산은 스캐너 GET /api/rs-drawdown(v5.385, rs_drawdown.py)이 한다 — 여기는 받아서 보여 주기만.
**관심 신호 · 측정 전.** 덜 빠진 종목이 회복기에 먼저 오른다는 건 O'Neil·Minervini의 관찰이고 잰 적 없다. 방어주도 덜 빠진다 → 업종을 같이 본다.
"""
from __future__ import annotations

import requests

SHOW = 15
ALIASES = ("덜빠진", "덜빠진종목", "덜 빠진", "상대강도", "버틴종목")


def _link(r):
    t = str(r.get("ticker") or "").upper()[:6]
    return f'<a href="https://www.tradingview.com/chart/?symbol=KRX:{t}">{r.get("name") or t}</a>'


def _p(x, d=0):
    return f"{x * 100:+.{d}f}%"


def _idx_line(name, w):
    nm = {"KOSPI": "코스피", "KOSDAQ": "코스닥"}.get(name, name)
    s = f"📉 {nm} <b>{_p(w['dd'], 1)}</b> ({w['peak_date'][5:]}→{w['trough_date'][5:]}) · 반등 {_p(w['reb'], 1)}"
    return s + ("" if w.get("meaningful") else " ⚠얕음")


def flag(r):
    """🟢 저점 유지 + 50일선 위 · 🟡 둘 중 하나 · ⚪ 둘 다 아님."""
    n = bool(r.get("low_ok")) + bool(r.get("ma50_ok"))
    return "🟢🟡⚪"[2 - n]


def row_text(i, r):
    """v2.89 사용자 "텍스트 한눈에 보기쉽게" — 종목당 한 줄. 이름에 차트 링크, ▲ = 지수보다 덜 빠진 %p."""
    sec = r.get("sector") or ""
    return (f"{i}. {flag(r)} {_link(r)} <b>▲{r['rel_dd'] * 100:.0f}</b> · {_p(r['dd'])} · 반등 {_p(r['reb'])}"
            + (f" · <i>{sec}</i>" if sec and sec != "기타" else ""))


def format_text(p):
    w = p.get("index") or {}
    out = [f"🛡 <b>덜 빠진 종목</b> · {p.get('session_date', '')} 확정", ""]
    out += [_idx_line(k, w[k]) for k in ("KOSPI", "KOSDAQ") if k in w]
    sec = p.get("sectors") or []
    if sec:
        out += ["", "🏆 <b>업종</b> (지수보다 덜 빠진 %p)", " · ".join(f"{s['sector']} ▲{s['rel_dd_med'] * 100:.0f}" for s in sec[:6])]
    rows = (p.get("top") or [])[:SHOW]
    out += ["", f"📋 <b>종목 TOP {len(rows)}</b> / {p.get('n_checked')}"] + [row_text(i + 1, r) for i, r in enumerate(rows)]
    out += ["", "<i>▲ 지수보다 덜 빠진 %p · 가운데 % = 구간 등락 · 🟢 저점 유지+50일선 위 🟡 하나만 ⚪ 없음 · "
            "구간 = 지수 최근 60일 고점→저점 · 방어주도 덜 빠짐 · 측정 전</i>"]
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
