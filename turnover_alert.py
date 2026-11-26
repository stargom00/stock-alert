"""v2.97 거래대금 상위 10 — 스캐너(v5.403 GET /api/turnover-rank)가 장 마감 후 기록한 순위를 하루 한 번 보내고, `거래대금` = 지금 현황.
사용자 2026-11-26 "응 붙여줘" — 트레이더 율 채널 "거래대금 상위 1~10위가 그 순위를 계속 유지하는지 매일 기록 … 치고 올라오는 애들".
판정(순위 이동·새 진입·연속 일수)은 전부 스캐너 turnover_rank.py — 여기는 받아서 보내기만. 기록일마다 한 번(DATA_DIR/turnover_alert_state.json).
보내는 창(AI 어림값): 평일 20:25~23:59 KST, 15분마다 — 스캐너가 20:10 뒤 4분 주기로 기록하니 그 뒤.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

ALIASES = ("거래대금", "거래대금상위", "거래대금 상위", "대금")
_cfg = {"url": None, "headers": None}


def state_path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "turnover_alert_state.json")


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
    k = (now or datetime.now(ZoneInfo("UTC"))).astimezone(ZoneInfo("Asia/Seoul"))
    return k.weekday() < 5 and (k.hour, k.minute) >= (20, 25)


def _link(r):
    t = str(r.get("ticker") or r.get("code") or "")
    return f'<a href="https://www.tradingview.com/chart/?symbol=KRX:{t[:6]}">{r.get("name") or t}</a>'


def move_text(r):
    if r.get("new"):
        return f"🆕{'(' + str(r['prev_rank']) + '위→)' if r.get('prev_rank') else ''}"
    m = r.get("move")
    if m is None:
        return ""
    return f"▲{m}" if m > 0 else (f"▼{-m}" if m < 0 else "=")


def rank_text(p):
    a = p.get("analysis")
    if not a:
        return "💰 거래대금 순위 기록이 아직 없어 — 거래일 20:10(한국 시간) 뒤에 처음 쌓여."
    out = [f"💰 <b>거래대금 상위 10</b> · {a['day'][5:]}", ""]
    for r in a["top"]:
        tail = " · ".join(x for x in (move_text(r), f"{r['streak']}일째" if r.get("streak", 0) > 1 else "") if x)
        out.append(f"{r['rank']}. {_link(r)} {r['chg_pct']:+.1f}% · {round(r['value_mil'] / 100):,}억" + (f" · {tail}" if tail else ""))
    news = [r["name"] for r in a["top"] if r.get("new")]
    if news:
        out += ["", f"🆕 새로 10위 안: {', '.join(news)}"]
    if a.get("dropped"):
        out.append("↘ 빠짐: " + ", ".join(f"{x['name']}({x['prev_rank']}위→{str(x['rank']) + '위' if x.get('rank') else '20위 밖'})" for x in a["dropped"]))
    out += ["", f"<i>naver 기준(키움 0186과 조금 다를 수 있음) · ETF·ETN 제외 · 기록 {a['n_days']}일째 · 순위 흐름은 스캐너 추적 장부(/track)</i>"]
    return "\n".join(out)


def fetch(get=requests.get):
    p = get(f"{_cfg['url']}/api/turnover-rank", timeout=60, headers=_cfg["headers"]).json()
    if not p.get("ok"):
        raise RuntimeError(p.get("error") or "응답 오류")
    return p


def on_demand(get=requests.get):
    try:
        return rank_text(fetch(get))
    except Exception as e:
        return f"💰 거래대금 순위 조회 실패: {e}"


def check(send, get=requests.get, state=None, now=None):
    """오늘(KST) 기록이 생겼으면 한 번 보낸다. 반환 = 보낸 기록일 또는 None."""
    if not window_open(now):
        return None
    p = fetch(get)
    a = p.get("analysis")
    today = (now or datetime.now(ZoneInfo("UTC"))).astimezone(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
    st = state if state is not None else load_state()
    if not a or a["day"] != today or st.get("last_sent") == today:
        print(f"[거래대금] 보낼 것 없음(기록일 {a and a['day']} · 오늘 {today} · 보낸 날 {st.get('last_sent')})", flush=True)
        return None
    send(rank_text(p))
    st["last_sent"] = today
    if state is None:
        save_state(st)
    return today


def register(sched, send, scanner_url, scanner_headers=None):
    _cfg["url"], _cfg["headers"] = scanner_url, scanner_headers

    def run():
        try:
            check(send)
        except Exception as e:
            print(f"[거래대금] 확인 오류: {e}", flush=True)
    sched.every(15).minutes.do(run)
