"""v2.95 토요일 주간 리뷰 — 한 주 동안 내 선·테마·매수 추적에서 무슨 일이 있었는지 한 통으로.
사용자 2026-11-19 "종목 추적이 잘안돼 … 그려놓고 까먹어서 매매못함" 추적 설계 3단계 → 2026-11-22 "3번 만들고".

봇이 알림을 보낼 때마다 한 줄씩 적어 두고(DATA_DIR/review_log.json), 토요일 09:05 KST에 지난 7일을 묶어 보낸다.
텔레그램 `주간리뷰` = 지금 다시. 기록은 알림이 실제로 나간 것만 — 리뷰가 따로 판정하지 않는다.
지금 상태(선 근처·테마 금요일 마감·매수 추적 등락률)는 그 자리에서 다시 읽는다.
상수(AI 어림값): 토요일 09:05 KST(미국 금요일 장이 끝난 뒤), 기록 보관 60일, 테마는 강한 순 위·아래 3개씩.
**정리용 · 측정 전.** 무엇이 잘 맞았는지 세는 통계가 아니다.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
KEEP_DAYS = 60
KINDS = ("line", "theme", "buy")
ALIASES = ("주간리뷰", "주간 리뷰", "토요리뷰", "리뷰")
TOP_THEMES = 3


def log_path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "review_log.json")


def load_log(path=None):
    try:
        with open(path or log_path(), encoding="utf-8") as f:
            rows = json.load(f)
        return rows if isinstance(rows, list) else []
    except (OSError, ValueError):
        return []


def log(kind, name, what, on=None, path=None):
    """알림 한 건을 적는다. 기록 실패가 알림을 막으면 안 되므로 여기서 잡고 로그만 남긴다."""
    assert kind in KINDS, kind
    try:
        on = str(on or datetime.now(KST).date())
        rows = load_log(path)
        row = {"date": on, "kind": kind, "name": str(name), "what": str(what)}
        if row in rows:
            return False
        cut = str(date.fromisoformat(on) - timedelta(days=KEEP_DAYS))
        rows = [r for r in rows if r.get("date", "") >= cut] + [row]
        p = path or log_path()
        with open(p + ".tmp", "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)
        os.replace(p + ".tmp", p)
        return True
    except Exception as e:
        print(f"[리뷰] 기록 실패({kind} {name}): {e}", flush=True)
        return False


def week_range(today):
    """리뷰가 덮는 7일 — 오늘 포함, 6일 전부터."""
    return today - timedelta(days=6), today


def week_rows(rows, today):
    a, b = (str(x) for x in week_range(today))
    return sorted((r for r in rows if a <= r.get("date", "") <= b), key=lambda r: (r["date"], r["name"]))


def _p(x):
    return "—" if x is None else f"{x:+.1f}%"


def _events(rows, kind):
    return [f"· {r['date'][5:]} <b>{r['name']}</b> {r['what']}" for r in rows if r["kind"] == kind]


def build_text(rows, today, lines=None, baskets=None, buys=None, errors=()):
    """rows = 이번 주 기록. lines = 장부 선(view 포함), baskets = 테마 바구니(summary 포함), buys = [(item, 지금 등락률)] — None이면 못 읽은 것."""
    a, b = week_range(today)
    out = [f"🗓 <b>주간 리뷰</b> · {str(a)[5:]} ~ {str(b)[5:]}", ""]
    ev = _events(rows, "line")
    out.append(f"📐 <b>내 선</b> · 이번 주 알림 {len(ev)}건")
    out += ev or ["· 선에 닿거나 넘은 종목이 없었어"]
    if lines is not None:
        near = [x for x in lines if x.get("status") != "종료" and (x.get("view") or {}).get("zone") == "near"]
        if near:
            out.append("· 지금 선 근처: " + ", ".join(f"{x.get('name')}({x.get('label') or '선'} {_p((x.get('view') or {}).get('to_line_pct'))})" for x in near))
        out.append(f"· 추적 중인 선 {sum(1 for x in lines if x.get('status') != '종료')}개 — 지나간 선은 장부에서 종료해 줘")
    out.append("")
    ev = _events(rows, "theme")
    out.append(f"🔥 <b>테마</b> · 이번 주 점화 {len(ev)}건")
    out += ev or ["· 점화한 테마가 없었어"]
    if baskets is not None:
        act = [x for x in baskets if x.get("status") != "종료" and (x.get("summary") or {}).get("n_px")]
        act.sort(key=lambda x: -(x["summary"].get("avg_pct") or 0))
        if act:
            day = str(act[0]["summary"].get("date") or "")[5:]
            fmt = lambda x: f"{x['name']} {_p(x['summary'].get('avg_pct'))}({x['summary']['strong']}/{x['summary']['n_px']})"
            out.append(f"· 마지막 거래일({day}) 강한 순: " + " · ".join(fmt(x) for x in act[:TOP_THEMES]))
            if len(act) > TOP_THEMES * 2:
                out.append("· 약한 순: " + " · ".join(fmt(x) for x in act[-TOP_THEMES:][::-1]))
    out.append("")
    ev = _events(rows, "buy")
    out.append(f"🔔 <b>매수 추적</b> · 이번 주 알림 {len(ev)}건")
    out += ev
    if buys is not None:
        if buys:
            out.append("· 지금: " + " · ".join(f"{it['name']} {_p(pct)}" for it, pct in sorted(buys, key=lambda r: -(r[1] if r[1] is not None else -999))))
        elif not ev:
            out.append("· 추적 중인 매수가 없어")
    for e in errors:
        out.append(f"⚠ {e}")
    out += ["", "✍ <b>이번 주에 할 일</b>", "1. 위 사건 중 하나를 골라 매매 복기에 한 줄", "2. 끝난 선은 추적 장부에서 종료", "3. 다음 주에 볼 테마 하나 정하기",
            "", "<i>봇이 실제로 보낸 알림만 모았어 · 정리용(성과 통계 아님)</i>"]
    return "\n".join(out)


def weekly_text(get_price, scanner_url, headers, get=None, now=None, rows=None):
    """지금 상태를 읽어 리뷰 글을 만든다. 한 군데를 못 읽어도 나머지는 보낸다(못 읽은 곳은 ⚠로 적는다)."""
    import requests
    get = get or requests.get
    now = now or datetime.now(KST)
    today = now.astimezone(KST).date()
    errors, lines, baskets, buys = [], None, None, None
    try:
        p = get(f"{scanner_url}/api/track-lines", params={"prices": 1}, timeout=120, headers=headers).json()
        if not p.get("ok"):
            raise RuntimeError(p.get("error") or "응답 오류")
        lines = p.get("lines") or []
    except Exception as e:
        errors.append(f"추적 장부를 못 읽었어: {e}")
    try:
        p = get(f"{scanner_url}/api/theme-baskets", params={"prices": 1}, timeout=120, headers=headers).json()
        if not p.get("ok"):
            raise RuntimeError(p.get("error") or "응답 오류")
        baskets = p.get("baskets") or []
    except Exception as e:
        errors.append(f"테마를 못 읽었어: {e}")
    try:
        import buy_track
        buys = []
        for it in buy_track.load_state()["items"]:
            close = (get_price(it["ticker"]) or {}).get("price")
            buys.append((it, buy_track.pct_of(close, it["buy"]) if close else None))
    except Exception as e:
        errors.append(f"매수 추적을 못 읽었어: {e}")
    return build_text(week_rows(rows if rows is not None else load_log(), today), today, lines, baskets, buys, errors)


def register(sched, get_price, send, scanner_url, headers):
    def run():
        try:
            send(weekly_text(get_price, scanner_url, headers))
            print("[리뷰] 주간 리뷰 발송", flush=True)
        except Exception as e:
            print(f"[리뷰] 주간 리뷰 오류: {e}", flush=True)
    sched.every().saturday.at("09:05", "Asia/Seoul").do(run)
