"""
main.py (얼마냐봇) 패치 (v2.7)

[변경 내용]
1. 게이트 캐시 + _gate_line() 헬퍼 신규 추가
2. check_market_gate() 교체 — 4개 지수 표시
3. check_pivot_breakout() 안의 3개 알림에 게이트 헤더 삽입
4. check_ma_near() 알림에 게이트 헤더 삽입

[배경]
기존 봇은 "시장 게이트 확인 후 진입"이라는 '문구만' 넣고 실제 게이트 값을
안 붙였음. 사람이 따로 스캐너를 열어봐야 했고, 그 사이에 감정으로 진입함.
게이트를 알림 안에 박아 넣으면 판단이 알림 시점에 끝난다.

[적용 위치]
- 아래 블록 A는 _gate_last 선언 근처(check_market_gate 위)에 추가
- 블록 B는 기존 check_market_gate 전체를 교체
- 블록 C의 헤더 삽입은 각 send_telegram lines 리스트 맨 앞에 넣음
"""

# ══════════════════════════════════════════════════════════════
# 블록 A — 게이트 캐시 + 헤더 헬퍼 (신규)
# _gate_last = {"suggest": None} 선언 아래에 추가
# ══════════════════════════════════════════════════════════════

_gate_cache = {"ts": 0, "data": None}
_GATE_CACHE_TTL = 300      # 5분. 알림마다 API 때리지 않게.

_GATE_EMOJI = {
    "confirmed": "🟢 확인된 상승",
    "pressure": "🟡 조정 압박",
    "correction": "🔴 조정",
}


def get_gate(force=False):
    """스캐너 /api/market/gate 조회 (5분 캐시). 실패 시 None."""
    now = _time.time()
    if not force and _gate_cache["data"] and now - _gate_cache["ts"] < _GATE_CACHE_TTL:
        return _gate_cache["data"]
    try:
        res = requests.get(f"{SCANNER_URL}/api/market/gate", timeout=15)
        j = res.json()
        if not j.get("ok"):
            return _gate_cache["data"]      # 실패 시 옛 캐시라도
    except Exception as e:
        print(f"[게이트] 조회 실패: {e}")
        return _gate_cache["data"]
    _gate_cache["ts"] = now
    _gate_cache["data"] = j
    return j


def _gate_line(ticker):
    """알림 헤더용 시장 게이트 한 줄. 종목의 시장(KR/US)에 맞는 게이트를 씀.

    한국 종목 → KOSPI/KOSDAQ 중 나쁜 쪽
    미국 종목 → S&P500/나스닥 중 나쁜 쪽

    ⚠️ 노출 %가 아니라 오픈 리스크 상한(R)을 보여준다. R 설정에 이미
       정의된 값(3R/1.5R/0)이고, 근거 없는 % 숫자를 새로 만들지 않는다.
    반환: 문자열 (실패 시 None)
    """
    j = get_gate()
    if not j:
        return None
    is_kr = bool(_kr_code(ticker))
    gate = j.get("gate_kr") if is_kr else j.get("gate_us")
    max_r = j.get("max_open_r_kr") if is_kr else j.get("max_open_r_us")
    if not gate:
        return None

    idx = j.get("indices") or {}
    codes = ("KOSPI", "KOSDAQ") if is_kr else ("^GSPC", "^IXIC")
    parts = []
    for code in codes:
        v = idx.get(code)
        if not v:
            continue
        d = v.get("dist_days")
        # dist_days가 None = 거래량 없어 판정 불가. 0으로 위장하지 않음.
        dtxt = "분산?" if d is None else f"분산{d}"
        parts.append(f"{v['label']} {dtxt}")

    em = _GATE_EMOJI.get(gate, gate)
    line = f"[시장] {em} · {' · '.join(parts)}"
    if gate == "correction":
        line += "\n         신규 진입 0 — 관찰만"
    else:
        line += f"\n         신규 오픈 리스크 상한 {max_r}R"
    return line


# ══════════════════════════════════════════════════════════════
# 블록 B — check_market_gate() 교체
# ══════════════════════════════════════════════════════════════

def check_market_gate():
    """시장 게이트 자동 제안 감시 (v2.7) — 30분마다, 4개 지수.
    제안이 바뀌는 순간(특히 FTD 발생 → 🟢) 텔레그램 알림."""
    j = get_gate(force=True)
    if not j:
        return
    sug = j.get("suggest")
    if not sug or sug == _gate_last["suggest"]:
        _gate_last["suggest"] = sug
        return
    prev = _gate_last["suggest"]
    _gate_last["suggest"] = sug
    if prev is None:
        return   # 봇 시작 직후 첫 관측은 알림 없이 기억만

    lines = [
        "📢 <b>시장 게이트 제안 변경</b>",
        "",
        f"{_GATE_EMOJI.get(prev, prev)} → <b>{_GATE_EMOJI.get(sug, sug)}</b>",
        f"근거: {j.get('why', '')}",
        "",
        "<b>지수별</b>",
    ]
    idx = j.get("indices") or {}
    for code in ("KOSPI", "KOSDAQ", "^GSPC", "^IXIC"):
        v = idx.get(code)
        if not v:
            lines.append(f"· {code}: 조회 실패")
            continue
        d = v.get("dist_days")
        dtxt = "판정불가(거래량없음)" if d is None else f"분산 {d}개"
        raw = v.get("dist_raw")
        # 제거 규칙으로 몇 개가 빠졌는지 — 카운트가 왜 낮은지 보여줌
        detail = ""
        if raw is not None and d is not None and raw != d:
            drops = []
            if v.get("dist_pre_ftd"):
                drops.append(f"FTD전 {v['dist_pre_ftd']}")
            if v.get("dist_expired"):
                drops.append(f"5%만료 {v['dist_expired']}")
            if drops:
                detail = f" (원시 {raw} − {', '.join(drops)})"
        ftd_txt = ""
        if v.get("ftd"):
            ftd_txt = f" · FTD {v['ftd_days_ago']}일전"
        elif v.get("in_correction"):
            ftd_txt = f" · 반등 {v.get('rally_day')}일차"
        vs = v.get("vol_source")
        vsrc = f" [거래량:{vs}]" if vs and vs not in ("index",) else ""
        lines.append(
            f"· <b>{v['label']}</b> {_GATE_EMOJI.get(v['gate'], '')[:2]} "
            f"{dtxt}{detail}{ftd_txt}{vsrc}"
        )

    lines += [
        "",
        f"한국 종목: {_GATE_EMOJI.get(j.get('gate_kr'), '?')} "
        f"(상한 {j.get('max_open_r_kr')}R)",
        f"미국 종목: {_GATE_EMOJI.get(j.get('gate_us'), '?')} "
        f"(상한 {j.get('max_open_r_us')}R)",
    ]
    if j.get("ftd"):
        lines += ["", "🔔 FTD 확인 — 시험 매수 0.5R 1~2건부터. 풀사이즈 금지."]
    cur = j.get("current")
    if cur and cur != sug:
        lines += ["", f"현재 설정({_GATE_EMOJI.get(cur, cur)})과 다름 — "
                      f"스캐너 일지 탭에서 [적용] 확인하세요."]
    send_telegram("\n".join(lines))
    print(f"[게이트] {prev} → {sug}")


# ══════════════════════════════════════════════════════════════
# 블록 C — 알림 헤더 삽입 (3곳)
#
# 각 send_telegram("\n".join(lines)) 앞에서 lines 맨 위에 게이트를 끼운다.
# 아래 패턴을 각 알림마다 적용:
#
#     _gl = _gate_line(ticker)
#     if _gl:
#         lines = [_gl, ""] + lines
#     send_telegram("\n".join(lines))
#
# 적용 대상 (main.py 안):
#   1. check_pivot_breakout() 의 🎯 목표가 도달 알림
#   2. check_pivot_breakout() 의 ⚡ 피벗 접근 알림
#   3. check_pivot_breakout() 의 🚀 피벗 돌파 알림  ← 가장 중요
#   4. check_ma_near()       의 🎯 이평 지지 접근 알림
#
# ⚠️ check_positions()의 손절/2R/마일스톤 알림에는 넣지 않는다.
#    그건 '이미 보유 중인 포지션'의 청산 관리이고, 시장 게이트와 무관하게
#    무조건 실행해야 하는 규칙이다. 게이트를 붙이면 "게이트 좋으니 손절
#    미뤄도 되나?" 하는 여지를 준다.
# ══════════════════════════════════════════════════════════════

# ── 예시: 🚀 피벗 돌파 알림 (기존 코드 수정본) ──
"""
        if price >= pivot:                     # 피벗 돌파!
            _pivot_fired.add(wid)
            name = w.get("name") or ticker
            entry = w.get("entry")
            stop = w.get("stop")
            cur = data["currency"]
            lines = [
                "🚀 <b>피벗 돌파!</b> 진입 검토",
                "",
                f"종목: <b>{name}</b> ({ticker})",
                f"현재가: <b>{format_price(price, cur)}</b>",
                f"피벗: {format_price(pivot, cur)} 돌파 ✅",
            ]
            if stop:
                lines.append(f"손절: {format_price(stop, cur)}")
            vtag, vok = volume_confirm(ticker, data.get("volume"), datetime.now(KST))
            lines.append("")
            if vok:
                lines.append(vtag)
                lines.append("→ 🟢면 진입 검토 · 🔴면 다음 기회")
            else:
                lines.append("⚠️ 거래량은 HTS에서 직접 확인 (전일 동시간 대비)")
            lines.append("피벗 +2% 추격 금지")
            lines.append(f"시각: {datetime.now(KST).strftime('%Y-%m-%d %H:%M')}")

            # ── v2.7: 게이트 헤더 삽입 ──
            # 기존엔 "시장 게이트 확인 후 진입"이라는 문구만 있어서 사람이
            # 스캐너를 따로 열어야 했다. 그 사이에 감정으로 진입함.
            # 이제 게이트가 알림 안에 박혀 있어 판단이 알림 시점에 끝난다.
            _gl = _gate_line(ticker)
            if _gl:
                lines = [_gl, ""] + lines

            send_telegram("\n".join(lines))
            print(f"  🚀 {name} 피벗돌파 {price} >= {pivot}")
"""

# ── 예시 출력 ──
"""
[시장] 🟡 조정 압박 · 코스피 분산3 · 코스닥 분산5
         신규 오픈 리스크 상한 1.5R

🚀 피벗 돌파! 진입 검토

종목: 삼성전자 (005930)
현재가: ₩74,500
피벗: ₩74,000 돌파 ✅
손절: ₩70,300

🟢 거래량 확증 (예상 180%)
→ 🟢면 진입 검토 · 🔴면 다음 기회
피벗 +2% 추격 금지
시각: 2026-07-15 10:23
"""
