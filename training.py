"""매일 훈련 알림 — v2.60.

사용자 지시(2026-11-13): "내가 계속 뭘 만들고 훈련하는데 하나도 정리가 안되는 느낌이고 지속이 안돼 다 흩어져있는걸 모아야할거같아.
니가 나에게 알림을 보내고 내가 흩어져잇는걸 꺼내서 사용할수있는 시스템으로 바꿔줘. 이거 써봤자 또 까먹어"

매일 정해진 시각(기본 오클랜드 09:30 — 사용자 일정 "하루 시작은 오전 9시" 기준, AI 판단)에 요일별 오늘 할 일 하나 + 매매 훈련 홈 링크를 보낸다.
요일별 내용은 훈련장 홈(chart-trainer index.html의 DAYS)과 같다 — 바꿀 때 둘 다.
텔레그램: `훈련` 오늘 할 일 다시 받기 · `훈련시간 09:30` 시각 변경 · `훈련시간 10:00 한국` / `훈련시간 09:30 뉴질랜드` 시간대까지 변경 · `훈련끄기`/`훈련켜기`.
상태: DATA_DIR/training.json {hm, tz, on, last}.
"""
import json
import os
from datetime import datetime

import pytz

HUB_URL = "https://claude.ai/artifact/7yf8NRj7gefnok5fNWZFee"
TODO_URL = "https://claude.ai/artifact/PhZpK3F9JJjB6PoDSiw9Ny"   # v2.65 슬기 도구함(오늘 할 일 포스트잇)
TZ_ALIAS = {"한국": "Asia/Seoul", "서울": "Asia/Seoul", "뉴질랜드": "Pacific/Auckland", "오클랜드": "Pacific/Auckland"}
DEFAULT = {"hm": "09:30", "tz": "Pacific/Auckland", "on": True, "last": ""}
WINDOW_MIN = 60          # 봇이 잠깐 꺼져 있어도 이 시간 안이면 보낸다

# 월=0 … 일=6 (Python weekday). 훈련장 DAYS(JS getDay: 일=0)와 같은 내용.
DAYS = [
    ("내 계획서 점검", "진행 중 계획서마다 오늘 종가를 ③ 무효선·④ 1차 매도와 비교해서 한 줄 기록."),
    ("계획 훈련 1문제", "\"매매 계획 세우기\" 단계에서 한 문제. 미래가 가려진 실제 차트로 5칸을 고르고 결과를 확인."),
    ("빗각 추적 한 줄", "봇에 <code>빗각</code>을 보내서 선 위치를 보고, 추적 표에 닿음·지킴·이탈 한 줄."),
    ("문제 1개 + 오답 1개", "안 푼 문제 하나, 오답 노트에서 하나."),
    ("알림 종목으로 계획서 1장", "이번 주 받은 알림(이평 돌파·거래량 폭발·빗각) 중 하나로 5칸 채우기. 사지 않아도 된다 — 계획만."),
    ("주간 복기", "끝난 계획서에 결과 칸 채우기: 어느 칸이 틀렸나 한 줄. 끝난 게 없으면 진행 중인 것 점검."),
    ("쉬는 날", "원칙 카드 하나만 읽고 끝."),
]
WD = "월화수목금토일"


def _path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "training.json")


def load(path=None):
    try:
        with open(path or _path(), encoding="utf-8") as f:
            return {**DEFAULT, **json.load(f)}
    except (OSError, ValueError):
        return dict(DEFAULT)


def save(cfg, path=None):
    p = path or _path()
    with open(p + ".tmp", "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False)
    os.replace(p + ".tmp", p)


def message(local_dt):
    t, d = DAYS[local_dt.weekday()]
    return (f"📚 <b>오늘 할 일 — {WD[local_dt.weekday()]}요일 · 10분</b>\n"
            f"<b>{t}</b>\n{d}\n\n"
            f"✅ <a href=\"{TODO_URL}\">오늘 할 일 체크</a> (슬기 도구함 포스트잇)\n"
            f"👉 <a href=\"{HUB_URL}\">매매 훈련 홈 열기</a> (계획서·원칙 카드·문제가 다 여기)\n"
            f"· 매일: 주식 일지 한 줄 + 차트 사진(펜으로 산 자리·판 자리 표시)\n"
            f"· 사기 전에 계획서 5칸: ① 전략 ② 매수 ③ 무효선(종가) ④ 1차 매도 ⑤ 최종 매도")


def due(now_utc, cfg):
    """보낼 때면 그 시간대의 현지 datetime, 아니면 None. 순수 함수."""
    if not cfg.get("on", True):
        return None
    local = now_utc.astimezone(pytz.timezone(cfg["tz"]))
    h, m = map(int, cfg["hm"].split(":"))
    cur, start = local.hour * 60 + local.minute, h * 60 + m
    if not (start <= cur < start + WINDOW_MIN) or cfg.get("last") == str(local.date()):
        return None
    return local


def check(send, now_utc=None, path=None):
    cfg = load(path)
    local = due(now_utc or datetime.now(pytz.utc), cfg)
    if local is None:
        return False
    cfg["last"] = str(local.date())
    save(cfg, path)                      # 먼저 기록 — 발송 예외로 같은 날 반복 발송되지 않게
    send(message(local))
    print(f"[훈련] {local.date()} {WD[local.weekday()]} 발송")
    return True


def handle(text, path=None, now_utc=None):
    """텔레그램 명령 → 답장 문자열, 해당 없으면 None."""
    parts = text.split()
    if not parts:
        return None
    cfg = load(path)
    if parts[0] == "훈련" and len(parts) == 1:
        return message((now_utc or datetime.now(pytz.utc)).astimezone(pytz.timezone(cfg["tz"])))
    if parts[0] in ("훈련끄기", "훈련켜기") and len(parts) == 1:
        cfg["on"] = parts[0] == "훈련켜기"
        save(cfg, path)
        return f"📚 매일 훈련 알림 {'켬' if cfg['on'] else '끔'} ({cfg['hm']} {cfg['tz']})"
    if parts[0] == "훈련시간":
        if len(parts) == 1:
            return f"📚 지금 설정: 매일 {cfg['hm']} ({cfg['tz']}) · {'켜짐' if cfg['on'] else '꺼짐'}\n바꾸기: <code>훈련시간 09:30</code> 또는 <code>훈련시간 10:00 한국</code>"
        hm = parts[1].replace("시", ":00") if parts[1].endswith("시") else parts[1]
        try:
            h, m = map(int, hm.split(":"))
            assert 0 <= h < 24 and 0 <= m < 60
        except (ValueError, AssertionError):
            return "형식: <code>훈련시간 09:30</code> (뒤에 한국/뉴질랜드를 붙이면 시간대도 바뀜)"
        cfg["hm"] = f"{h:02d}:{m:02d}"
        if len(parts) >= 3:
            if parts[2] not in TZ_ALIAS:
                return "시간대는 한국 또는 뉴질랜드"
            cfg["tz"] = TZ_ALIAS[parts[2]]
        cfg["last"] = ""                 # 바꾼 시각이 오늘 아직 안 지났으면 오늘도 받는다
        save(cfg, path)
        return f"📚 매일 {cfg['hm']} ({cfg['tz']})에 오늘 할 일을 보낼게"
    return None


def register(sched, send):
    def run():
        try:
            check(send)
        except Exception as e:
            print(f"[훈련] 오류: {e}")
    sched.every(1).minutes.do(run)
