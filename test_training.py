"""v2.60 매일 훈련 알림."""
from datetime import datetime

import pytz

import training as t

UTC = pytz.utc
AKL = pytz.timezone("Pacific/Auckland")


def at(y, mo, d, h, mi, tz=AKL):
    return tz.localize(datetime(y, mo, d, h, mi)).astimezone(UTC)


def test_due_window_and_once_per_day(tmp_path):
    p = str(tmp_path / "tr.json")
    sent = []
    assert not t.check(sent.append, at(2026, 11, 13, 9, 29), p)
    assert t.check(sent.append, at(2026, 11, 13, 9, 31), p)          # 금요일
    assert not t.check(sent.append, at(2026, 11, 13, 9, 45), p)      # 같은 날 두 번 안 보냄
    assert not t.check(sent.append, at(2026, 11, 13, 10, 31), p)     # 창(60분) 밖
    assert len(sent) == 1 and "금요일" in sent[0] and "알림 종목으로 계획서 1장" in sent[0] and t.HUB_URL in sent[0]
    assert t.check(sent.append, at(2026, 11, 14, 9, 30), p) and "주간 복기" in sent[-1]


def test_days_match_trainer_hub():
    # 훈련장 홈 DAYS(JS, 일=0)와 같은 제목 — 한쪽만 바꾸면 여기서 깨진다(훈련장은 별도 저장소라 문자열로 고정)
    assert [d[0] for d in t.DAYS] == ["내 계획서 점검", "계획 훈련 1문제", "빗각 추적 한 줄", "문제 1개 + 오답 1개",
                                     "알림 종목으로 계획서 1장", "주간 복기", "쉬는 날"]


def test_commands(tmp_path):
    p = str(tmp_path / "tr.json")
    assert "10:00 (Asia/Seoul)" in t.handle("훈련시간 10:00 한국", p)
    cfg = t.load(p)
    assert cfg["hm"] == "10:00" and cfg["tz"] == "Asia/Seoul"
    sent = []
    assert t.check(sent.append, at(2026, 11, 16, 10, 5, pytz.timezone("Asia/Seoul")), p) and "월요일" in sent[0]
    assert "형식" in t.handle("훈련시간 25:00", p)
    assert "끔" in t.handle("훈련끄기", p)
    assert not t.check(sent.append, at(2026, 11, 17, 10, 5, pytz.timezone("Asia/Seoul")), p)
    assert "켬" in t.handle("훈련켜기", p)
    assert "오늘 할 일" in t.handle("훈련", p)
    assert t.handle("NVDA", p) is None and t.handle("훈련 계획", p) is None


def test_wired_in_main():
    src = open("main.py", encoding="utf-8").read()
    assert src.count("training.register(schedule, send_telegram)") == 1
    assert src.count("_tr = training.handle(text)") == 1
