"""v2.31 — 매크로 아침 요약 포맷 + 스케줄 판정 테스트.

main.py는 import하면 while True로 빠지므로(test_holiday_alerts.py와 같은
이유) 대상 함수만 AST로 뽑아 스텁 네임스페이스에서 돌린다."""
import ast
import os
import time
from datetime import datetime, timezone, timedelta

import pytest
import schedule

MAIN_PY = os.path.join(os.path.dirname(__file__), "main.py")
KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 9, 30, 8, 45, tzinfo=KST)


def _load(names, ns):
    with open(MAIN_PY, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names), f"main.py에서 {sorted(names)} 중 일부를 못 찾음"
    exec(compile(ast.Module(body=nodes, type_ignores=[]), MAIN_PY, "exec"), ns)
    return ns


@pytest.fixture
def m():
    return _load({"format_macro_line", "format_macro_message", "macro_is_holiday"}, {})


def q(price, prev, is_rate=False, high52=None, low52=None):
    return {"price": price, "prev_close": prev, "is_rate": is_rate,
            "high52": high52, "low52": low52}


NORMAL = [
    ("나스닥100 선물", q(30699.75, 30613.25)),
    ("미국 30년", q(5.573, 5.594, is_rate=True)),
    ("미국 10년", q(5.244, 5.260, is_rate=True)),
    ("금", q(4216.30, 4179.70)),
    ("WTI", q(89.23, 89.38)),
    ("브렌트", q(95.71, 97.83)),
]


def test_normal(m):
    assert m["format_macro_message"](NORMAL, NOW).split("\n") == [
        "📊 매크로 · 09-30 08:45 KST",
        "나스닥100 선물  30,699.75  +0.28%",
        "미국 30년  5.573%  -0.021",
        "미국 10년  5.244%  -0.016",
        "금  4,216.30  +0.88%",
        "WTI  89.23  -0.17%",
        "브렌트  95.71  -2.17%",
    ]


def test_partial_failure(m):
    rows = list(NORMAL)
    rows[1] = ("미국 30년", None)
    rows[4] = ("WTI", None)
    lines = m["format_macro_message"](rows, NOW).split("\n")
    assert lines[0] == "📊 매크로 · 09-30 08:45 KST"
    assert lines[2] == "미국 30년  조회 실패"
    assert lines[5] == "WTI  조회 실패"
    assert lines[1] == "나스닥100 선물  30,699.75  +0.28%"
    assert len(lines) == 7


def test_total_failure(m):
    rows = [(label, None) for label, _ in NORMAL]
    assert m["format_macro_message"](rows, NOW) == "📊 매크로 조회 실패 · 09-30 08:45 KST"


def test_52w_high_low(m):
    f = m["format_macro_line"]
    assert f("금", q(4300.0, 4200.0, high52=4299.9, low52=3800)).endswith("+2.38% · 52주 최고")
    assert f("금", q(4300.0, 4200.0, high52=4300.0, low52=3800)).endswith(" · 52주 최고")  # 타이도 경신
    assert "52주" not in f("금", q(4299.0, 4200.0, high52=4300.0, low52=3800))
    assert f("WTI", q(54.0, 55.0, high52=119, low52=54.5)).endswith(" · 52주 최저")
    assert f("미국 10년", q(5.30, 5.25, is_rate=True, high52=5.29, low52=3.9)) == \
        "미국 10년  5.300%  +0.050 · 52주 최고"
    # 52주 데이터를 못 구하면(None) 표기 생략
    assert "52주" not in f("금", q(9999.0, 4200.0))


def test_holiday(m):
    last = {label: qq["price"] for label, qq in NORMAL}
    assert m["macro_is_holiday"](NORMAL, last) is True
    assert "휴장 — 전일 값" in m["format_macro_message"](NORMAL, NOW, holiday=True).split("\n")[0]
    changed = list(NORMAL)
    changed[0] = ("나스닥100 선물", q(30700.00, 30613.25))
    assert m["macro_is_holiday"](changed, last) is False
    assert m["macro_is_holiday"](NORMAL, {}) is False           # 직전 기록 없음
    rows = [(l, None if l == "금" else qq) for l, qq in NORMAL]  # 실패 줄은 비교 제외
    assert m["macro_is_holiday"](rows, last) is True


# ── 스케줄: 서버 TZ와 무관하게 08:45 KST ──────────────────────────────
@pytest.mark.parametrize("server_tz", ["UTC", "America/New_York", "Pacific/Auckland", "Asia/Seoul"])
def test_schedule_is_0845_kst_regardless_of_server_tz(server_tz, monkeypatch):
    monkeypatch.setenv("TZ", server_tz)
    time.tzset()
    try:
        ns = _load({"register_macro_schedule"}, {"MACRO_TIME": "08:45", "macro_report": lambda: None})
        sched = schedule.Scheduler()
        job = ns["register_macro_schedule"](sched)
        # schedule의 next_run은 서버 로컬 naive datetime
        next_kst = job.next_run.astimezone().astimezone(KST)
        assert (next_kst.hour, next_kst.minute) == (8, 45), f"{server_tz}: {next_kst}"
        assert timedelta(0) < next_kst - datetime.now(KST) <= timedelta(days=1)
    finally:
        monkeypatch.delenv("TZ")
        time.tzset()
