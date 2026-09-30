"""v2.31/v2.32 — 매크로 아침 요약 포맷(v2.32: <pre> 열 정렬) + 스케줄 판정 테스트.

main.py는 import하면 while True로 빠지므로(test_holiday_alerts.py와 같은
이유) 대상 함수만 AST로 뽑아 스텁 네임스페이스에서 돌린다."""
import ast
import html
import os
import unicodedata
import time
from datetime import datetime, timezone, timedelta

import pytest
import schedule

MAIN_PY = os.path.join(os.path.dirname(__file__), "main.py")
KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 9, 30, 8, 45, tzinfo=KST)


def _load(names, ns):
    """함수 + MACRO_COL_* 상수만 뽑아 실행."""
    with open(MAIN_PY, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names), f"main.py에서 {sorted(names)} 중 일부를 못 찾음"
    consts = [n for n in tree.body if isinstance(n, ast.Assign)
              and any(getattr(t, "id", "").startswith("MACRO_COL_") for t in n.targets)]
    ns.setdefault("html", html)
    ns.setdefault("unicodedata", unicodedata)
    exec(compile(ast.Module(body=consts + nodes, type_ignores=[]), MAIN_PY, "exec"), ns)
    return ns


@pytest.fixture
def m():
    return _load({"_disp_width", "_pad_left", "_pad_right", "format_macro_line",
                  "format_macro_message", "macro_is_holiday"}, {})


def q(price, prev, is_rate=False, high52=None, low52=None):
    return {"price": price, "prev_close": prev, "is_rate": is_rate,
            "high52": high52, "low52": low52}


NORMAL = [
    ("나스닥 선물", q(30708.50, 30613.25)),
    ("미국 30년", q(5.594, 5.561, is_rate=True, high52=5.561)),
    ("미국 10년", q(5.255, 5.240, is_rate=True, high52=5.240)),
    ("금", q(4215.20, 4179.70)),
    ("WTI", q(89.33, 89.38)),
    ("브렌트", q(96.12, 96.16)),
]

EXPECTED_BODY = [
    "나스닥 선물 30,708.50   +0.31%",
    "미국 30년      5.594%   +0.033  ▲52주",
    "미국 10년      5.255%   +0.015  ▲52주",
    "금           4,215.20   +0.85%",
    "WTI             89.33   -0.06%",
    "브렌트          96.12   -0.04%",
]


def _body(msg):
    head, rest = msg.split("\n", 1)
    assert rest.startswith("<pre>") and rest.endswith("</pre>")
    return head, html.unescape(rest[len("<pre>"):-len("</pre>")]).split("\n")


def test_disp_width(m):
    w = m["_disp_width"]
    assert w("WTI") == 3
    assert w("금") == 2
    assert w("나스닥 선물") == 11          # 한글 5자×2 + 공백 1
    assert w("미국 30년") == 9             # 한글 3자×2 + 공백 + 숫자 2
    assert w("▲52주") == 5                 # ▲는 1칸(ambiguous), 주는 2칸


def test_normal_layout(m):
    head, body = _body(m["format_macro_message"](NORMAL, NOW))
    assert head == "📊 매크로 · 09-30 08:45 KST"
    assert body == EXPECTED_BODY


def test_columns_line_up(m):
    """값·등락 열의 오른쪽 끝 표시 위치가 모든 줄에서 같다."""
    _, body = _body(m["format_macro_message"](NORMAL, NOW))
    w = m["_disp_width"]
    for line in body:
        core = line.split("  ▲")[0]
        assert w(core) == 12 + 9 + 2 + 7, line


def test_negative_and_large_values(m):
    f, w = m["format_macro_line"], m["_disp_width"]
    assert f("WTI", q(54.0, 60.0)) == "WTI             54.00  -10.00%"   # 등락 7칸 꽉 채움
    # 값이 9칸 초과(123,456.78 = 10칸) → 그 줄만 밀리고 잘리지 않음
    big = f("나스닥 선물", q(123456.78, 120000.0))
    assert "123,456.78" in big and "+2.88%" in big
    assert w(big) == 12 + 10 + 2 + 7
    # 이름이 12칸 초과 → 잘리지 않고 밀림
    long = f("아주긴라벨이름입니다", q(1.0, 1.0))
    assert long.startswith("아주긴라벨이름입니다")


def test_52w_marks(m):
    f = m["format_macro_line"]
    assert f("금", q(4300.0, 4200.0, high52=4300.0, low52=3800)).endswith("  +2.38%  ▲52주")
    assert f("WTI", q(54.0, 55.0, high52=119, low52=54.5)).endswith("  ▼52주")
    no = f("금", q(4299.0, 4200.0, high52=4300.0, low52=3800))
    assert "52주" not in no and no.endswith("+2.36%")
    assert "52주" not in f("금", q(9999.0, 4200.0))            # 52주 데이터 없음 → 생략


def test_partial_failure_keeps_alignment(m):
    rows = list(NORMAL)
    rows[1] = ("미국 30년", None)
    rows[4] = ("WTI", None)
    _, body = _body(m["format_macro_message"](rows, NOW))
    assert body[1] == "미국 30년   조회 실패"
    assert body[4] == "WTI         조회 실패"
    w = m["_disp_width"]
    # 실패 줄의 '조회 실패' 오른쪽 끝 = 값 열 오른쪽 끝
    assert w(body[1]) == w(body[4]) == 12 + 9
    assert body[0] == EXPECTED_BODY[0]


def test_total_failure(m):
    rows = [(label, None) for label, _ in NORMAL]
    assert m["format_macro_message"](rows, NOW) == "📊 매크로 조회 실패 · 09-30 08:45 KST"


def test_html_escape(m):
    rows = [("A<B>&C", q(1.0, 1.0)), ("</pre><b>", None)]
    msg = m["format_macro_message"](rows, NOW)
    assert msg.count("<pre>") == 1 and msg.count("</pre>") == 1   # 태그 탈출 불가
    assert "A&lt;B&gt;&amp;C" in msg
    assert "&lt;/pre&gt;&lt;b&gt;" in msg
    assert "<b>" not in msg


def test_holiday(m):
    last = {label: qq["price"] for label, qq in NORMAL}
    assert m["macro_is_holiday"](NORMAL, last) is True
    head, _ = _body(m["format_macro_message"](NORMAL, NOW, holiday=True))
    assert head == "📊 매크로 · 09-30 08:45 KST · 휴장 — 전일 값"
    changed = list(NORMAL)
    changed[0] = ("나스닥 선물", q(30700.00, 30613.25))
    assert m["macro_is_holiday"](changed, last) is False
    assert m["macro_is_holiday"](NORMAL, {}) is False
    rows = [(l, None if l == "금" else qq) for l, qq in NORMAL]
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
