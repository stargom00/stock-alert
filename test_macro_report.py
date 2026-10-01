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
    """함수 + MACRO_COL_*/MACRO_DIR_* 상수만 뽑아 실행."""
    with open(MAIN_PY, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names), f"main.py에서 {sorted(names)} 중 일부를 못 찾음"
    consts = [n for n in tree.body if isinstance(n, ast.Assign)
              and any(getattr(t, "id", "").startswith(("MACRO_COL_", "MACRO_DIR_")) for t in n.targets)]
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
    ("나스닥100", q(30708.50, 30613.25)),
    ("S&P 선물", q(7739.50, 7715.50)),
    ("미국 30년", q(5.594, 5.561, is_rate=True, high52=5.561)),
    ("미국 10년", q(5.255, 5.240, is_rate=True, high52=5.240)),
    ("코스피", q(6918.25, 6838.04)),          # v2.36 ^KS11 (2026-10-01 실조회값)
    ("코스닥", q(885.57, 849.80)),            # v2.36 ^KQ11
    ("금", q(4215.20, 4179.70)),
    ("WTI", q(89.33, 89.38)),
    ("브렌트", q(96.12, 96.16)),
]

EXPECTED_BODY = [
    "🔴나스닥100   30,708.50   +0.31%",
    "🔴S&P 선물     7,739.50   +0.31%",
    "🔴미국 30년      5.594%   +0.033  ▲52주",
    "🔴미국 10년      5.255%   +0.015  ▲52주",
    "🔴코스피       6,918.25   +1.17%",
    "🔴코스닥         885.57   +4.21%",
    "🔴금           4,215.20   +0.85%",
    "🔵WTI             89.33   -0.06%",
    "🔵브렌트          96.12   -0.04%",
]
DIRS = ("🔴", "🔵", "⚪")


def _split_dir(line):
    """맨 앞 방향 이모지 1개를 떼어 (이모지, 나머지)."""
    for d in DIRS:
        if line.startswith(d):
            return d, line[len(d):]
    raise AssertionError(f"방향 이모지 없음: {line!r}")


def _body(msg):
    head, rest = msg.split("\n", 1)
    assert rest.startswith("<pre>") and rest.endswith("</pre>")
    return head, html.unescape(rest[len("<pre>"):-len("</pre>")]).split("\n")


def test_disp_width(m):
    w = m["_disp_width"]
    assert w("WTI") == 3
    assert w("금") == 2
    assert w("나스닥 선물") == 11          # 한글 5자×2 + 공백 1
    assert w("나스닥100") == 9             # 한글 3자×2 + 숫자 3
    assert w("S&P 선물") == 8              # 영문·기호 3 + 공백 + 한글 2자
    assert w("미국 30년") == 9             # 한글 3자×2 + 공백 + 숫자 2
    assert w("코스피") == 6                # v2.36 한글 3자×2 — MACRO_COL_NAME(12) 안
    assert w("코스닥") == 6
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
        _, rest = _split_dir(line)
        assert not any(d in rest for d in DIRS), line          # 이모지 정확히 1개
        core = rest.split("  ▲")[0]
        assert w(core) == 12 + 9 + 2 + 7, line


def test_negative_and_large_values(m):
    f, w = m["format_macro_line"], m["_disp_width"]
    assert f("WTI", q(54.0, 60.0)) == "🔵WTI             54.00  -10.00%"   # 등락 7칸 꽉 채움
    # 값이 9칸 초과(123,456.78 = 10칸) → 그 줄만 밀리고 잘리지 않음
    big = f("나스닥 선물", q(123456.78, 120000.0))
    assert "123,456.78" in big and "+2.88%" in big
    assert w(_split_dir(big)[1]) == 12 + 10 + 2 + 7
    # 이름이 12칸 초과 → 잘리지 않고 밀림
    long = f("아주긴라벨이름입니다", q(1.0, 1.0))
    assert long.startswith("⚪아주긴라벨이름입니다")


def test_52w_marks(m):
    f = m["format_macro_line"]
    assert f("금", q(4300.0, 4200.0, high52=4300.0, low52=3800)).endswith("  +2.38%  ▲52주")
    assert f("WTI", q(54.0, 55.0, high52=119, low52=54.5)).endswith("  ▼52주")
    no = f("금", q(4299.0, 4200.0, high52=4300.0, low52=3800))
    assert "52주" not in no and no.endswith("+2.36%")
    assert "52주" not in f("금", q(9999.0, 4200.0))            # 52주 데이터 없음 → 생략


def test_partial_failure_keeps_alignment(m):
    rows = list(NORMAL)
    rows[2] = ("미국 30년", None)
    rows[5] = ("WTI", None)
    _, body = _body(m["format_macro_message"](rows, NOW))
    assert body[2] == "⚪미국 30년   조회 실패"
    assert body[5] == "⚪WTI         조회 실패"
    w = m["_disp_width"]
    # 실패 줄의 '조회 실패' 오른쪽 끝 = 값 열 오른쪽 끝
    assert w(_split_dir(body[2])[1]) == w(_split_dir(body[5])[1]) == 12 + 9
    assert body[:2] == EXPECTED_BODY[:2]


def test_direction_emoji(m):
    f = m["format_macro_line"]
    assert f("금", q(101.0, 100.0)).startswith("🔴")
    assert f("금", q(99.0, 100.0)).startswith("🔵")
    assert f("금", q(100.0, 100.0)) .startswith("⚪")
    assert f("금", q(100.001, 100.0)).startswith("⚪")                   # 표시상 +0.00% → 보합
    assert f("금", q(99.999, 100.0)).endswith("+0.00%")                  # -0.00% 안 나옴
    assert f("미국 10년", q(5.2501, 5.2502, is_rate=True)).startswith("⚪")
    assert f("미국 10년", q(5.2501, 5.2502, is_rate=True)).endswith("+0.000")
    assert f("미국 10년", q(5.24, 5.26, is_rate=True)).startswith("🔵")
    assert f("금", None).startswith("⚪")
    for line in (f("금", q(101.0, 100.0)), f("금", None), f("금", q(100.0, 100.0))):
        assert sum(line.count(d) for d in DIRS) == 1


def test_rate_value_percent_change_pp(m):
    """금리 값은 5.594%(값 열 오른쪽 정렬), 등락은 %p라 % 없음."""
    f, w = m["format_macro_line"], m["_disp_width"]
    rate = f("미국 30년", q(5.594, 5.561, is_rate=True))
    assert rate == "🔴미국 30년      5.594%   +0.033"
    assert rate.count("%") == 1 and rate.endswith("+0.033")
    other = f("금", q(4215.20, 4179.70))
    # 값 열 오른쪽 끝(이름 12 + 값 9 = 21칸)이 금리·일반 줄에서 같다.
    # 뒤 9자 = 간격 2 + 등락 7(ASCII)
    for line in (rate, other):
        rest = _split_dir(line)[1]
        assert w(rest[:-9]) == 12 + 9, line
    assert _split_dir(rate)[1][:-9].endswith("5.594%")
    assert f("금", q(4215.20, 4179.70)).endswith("   +0.85%")          # 일반 등락 % 유지


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


def test_sp_label_escaped(m):
    """기본 라벨 'S&P 선물'의 &는 본문에서 &amp;로, 정렬은 원문 폭 기준."""
    msg = m["format_macro_message"](NORMAL, NOW)
    assert "S&amp;P 선물" in msg and "S&P" not in msg
    _, body = _body(msg)
    assert body[1] == "🔴S&P 선물     7,739.50   +0.31%"


def test_holiday(m):
    last = {label: qq["price"] for label, qq in NORMAL}
    assert m["macro_is_holiday"](NORMAL, last) is True
    head, _ = _body(m["format_macro_message"](NORMAL, NOW, holiday=True))
    assert head == "📊 매크로 · 09-30 08:45 KST · 휴장 — 전일 값"
    changed = list(NORMAL)
    changed[0] = ("나스닥100", q(30700.00, 30613.25))
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


# ── v2.36: 매크로 심볼 목록·순서 ────────────────────────────────────

def _macro_symbols_raw():
    """main.py의 MACRO_SYMBOLS_RAW 기본값(환경변수 미설정 상태)만 뽑는다."""
    import os as _os
    tree = ast.parse(open(MAIN_PY, encoding="utf-8").read())
    for n in tree.body:
        if isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "MACRO_SYMBOLS_RAW"
                                             for t in n.targets):
            ns = {"os": _os}
            exec(compile(ast.Module(body=[n], type_ignores=[]), MAIN_PY, "exec"), ns)
            return ns["MACRO_SYMBOLS_RAW"]
    raise AssertionError("MACRO_SYMBOLS_RAW를 못 찾음")


def _pairs():
    return [tuple(p.split("|")) for p in _macro_symbols_raw().split(";") if p.strip()]


def test_kr_indices_present():
    """코스피·코스닥이 기본 목록에 있어야 한다(v2.36 — 전일 KR 마감 확인용)."""
    syms = dict(_pairs())
    assert syms.get("^KS11") == "코스피", "^KS11(코스피)이 빠졌다"
    assert syms.get("^KQ11") == "코스닥", "^KQ11(코스닥)이 빠졌다"


def test_macro_symbol_order():
    """사용자 지정 배치 순서."""
    assert [label for _, label in _pairs()] == [
        "나스닥100", "S&P 선물", "미국 30년", "미국 10년",
        "코스피", "코스닥", "금", "WTI", "브렌트",
    ]


def test_kr_indices_are_not_rate_symbols():
    """금리가 아니므로 %p가 아니라 %로 찍혀야 한다 — MACRO_RATE_SYMBOLS에 없어야 한다."""
    src = open(MAIN_PY, encoding="utf-8").read()
    line = [l for l in src.splitlines() if l.startswith("MACRO_RATE_SYMBOLS")]
    assert len(line) == 1
    assert "KS11" not in line[0] and "KQ11" not in line[0]


def test_kr_index_line_format(m):
    """지수 형식: 값 소수 둘째 자리 + 등락 % + 색 규칙(상승 🔴 / 하락 🔵 / 보합 ⚪)."""
    f = m["format_macro_line"]
    assert f("코스피", q(6918.25, 6838.04)) .startswith("🔴")
    assert "6,918.25" in f("코스피", q(6918.25, 6838.04))
    assert "+1.17%" in f("코스피", q(6918.25, 6838.04))
    assert f("코스닥", q(849.80, 885.57)).startswith("🔵")
    assert "-4.04%" in f("코스닥", q(849.80, 885.57))
    assert f("코스피", q(6918.25, 6918.25)).startswith("⚪")
