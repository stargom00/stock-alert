"""ALERTS 환경변수(파일명은 alerts.txt로 부르지만 실제로는 세미콜론 구분
env var — README 참고) 파싱의 조용한 실패를 시끄러운 실패로 바꾼 v2.16
검증. main.py를 그냥 import하면 모듈 최상단에서 check_alerts()가 즉시
실행되고 while True 루프로 빠져서 테스트가 멈추므로, 필요한 함수만 AST로
뽑아 격리된 네임스페이스에서 돌린다(main.py 자체는 절대 실행 안 함).
"""
import ast
import os
import re

MAIN_PY = os.path.join(os.path.dirname(__file__), "main.py")


def _extract_functions(names):
    with open(MAIN_PY, encoding="utf-8") as f:
        source = f.read()
    tree = ast.parse(source)
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names), (
        f"main.py에서 대상 함수를 못 찾음(찾은 것: {[n.name for n in nodes]}, "
        f"찾던 것: {sorted(names)}) — 함수 이름이 바뀌었을 수 있음"
    )
    return ast.Module(body=nodes, type_ignores=[])


def _load_parse_alerts(alerts_raw):
    module = _extract_functions({"parse_alerts", "format_alert_problems_message"})
    ns = {"re": re, "ALERTS_RAW": alerts_raw, "KR_TICKER_NO_SUFFIX_RE": re.compile(r"^\d{6}$")}
    exec(compile(module, MAIN_PY, "exec"), ns)
    return ns["parse_alerts"], ns["format_alert_problems_message"]


def _load_notify(sent_calls):
    module = _extract_functions({"format_alert_problems_message", "notify_alert_parse_problems"})

    def fake_send_telegram(message, chat_id=None):
        sent_calls.append(message)

    ns = {"send_telegram": fake_send_telegram}
    exec(compile(module, MAIN_PY, "exec"), ns)
    return ns["notify_alert_parse_problems"]


# ── 정상 케이스 — 기존 동작 회귀 없음 ──

def test_valid_alerts_produce_no_problems():
    parse_alerts, _ = _load_parse_alerts("AAPL,above,200;TSLA,below,150")
    alerts, problems = parse_alerts()
    assert len(alerts) == 2
    assert alerts[0] == {"ticker": "AAPL", "condition": "above", "target": 200.0, "triggered": False}
    assert problems == []


def test_kr_ticker_with_suffix_no_warning():
    parse_alerts, _ = _load_parse_alerts("005930.KS,below,75000")
    alerts, problems = parse_alerts()
    assert len(alerts) == 1
    assert problems == []


# ── 문제 ① 파트 개수 부족/과다 ──

def test_missing_parts_flagged_and_excluded():
    parse_alerts, _ = _load_parse_alerts("AAPL,above")
    alerts, problems = parse_alerts()
    assert alerts == []
    assert len(problems) == 1
    assert problems[0]["reason"] == "파트 부족 또는 형식 오류"
    assert problems[0]["raw"] == "AAPL,above"
    assert problems[0]["index"] == 1


# ── 문제 ② 목표가 숫자 변환 실패 ──

def test_non_numeric_target_flagged_and_excluded():
    parse_alerts, _ = _load_parse_alerts("AAPL,above,abc")
    alerts, problems = parse_alerts()
    assert alerts == []
    assert problems[0]["reason"] == "목표가 숫자 변환 실패"


# ── 문제 ③ condition 오타 — v2.16 이전엔 파싱은 통과하고 영원히 발동만
#    안 됐던 케이스. 이제 파싱 시점에 걸러서 alerts에서 아예 빠진다.

def test_bad_condition_flagged_and_excluded_from_alerts():
    parse_alerts, _ = _load_parse_alerts("PLTR,belw,114")
    alerts, problems = parse_alerts()
    assert alerts == []
    assert len(problems) == 1
    assert problems[0]["reason"] == "조건 오류"
    assert problems[0]["raw"] == "PLTR,belw,114"


# ── 문제 ④(신규) 한국 종목 접미사 누락 — 형식상 유효해 보여 alerts에는
#    그대로 등록되지만(발동 로직 변경 금지, 요청 4) 경고에는 포함된다.

def test_kr_ticker_missing_suffix_warns_but_still_registers():
    parse_alerts, _ = _load_parse_alerts("005930,below,75000")
    alerts, problems = parse_alerts()
    assert len(alerts) == 1
    assert alerts[0]["ticker"] == "005930"
    assert len(problems) == 1
    assert problems[0]["reason"] == "접미사 누락 — .KS 또는 .KQ 필요"


# ── 여러 항목 섞인 경우 — index가 세미콜론 기준 순번(1부터)인지 ──

def test_mixed_alerts_index_is_1_based_by_position():
    parse_alerts, _ = _load_parse_alerts("AAPL,above,200;PLTR,belw,114;TSLA,below,abc")
    alerts, problems = parse_alerts()
    assert len(alerts) == 1
    assert alerts[0]["ticker"] == "AAPL"
    assert [p["index"] for p in problems] == [2, 3]
    assert [p["reason"] for p in problems] == ["조건 오류", "목표가 숫자 변환 실패"]


# ── 발동 로직 자체는 안 건드렸는지 — above/below 조건인 항목의 필드 구조가
#    기존과 동일한지(check_alerts가 그대로 쓸 수 있어야 함).

def test_alert_dict_shape_unchanged_for_check_alerts():
    parse_alerts, _ = _load_parse_alerts("AAPL,above,200")
    alerts, _ = parse_alerts()
    assert set(alerts[0].keys()) == {"ticker", "condition", "target", "triggered"}
    assert alerts[0]["triggered"] is False


# ── 경고 메시지 포맷 ──

def test_format_alert_problems_message():
    _, format_msg = _load_parse_alerts("")
    problems = [
        {"index": 3, "raw": "PLTR,belw,114", "reason": "조건 오류"},
        {"index": 7, "raw": "005930,below,75000", "reason": "접미사 누락 — .KS 또는 .KQ 필요"},
    ]
    msg = format_msg(problems)
    lines = msg.split("\n")
    assert lines[0] == "⚠️ ALERTS 무시된 항목 2개:"
    assert "3번: PLTR,belw,114 (조건 오류)" in lines
    assert "7번: 005930,below,75000 (접미사 누락 — .KS 또는 .KQ 필요)" in lines


# ── 시작 시 발송 여부 — 정상일 땐 조용히, 문제 있을 때만 send_telegram ──

def test_notify_sends_telegram_when_problems_exist():
    sent = []
    notify = _load_notify(sent)
    notify([{"index": 1, "raw": "PLTR,belw,114", "reason": "조건 오류"}])
    assert len(sent) == 1
    assert "1번: PLTR,belw,114 (조건 오류)" in sent[0]


def test_notify_stays_silent_when_no_problems():
    sent = []
    notify = _load_notify(sent)
    notify([])
    assert sent == []


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
