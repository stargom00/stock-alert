"""v2.30 — 휴장일(2026-09-25 추석) 알림 3종 회귀 테스트.

1. 거래대금: 네이버 데이터 기준일(09-23) ≠ 오늘이면 스케줄 발송 안 함.
2. 분산 경고: 중복 방지 키 = 평가한 봉 날짜(pullback bar_date) — 같은 봉을
   다른 날(휴장일) 재평가해도 재발송 안 함.
3. 장초반 급증: pullback이 reason="no_today_bar"를 주면 hits가 있어도 발송 안 함.

main.py를 import하면 모듈 최상단에서 while True 루프로 빠지므로
(test_alerts_parsing.py와 같은 이유) 대상 함수만 AST로 뽑아 스텁
네임스페이스에서 돌린다(main.py 자체는 실행 안 함)."""
import ast
import os
from datetime import datetime, timezone, timedelta

import pytest

MAIN_PY = os.path.join(os.path.dirname(__file__), "main.py")
KST = timezone(timedelta(hours=9))
HOLIDAY = datetime(2026, 9, 25, 16, 0, tzinfo=KST)   # 금요일, 추석 휴장


def _load(names, ns):
    with open(MAIN_PY, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names), f"main.py에서 {sorted(names)} 중 일부를 못 찾음"
    exec(compile(ast.Module(body=nodes, type_ignores=[]), MAIN_PY, "exec"), ns)
    return ns


def _fixed_datetime(now):
    class _DT(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    return _DT


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


# ── 1. 거래대금 ──

def _load_trading_value(now, base_date, sent):
    ns = {
        "datetime": _fixed_datetime(now),
        "KST": KST,
        "get_market_trading_value": lambda: {
            "date": base_date, "kospi_value": 9_000_000_000_000, "kosdaq_value": 8_100_000_000_000},
        "get_upbit_trading_value": lambda: None,
        "format_trillion": lambda won: f"{won / 1e12:.1f}조원",
        "send_telegram": lambda msg, chat_id=None: sent.append(msg),
    }
    return _load({"trading_value_report", "scheduled_trading_value_report"}, ns)


def test_scheduled_trading_value_skips_when_naver_base_date_is_not_today():
    sent = []
    ns = _load_trading_value(HOLIDAY, "2026-09-23", sent)
    ns["scheduled_trading_value_report"]()
    assert sent == []   # 휴장일 — 09-23 값을 09-25 제목으로 보내면 안 됨


def test_scheduled_trading_value_sends_on_trading_day_with_base_date_title():
    sent = []
    ns = _load_trading_value(datetime(2026, 9, 23, 16, 0, tzinfo=KST), "2026-09-23", sent)
    ns["scheduled_trading_value_report"]()
    assert len(sent) == 1 and "(2026-09-23)" in sent[0]


# ── 2. 분산 경고 ──

def _load_distribution(now, fired, sent, bar_date="2026-09-23"):
    class _Requests:
        def get(self, url, timeout=None, headers=None):
            if url.endswith("/api/watch/positions"):
                return _Resp({"positions": [{"id": "p1", "ticker": "003490.KS", "name": "대한항공"}]})
            return _Resp({"ok": True, "level": "danger", "signals": ["최대급락일"],
                          "detail": {}, "bar_date": bar_date})

    ns = {
        "datetime": _fixed_datetime(now),
        "KST": KST,
        "requests": _Requests(),
        "SCANNER_URL": "https://example.invalid",
        "_SCANNER_HEADERS": {},
        "_dist_fired": fired,
        "_kr_code": lambda t: t.split(".")[0],
        "_save_sent_log": lambda: None,
        "send_telegram": lambda msg, chat_id=None: sent.append(msg),
    }
    return _load({"check_distribution"}, ns)


def test_distribution_same_bar_is_not_resent_on_a_later_holiday():
    fired, sent = {}, []
    _load_distribution(datetime(2026, 9, 23, 16, 10, tzinfo=KST), fired, sent)["check_distribution"]()
    assert len(sent) == 1 and fired == {"p1": "2026-09-23"}
    # 09-24·25 휴장일: pullback은 같은 09-23 봉을 평가 — 날짜만 바뀐 재발송 금지.
    for day in (24, 25):
        _load_distribution(datetime(2026, 9, day, 16, 10, tzinfo=KST), fired, sent)["check_distribution"]()
    assert len(sent) == 1


def test_distribution_new_bar_fires_again():
    fired, sent = {"p1": "2026-09-23"}, []
    _load_distribution(datetime(2026, 9, 28, 16, 10, tzinfo=KST), fired, sent,
                       bar_date="2026-09-28")["check_distribution"]()
    assert len(sent) == 1 and fired == {"p1": "2026-09-28"}


def test_distribution_without_bar_date_falls_back_to_send_date_and_logs(capsys):
    fired, sent = {}, []
    _load_distribution(datetime(2026, 9, 23, 16, 10, tzinfo=KST), fired, sent,
                       bar_date=None)["check_distribution"]()
    assert len(sent) == 1 and fired == {"p1": "2026-09-23"}
    assert "bar_date 없음" in capsys.readouterr().out


# ── 3. 장초반 급증 ──

def test_opening_surge_not_sent_when_pullback_reports_no_today_bar():
    sent = []

    class _Requests:
        def get(self, url, timeout=None, headers=None):
            # hits가 섞여 와도 사유가 no_today_bar면 발송 안 해야 함.
            return _Resp({"hits": [{"name": "토마토시스템", "ticker": "393210.KQ", "change_pct": 12.1,
                                    "surge_ratio": 4235.9, "value_eok": 565.0}],
                          "reason": "no_today_bar", "n_stale_excluded": 1483})

    ns = _load({"check_opening_surge"}, {
        "datetime": _fixed_datetime(datetime(2026, 9, 25, 9, 10, tzinfo=KST)),
        "KST": KST,
        "requests": _Requests(),
        "SCANNER_URL": "https://example.invalid",
        "_SCANNER_HEADERS": {},
        "_opening_surge_fired_date": None,
        "_save_sent_log": lambda: None,
        "_fmt_time_dual": lambda *a, **k: "",
        "send_telegram": lambda msg, chat_id=None: sent.append(msg),
    })
    ns["check_opening_surge"]()
    assert sent == []


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
