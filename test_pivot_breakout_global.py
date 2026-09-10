"""v2.28(f0dfae5)에서 check_pivot_breakout()에 넣은 _target_fired/_pivot_near
정리 코드(`_target_fired -= _stale_target` 등)가 해당 이름에 대한 global
선언 없이 들어가는 바람에, 파이썬이 두 이름을 함수 전체에서 로컬 변수로
컴파일해버려 UnboundLocalError로 크래시하던 버그의 회귀 테스트.

main.py를 그냥 import하면 모듈 최상단에서 while True 루프로 빠져 테스트가
멈추므로(test_alerts_parsing.py와 동일한 이유), check_pivot_breakout()만
AST로 뽑아 필요한 의존성만 스텁으로 채운 네임스페이스에서 돌린다
(main.py 자체는 절대 실행 안 함).

재현 조건: pending 목록의 모든 항목이 루프 안에서 _target_fired/
_pivot_near를 만지기 전에 continue되는 상황(여기선 장외로 재현) — 실제
크래시가 났던 조건과 동일. 이 상황이 아니면 루프 중간에 먼저 죽어서
증상 위치가 달라질 뿐 근본 원인은 같다(README 대신 세션 보고 참고)."""
import ast
import os
from datetime import datetime, timezone, timedelta

import pytest

MAIN_PY = os.path.join(os.path.dirname(__file__), "main.py")
KST = timezone(timedelta(hours=9))


def _load_check_pivot_breakout():
    with open(MAIN_PY, encoding="utf-8") as f:
        source = f.read()
    tree = ast.parse(source)
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "check_pivot_breakout"]
    assert len(nodes) == 1, "main.py에서 check_pivot_breakout()을 못 찾음 — 함수 이름이 바뀌었을 수 있음"
    module = ast.Module(body=nodes, type_ignores=[])

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    class _FakeRequests:
        def get(self, url, timeout=None, headers=None):
            return _FakeResponse({"pending": [{"id": "w1", "ticker": "AAPL", "pivot": 100.0}]})

    ns = {
        "datetime": datetime,
        "KST": KST,
        "requests": _FakeRequests(),
        "SCANNER_URL": "https://example.invalid",
        "_SCANNER_HEADERS": {},
        "_pivot_state": {},
        "_kr_code": lambda ticker: None,          # AAPL -> 미국 종목 취급
        "_kr_market_open": lambda: False,
        "_us_market_open": lambda: False,         # 장외 — 루프가 여기서 continue
        "_target_fired": set(),
        "_pivot_near": set(),
    }
    exec(compile(module, MAIN_PY, "exec"), ns)
    return ns["check_pivot_breakout"], ns


def test_check_pivot_breakout_survives_when_every_pending_item_skips_early():
    """이번 크래시의 정확한 조건: pending 항목 전부가 장외/누락 등으로
    _target_fired·_pivot_near에 닿기 전에 continue되어도, 루프 뒤 정리
    코드(_stale_target = _target_fired - live_wids)에서 죽으면 안 된다."""
    check_pivot_breakout, ns = _load_check_pivot_breakout()
    check_pivot_breakout()   # 예외 없이 끝나야 함 — UnboundLocalError였다면 여기서 터짐
    # 정리 대상이 없었으니(둘 다 원소 없음) 그대로 빈 set 유지.
    assert ns["_target_fired"] == set()
    assert ns["_pivot_near"] == set()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
