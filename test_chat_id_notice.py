"""v2.61 새 방 chat id 안내. main.py는 import하면 while True로 빠지므로
test_alerts_parsing.py와 같이 AST로 함수만 뽑아 격리 실행한다."""
import ast
import os
import re

import pytest

MAIN_PY = os.path.join(os.path.dirname(__file__), "main.py")
SRC = open(MAIN_PY, encoding="utf-8").read()


def _load(names, ns=None):
    tree = ast.parse(SRC)
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names), f"main.py에서 못 찾음: {names - {n.name for n in nodes}}"
    ns = ns or {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), MAIN_PY, "exec"), ns)
    return ns


def test_notice_has_chat_id_and_var_name():
    ns = _load({"chat_id_notice"})
    out = ns["chat_id_notice"]("-1001234567890", "코인 신호")
    assert "<code>-1001234567890</code>" in out      # 복사해 넣을 값
    assert "COIN_CHAT_ID" in out                     # 어디에 넣는지
    assert "코인 신호" in out
    assert "<code>-100777</code>" in ns["chat_id_notice"]("-100777")   # 제목 없어도 됨


def test_get_updates_requests_my_chat_member():
    """allowed_updates에 my_chat_member가 없으면 방 추가 이벤트가 아예 안 온다."""
    seen = {}

    def fake_get(url, timeout=None):
        seen["url"] = url
        class R:
            @staticmethod
            def json(): return {"result": []}
        return R()

    ns = _load({"get_updates"}, {"requests": type("m", (), {"get": staticmethod(fake_get)}),
                                 "TELEGRAM_TOKEN": "T"})
    ns["get_updates"]()
    assert "my_chat_member" in seen["url"] and "message" in seen["url"]


def test_update_loop_answers_join_and_ignores_leave():
    """main.py 본문의 루프 조각을 그대로 떼어 돌린다(재구현 금지)."""
    m = re.search(r"\n(        cm = update\.get\(\"my_chat_member\"\).*?handle_message\(text, chat_id\))",
                  SRC, re.S)
    assert m, "루프 조각을 못 찾음 — main.py 구조가 바뀌었을 수 있음"
    body = "\n".join(l[8:] for l in m.group(1).split("\n"))   # 들여쓰기 8칸 제거
    sent, handled = [], []

    def run(update):
        ns = {"update": update, "send_telegram": lambda msg, cid=None: sent.append((cid, msg)),
              "handle_message": lambda t, c: handled.append((t, c)),
              "chat_id_notice": lambda cid, title="": f"id={cid}"}
        exec(compile(ast.parse("for _once in [0]:\n" + "\n".join("    " + l for l in body.split("\n"))),
                     "<loop>", "exec"), ns)

    run({"my_chat_member": {"chat": {"id": -100123, "title": "코인"},
                            "new_chat_member": {"status": "member"}}})
    assert sent == [("-100123", "id=-100123")] and handled == []
    sent.clear()
    run({"my_chat_member": {"chat": {"id": -100123}, "new_chat_member": {"status": "left"}}})
    assert sent == []                                  # 쫓겨난 건 안내 안 함
    run({"message": {"text": "아이디", "chat": {"id": 55}}})
    assert handled == [("아이디", "55")] and sent == []  # 일반 메시지는 그대로
