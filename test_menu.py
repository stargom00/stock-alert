"""v2.87 명령어 메뉴 — 메뉴에 적힌 명령은 전부 실제로 처리돼야 한다(소스 정적 검사; main.py는 import 시 네트워크가 돈다)."""
import ast, os, re

SRC = open(os.path.join(os.path.dirname(__file__), "main.py"), encoding="utf-8").read()
TREE = ast.parse(SRC)


def _const(name):
    for n in TREE.body:
        if isinstance(n, ast.Assign) and any(getattr(t, "id", None) == name for t in n.targets):
            return ast.literal_eval(n.value)
    raise AssertionError(name)


def _handled_words():
    """handle_message 안의 `text in (...)` 튜플 + `text.startswith(...)` + 첫 단어 비교에 쓰인 모든 문자열."""
    fn = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == "handle_message")
    words = set(_const("MENU_ALIASES"))
    for n in ast.walk(fn):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            words.add(n.value)
    return words


def test_menu_commands_are_routed():
    menu = _const("MENU_MESSAGE")
    cmds = re.findall(r"<code>(.*?)</code>", menu)
    assert len(cmds) >= 15
    handled = _handled_words()
    # 종목·코인 이름은 일반 조회 경로(첫 단어 = 종목, 둘째 = 등락/52주), 훈련 명령은 training.handle로 간다
    lookup = {"삼성전자", "AAPL", "BTC"}
    training = open(os.path.join(os.path.dirname(__file__), "training.py"), encoding="utf-8").read()
    missing = []
    for c in cmds:
        w = c.split()
        if w[0] in lookup:
            if len(w) > 1:
                assert w[1] in ("등락", "52주"), c
            continue
        if w[0].startswith("훈련"):
            if w[0] not in training:
                missing.append(c)
            continue
        if c not in handled and w[0] not in handled:
            missing.append(c)
    assert not missing, missing


def test_menu_aliases_and_slash():
    al = _const("MENU_ALIASES")
    for a in ("메뉴", "명령어", "/메뉴", "/start"):
        assert a in al
    assert "이평마감" in SRC and '"이평마감", "이평 마감"' in SRC
    # 슬래시 메시지는 메뉴 별칭만 handle_message로 넘어간다 + /start@봇 → /start
    assert 'text.split("@")[0].strip() in MENU_ALIASES' in SRC
    assert 'text = text.split("@")[0].strip()' in SRC
    assert "명령어 목록: <code>메뉴</code>" in SRC          # 못 찾은 종목 답에 메뉴 안내
