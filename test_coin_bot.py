"""v2.91 코인 전용 봇 — 토큰 유무에 따른 발송 경로, 실패 시 얼마냐봇 경고, 명령 처리."""
import re

import coin_bot
import signal_tracker


class Resp:
    def __init__(self, body, code=200):
        self.body, self.status_code = body, code

    def json(self):
        return self.body


class FakePost:
    def __init__(self, body=None):
        self.calls = []
        self.body = body if body is not None else {"ok": True}

    def __call__(self, url, json=None, timeout=None):
        self.calls.append((url, json))
        return Resp(self.body)


def _old():
    sent = []

    def send_telegram(msg, chat_id=None):
        sent.append((msg, chat_id))
    return sent, send_telegram


def test_no_token_keeps_old_bot_path():
    sent, st = _old()
    send, _ = signal_tracker.senders(st, env={"COIN_CHAT_ID": "-100"})
    send("hi")
    assert sent == [("hi", "-100")]


def test_token_sends_via_new_bot_to_coin_chat(monkeypatch):
    post = FakePost()
    monkeypatch.setattr(coin_bot.requests, "post", post)
    sent, st = _old()
    send, sig = signal_tracker.senders(st, env={"COIN_TELEGRAM_TOKEN": "NEWTOK", "COIN_CHAT_ID": "777",
                                                "TELEGRAM_CHAT_ID": "111"})
    send("a")
    sig("b")
    assert sent == []
    assert [(u, j["chat_id"], j["text"]) for u, j in post.calls] == [
        ("https://api.telegram.org/botNEWTOK/sendMessage", "777", "a"),
        ("https://api.telegram.org/botNEWTOK/sendMessage", "777", "b")]


def test_token_without_coin_chat_uses_personal_chat(monkeypatch):
    post = FakePost()
    monkeypatch.setattr(coin_bot.requests, "post", post)
    sent, st = _old()
    send, _ = signal_tracker.senders(st, env={"COIN_TELEGRAM_TOKEN": "T", "TELEGRAM_CHAT_ID": "111"})
    send("a")
    assert post.calls[0][1]["chat_id"] == "111" and sent == []


def test_new_bot_failure_falls_back_with_warning(monkeypatch):
    post = FakePost({"ok": False, "description": "Forbidden: bot can't initiate conversation with a user"})
    monkeypatch.setattr(coin_bot.requests, "post", post)
    sent, st = _old()
    send, _ = signal_tracker.senders(st, env={"COIN_TELEGRAM_TOKEN": "T", "COIN_CHAT_ID": "777"})
    send("본문<b>x</b>")
    assert len(sent) == 1
    msg, cid = sent[0]
    assert cid is None                                    # 얼마냐봇 기본 방
    assert msg.startswith("⚠️ <b>새 코인봇 발송 실패</b>: Forbidden: bot can&#x27;t initiate")
    assert msg.endswith("본문<b>x</b>")


def test_network_error_also_falls_back(monkeypatch):
    def boom(*a, **k):
        raise ConnectionError("down")
    monkeypatch.setattr(coin_bot.requests, "post", boom)
    sent, st = _old()
    send, _ = signal_tracker.senders(st, env={"COIN_TELEGRAM_TOKEN": "T", "COIN_CHAT_ID": "7"})
    send("m")
    assert len(sent) == 1 and "ConnectionError" in sent[0][0]


def test_reply_routing():
    b = lambda: "BOARD"
    d = lambda: "DETAIL"
    r = lambda t: coin_bot.reply_for(t, "555", board=b, detail=d)
    assert r("상황") == "BOARD" and r(" 비트상황 ") == "BOARD"
    assert r("요약 자세히") == "DETAIL" and r("자세히") == "DETAIL"
    assert "<code>555</code>" in r("아이디")
    s = r("/start")
    assert s.startswith(coin_bot.MENU_MESSAGE) and "<code>555</code>" in s
    assert r("/start@seulki_coin_bot") == s
    assert r("아무말") == coin_bot.MENU_MESSAGE
    assert r("메뉴") == coin_bot.MENU_MESSAGE


def test_every_command_in_menu_is_handled():
    """메뉴에 적힌 명령이 실제로 처리되는지(없는 명령을 메뉴에 넣으면 FAIL)."""
    cmds = re.findall(r"<code>(.+?)</code>", coin_bot.MENU_MESSAGE)
    assert len(cmds) == 3
    handled = coin_bot.BOARD_CMDS + coin_bot.DETAIL_CMDS + coin_bot.ID_CMDS
    for c in cmds:
        assert c in handled, c


def test_poller_replies_and_advances_offset():
    urls = []
    batches = [{"result": [
        {"update_id": 10, "message": {"text": "/start", "chat": {"id": 555}}},
        {"update_id": 11, "message": {"chat": {"id": 555}}},               # 사진 등 글 없는 메시지
        {"update_id": 12, "message": {"text": "상황", "chat": {"id": 555}}}]},
        {"result": []}]

    def get(url, timeout=None):
        urls.append(url)
        return Resp(batches.pop(0))
    post = FakePost()
    p = coin_bot.Poller("TOK", get=get, post=post)
    assert p.poll(reply=lambda t, c: f"{t}->{c}") == 2
    assert [j["text"] for _, j in post.calls] == ["/start->555", "상황->555"]
    assert all(j["chat_id"] == "555" for _, j in post.calls)
    p.poll()
    assert urls[0].endswith("getUpdates?timeout=0") and urls[1].endswith("&offset=13")


def test_poller_reply_error_is_reported_not_raised():
    def get(url, timeout=None):
        return Resp({"result": [{"update_id": 1, "message": {"text": "상황", "chat": {"id": 9}}}]})

    def bad(t, c):
        raise RuntimeError("klines down")
    post = FakePost()
    coin_bot.Poller("T", get=get, post=post).poll(reply=bad)
    assert post.calls[0][1]["text"].startswith("⚠️ 처리 중 오류: klines down")
