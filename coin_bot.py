"""v2.91 코인 전용 봇 — 사용자 2026-11-19 "지금 얼마냐봇에 오는 코인 알람을 새로운 봇으로 옮기고싶어",
명령어도 새 봇에서("2 응"), 받는 곳은 새 봇과 1:1 대화.

- COIN_TELEGRAM_TOKEN 이 있으면 코인 알림(signal_tracker.senders 를 거치는 것 전부: 50일선·상황판·급등숏·
  신호·포지션·요약)을 이 봇으로 보낸다. 없으면 지금처럼 얼마냐봇(TELEGRAM_TOKEN)으로 — 변수를 안 넣으면 무변화.
- 보낼 방 = COIN_CHAT_ID, 없으면 TELEGRAM_CHAT_ID. 1:1 대화의 chat id는 사람 id라 봇이 달라도 같다
  (단 새 봇에 먼저 시작(/start)을 눌러야 텔레그램이 발송을 허락한다).
- 새 봇 발송이 실패하면(시작 안 누름·방 번호 틀림·토큰 틀림) 조용히 버리지 않고, 얼마냐봇 기본 방으로
  "⚠️ 새 코인봇 발송 실패: 이유" 머리줄을 붙여 같은 내용을 보낸다.
- 새 봇이 받는 명령: 상황, 요약 자세히, 아이디, 메뉴(/start 포함). 그 밖의 말엔 메뉴로 답한다.
  주식 명령은 얼마냐봇에 그대로 있다(얼마냐봇의 코인 명령도 그대로 둔다).
"""
import html
import os

import requests

TOKEN_ENV = "COIN_TELEGRAM_TOKEN"

BOARD_CMDS = ("상황", "상황판", "코인상황", "비트상황")
DETAIL_CMDS = ("요약 자세히", "요약자세히", "자세히")
ID_CMDS = ("아이디", "방아이디", "chatid", "chat id")
MENU_CMDS = ("메뉴", "명령어", "명령", "목록", "도움말", "도움", "알려줘", "help", "menu", "?",
             "/start", "/help", "/menu", "/메뉴", "/명령어")


def token(env=None):
    env = os.environ if env is None else env
    return (env.get(TOKEN_ENV) or "").strip()


def target_chat(env=None):
    env = os.environ if env is None else env
    return (env.get("COIN_CHAT_ID") or env.get("TELEGRAM_CHAT_ID") or "").strip() or None


def send(tok, chat_id, message, post=None):
    """새 봇으로 보낸다. (성공 여부, 실패 이유). 텔레그램은 실패해도 HTTP 응답에 ok:false·description을 준다."""
    post = post or requests.post
    if not tok or not chat_id:
        return False, "토큰 또는 방 번호 없음"
    try:
        r = post(f"https://api.telegram.org/bot{tok}/sendMessage",
                 json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"}, timeout=10)
        try:
            body = r.json()
        except Exception:
            body = {}
        if body.get("ok"):
            return True, ""
        return False, str(body.get("description") or f"HTTP {getattr(r, 'status_code', '?')}")
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def make_sender(send_telegram, env=None, post=None):
    """코인 알림 발송 함수. 새 봇 토큰이 없으면 None(→ 기존 얼마냐봇 경로)."""
    env = os.environ if env is None else env
    tok = token(env)
    if not tok:
        return None
    chat = target_chat(env)

    def _send(msg):
        ok, why = send(tok, chat, msg, post=post)
        if ok:
            return
        print(f"[코인봇] 발송 실패 — {why}")
        send_telegram(f"⚠️ <b>새 코인봇 발송 실패</b>: {html.escape(why)}\n"
                      f"(새 봇에서 시작을 눌렀는지, COIN_CHAT_ID가 맞는지 확인)\n\n{msg}")
    return _send


MENU_MESSAGE = (
    "🪙 <b>코인 알림 봇</b>\n\n"
    "<code>상황</code> BTC 상황판\n"
    "<code>요약 자세히</code> 코인 요약 전체\n"
    "<code>아이디</code> 이 방 번호\n\n"
    "자동 알림: 상황판(09·15·21시) · 50일선 · 급등숏 · 신호·요약\n"
    "주식·코인 현재가 조회는 얼마냐봇에서"
)


def id_notice(chat_id):
    return "\n".join([
        f"이 방 번호: <code>{chat_id}</code>",
        "",
        "Railway Variables의 <code>COIN_CHAT_ID</code>가 이 값과 같으면 끝.",
        "다르면(또는 없고 알림이 안 오면) 이 값으로 바꾸고 재배포.",
    ])


def reply_for(text, chat_id, board=None, detail=None):
    """받은 말 → 답장 문구. board/detail은 테스트에서 바꿔 끼울 수 있게 인자로."""
    t = (text or "").strip()
    head = t.split("@")[0].strip()       # /start@봇이름
    if head in BOARD_CMDS:
        if board is None:
            import coin_board
            board = coin_board.board
        return board()
    if head in DETAIL_CMDS:
        if detail is None:
            import signal_tracker
            detail = signal_tracker.detail_text
        return detail()
    if head in ID_CMDS:
        return id_notice(chat_id)
    if head == "/start":
        return MENU_MESSAGE + "\n\n" + id_notice(chat_id)
    return MENU_MESSAGE


class Poller:
    """새 봇의 받은 메시지를 읽어 답한다. 메인 루프가 매 바퀴 poll()을 부른다(대기 없이 timeout=0)."""

    def __init__(self, tok, get=None, post=None):
        self.tok = tok
        self.offset = None
        self.get = get or requests.get
        self.post = post

    def poll(self, reply=reply_for):
        url = f"https://api.telegram.org/bot{self.tok}/getUpdates?timeout=0"
        if self.offset:
            url += f"&offset={self.offset}"
        try:
            ups = self.get(url, timeout=5).json().get("result", [])
        except Exception as e:
            print(f"[코인봇] 받기 실패 — {e}")
            return 0
        n = 0
        for u in ups:
            self.offset = u["update_id"] + 1
            m = u.get("message") or {}
            text = m.get("text") or ""
            cid = str((m.get("chat") or {}).get("id", ""))
            if not text or not cid:
                continue
            try:
                out = reply(text, cid)
            except Exception as e:
                out = f"⚠️ 처리 중 오류: {html.escape(str(e))}"
            ok, why = send(self.tok, cid, out, post=self.post)
            if not ok:
                print(f"[코인봇] 답장 실패 — {why}")
            n += 1
        return n
