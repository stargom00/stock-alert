import os
import time
import requests
import schedule
from datetime import datetime

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ALERTS_RAW = os.environ.get("ALERTS", "")

def parse_alerts():
    alerts = []
    for item in ALERTS_RAW.split(";"):
        item = item.strip()
        if not item:
            continue
        parts = item.split(",")
        if len(parts) != 3:
            continue
        ticker = parts[0].strip().upper()
        condition = parts[1].strip().lower()
        target = parts[2].strip()
        try:
            alerts.append({
                "ticker": ticker,
                "condition": condition,
                "target": float(target),
                "triggered": False
            })
        except ValueError:
            pass
    return alerts

alerts = parse_alerts()
print(f"[시작] 총 {len(alerts)}개 알림 설정됨")

def get_price(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
        currency = data["chart"]["result"][0]["meta"].get("currency", "USD")
        return float(price), currency
    except Exception as e:
        print(f"[오류] {ticker} 가격 조회 실패: {e}")
        return None, None

def send_telegram(message, chat_id=None):
    if not TELEGRAM_TOKEN:
        return
    cid = chat_id or TELEGRAM_CHAT_ID
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": cid, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[텔레그램 오류] {e}")

def get_updates(offset=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?timeout=1"
    if offset:
        url += f"&offset={offset}"
    try:
        res = requests.get(url, timeout=5)
        return res.json().get("result", [])
    except:
        return []

def handle_message(text, chat_id):
    text = text.strip()
    # "AAPL 얼마냐" 또는 "AAPL" 형태 처리
    parts = text.upper().split()
    ticker = parts[0]
    
    price, currency = get_price(ticker)
    if price:
        symbol = "₩" if currency == "KRW" else "$"
        send_telegram(
            f"📈 <b>{ticker}</b> 현재가\n{symbol}{price:,.2f} ({currency})\n"
            f"조회 시각: {datetime.now().strftime('%H:%M:%S')}",
            chat_id
        )
    else:
        send_telegram(f"❌ <b>{ticker}</b> 가격을 찾을 수 없어요.\n종목 코드를 확인해주세요.", chat_id)

def check_alerts():
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] 가격 확인 중...")
    for a in alerts:
        if a["triggered"]:
            continue
        price, currency = get_price(a["ticker"])
        if price is None:
            continue
        print(f"  {a['ticker']}: 현재 {price} / 목표 {a['condition']} {a['target']}")
        fired = (a["condition"] == "above" and price >= a["target"]) or \
                (a["condition"] == "below" and price <= a["target"])
        if fired:
            a["triggered"] = True
            direction = "도달 🚀" if a["condition"] == "above" else "하락 📉"
            symbol = "₩" if currency == "KRW" else "$"
            msg = (f"🔔 <b>주식 알림 발동!</b>\n\n"
                   f"종목: <b>{a['ticker']}</b>\n"
                   f"현재가: <b>{symbol}{price:,.2f}</b>\n"
                   f"목표가 {symbol}{a['target']:,.2f} {direction}\n"
                   f"시각: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            send_telegram(msg)

# 시작 알림
send_telegram(
    "✅ <b>얼마냐봇 시작!</b>\n\n"
    "📌 모니터링 중인 종목:\n" +
    "\n".join([f"• {a['ticker']} {'이상' if a['condition']=='above' else '이하'} {a['target']}" for a in alerts]) +
    "\n\n💬 종목 코드를 보내면 현재가를 알려드려요!\n예: <code>AAPL</code> 또는 <code>005930.KS</code>"
)

# 알림 체크 스케줄
schedule.every(30).seconds.do(check_alerts)
check_alerts()

# 메시지 수신 루프
last_update_id = None

while True:
    schedule.run_pending()
    
    # 사용자 메시지 확인
    updates = get_updates(offset=last_update_id)
    for update in updates:
        last_update_id = update["update_id"] + 1
        msg = update.get("message", {})
        text = msg.get("text", "")
        chat_id = str(msg.get("chat", {}).get("id", ""))
        
        if text and not text.startswith("/"):
            handle_message(text, chat_id)
    
    time.sleep(1)
