import os
import time
import requests
import schedule
from datetime import datetime

# ── 환경변수에서 설정 읽기 ──────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ALERTS_RAW = os.environ.get("ALERTS", "")
# ALERTS 형식: "AAPL,above,200;TSLA,below,150;005930.KS,above,80000"

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
for a in alerts:
    print(f"  - {a['ticker']} {a['condition']} {a['target']}")

def get_price(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
        return float(price)
    except Exception as e:
        print(f"[오류] {ticker} 가격 조회 실패: {e}")
        return None

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[알림 미전송] 토큰 또는 채팅 ID 없음: {message}")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    res = requests.post(url, json=payload)
    if res.status_code == 200:
        print(f"[텔레그램 전송 성공] {message}")
    else:
        print(f"[텔레그램 전송 실패] {res.status_code} {res.text}")

def check():
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] 가격 확인 중...")
    for a in alerts:
        if a["triggered"]:
            continue
        price = get_price(a["ticker"])
        if price is None:
            continue
        print(f"  {a['ticker']}: 현재 {price} / 목표 {a['condition']} {a['target']}")
        fired = (a["condition"] == "above" and price >= a["target"]) or \
                (a["condition"] == "below" and price <= a["target"])
        if fired:
            a["triggered"] = True
            direction = "도달 🚀" if a["condition"] == "above" else "하락 📉"
            msg = (f"🔔 <b>주식 알림 발동!</b>\n\n"
                   f"종목: <b>{a['ticker']}</b>\n"
                   f"현재가: <b>${price:,.2f}</b>\n"
                   f"목표가 ${a['target']:,.2f} {direction}\n"
                   f"시각: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            send_telegram(msg)

# 시작 알림
send_telegram("✅ 주식 알림 봇 시작!\n\n모니터링 중인 종목:\n" +
              "\n".join([f"• {a['ticker']} {'이상' if a['condition']=='above' else '이하'} {a['target']}" for a in alerts]))

# 30초마다 실행
schedule.every(30).seconds.do(check)

# 시작 시 즉시 1회 실행
check()

while True:
    schedule.run_pending()
    time.sleep(1)
