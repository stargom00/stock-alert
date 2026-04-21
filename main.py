import os
import time
import requests
import schedule
from datetime import datetime

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ALERTS_RAW = os.environ.get("ALERTS", "")
MORNING_TICKERS = os.environ.get("MORNING_TICKERS", "")  # 아침 요약 종목 (예: AAPL;TSLA;005930.KS)
SURGE_THRESHOLD = float(os.environ.get("SURGE_THRESHOLD", "5"))  # 급등락 기준 % (기본 5%)

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
            alerts.append({"ticker": ticker, "condition": condition, "target": float(target), "triggered": False})
        except ValueError:
            pass
    return alerts

alerts = parse_alerts()
prev_prices = {}
print(f"[시작] 총 {len(alerts)}개 알림 설정됨")

def get_stock_data(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=5d"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        meta = data["chart"]["result"][0]["meta"]
        price = float(meta["regularMarketPrice"])
        currency = meta.get("currency", "USD")
        week52_high = float(meta.get("fiftyTwoWeekHigh", 0))
        week52_low = float(meta.get("fiftyTwoWeekLow", 0))

        # 등락률: API에서 직접 가져오기
        change_pct = float(meta.get("regularMarketChangePercent", 0)) * 100
        change = float(meta.get("regularMarketChange", 0))
        prev_close = price - change if change else price

        return {
            "price": price,
            "prev_close": round(prev_close, 2),
            "currency": currency,
            "change": round(change, 2),
            "change_pct": round(change_pct, 2),
            "week52_high": week52_high,
            "week52_low": week52_low,
        }
    except Exception as e:
        print(f"[오류] {ticker} 조회 실패: {e}")
        return None

def get_crypto_data(ticker):
    try:
        symbol_map = {
            "BTC": "bitcoin", "ETH": "ethereum", "XRP": "ripple",
            "SOL": "solana", "DOGE": "dogecoin", "ADA": "cardano",
            "LINK": "chainlink", "ONDO": "ondo-finance"
        }
        coin_id = symbol_map.get(ticker.upper())
        if not coin_id:
            return None
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd&include_24hr_change=true"
        res = requests.get(url, timeout=10)
        data = res.json()
        price = data[coin_id]["usd"]
        change_pct = data[coin_id].get("usd_24h_change", 0)
        return {"price": price, "currency": "USD", "change_pct": change_pct}
    except:
        return None

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

def format_price(price, currency):
    symbol = "₩" if currency == "KRW" else "$"
    if currency == "KRW":
        return f"{symbol}{price:,.0f}"
    return f"{symbol}{price:,.2f}"

def handle_message(text, chat_id):
    text = text.strip()
    parts = text.upper().split()
    ticker = parts[0]
    command = parts[1] if len(parts) > 1 else ""

    # 코인 먼저 시도
    crypto_tickers = ["BTC", "ETH", "XRP", "SOL", "DOGE", "ADA", "LINK", "ONDO"]
    if ticker in crypto_tickers:
        data = get_crypto_data(ticker)
        if data:
            emoji = "🚀" if data["change_pct"] >= 0 else "📉"
            send_telegram(
                f"🪙 <b>{ticker}</b> 현재가\n"
                f"${data['price']:,.2f} (USD)\n"
                f"{emoji} 24시간 등락: {data['change_pct']:+.2f}%\n"
                f"조회 시각: {datetime.now().strftime('%H:%M:%S')}",
                chat_id
            )
            return

    data = get_stock_data(ticker)
    if not data:
        send_telegram(f"❌ <b>{ticker}</b> 를 찾을 수 없어요.\n종목 코드를 확인해주세요.\n\n예: AAPL, TSLA, 005930.KS\n코인: BTC, ETH, SOL", chat_id)
        return

    price_str = format_price(data["price"], data["currency"])
    emoji = "🚀" if data["change_pct"] >= 0 else "📉"

    if command == "52주" or command == "52":
        send_telegram(
            f"📊 <b>{ticker}</b> 52주 고저\n\n"
            f"현재가: {price_str}\n"
            f"52주 최고: {format_price(data['week52_high'], data['currency'])}\n"
            f"52주 최저: {format_price(data['week52_low'], data['currency'])}\n"
            f"조회 시각: {datetime.now().strftime('%H:%M:%S')}",
            chat_id
        )
    elif command == "등락":
        send_telegram(
            f"{emoji} <b>{ticker}</b> 오늘 등락\n\n"
            f"현재가: {price_str}\n"
            f"전일 종가: {format_price(data['prev_close'], data['currency'])}\n"
            f"등락: {data['change']:+.2f} ({data['change_pct']:+.2f}%)\n"
            f"조회 시각: {datetime.now().strftime('%H:%M:%S')}",
            chat_id
        )
    else:
        # 기본 현재가
        send_telegram(
            f"📈 <b>{ticker}</b> 현재가\n"
            f"{price_str} ({data['currency']})\n"
            f"{emoji} {data['change_pct']:+.2f}% (오늘)\n"
            f"조회 시각: {datetime.now().strftime('%H:%M:%S')}",
            chat_id
        )

def check_alerts():
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] 가격 확인 중...")
    for a in alerts:
        if a["triggered"]:
            continue
        data = get_stock_data(a["ticker"])
        if not data:
            continue
        price = data["price"]
        print(f"  {a['ticker']}: 현재 {price} / 목표 {a['condition']} {a['target']}")
        fired = (a["condition"] == "above" and price >= a["target"]) or \
                (a["condition"] == "below" and price <= a["target"])
        if fired:
            a["triggered"] = True
            direction = "도달 🚀" if a["condition"] == "above" else "하락 📉"
            msg = (f"🔔 <b>주식 알림 발동!</b>\n\n"
                   f"종목: <b>{a['ticker']}</b>\n"
                   f"현재가: <b>{format_price(price, data['currency'])}</b>\n"
                   f"목표가 {format_price(a['target'], data['currency'])} {direction}\n"
                   f"시각: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            send_telegram(msg)

def check_surge():
    """급등락 감지"""
    tickers = [a["ticker"] for a in alerts]
    if MORNING_TICKERS:
        tickers += [t.strip().upper() for t in MORNING_TICKERS.split(";")]
    tickers = list(set(tickers))

    for ticker in tickers:
        data = get_stock_data(ticker)
        if not data:
            continue
        pct = data["change_pct"]
        if abs(pct) >= SURGE_THRESHOLD:
            emoji = "🚀" if pct > 0 else "📉"
            prev = prev_prices.get(ticker)
            if prev != round(pct, 1):
                prev_prices[ticker] = round(pct, 1)
                send_telegram(
                    f"{emoji} <b>{ticker} 급{'등' if pct > 0 else '락'} 알림!</b>\n\n"
                    f"현재가: {format_price(data['price'], data['currency'])}\n"
                    f"등락률: {pct:+.2f}%\n"
                    f"시각: {datetime.now().strftime('%H:%M:%S')}"
                )

def morning_summary():
    """아침 9시 요약"""
    if not MORNING_TICKERS:
        return
    tickers = [t.strip().upper() for t in MORNING_TICKERS.split(";")]
    msg = f"🌅 <b>아침 시세 요약</b> ({datetime.now().strftime('%Y-%m-%d')})\n\n"
    for ticker in tickers:
        data = get_stock_data(ticker)
        if data:
            emoji = "🟢" if data["change_pct"] >= 0 else "🔴"
            msg += f"{emoji} <b>{ticker}</b>: {format_price(data['price'], data['currency'])} ({data['change_pct']:+.2f}%)\n"
        else:
            # 코인 시도
            crypto_data = get_crypto_data(ticker)
            if crypto_data:
                emoji = "🟢" if crypto_data["change_pct"] >= 0 else "🔴"
                msg += f"{emoji} <b>{ticker}</b>: ${crypto_data['price']:,.2f} ({crypto_data['change_pct']:+.2f}%)\n"
    send_telegram(msg)

# 시작 알림
send_telegram(
    "✅ <b>얼마냐봇 시작!</b>\n\n"
    "📌 모니터링 중인 종목:\n" +
    "\n".join([f"• {a['ticker']} {'이상' if a['condition']=='above' else '이하'} {a['target']}" for a in alerts]) +
    f"\n⚡ 급등락 기준: ±{SURGE_THRESHOLD}%\n\n"
    "💬 <b>사용법:</b>\n"
    "• <code>AAPL</code> → 현재가\n"
    "• <code>AAPL 등락</code> → 오늘 등락률\n"
    "• <code>AAPL 52주</code> → 52주 고저\n"
    "• <code>BTC</code> → 비트코인 현재가\n"
    "• 코인: BTC, ETH, SOL, XRP, DOGE, ADA, LINK, ONDO"
)

# 스케줄
schedule.every(30).seconds.do(check_alerts)
schedule.every(5).minutes.do(check_surge)
schedule.every().day.at("09:00").do(morning_summary)

check_alerts()

last_update_id = None
while True:
    schedule.run_pending()
    updates = get_updates(offset=last_update_id)
    for update in updates:
        last_update_id = update["update_id"] + 1
        msg = update.get("message", {})
        text = msg.get("text", "")
        chat_id = str(msg.get("chat", {}).get("id", ""))
        if text and not text.startswith("/"):
            handle_message(text, chat_id)
    time.sleep(1)
