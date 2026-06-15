import os
from names import resolve_ticker
import time
import requests
import schedule
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))

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

import time as _time

def _pick_prev_close(result, price):
    """일봉 종가 배열에서 '어제 종가'를 안전하게 선택.
    마지막 봉이 오늘(거래소 시간 기준)이면 그 앞 봉, 아니면 마지막 봉."""
    try:
        closes = [c for c in result["indicators"]["quote"][0]["close"] if c is not None]
        ts = result.get("timestamp") or []
        if not closes:
            return None
        gmtoff = result["meta"].get("gmtoffset", 0)
        if ts:
            last_day = _time.gmtime(ts[-1] + gmtoff)[:3]   # (년,월,일)
            today = _time.gmtime(_time.time() + gmtoff)[:3]
            if last_day == today:
                return float(closes[-2]) if len(closes) >= 2 else None
            return float(closes[-1])
        # 타임스탬프가 없으면: 마지막 값이 현재가와 사실상 같으면 오늘 봉으로 간주
        if abs(closes[-1] - price) / price < 0.001 and len(closes) >= 2:
            return float(closes[-2])
        return float(closes[-1])
    except Exception:
        return None


def get_stock_data(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=10d"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        meta = data["chart"]["result"][0]["meta"]
        price = float(meta["regularMarketPrice"])
        currency = meta.get("currency", "USD")
        week52_high = float(meta.get("fiftyTwoWeekHigh", 0))
        week52_low = float(meta.get("fiftyTwoWeekLow", 0))

        # ✅ 수정2: 일봉 종가 배열에서 직접 전일 종가 추출
        # (meta의 previousClose는 종종 누락되고, chartPreviousClose는
        #  "차트 범위 시작 전 종가"라 며칠 전 가격이 잡히는 버그가 있었음)
        prev_close = _pick_prev_close(data["chart"]["result"][0], price)
        if prev_close is None:
            prev_close = float(meta.get("previousClose") or price)
        change = round(price - prev_close, 2)
        change_pct = round((change / prev_close) * 100, 2) if prev_close else 0.0

        return {
            "price": price,
            "prev_close": round(prev_close, 2),
            "currency": currency,
            "change": change,
            "change_pct": change_pct,
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


def get_market_trading_value():
    """코스피/코스닥 시장 전체 거래대금(KRX 기준) 조회.
    pykrx 사용. 실패 시 None 반환(KRX 차단·휴장 등)."""
    try:
        from pykrx import stock
        from datetime import datetime, timedelta
        # 최근 거래일 찾기 (오늘이 휴장이면 직전 영업일)
        for back in range(0, 7):
            d = (datetime.now(KST) - timedelta(days=back)).strftime("%Y%m%d")
            try:
                kospi = stock.get_index_ohlcv(d, d, "1001")    # 코스피 지수
                kosdaq = stock.get_index_ohlcv(d, d, "2001")   # 코스닥 지수
                if not kospi.empty and not kosdaq.empty:
                    return {
                        "date": d,
                        "kospi_value": int(kospi["거래대금"].iloc[0]),
                        "kosdaq_value": int(kosdaq["거래대금"].iloc[0]),
                    }
            except Exception:
                continue
        return None
    except Exception as e:
        print(f"[거래대금 조회 실패] {e}")
        return None


def get_upbit_trading_value():
    """업비트 원화마켓 24시간 누적 거래대금(KRW) 합계."""
    try:
        # 원화마켓 전체 티커
        mk = requests.get("https://api.upbit.com/v1/market/all", timeout=10).json()
        krw_markets = [m["market"] for m in mk if m["market"].startswith("KRW-")]
        total = 0.0
        # 티커를 100개씩 묶어 조회
        for i in range(0, len(krw_markets), 100):
            chunk = krw_markets[i:i+100]
            params = {"markets": ",".join(chunk)}
            r = requests.get("https://api.upbit.com/v1/ticker", params=params, timeout=10).json()
            for t in r:
                total += t.get("acc_trade_price_24h", 0)
        return total
    except Exception as e:
        print(f"[업비트 거래대금 실패] {e}")
        return None


def format_trillion(won):
    """원 단위 → '조/억' 읽기 쉽게."""
    jo = won / 1_0000_0000_0000   # 1조
    if jo >= 1:
        return f"{jo:,.1f}조원"
    eok = won / 1_0000_0000       # 1억
    return f"{eok:,.0f}억원"


def trading_value_report():
    """코스피/코스닥/업비트 거래대금 리포트 발송."""
    msg = f"💰 <b>일일 거래대금</b> ({datetime.now(KST).strftime('%Y-%m-%d')})\n"
    msg += "<i>KRX 기준 (NXT 미포함)</i>\n\n"
    mv = get_market_trading_value()
    if mv:
        total = mv["kospi_value"] + mv["kosdaq_value"]
        msg += f"📊 <b>코스피</b>: {format_trillion(mv['kospi_value'])}\n"
        msg += f"📈 <b>코스닥</b>: {format_trillion(mv['kosdaq_value'])}\n"
        msg += f"🔢 <b>합계</b>: {format_trillion(total)}\n"
    else:
        msg += "📊 코스피/코스닥: 조회 실패 (휴장 또는 일시 차단)\n"
    up = get_upbit_trading_value()
    if up:
        msg += f"\n🪙 <b>업비트(원화)</b>: {format_trillion(up)}\n"
    send_telegram(msg)


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

HELP_MESSAGE = (
    "💬 <b>얼마냐봇 사용방법</b>\n\n"
    "📈 <b>주식 조회</b>\n"
    "• <code>삼성전자</code> 또는 <code>네이버</code> → 이름으로 검색\n"
    "• <code>AAPL</code> 또는 <code>애플</code> → 현재가\n"
    "• <code>AAPL 등락</code> → 오늘 등락률\n"
    "• <code>AAPL 52주</code> → 52주 최고/최저\n"
    "• 한국 주식은 코드 뒤에 .KS(코스피)/.KQ(코스닥)\n"
    "  예: <code>005930.KS</code> (삼성전자)\n\n"
    "🪙 <b>코인 조회</b>\n"
    "• <code>BTC</code> → 비트코인 현재가\n"
    "• <code>거래대금</code> → 코스피/코스닥/업비트 거래대금\n"
    "• 지원: BTC, ETH, SOL, XRP, DOGE, ADA, LINK, ONDO\n\n"
    "🔔 <b>자동 알림</b>\n"
    "• 목표가 도달 알림 (30초마다 확인)\n"
    "• 급등락 알림 (5분마다 확인)\n"
    "• 아침 9시 시세 요약\n"
    "※ 알림 종목 변경은 Railway의 Variables에서\n"
    "  (ALERTS, MORNING_TICKERS)\n\n"
    "❓ 이 메시지 다시 보기: <code>알려줘</code> 또는 <code>사용방법</code>"
)

def handle_message(text, chat_id):
    text = text.strip()

    # 도움말 요청
    if text in ("알려줘", "사용방법", "사용법", "도움말", "도움", "help", "HELP", "?"):
        send_telegram(HELP_MESSAGE, chat_id)
        return

    # 거래대금 조회
    if text in ("거래대금", "거래대금조회", "시장", "시장거래대금"):
        send_telegram("💰 거래대금 조회 중... (몇 초 걸려요)", chat_id)
        trading_value_report()
        return

    parts = text.split()
    raw = parts[0]
    command = parts[1].upper() if len(parts) > 1 else ""
    # 한글/영문 이름이면 코드로 변환 (예: 네이버 → 035420.KS, 애플 → AAPL)
    ticker = resolve_ticker(raw).upper()

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
                f"조회 시각: {datetime.now(KST).strftime('%H:%M:%S')}",
                chat_id
            )
            return

    data = get_stock_data(ticker)
    if not data:
        send_telegram(f"❌ <b>{raw}</b> 를 찾을 수 없어요.\n이름이나 코드를 확인해주세요.\n\n예: 삼성전자, 네이버, 애플\n또는: AAPL, 005930.KS\n코인: BTC, ETH, SOL", chat_id)
        return

    price_str = format_price(data["price"], data["currency"])
    emoji = "🚀" if data["change_pct"] >= 0 else "📉"

    if command == "52주" or command == "52":
        send_telegram(
            f"📊 <b>{ticker}</b> 52주 고저\n\n"
            f"현재가: {price_str}\n"
            f"52주 최고: {format_price(data['week52_high'], data['currency'])}\n"
            f"52주 최저: {format_price(data['week52_low'], data['currency'])}\n"
            f"조회 시각: {datetime.now(KST).strftime('%H:%M:%S')}",
            chat_id
        )
    elif command == "등락":
        send_telegram(
            f"{emoji} <b>{ticker}</b> 오늘 등락\n\n"
            f"현재가: {price_str}\n"
            f"전일 종가: {format_price(data['prev_close'], data['currency'])}\n"
            f"등락: {data['change']:+.2f} ({data['change_pct']:+.2f}%)\n"
            f"조회 시각: {datetime.now(KST).strftime('%H:%M:%S')}",
            chat_id
        )
    else:
        send_telegram(
            f"📈 <b>{ticker}</b> 현재가\n"
            f"{price_str} ({data['currency']})\n"
            f"{emoji} {data['change_pct']:+.2f}% (오늘)\n"
            f"조회 시각: {datetime.now(KST).strftime('%H:%M:%S')}",
            chat_id
        )

def check_alerts():
    now = datetime.now(KST).strftime("%H:%M:%S")
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
                   f"시각: {datetime.now(KST).strftime('%Y-%m-%d %H:%M')}")
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
            # ✅ 이전 값과 1% 이상 차이날 때만 재알림
            if prev is None or abs(prev - pct) >= 1.0:
                prev_prices[ticker] = pct
                send_telegram(
                    f"{emoji} <b>{ticker} 급{'등' if pct > 0 else '락'} 알림!</b>\n\n"
                    f"현재가: {format_price(data['price'], data['currency'])}\n"
                    f"등락률: {pct:+.2f}%\n"
                    f"시각: {datetime.now(KST).strftime('%H:%M:%S')}"
                )
        else:
            # ✅ 급등락 구간 벗어나면 초기화 (재진입 감지용)
            prev_prices.pop(ticker, None)

def morning_summary():
    """아침 9시 요약"""
    if not MORNING_TICKERS:
        return
    tickers = [t.strip().upper() for t in MORNING_TICKERS.split(";")]
    msg = f"🌅 <b>아침 시세 요약</b> ({datetime.now(KST).strftime('%Y-%m-%d')})\n\n"
    for ticker in tickers:
        data = get_stock_data(ticker)
        if data:
            emoji = "🟢" if data["change_pct"] >= 0 else "🔴"
            msg += f"{emoji} <b>{ticker}</b>: {format_price(data['price'], data['currency'])} ({data['change_pct']:+.2f}%)\n"
        else:
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
    "💬 사용방법이 궁금하면 <code>알려줘</code> 라고 보내주세요!"
)

# 스케줄
schedule.every(30).seconds.do(check_alerts)
schedule.every(5).minutes.do(check_surge)
schedule.every().day.at("09:00").do(morning_summary)
schedule.every().day.at("18:00").do(trading_value_report)  # 장 마감 후 거래대금 (KST)

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
