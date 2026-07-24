import os
from names import resolve_ticker
import time
import re
import requests
import schedule
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ALERTS_RAW = os.environ.get("ALERTS", "")
MORNING_TICKERS = os.environ.get("MORNING_TICKERS", "")  # 아침 요약 종목 (예: AAPL;TSLA;005930.KS)
SURGE_THRESHOLD = float(os.environ.get("SURGE_THRESHOLD", "5"))  # 급등락 기준 % (기본 5%)
# 눌림목 스캐너 대기종목 API (피벗 돌파 감시용)
SCANNER_URL = os.environ.get("SCANNER_URL", "https://pullback-production.up.railway.app")
_pivot_fired = {}      # v2.8: {종목id: "YYYY-MM-DD"} — 하루 1회. 날짜 바뀌면 재돌파 알림 가능
                       # (기존 영구 set은 한번 알림 후 봇 재시작 전까지 재돌파를 영원히 무시했음)
_pivot_state = {}      # v2.8: {종목id: {"pivot": float, "fired": "date", "retest_fired": bool}}
                       # 돌파 알림 후 리테스트(피벗 되돌림) 감시용.
                       # ⚠️ 메모리 저장 — 봇 재배포 시 리셋됨 (봇에 영구볼륨 없음, 선언된 한계)
_target_fired = set()  # 하향 목표가 도달 알림 보낸 종목 id (v2.6)

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


import re as _re

def _kr_code(ticker):
    """한국 종목이면 6자리 코드 반환, 아니면 None. 접미사(.KS/.KQ)는 무시 —
    사용자가 잘못 붙여도(코스닥 종목에 .ks 등) 정확히 조회되게."""
    m = _re.match(r"^(\d{6})(\.(KS|KQ))?$", ticker.strip().upper())
    return m.group(1) if m else None


def get_kr_quote_naver(code6):
    """네이버 실시간 시세 — 한국 종목의 정확한 소스 (v2.x 수정).
    배경: 야후에 잘못된 접미사(예: 코스닥 종목의 .KS)로 유령 데이터가 존재해
    몇 달 전 가격이 현재가로 나오는 사고 발생 (094840 → -43.82% 오표시).
    네이버는 6자리 코드만 쓰므로 접미사 문제 자체가 없음."""
    try:
        url = f"https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{code6}"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        j = res.json()
        datas = j.get("result", {}).get("areas", [{}])[0].get("datas", [])
        if not datas:
            return None
        d = datas[0]
        price = float(d.get("nv") or 0)            # 현재가
        prev = float(d.get("sv") or 0)             # 기준가(전일종가)
        if price <= 0 or prev <= 0:
            return None
        change = round(price - prev, 2)
        return {
            "price": price,
            "prev_close": prev,
            "currency": "KRW",
            "change": change,
            "change_pct": round(change / prev * 100, 2),
            "week52_high": float(d.get("hv52") or 0) or 0.0,
            "week52_low": float(d.get("lv52") or 0) or 0.0,
            "volume": float(d.get("aq") or 0),   # 당일 누적 거래량 (v2.3)
        }
    except Exception as e:
        print(f"[네이버] {code6} 조회 실패: {e}")
        return None


def _find_code_candidates(obj, out):
    """JSON 어디에 있든 6자리 숫자 코드를 수집 (응답 구조 무관 방어적 탐색).
    dict이면 code/cd/itemCode 키 우선, 아니면 모든 값 재귀."""
    if isinstance(obj, dict):
        for key in ("code", "cd", "itemCode", "cmp_cd", "symbolCode"):
            v = obj.get(key)
            if isinstance(v, str) and _re.match(r"^\d{6}$", v):
                name = obj.get("name") or obj.get("nm") or obj.get("itemName") or ""
                out.append((v, str(name)))
        for v in obj.values():
            _find_code_candidates(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _find_code_candidates(v, out)
    elif isinstance(obj, str):
        if _re.match(r"^\d{6}$", obj):
            out.append((obj, ""))


def naver_name_search(text):
    """한글 종목명 → 6자리 코드. 네이버 자동완성 엔드포인트 3종을 순서대로 시도,
    응답 구조가 달라도 6자리 코드를 재귀 탐색으로 추출 (v2 — 구조 추정 실패 대응)."""
    q = text.strip()
    headers = {"User-Agent": "Mozilla/5.0",
               "Referer": "https://finance.naver.com/"}
    endpoints = [
        # 1) 모바일 프론트 API
        ("https://m.stock.naver.com/front-api/search/autoComplete",
         {"query": q, "target": "stock"}),
        # 2) 주식 자동완성
        ("https://ac.stock.naver.com/ac",
         {"q": q, "target": "stock,index,marketindicator"}),
        # 3) 구형 금융 자동완성 (EUC-KR)
        ("https://ac.finance.naver.com/ac",
         {"q": q, "q_enc": "euc-kr", "st": "111", "frm": "stock",
          "r_format": "json", "r_enc": "utf-8", "r_unicode": "0", "t_koreng": "1"}),
    ]
    for url, params in endpoints:
        try:
            res = requests.get(url, params=params, headers=headers, timeout=8)
            if res.status_code != 200:
                continue
            j = res.json()
            cands = []
            _find_code_candidates(j, cands)
            if not cands:
                continue
            # 이름이 질의와 겹치는 후보 우선, 없으면 첫 후보
            qn = q.replace(" ", "")
            for code, name in cands:
                nn = name.replace(" ", "")
                if nn and (qn in nn or nn in qn):
                    return code
            return cands[0][0]
        except Exception as e:
            print(f"[자동완성] {url} 실패: {e}")
            continue
    return None


def get_stock_data(ticker):
    # ── 한국 종목: 네이버 우선, 실패 시 야후 폴백(.KQ→.KS, ±31% 새너티) ──
    code6 = _kr_code(ticker)
    if code6:
        q = get_kr_quote_naver(code6)
        if q:
            return q
        # 네이버 실패 → 야후 폴백: 두 접미사 모두 시도, 상하한(±30%) 위반 데이터 거부
        for sfx in (".KQ", ".KS"):
            q = _get_stock_data_yahoo(code6 + sfx)
            if q and abs(q.get("change_pct", 0)) <= 31:
                return q
        return None
    return _get_stock_data_yahoo(ticker)


def _get_stock_data_yahoo(ticker):
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
            "volume": float(meta.get("regularMarketVolume") or 0),  # 당일 누적 거래량 (v2.9)
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


# ══════════════════════════════════════════════════════════════
# 코스피/코스닥 거래대금 — 네이버 금융 버전
# (pykrx가 KRX 로그인 요구로 막혀서 교체. 스캐너와 동일한 네이버 패턴)
# ══════════════════════════════════════════════════════════════
_NAVER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Referer": "https://m.stock.naver.com/",
}

# 거래대금이 들어있을 법한 키 후보 (네이버가 쓰는 이름들)
_VALUE_KEYS = [
    "accumulatedTradingValue",   # 누적 거래대금
    "tradingValue",
    "accTradePrice",
    "tradeAmount",
    "amount",
]


def _to_won(raw):
    """'12,345' / 12345 / '12345.6' 등 → float. 실패 시 None.
    단위(원/백만원) 보정은 호출부에서."""
    if raw is None:
        return None
    try:
        if isinstance(raw, (int, float)):
            return float(raw)
        s = str(raw).replace(",", "").strip()
        m = re.search(r"-?\d+(\.\d+)?", s)
        if not m:
            return None
        return float(m.group())
    except Exception:
        return None


def _naver_index_trading_value(symbol):
    """네이버에서 지수(KOSPI/KOSDAQ)의 당일 거래대금을 가져온다.
    여러 엔드포인트를 순서대로 시도(하나 막혀도 다른 게 동작). 실패 시 None."""
    endpoints = [
        f"https://m.stock.naver.com/api/index/{symbol}/basic",
        f"https://m.stock.naver.com/api/index/{symbol}/integration",
        f"https://api.stock.naver.com/index/{symbol}/basic",
    ]
    for url in endpoints:
        try:
            res = requests.get(url, headers=_NAVER_HEADERS, timeout=10)
            if res.status_code != 200:
                continue
            data = res.json()

            # 탐색 대상 dict들을 모음 (응답 구조가 엔드포인트마다 다름)
            candidates = []
            if isinstance(data, dict):
                candidates.append(data)
                for k in ("result", "stockInfo", "indexInfo"):
                    v = data.get(k)
                    if isinstance(v, dict):
                        candidates.append(v)
                    elif isinstance(v, list):
                        candidates.extend([x for x in v if isinstance(x, dict)])

            # 1) 직접 키로 탐색
            for obj in candidates:
                for key in _VALUE_KEYS:
                    if key in obj and obj[key] not in (None, ""):
                        return _to_won(obj[key])

            # 2) totalInfos 형태: [{"code":"accumulatedTradingValue","value":"12,345"}...]
            for src in [data] + candidates:
                tis = src.get("totalInfos") if isinstance(src, dict) else None
                if isinstance(tis, list):
                    for it in tis:
                        if isinstance(it, dict) and it.get("code") in _VALUE_KEYS:
                            return _to_won(it.get("value"))
        except Exception as e:
            print(f"[네이버 지수 조회 오류] {symbol} {url}: {e}")
            continue
    return None


def get_market_trading_value():
    """코스피/코스닥 거래대금 — 네이버 금융에서 조회.
    반환: {"date","kospi_value","kosdaq_value"} (원 단위) 또는 None.

    ⚠️ 단위 주의: 네이버 거래대금 필드 단위가 '백만원'일 수 있음.
       Railway 로그의 [네이버 거래대금 원시값] 을 보고 자릿수가 안 맞으면
       _UNIT_MULTIPLIER 를 조정:
       - 결과가 너무 작게(예 0.0x조) 나오면 → 백만원 단위 → 1_000_000
       - 결과가 정상이면 → 1
    """
    _UNIT_MULTIPLIER = 1_000_000   # 네이버가 '백만원' 단위로 줄 때. 안 맞으면 1로.

    kospi_raw = _naver_index_trading_value("KOSPI")
    kosdaq_raw = _naver_index_trading_value("KOSDAQ")

    # 진단용 출력 (Railway 로그에서 실제 원시값 확인 → 단위/엔드포인트 검증)
    print(f"[네이버 거래대금 원시값] KOSPI={kospi_raw}, KOSDAQ={kosdaq_raw}")

    if kospi_raw is None or kosdaq_raw is None:
        return None

    return {
        "date": datetime.now(KST).strftime("%Y%m%d"),
        "kospi_value": int(kospi_raw * _UNIT_MULTIPLIER),
        "kosdaq_value": int(kosdaq_raw * _UNIT_MULTIPLIER),
    }


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

def scheduled_trading_value_report():
    """스케줄(매일 16:00)용 거래대금 리포트. 주말(토/일)엔 자동 발송 안 함.
    수동 '거래대금' 검색은 trading_value_report()를 직접 부르므로 영향 없음."""
    if datetime.now(KST).weekday() >= 5:   # 5=토, 6=일
        print(f"[거래대금 스케줄] 주말이라 건너뜀")
        return
    trading_value_report()

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
    "• 🚀 일지 대기종목 피벗 돌파 알림 (1분마다 확인)\n"
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
    if not data and _re.search(r"[가-힣]", raw):
        # names.py에 없는 한글 이름 → 네이버 자동완성으로 코드 해석 (신규 상장·개명 대응)
        code = naver_name_search(raw)
        if code:
            ticker = code
            data = get_stock_data(code)
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

def _session_elapsed_ratio(now_kst):
    """한국장(09:00~15:30, 6.5h) 경과 비율. 장외(장 시작 전 포함)면 1.0(종가 확정).

    v2.11 버그수정: 원래 코드는 `cur_min <= open_min`(09:00 이하 전부)일 때
    0.01을 반환했음 — 이건 "막 개장한 순간"의 0나눗 방지용인데, 자정(00:00)부터
    새벽까지도 전부 이 조건에 걸려서 같은 0.01이 나왔음. 자정엔 실제로는 어제
    장이 끝난 지 한참 지나 거래량이 그날 최종치로 멈춰있는 상태인데, 이걸 "1%만
    지났다"고 계산해 100배 가까이 부풀림 → 실거래량 부족한 종목이 "거래량 확증
    (예상 1000%+)"로 잘못 뜨는 사고 발생(안국약품/엘티씨, 2026-07-22 00:00 KST).
    장 시작 전(자정~09:00)과 장 마감 후를 구분 없이 전부 '장외 = 1.0'으로
    통일하고, 개장 직후 0나눗 방지만 elapsed에 최소값(0.01)으로 처리."""
    open_min, close_min = 9 * 60, 15 * 60 + 30
    cur_min = now_kst.hour * 60 + now_kst.minute
    if cur_min < open_min or cur_min >= close_min:
        return 1.0
    return max(0.01, (cur_min - open_min) / (close_min - open_min))


def _session_elapsed_ratio_us(now_kst):
    """미국장(22:30~05:00 KST, 6.5h) 경과 비율. 자정을 넘어가는 세션이라
    KST 00:00~06:00은 '전날 22:30부터 이어진 시간'으로 취급해 분 단위를 24h+로 보정.

    v2.11 버그수정: 한국장 버전과 같은 종류의 버그 — 개장 전 전부를 0.01로
    처리해서, 낮 시간대(예: 오전 10시, 미국장은 새벽 5시에 이미 마감해 몇 시간째
    닫혀있는 상태)도 "막 개장한 순간"과 똑같이 0.01로 계산돼 거래량이 크게
    부풀려짐. 장외(개장 전 포함)는 전부 1.0으로 통일."""
    open_min, close_min = 22 * 60 + 30, 24 * 60 + 5 * 60
    cur_min = now_kst.hour * 60 + now_kst.minute
    if cur_min < 6 * 60:
        cur_min += 24 * 60
    if cur_min < open_min or cur_min >= close_min:
        return 1.0
    return max(0.01, (cur_min - open_min) / (close_min - open_min))


def volume_confirm(ticker, cur_volume, now_kst):
    """돌파 거래량 확증 (v2.3): 실시간 누적 거래량을 시간보정해 예상 종가
    거래량비 계산. 평균은 스캐너 /api/vol에서. 반환 (표시문자열, 신뢰여부).

    v2.9: 한국 전용이던 걸 미국 종목도 지원. 원인은 두 가지였음 —
    (1) _get_stock_data_yahoo가 애초에 실시간 누적 거래량(volume)을 안 돌려줘서
        cur_volume가 항상 None → 이 함수 진입 자체를 못 함.
    (2) code = _kr_code(ticker)가 미국 티커에선 항상 None이라, 그 이후 로직이
        전부 한국 전용(코드 있어야 스캐너 조회, 한국장 시간으로 경과비율 계산)
        구조였음. 미국은 스캐너에 티커 그대로(.KQ/.KS 없이) 조회하고, 경과비율도
        미국장 시간(22:30~05:00 KST) 기준으로 따로 계산하게 분기."""
    if not cur_volume:
        return None, False
    code = _kr_code(ticker)
    try:
        if code:
            res = requests.get(f"{SCANNER_URL}/api/vol/{code}.KQ", timeout=8)
            j = res.json()
            if not j.get("ok"):
                res = requests.get(f"{SCANNER_URL}/api/vol/{code}.KS", timeout=8)
                j = res.json()
        else:
            res = requests.get(f"{SCANNER_URL}/api/vol/{ticker.upper()}", timeout=8)
            j = res.json()
        avg = j.get("avg_volume_50") or 0
    except Exception:
        return None, False
    if not avg:
        return None, False
    session_ratio = _session_elapsed_ratio(now_kst) if code else _session_elapsed_ratio_us(now_kst)
    ratio = (cur_volume / session_ratio) / avg
    pct = int(ratio * 100)
    early = session_ratio <= 0.05
    if ratio >= 1.5:
        tag = f"🟢 거래량 확증 (예상 {pct}%)"
    elif ratio >= 1.0:
        tag = f"🟡 거래량 애매 (예상 {pct}%)"
    else:
        tag = f"🔴 거래량 부족 (예상 {pct}%) — 가짜 돌파 의심"
    if early:
        tag += " ※장초반 추정 신뢰↓"
    return tag, True


_pivot_near = set()      # 접근 예고 발송 기록
_pos_fired = {}          # {포지션id: {'stop', '2R', '3R', ...}} 발송 기록 (재시작 시 초기화)
_pos_last_price = {}     # {포지션id: 직전 폴링(2분 전) 가격} — 급락 감지용 (v2.12)
_flash_fired = {}        # {포지션id: 마지막 급락 알림 시각} — 같은 하락에 반복 알림 방지 (재시작 시 초기화)
FLASH_DROP_PCT = -5.0    # 진입 종목이 폴링 주기(2분) 사이 이 이상 빠지면 급락 알림
FLASH_COOLDOWN_MIN = 10  # 종목당 재알림 쿨다운(분)


def check_positions():
    """진입중 포지션의 R 진행률 감시 (v2.1) — 2분마다.
    +2R: 절반 익절 + 손절 본전 이동 안내 / +3R 이상: 마일스톤 / 손절가 터치: 실행 알림.
    각 이벤트는 포지션당 1회만.
    v2.12: 폴링 주기(2분) 사이 -5% 이상 급락하면 별도 급락 알림(반복 쿨다운 10분).
    R마일스톤과 달리 "짧은 시간 급변"이 신호라 며칠에 걸친 하락과는 구분해야 해서
    직전 폴링 가격(_pos_last_price)과만 비교한다 — entry/stop 대비가 아님."""
    now = datetime.now(KST).strftime("%H:%M:%S")
    try:
        res = requests.get(f"{SCANNER_URL}/api/watch/positions", timeout=10)
        positions = res.json().get("positions", [])
    except Exception as e:
        print(f"[{now}] 포지션 조회 실패: {e}")
        return
    if not positions:
        return
    print(f"[{now}] 포지션 R 감시: {len(positions)}종목")
    for p in positions:
        pid = p.get("id")
        entry, stop = p.get("entry"), p.get("stop")
        ticker = p.get("ticker")
        if not pid or not ticker or not entry or not stop or stop >= entry:
            continue
        data = get_stock_data(ticker)
        if not data:
            continue
        price = data["price"]
        cur = data["currency"]
        name = p.get("name") or ticker
        fired = _pos_fired.setdefault(pid, set())
        r_now = (price - entry) / (entry - stop)

        # 🔻 급락 감지 (v2.12): 직전 폴링(2분 전) 대비 -5% 이상 하락.
        prev_price = _pos_last_price.get(pid)
        _pos_last_price[pid] = price
        if prev_price and prev_price > 0:
            drop_pct = (price - prev_price) / prev_price * 100
            if drop_pct <= FLASH_DROP_PCT:
                last_fire = _flash_fired.get(pid)
                now_dt = datetime.now(KST)
                if not last_fire or (now_dt - last_fire).total_seconds() >= FLASH_COOLDOWN_MIN * 60:
                    _flash_fired[pid] = now_dt
                    send_telegram("\n".join([
                        "🔻 <b>급락 감지</b>",
                        "",
                        f"종목: <b>{name}</b> ({ticker})",
                        f"{format_price(prev_price, cur)} → <b>{format_price(price, cur)}</b> ({round(drop_pct, 1)}%, 2분 폴링 기준)",
                        f"현재 {round(r_now, 2)}R",
                        "",
                        "뉴스/공시 확인 후 대응하세요. 셋업 붕괴면 손절가 전이라도 이탈 검토.",
                    ]))
                    print(f"  🔻 {name} 급락 {round(drop_pct, 1)}% ({prev_price}→{price})")

        # 🛑 손절가 터치
        if price <= stop and "stop" not in fired:
            fired.add("stop")
            send_telegram("\n".join([
                "🛑 <b>손절가 도달 — 실행하세요</b>",
                "",
                f"종목: <b>{name}</b> ({ticker})",
                f"현재가: <b>{format_price(price, cur)}</b> ≤ 손절 {format_price(stop, cur)}",
                f"현재 {round(r_now, 2)}R",
                "",
                "규칙: -1R = 손절. 협상 없음. 종료 사유 기록까지가 시스템.",
            ]))
            print(f"  🛑 {name} 손절 도달 {price} <= {stop}")
            continue

        # 💰 +2R: 절반 익절 + 본전 이동
        if r_now >= 2 and "2R" not in fired:
            fired.add("2R")
            send_telegram("\n".join([
                "💰 <b>+2R 도달 — 절반 익절 + 손절 본전 이동</b>",
                "",
                f"종목: <b>{name}</b> ({ticker})",
                f"현재가: <b>{format_price(price, cur)}</b> (진입 {format_price(entry, cur)})",
                f"진행: <b>+{round(r_now, 2)}R</b>",
                "",
                "① 절반 익절 → 시스템 월급 확정",
                "② 나머지 손절을 진입가로 이동 → 프리 트레이드",
                "③ 이후 10/21일선 트레일링 — 러너는 조급하게 걷지 않기",
                "실행 후 일지에 부분청산 기록하세요.",
            ]))
            print(f"  💰 {name} +2R 도달 ({round(r_now, 2)}R)")
            continue

        # 🏔 +3R 이상 마일스톤 (각 1회)
        if r_now >= 3:
            ms = f"{int(r_now)}R"
            if ms not in fired:
                fired.add(ms)
                send_telegram("\n".join([
                    f"🏔 <b>+{int(r_now)}R 마일스톤</b> — 러너가 달리는 중",
                    "",
                    f"종목: <b>{name}</b> ({ticker})",
                    f"현재가: <b>{format_price(price, cur)}</b> · <b>+{round(r_now, 2)}R</b>",
                    "",
                    "행동: 없음. 10/21일선 종가 이탈 전까지 보유.",
                ]))
                print(f"  🏔 {name} +{int(r_now)}R")


_gate_last = {"suggest": None}

# ── v2.7: 게이트 캐시 + 알림 헤더 ──
_gate_cache = {"ts": 0, "data": None}
_GATE_CACHE_TTL = 300      # 5분. 알림마다 API 때리지 않게.
_GATE_EMOJI = {
    "confirmed": "🟢 확인된 상승",
    "pressure": "🟡 조정 압박",
    "correction": "🔴 조정",
}


def get_gate(force=False):
    """스캐너 /api/market/gate 조회 (5분 캐시). 실패 시 옛 캐시 또는 None."""
    now = _time.time()
    if not force and _gate_cache["data"] and now - _gate_cache["ts"] < _GATE_CACHE_TTL:
        return _gate_cache["data"]
    try:
        res = requests.get(f"{SCANNER_URL}/api/market/gate", timeout=15)
        j = res.json()
        if not j.get("ok"):
            return _gate_cache["data"]
    except Exception as e:
        print(f"[게이트] 조회 실패: {e}")
        return _gate_cache["data"]
    _gate_cache["ts"] = now
    _gate_cache["data"] = j
    return j


def _gate_line(ticker):
    """알림 헤더용 시장 게이트 한 줄. 종목 시장(KR/US)에 맞는 게이트 사용.
    한국 종목 → 코스피/코스닥 중 나쁜 쪽, 미국 종목 → S&P/나스닥 중 나쁜 쪽.
    노출 %가 아니라 오픈 리스크 상한(R)을 보여준다 (R설정에 이미 있는 규칙).
    반환: 문자열 (실패 시 None)."""
    j = get_gate()
    if not j:
        return None
    is_kr = bool(_kr_code(ticker))
    gate = j.get("gate_kr") if is_kr else j.get("gate_us")
    max_r = j.get("max_open_r_kr") if is_kr else j.get("max_open_r_us")
    if not gate:
        return None
    idx = j.get("indices") or {}
    codes = ("KOSPI", "KOSDAQ") if is_kr else ("^GSPC", "^IXIC")
    parts = []
    for code in codes:
        v = idx.get(code)
        if not v:
            continue
        d = v.get("dist_days")
        dtxt = "분산?" if d is None else f"분산{d}"   # None=판정불가 (0 아님)
        parts.append(f"{v['label']} {dtxt}")
    em = _GATE_EMOJI.get(gate, gate)
    line = f"[시장] {em} · {' · '.join(parts)}"
    if gate == "correction":
        line += "\n         신규 진입 0 — 관찰만"
    else:
        line += f"\n         신규 오픈 리스크 상한 {max_r}R"
    return line


def check_market_gate():
    """시장 게이트 자동 제안 감시 (v2.7) — 30분마다, 4개 지수.
    제안이 바뀌는 순간(특히 FTD 발생 → 🟢) 텔레그램 알림."""
    j = get_gate(force=True)
    if not j:
        return
    sug = j.get("suggest")
    if not sug or sug == _gate_last["suggest"]:
        _gate_last["suggest"] = sug
        return
    prev = _gate_last["suggest"]
    _gate_last["suggest"] = sug
    if prev is None:
        return   # 봇 시작 직후 첫 관측은 알림 없이 기억만
    lines = [
        "📢 <b>시장 게이트 제안 변경</b>",
        "",
        f"{_GATE_EMOJI.get(prev, prev)} → <b>{_GATE_EMOJI.get(sug, sug)}</b>",
        f"근거: {j.get('why', '')}",
        "",
        "<b>지수별</b>",
    ]
    idx = j.get("indices") or {}
    for code in ("KOSPI", "KOSDAQ", "^GSPC", "^IXIC"):
        v = idx.get(code)
        if not v:
            lines.append(f"· {code}: 조회 실패")
            continue
        d = v.get("dist_days")
        dtxt = "판정불가(거래량없음)" if d is None else f"분산 {d}개"
        raw = v.get("dist_raw")
        detail = ""
        if raw is not None and d is not None and raw != d:
            drops = []
            if v.get("dist_pre_ftd"):
                drops.append(f"FTD전 {v['dist_pre_ftd']}")
            if v.get("dist_expired"):
                drops.append(f"5%만료 {v['dist_expired']}")
            if drops:
                detail = f" (원시 {raw} − {', '.join(drops)})"
        ftd_txt = ""
        if v.get("ftd"):
            ftd_txt = f" · FTD {v['ftd_days_ago']}일전"
        elif v.get("in_correction"):
            ftd_txt = f" · 반등 {v.get('rally_day')}일차"
        vs = v.get("vol_source")
        vsrc = f" [거래량:{vs}]" if vs and vs != "index" else ""
        lines.append(
            f"· <b>{v['label']}</b> {_GATE_EMOJI.get(v['gate'], '')[:2]} "
            f"{dtxt}{detail}{ftd_txt}{vsrc}"
        )
    lines += [
        "",
        f"한국 종목: {_GATE_EMOJI.get(j.get('gate_kr'), '?')} (상한 {j.get('max_open_r_kr')}R)",
        f"미국 종목: {_GATE_EMOJI.get(j.get('gate_us'), '?')} (상한 {j.get('max_open_r_us')}R)",
    ]
    if j.get("ftd"):
        lines += ["", "🔔 FTD 확인 — 시험 매수 0.5R 1~2건부터. 풀사이즈 금지."]
    cur = j.get("current")
    if cur and cur != sug:
        lines += ["", f"현재 설정({_GATE_EMOJI.get(cur, cur)})과 다름 — 스캐너 일지 탭에서 [적용] 확인하세요."]
    send_telegram("\n".join(lines))
    print(f"[게이트] {prev} → {sug}")


def weekly_report():
    """주간 리포트 (v2.2) — 일요일 09:00 KST.
    주말 루틴의 자동화: 주간 R·승패·충동 카운트·진입중 포지션."""
    try:
        res = requests.get(f"{SCANNER_URL}/api/journal", timeout=30)
        rows = res.json()
        if isinstance(rows, dict):
            rows = rows.get("journal", rows.get("rows", []))
    except Exception as e:
        print(f"[주간리포트] 일지 조회 실패: {e}")
        return
    from datetime import timedelta
    now = datetime.now(KST)
    week_start = (now - timedelta(days=now.weekday() + 7)).strftime("%Y-%m-%d")  # 지난 월요일
    week_end = (now - timedelta(days=1)).strftime("%Y-%m-%d")                    # 어제(토)
    wins = losses = impulse = 0
    r_sum = 0.0
    open_pos = []
    for r in rows if isinstance(rows, list) else []:
        status = r.get("status") or "entered"
        if status == "entered" and r.get("result_r") == "":
            open_pos.append(r.get("name") or r.get("ticker") or "?")
            continue
        if (r.get("category") or "추세추종") == "관찰":
            continue
        d = r.get("closed_date") or r.get("last_checked") or r.get("date") or ""
        if not (week_start <= d <= week_end):
            continue
        try:
            rv = float(r.get("result_r"))
        except (TypeError, ValueError):
            continue
        r_sum += rv
        wins += rv > 0
        losses += rv < 0
        reason = (r.get("exit_reason") or r.get("closed_reason") or "")
        impulse += "충동" in str(reason)
    total = wins + losses
    lines = [
        "📊 <b>주간 리포트</b>",
        f"{week_start} ~ {week_end}",
        "",
        f"종료 매매: {total}건 (승 {wins} / 패 {losses})",
        f"주간 합계: <b>{'+' if r_sum > 0 else ''}{round(r_sum, 2)}R</b>",
        f"충동 청산: {impulse}건" + (" ⚠️" if impulse else " ✅"),
        f"진입중: {len(open_pos)}종목" + (f" ({', '.join(open_pos[:5])})" if open_pos else ""),
        "",
        "주말 루틴: 섹터 탭 주도업종 확인 · 관찰 리스트 정리 · CSV 백업",
    ]
    if total == 0:
        lines.insert(4, "(이번 주 종료 매매 없음 — 🔴 게이트면 그게 정상)")
    send_telegram("\n".join(lines))
    print("[주간리포트] 발송")


_dist_fired = {}         # {포지션id: 마지막 경고 날짜} — 하루 1회만


def check_distribution():
    """보유 종목 분산(매도) 신호 감시 (v2.4) — 하루 2회(11:00, 15:00 KST).
    진입 종목이 분산 danger면 ⚠️ 알림. 같은 종목 하루 1회만."""
    now = datetime.now(KST)
    today = now.strftime("%Y-%m-%d")
    try:
        res = requests.get(f"{SCANNER_URL}/api/watch/positions", timeout=10)
        positions = res.json().get("positions", [])
    except Exception as e:
        print(f"[분산] 포지션 조회 실패: {e}")
        return
    for p in positions:
        pid = p.get("id")
        ticker = p.get("ticker")
        if not pid or not ticker:
            continue
        if _dist_fired.get(pid) == today:      # 오늘 이미 경고함
            continue
        code = _kr_code(ticker)
        q = f"{code}.KQ" if code else ticker
        try:
            res = requests.get(f"{SCANNER_URL}/api/dist/{q}", timeout=10)
            j = res.json()
            if not j.get("ok") and code:
                res = requests.get(f"{SCANNER_URL}/api/dist/{code}.KS", timeout=10)
                j = res.json()
        except Exception:
            continue
        if not j.get("ok") or j.get("level") != "danger":
            continue
        _dist_fired[pid] = today
        name = p.get("name") or ticker
        sigs = j.get("signals", [])
        detail = j.get("detail", {})
        send_telegram("\n".join([
            "⚠️ <b>분산 경고 — 보유 종목 매도 신호</b>",
            "",
            f"종목: <b>{name}</b> ({ticker})",
            f"신호: {', '.join(sigs)}",
            f"당일 {detail.get('day_ret_pct', '?')}% · 거래량 {detail.get('vol_ratio', '?')}배",
            "",
            "기관 매도 신호가 감지됐어요. 규칙 점검:",
            "· +2R 넘었으면 이미 절반 익절했는지",
            "· 남은 물량 트레일링 손절(10/21일선) 확인",
            "· 셋업 훼손이면 손절가 전이라도 이탈 검토 (BHE 교훈)",
        ]))
        print(f"  ⚠️ {name} 분산 danger: {sigs}")


_ma_break_fired = {}     # {포지션id: 날짜} — 이평 이탈 하루 1회
_ma_near_fired = {}      # {티커: 날짜} — 이평 접근 하루 1회


def _get_ma(ticker):
    """스캐너에서 종목 이평값 받기. 실패 시 None."""
    code = _kr_code(ticker)
    for q in ([f"{code}.KQ", f"{code}.KS"] if code else [ticker]):
        try:
            res = requests.get(f"{SCANNER_URL}/api/ma/{q}", timeout=8)
            j = res.json()
            if j.get("ok"):
                return j
        except Exception:
            continue
    return None


def check_ma_break():
    """보유 종목 이평 이탈 감시 (v2.5) — 종가 기준, 장 마감 15:20 KST.
    진입 종목이 10/21(20)일선을 오늘 종가로 하향 이탈하면 트레일링 손절 알림.
    +2R 이후 러너 관리용 — 종가 기준이라 장중 페이크에 안 속음."""
    now = datetime.now(KST)
    today = now.strftime("%Y-%m-%d")
    try:
        res = requests.get(f"{SCANNER_URL}/api/watch/positions", timeout=10)
        positions = res.json().get("positions", [])
    except Exception as e:
        print(f"[이평이탈] 조회 실패: {e}")
        return
    for p in positions:
        pid = p.get("id"); ticker = p.get("ticker")
        if not pid or not ticker or _ma_break_fired.get(pid) == today:
            continue
        ma = _get_ma(ticker)
        if not ma:
            continue
        name = p.get("name") or ticker
        cur = "KRW" if _kr_code(ticker) else "USD"
        # 10일선 먼저(더 민감), 없으면 20일선
        broke = None
        if ma.get("broke10"):
            broke = ("10일선", ma.get("ma10"))
        elif ma.get("broke20"):
            broke = ("20일선", ma.get("ma20"))
        if not broke:
            continue
        _ma_break_fired[pid] = today
        line, maval = broke
        send_telegram("\n".join([
            f"📉 <b>{line} 이탈 — 트레일링 손절 검토</b>",
            "",
            f"종목: <b>{name}</b> ({ticker})",
            f"종가 {format_price(ma.get('close'), cur)} < {line} {format_price(maval, cur)}",
            "",
            "러너 관리 규칙:",
            "· +2R 넘겨 본전 이동한 물량이면 → 이탈 시 트레일링 청산",
            "· 아직 초기 진입이면 → 손절가와 함께 판단",
            "· 종가 이탈이라 장중 페이크 아님 (신뢰도 높음)",
        ]))
        print(f"  📉 {name} {line} 이탈")


def check_ma_near():
    """관찰 종목 이평 지지 접근 감시 (v2.5) — 장중 2분마다.
    관찰(watch) 종목이 20/50일선 ±1% 안에 들어오면 '지지선 접근' 알림.
    ※ 진입 신호 아님 — 반등일(양봉+거래량) 확인은 사용자 몫. 참고용 관찰 알림."""
    now = datetime.now(KST)
    today = now.strftime("%Y-%m-%d")
    try:
        res = requests.get(f"{SCANNER_URL}/api/watch/pending", timeout=10)
        pending = res.json().get("pending", [])
    except Exception:
        return
    for w in pending:
        ticker = w.get("ticker")
        # 관찰 카테고리만 (대기=피벗 돌파 감시는 별도)
        if not ticker or (w.get("category") or w.get("cat")) != "관찰":
            continue
        if _ma_near_fired.get(ticker) == today:
            continue
        ma = _get_ma(ticker)
        if not ma:
            continue
        name = w.get("name") or ticker
        cur = "KRW" if _kr_code(ticker) else "USD"
        # 20일선 또는 50일선 ±1% 접근 (아직 이탈 전 = 지지 테스트)
        near = None
        for w_ma in (20, 50):
            d = ma.get(f"dist{w_ma}_pct")
            if d is not None and -1.0 <= d <= 1.5 and not ma.get(f"below{w_ma}"):
                near = (f"{w_ma}일선", ma.get(f"ma{w_ma}"), d)
                break
        if not near:
            continue
        _ma_near_fired[ticker] = today
        line, maval, d = near
        _lines = [
            f"🎯 <b>{line} 지지 접근 — 관찰</b>",
            "",
            f"종목: <b>{name}</b> ({ticker})",
            f"현재 {format_price(ma.get('close'), cur)} · {line} {format_price(maval, cur)} ({d:+.1f}%)",
            "",
            "눌림목 진입 준비:",
            "· 지지선 찍고 반등일(양봉+거래량 회복) 확인 후 진입",
            "· 지금 사는 건 떨어지는 칼 — 반등 확인이 먼저",
        ]
        _gl = _gate_line(ticker)          # v2.7: 게이트를 알림 안에
        if _gl:
            _lines = [_gl, ""] + _lines
        send_telegram("\n".join(_lines))
        print(f"  🎯 {name} {line} 접근 ({d:+.1f}%)")


# ── 눌림 지지 진입 알림 (v2.14) ──────────────────────────────
# 기존 🚀피벗돌파와 별개의 알림 유형. RS90+ · U/D1.5+ · 주봉10EMA ±2% 이내인
# "강한 종목의 얕은 눌림"만 골라서 "지지 확인 후 진입" 알림 — 지금 사라는
# 뜻이 아니라 지지 캔들(양봉+거래량 회복)을 기다리라는 준비 신호.
# 스캐너 /api/pullback-signal/{ticker}(v5.04 신규)에서 RS 백분위·U/D·주봉
# 10EMA·ATR%·월봉 되돌림을 받아오고, 게이트 임계값 판정은 여기(봇)에서 —
# 운영하면서 튜닝하기 쉽게 하려는 의도.
_pullback_fired = {}      # {포지션id: 마지막 발송 날짜} — 같은 관찰종목 하루 1회
PULLBACK_RS_MIN = 90
PULLBACK_UD_MIN = 1.5
PULLBACK_NEAR_PCT = 2.0       # 주봉10EMA ±2% 이내면 "근접"
PULLBACK_BREAK_PCT = -3.0     # 10EMA 아래로 이만큼 넘게 이탈하면 붕괴로 보고 제외


def _get_pullback_signal(ticker):
    """스캐너 /api/pullback-signal 조회. 한국 종목은 접미사(.KQ/.KS) 둘 다
    시도(_get_ma와 동일 패턴 — 저널에 접미사 없이 등록된 구 레코드 대비)."""
    code = _kr_code(ticker)
    for q in ([f"{code}.KQ", f"{code}.KS"] if code else [ticker]):
        try:
            res = requests.get(f"{SCANNER_URL}/api/pullback-signal/{q}", timeout=8)
            j = res.json()
            if j.get("ok"):
                return j
        except Exception:
            continue
    return None


def check_pullback_support():
    """눌림 지지 진입 알림 — 장중 2분마다. 관찰(watch) 종목 중 RS90+ ·
    U/D1.5+ · 주봉10EMA ±2% 이내(단 -3% 넘게 이탈한 붕괴 종목은 제외)인
    것만 "지지 확인 후 진입" 알림. 자동 진입(대기→진입 플립) 로직을 타지
    않음 — 관찰 상태 그대로 유지해 R 통계를 오염시키지 않는다."""
    now = datetime.now(KST)
    today = now.strftime("%Y-%m-%d")
    try:
        res = requests.get(f"{SCANNER_URL}/api/watch/pending", timeout=10)
        pending = res.json().get("pending", [])
    except Exception as e:
        print(f"[눌림지지] 조회 실패: {e}")
        return
    for w in pending:
        if (w.get("category") or w.get("cat")) != "관찰":
            continue
        wid = w.get("id")
        ticker = w.get("ticker")
        if not wid or not ticker or _pullback_fired.get(wid) == today:
            continue
        sig = _get_pullback_signal(ticker)
        if not sig:
            continue
        rs = sig.get("rs_rank")
        ud = sig.get("ud_ratio")
        ema10w = sig.get("weekly_ema10")
        dist = sig.get("weekly_ema10_dist_pct")
        if rs is None or rs < PULLBACK_RS_MIN:
            continue
        if ud is None or ud < PULLBACK_UD_MIN:
            continue
        if ema10w is None or dist is None or dist < PULLBACK_BREAK_PCT:
            continue     # 데이터 없음 또는 이미 붕괴(10EMA -3% 넘게 이탈)
        if not (-PULLBACK_NEAR_PCT <= dist <= PULLBACK_NEAR_PCT):
            continue     # 아직 근접 전

        _pullback_fired[wid] = today
        name = w.get("name") or ticker
        cur = "KRW" if _kr_code(ticker) else "USD"
        close = sig.get("close")
        atr_pct = sig.get("atr_pct")
        stop = round(ema10w * 0.97, 2)
        risk_pct = round((close - stop) / close * 100, 2) if close else None
        atr_ok = (risk_pct is not None and atr_pct is not None
                  and risk_pct <= atr_pct * 1.5)

        lines = [
            "📉 <b>눌림 지지 접근 — 지지 확인 대기</b>",
            "",
            f"종목: <b>{name}</b> ({ticker})",
            f"현재가: <b>{format_price(close, cur)}</b> · 주봉10EMA {format_price(ema10w, cur)} ({dist:+.1f}%)",
            f"RS {rs} · U/D {ud} (매집 우위)",
            f"손절 참고가: {format_price(stop, cur)} (지지선 -3%)",
        ]
        if risk_pct is not None:
            lines.append(f"손절폭 {risk_pct:.1f}%" + (" ✅ ATR×1.5 이내" if atr_ok else " ⚠️ ATR×1.5 초과 — 사이즈 축소 검토"))
        if sig.get("confluence"):
            lines.append("🔀 월봉 상승분 50% 되돌림과 근접 — confluence")
        lines += [
            "",
            "⚠️ 지금 진입 아님 — 지지 캔들(양봉+거래량 회복) 확인 후 진입",
            "(관찰 상태 유지 — 자동으로 진입 처리되지 않음)",
        ]
        _gl = _gate_line(ticker)
        if _gl:
            lines = [_gl, ""] + lines
        send_telegram("\n".join(lines))
        print(f"  📉 {name} 눌림지지 접근 RS{rs} UD{ud} dist{dist:+.1f}%")


def check_pivot_breakout():
    """눌림목 스캐너의 대기(pending) 종목을 읽어, 피벗가 돌파 시 텔레그램 알림.
    스캐너 /api/watch/pending에서 {ticker, name, pivot, ...} 목록을 받아
    각 현재가가 피벗 이상이면 '돌파' 알림. 한 종목당 1회만(중복 방지)."""
    now = datetime.now(KST).strftime("%H:%M:%S")
    try:
        res = requests.get(f"{SCANNER_URL}/api/watch/pending", timeout=10)
        pending = res.json().get("pending", [])
    except Exception as e:
        print(f"[{now}] 대기종목 조회 실패: {e}")
        return
    if not pending:
        return
    print(f"[{now}] 피벗 돌파 감시: {len(pending)}종목")
    _today = datetime.now(KST).strftime("%Y-%m-%d")

    # ── v2.8.1 리테스트 감시 (pending 목록과 독립) ──
    # 대기 항목은 피벗 도달 시 '진입'으로 자동 전환돼 pending에서 사라진다.
    # pending 루프 안에서 리테스트를 보면 돌파한 종목이 바로 감시에서 빠지므로,
    # _pivot_state(발화 기록)를 직접 순회한다. 돌파날 놓쳐도 2차 기회 알림.
    for _wid, st in list(_pivot_state.items()):
        if st.get("retest_fired") or st.get("fired") == _today:
            continue
        _tk = st.get("ticker")
        if not _tk:
            continue
        try:
            _p = float(st["pivot"])
            _d = get_stock_data(_tk)
            if not _d:
                continue
            _pr = _d["price"]
            # 피벗 -3% ~ +1.5% 구간으로 복귀 = 리테스트 존
            if _p * 0.97 <= _pr <= _p * 1.015:
                st["retest_fired"] = True
                _nm = st.get("name") or _tk
                _cur = _d["currency"]
                _st = st.get("stop")
                _lines = [
                    "🔁 <b>피벗 리테스트</b> — 2차 진입 기회",
                    "",
                    f"종목: <b>{_nm}</b> ({_tk})",
                    f"현재가: <b>{format_price(_pr, _cur)}</b> · 피벗 {format_price(_p, _cur)}",
                    f"돌파일: {st.get('fired')} (그날 진입 못 했다면 지금이 재기회)",
                ]
                if _st:
                    _lines.append(f"손절: {format_price(_st, _cur)}")
                _lines += [
                    "",
                    "· 피벗 위에서 지지받고 반등하면 진입 유효",
                    "· 피벗 -3% 아래로 깨지면 리테스트 실패 — 관망",
                ]
                _gl = _gate_line(_tk)
                if _gl:
                    _lines = [_gl, ""] + _lines
                send_telegram("\n".join(_lines))
                print(f"  🔁 {_nm} 리테스트 {_pr} ~ 피벗 {_p}")
        except Exception as _e:
            print(f"  리테스트 체크 오류 {_tk}: {_e}")

    for w in pending:
        wid = w.get("id")
        ticker = w.get("ticker")
        pivot = w.get("pivot")
        if not ticker or not pivot:
            continue

        # v2.8: 날짜 기반 중복 방지 — 같은 날 1회. 다음날 재돌파면 다시 알림.
        # (기존 영구 set은 한번 알림 후 재돌파를 봇 재시작 전까지 영원히 무시)
        if _pivot_fired.get(wid) == _today:
            continue
        data = get_stock_data(ticker)
        if not data:
            continue
        price = data["price"]
        # ── 하향 목표가 도달 (v2.6) ── 눌림목 대기용.
        # 진입 목표가가 '현재가보다 낮게' 설정된 종목(RCUS $30인데 목표 $26)이,
        # 목표가까지 눌려 내려오면 "지지 도달, 반등 확인" 알림. 피벗(상향)과 반대.
        tb = w.get("target_below")
        try:
            tb = float(tb) if tb else None
        except (TypeError, ValueError):
            tb = None
        # 하향 목표 판별: 목표가(entry)가 '현재가보다 낮게' 설정 = 눌림목 대기.
        # RCUS 목표 $26 < 현재 $30 → 내려오길 기다리는 중. 이미 목표 아래로
        # 내려왔으면(price <= tb) 도달 알림. 목표가 현재가 위면 상향 대기라 skip.
        if tb and price > tb and wid not in _target_fired:
            # 목표까지 +2% 이내로 근접하면 알림 (딱 도달 전에 준비)
            if price <= tb * 1.02:
                _target_fired.add(wid)
                name = w.get("name") or ticker
                cur = data["currency"]
                stop = w.get("stop")
                _lines = [
                    "🎯 <b>목표가 도달 — 눌림목 진입 준비</b>",
                    "",
                    f"종목: <b>{name}</b> ({ticker})",
                    f"현재가: <b>{format_price(price, cur)}</b> (목표 {format_price(tb, cur)})",
                    f"손절: {format_price(stop, cur)}" if stop else "",
                    "",
                    "지지선까지 눌려 내려왔어요. 진입 규칙:",
                    "· 지지 찍고 반등일(양봉+거래량 회복) 확인 후 진입",
                    "· 지금 사는 건 떨어지는 칼 — 반등 확인 먼저",
                ]
                _gl = _gate_line(ticker)          # v2.7
                if _gl:
                    _lines = [_gl, ""] + _lines
                send_telegram("\n".join(_lines))
                print(f"  🎯 {name} 목표가 도달 {price} ≤ {tb}")
        if wid not in _pivot_near and pivot * 0.99 <= price < pivot:
            _pivot_near.add(wid)
            name = w.get("name") or ticker
            cur = data["currency"]
            _lines = [
                "⚡ <b>피벗 접근</b> — 준비",
                "",
                f"종목: <b>{name}</b> ({ticker})",
                f"현재가: <b>{format_price(price, cur)}</b> (피벗까지 {round((pivot / price - 1) * 100, 2)}%)",
                f"피벗: {format_price(pivot, cur)}",
                "",
                "돌파 시 다시 알림. 거래량 미리 확인해두세요.",
            ]
            _gl = _gate_line(ticker)          # v2.7
            if _gl:
                _lines = [_gl, ""] + _lines
            send_telegram("\n".join(_lines))
            print(f"  ⚡ {name} 피벗 접근 {price} → {pivot}")
        if price >= pivot:                     # 피벗 돌파!
            _pivot_fired[wid] = _today
            # v2.8: 리테스트 감시 시작 (같은 날은 리테스트 판정 안 함)
            _pivot_state[wid] = {"pivot": float(pivot), "fired": _today, "retest_fired": False,
                                 "ticker": ticker, "name": w.get("name") or ticker,
                                 "stop": w.get("stop")}
            name = w.get("name") or ticker
            entry = w.get("entry")
            stop = w.get("stop")
            cur = data["currency"]
            lines = [
                "🚀 <b>피벗 돌파!</b> 진입 검토",
                "",
                f"종목: <b>{name}</b> ({ticker})",
                f"현재가: <b>{format_price(price, cur)}</b>",
                f"피벗: {format_price(pivot, cur)} 돌파 ✅",
            ]
            if stop:
                lines.append(f"손절: {format_price(stop, cur)}")
                # ── v2.8: 손절폭 즉시 판정 — 알림 하나로 진입 판단 끝나게 ──
                try:
                    _sw = (float(pivot) - float(stop)) / float(pivot) * 100
                    _lim = 7.0 if _kr_code(ticker) else 5.0
                    if _sw > _lim:
                        lines.append(f"🚫 손절폭 {_sw:.1f}% (기준 {_lim:.0f}% 초과) — 이 자리 진입 비권장. 눌림 대기")
                    else:
                        lines.append(f"✅ 손절폭 {_sw:.1f}% (기준 {_lim:.0f}% 이내)")
                except (TypeError, ValueError, ZeroDivisionError):
                    pass
            # 거래량 확증 (v2.3): 실시간 누적 거래량 시간보정 → 예상 거래량비
            vtag, vok = volume_confirm(ticker, data.get("volume"), datetime.now(KST))
            lines.append("")
            if vok:
                lines.append(vtag)
            else:
                lines.append("⚠️ 거래량은 HTS에서 직접 확인 (전일 동시간 대비)")
            lines.append("피벗 +2% 추격 금지")
            lines.append(f"시각: {datetime.now(KST).strftime('%Y-%m-%d %H:%M')}")
            _gl = _gate_line(ticker)          # v2.7: 게이트를 알림 안에 (헤더)
            if _gl:
                lines = [_gl, ""] + lines
            send_telegram("\n".join(lines))
            print(f"  🚀 {name} 피벗돌파 {price} >= {pivot}")


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

_opening_surge_fired_date = None   # 오늘 이미 발송했는지 (같은 날 중복 방지)


def check_opening_surge():
    """장 시작 10분 돈 유입(거래량 급증) 알림 (v2.13) — 09:10 KST 1회.
    스캐너 /api/opening-surge를 호출해 유니버스 전체 중 평소 대비 거래량이
    급증(기본 3배↑)한 종목만 텔레그램으로 보낸다. 주말은 스킵.
    스케줄러가 09:10에 정확히 안 맞아 여러 번 걸릴 수 있어 날짜별 1회만 발송."""
    global _opening_surge_fired_date
    now = datetime.now(KST)
    if now.weekday() >= 5:
        print("[장초반 급증] 주말이라 건너뜀")
        return
    today = now.strftime("%Y-%m-%d")
    if _opening_surge_fired_date == today:
        return
    try:
        res = requests.get(f"{SCANNER_URL}/api/opening-surge", timeout=60)
        j = res.json()
        hits = j.get("hits", [])
    except Exception as e:
        print(f"[장초반 급증] 조회 실패: {e}")
        return
    _opening_surge_fired_date = today
    if not hits:
        print("[장초반 급증] 급증 종목 없음")
        return
    lines = [
        "💸 <b>장 시작 10분 — 돈 유입 급증 종목</b>",
        "",
        "평소(50일 평균) 대비 거래량이 시간보정 기준 3배 이상인 종목이에요.",
        "",
    ]
    for h in hits[:10]:
        lines.append(
            f"· <b>{h['name']}</b> ({h['ticker']}) {h['change_pct']:+.1f}% "
            f"· 평소대비 <b>{h['surge_ratio']}배</b> · 거래대금 {h['value_eok']:,.0f}억"
        )
    lines += [
        "",
        "※ 진입 신호 아님 — 왜 돈이 몰렸는지(뉴스/공시) 확인 후 판단하세요.",
        f"시각: {now.strftime('%H:%M')}",
    ]
    send_telegram("\n".join(lines))
    print(f"  💸 장초반 급증 {len(hits)}종목 발송")


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

def watch_digest():
    """v2.8 데일리 관찰 다이제스트 — 매 거래일 08:50 KST.

    [배경] Seulki의 반복 패턴: 스캐너에서 종목 발견 → 다음날 새 종목에 정신
    팔려 기존 관찰을 잊음 → 돌파 놓침 → 그 종목은 상승 추세를 탐.
    '다음날 다시 체크'를 사람이 아니라 봇이 매일 아침 하게 만든다.

    내용: 대기 종목 전체를 피벗 근접순으로 + 셋업 깨진 것(가격<손절) 정리 제안
    + 2주 넘게 트리거 없는 것 만료 제안. 관찰 목록을 항상 짧고 살아있게.
    """
    now = datetime.now(KST)
    if now.weekday() >= 5:      # 주말 스킵
        return
    try:
        res = requests.get(f"{SCANNER_URL}/api/watch/pending", timeout=15)
        pending = res.json().get("pending", [])
    except Exception as e:
        print(f"[다이제스트] 조회 실패: {e}")
        return
    if not pending:
        return

    near, waiting, broken, stale = [], [], [], []
    today = now.date()
    for w in pending:
        ticker = w.get("ticker")
        pivot = w.get("pivot")
        if not ticker or not pivot:
            continue
        d = get_stock_data(ticker)
        if not d:
            waiting.append((w, None, None))
            continue
        price = d["price"]
        cur = d["currency"]
        try:
            dist = (float(pivot) - price) / price * 100
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        stop = w.get("stop")
        # D: 셋업 무효 — 가격이 손절선 아래 = 대기 셋업 붕괴
        try:
            if stop and price < float(stop):
                broken.append((w, price, cur))
                continue
        except (TypeError, ValueError):
            pass
        # D: 만료 제안 — 등록 후 14일 넘게 트리거 없음
        try:
            reg = datetime.strptime(w.get("date", ""), "%Y-%m-%d").date()
            if (today - reg).days > 14:
                stale.append((w, dist, cur))
                continue
        except (TypeError, ValueError):
            pass
        if 0 <= dist <= 3.0:
            near.append((w, dist, cur, price))
        else:
            waiting.append((w, dist, cur))

    near.sort(key=lambda x: x[1])
    lines = ["📋 <b>아침 관찰 다이제스트</b>", ""]
    if near:
        lines.append("🎯 <b>임박 (피벗 3% 이내)</b> — 오늘 볼 것")
        for w, dist, cur, price in near:
            lines.append(f"· <b>{w.get('name')}</b> 피벗까지 +{dist:.1f}% "
                         f"(현재 {format_price(price, cur)} → {format_price(w.get('pivot'), cur)})")
        lines.append("")
    if waiting:
        lines.append(f"⏳ 대기 {len(waiting)}종목: "
                     + " · ".join(f"{w.get('name')}({dist:+.1f}%)" if dist is not None else f"{w.get('name')}(?)"
                                  for w, dist, cur in waiting[:8]))
        lines.append("")
    if broken:
        lines.append("💔 <b>셋업 무효 — 정리 권장</b> (가격이 손절선 아래)")
        for w, price, cur in broken:
            lines.append(f"· {w.get('name')} 현재 {format_price(price, cur)} < 손절 {format_price(w.get('stop'), cur)}")
        lines.append("  → 일지에서 '무산' 처리하세요. 죽은 항목이 쌓이면 목록을 안 보게 됩니다.")
        lines.append("")
    if stale:
        lines.append(f"🗓 14일+ 무반응 {len(stale)}종목: "
                     + " · ".join(w.get('name') for w, _, _ in stale[:6])
                     + " — 셋업 재검토/정리 권장")
    send_telegram("\n".join(lines).rstrip())
    print(f"[다이제스트] 임박{len(near)} 대기{len(waiting)} 무효{len(broken)} 만료{len(stale)}")


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
# 대기종목(피벗 감시) 개수 조회 (실패해도 무시)
_pending_cnt = 0
try:
    _r = requests.get(f"{SCANNER_URL}/api/watch/pending", timeout=10)
    _pending_cnt = _r.json().get("count", 0)
except Exception:
    pass
send_telegram(
    "✅ <b>얼마냐봇 시작!</b>\n\n"
    "📌 모니터링 중인 종목:\n" +
    "\n".join([f"• {a['ticker']} {'이상' if a['condition']=='above' else '이하'} {a['target']}" for a in alerts]) +
    f"\n⚡ 급등락 기준: ±{SURGE_THRESHOLD}%\n"
    f"🚀 피벗 돌파 감시: 일지 대기종목 {_pending_cnt}개 (1분마다)\n\n"
    "💬 사용방법이 궁금하면 <code>알려줘</code> 라고 보내주세요!"
)

# 스케줄
schedule.every(30).seconds.do(check_alerts)
schedule.every(1).minutes.do(check_pivot_breakout)   # 대기종목 피벗 돌파 감시
schedule.every(2).minutes.do(check_positions)        # 진입 포지션 R 마일스톤/손절 감시 (v2.1)
schedule.every(30).minutes.do(check_market_gate)     # 시장 게이트 제안 변경 감시 (v2.2)
# v2.6 [루트 수정] 분산/이평이탈은 '완성된 일봉' 기반 판정인데 장중에 돌리고 있었음.
#   [원인] 11:00(장 시작 2h) / 15:00·15:20(종가 동시호가 15:20~15:30 전)에 실행 →
#          스캐너 캐시가 8분마다 장중 갱신되므로 c.iloc[-1]이 '미완성 일봉'.
#          슈피겐 사례: 11:00 스냅샷 -4.6%/0.31배로 danger 발동 → 종가는 +0.71%/0.57배.
#          종가 데이터였으면 is_down_day=False라 신호 자체가 안 떴음.
#   [수정] KRX 종가 확정(15:30) + 데이터 반영 여유를 두고 16:10 KST 1회만 실행.
#          미국 종목은 이 시각 기준 직전 세션이 이미 마감이라 완성 봉으로 판정됨.
schedule.every().day.at("16:10", "Asia/Seoul").do(check_distribution)  # 분산 경고 (종가 확정 후)
schedule.every().day.at("16:10", "Asia/Seoul").do(check_ma_break)      # 보유 이평 이탈 (종가 확정 후)
schedule.every(2).minutes.do(check_ma_near)                            # 관찰 이평 접근 (v2.5, 장중)
schedule.every(2).minutes.do(check_pullback_support)                   # 눌림 지지 진입 (v2.14, 장중)
schedule.every().sunday.at("09:00", "Asia/Seoul").do(weekly_report)  # 주간 리포트 (v2.2)
schedule.every(5).minutes.do(check_surge)
schedule.every().day.at("09:00", "Asia/Seoul").do(morning_summary)
schedule.every().day.at("08:50", "Asia/Seoul").do(watch_digest)      # v2.8 아침 관찰 다이제스트
schedule.every().day.at("16:00", "Asia/Seoul").do(scheduled_trading_value_report)  # 장 마감 후 거래대금 (KST)
schedule.every().day.at("09:10", "Asia/Seoul").do(check_opening_surge)  # v2.13 장 시작 10분 돈 유입 급증

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
