"""알트코인 급등 뒤 숏(과열 되돌림) 백테스트용 자료 수집 (v2.73).

왜: 사용자 제안(2026-11-14) — "볼린저밴드를 벗어나거나 이격이 큰 자리에서 숏" 알림 채널 방식을 과거 자료로
잴 수 있는지. 분석 세션은 binance 도메인이 막혀 있어 Actions가 대신 받는다(btc_history.yml과 같은 사정).

무엇을 받나 (data.binance.vision 공개 파일, USDT 무기한 선물, **상장폐지 종목 포함** — 살아남은 종목만 보면
"급등 뒤 숏"이 실제보다 좋아 보인다):
  1) 전 종목 일봉 전체 기간                      → daily.csv.gz        (sym,t,o,h,l,c,q)
  2) 급등일(장중 고가가 5일선보다 EVENT_DEV 이상 위)과 그 뒤 EVENT_DAYS-1일의 5분봉 → ev5m_YYYY.csv.gz
  3) 채널 알림 정의 역산용: CALIB_SYMBOL 최근 1분봉  → calib_1m.csv.gz

"5일선 이격"은 진행 중인 당일 가격을 포함한 5일 평균 대비다: dev(p) = p / ((직전 4일 종가 합 + p) / 5) − 1.
EVENT_DEV는 채널의 Monitoring 문턱(관측값 +38~42%)보다 일부러 낮게 잡는다 — 문턱은 분석에서 고른다.
"""
import csv, gzip, io, os, re, sys, urllib.request, urllib.error, zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
BASE = "https://data.binance.vision"
PFX = "data/futures/um"
EVENT_DEV = 0.30
EVENT_BB = 0.10          # 볼린저 상단 대비 +10% (채널 Monitoring 관측값 +5~15%, Good short zone +13~26%)
EVENT_DAYS = 3
MAX_SYMBOL_DAYS = 30000
CALIB_SYMBOL = "SHELLUSDT"
CALIB_DAYS = 12
D = 86_400_000
THREADS = 48


def get(url, tries=3):
    for k in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
        except Exception:
            pass
    return None


def s3_list(prefix, delimiter="/"):
    """(하위 폴더들, 파일 키들). 1000개씩 끊겨 오므로 marker로 이어 받는다."""
    prefixes, keys, marker = [], [], ""
    while True:
        url = f"{S3}?delimiter={delimiter}&prefix={prefix}" + (f"&marker={marker}" if marker else "")
        blob = get(url)
        if blob is None:
            break
        x = blob.decode("utf-8", "replace")
        prefixes += re.findall(r"<Prefix>([^<]+)</Prefix>", x)[1:]      # 첫 Prefix는 요청한 것
        ks = re.findall(r"<Key>([^<]+)</Key>", x)
        keys += ks
        if "<IsTruncated>true</IsTruncated>" not in x:
            break
        nm = re.findall(r"<NextMarker>([^<]+)</NextMarker>", x)
        marker = nm[0] if nm else (ks[-1] if ks else "")
        if not marker:
            break
    return prefixes, keys


def rows_of(blob):
    """zip 안의 klines CSV → (t_ms, o, h, l, c, quote_volume) 문자열 그대로."""
    try:
        z = zipfile.ZipFile(io.BytesIO(blob))
        with z.open(z.namelist()[0]) as f:
            for r in csv.reader(io.TextIOWrapper(f, "utf-8")):
                if not r or not r[0].isdigit():
                    continue
                t = int(r[0])
                if t > 10 ** 14:
                    t //= 1000
                yield t, r[1], r[2], r[3], r[4], r[7]
    except (zipfile.BadZipFile, IndexError, KeyError):
        return


def find_events(daily, dev=EVENT_DEV, bb=None):
    """daily: {sym: [(t,o,h,l,c,q) ...] 시간순, 숫자}. 반환 [(sym, day_ms, 장중 최대 5일선 이격)].
    급등일 = 장중 고가 기준 5일선 이격 ≥ dev **또는**(bb가 주어지면) 볼린저 상단(20일·2σ, 고가 포함) 대비 ≥ bb.
    v2.74: 채널의 Good short zone이 5일선이 아니라 볼린저 상단 대비 %로 갈리는 것으로 보여(2026-11-14 알림 22개
    대조 — 5MA +30%에서도 뜨고 +68%에서도 안 뜸) 볼린저 기준을 추가했다."""
    ev = []
    for sym, rows in daily.items():
        for i in range(4, len(rows)):
            if rows[i][0] - rows[i - 4][0] != 4 * D:        # 사이에 빠진 날이 있으면 건너뜀
                continue
            s4 = sum(r[4] for r in rows[i - 4:i])
            hi = rows[i][2]
            if s4 <= 0:
                continue
            d = hi / ((s4 + hi) / 5) - 1
            hit = d >= dev
            if not hit and bb is not None and i >= 19 and rows[i][0] - rows[i - 19][0] == 19 * D:
                w = [r[4] for r in rows[i - 19:i]] + [hi]
                m = sum(w) / 20
                sd = (sum((x - m) ** 2 for x in w) / 20) ** 0.5
                hit = hi / (m + 2 * sd) - 1 >= bb
            if hit:
                ev.append((sym, rows[i][0], d))
    return ev


def event_symbol_days(ev, days=EVENT_DAYS):
    out = set()
    for sym, t, _ in ev:
        for k in range(days):
            out.add((sym, t + k * D))
    return out


def main(out_dir="out", min_daily=100_000):
    os.makedirs(out_dir, exist_ok=True)
    today = datetime.now(timezone.utc).date()
    folders, _ = s3_list(f"{PFX}/monthly/klines/")
    syms = sorted(p.rstrip("/").rsplit("/", 1)[-1] for p in folders)
    syms = [s for s in syms if s.endswith("USDT") and "_" not in s]
    print(f"종목 {len(syms)}개", flush=True)
    if len(syms) < 100:
        raise SystemExit("종목 목록이 비정상적으로 적다 — 중단")

    def month_keys(sym):
        _, keys = s3_list(f"{PFX}/monthly/klines/{sym}/1d/")
        return sym, [k for k in keys if k.endswith(".zip")]

    with ThreadPoolExecutor(THREADS) as ex:
        listing = dict(ex.map(month_keys, syms))
    jobs = [(s, k) for s, ks in listing.items() for k in ks]
    # 이번 달(월별 파일 미발행)은 일별 1d 파일로 — 지난달 월별 파일이 있는 종목만(= 아직 거래 중)
    first = today.replace(day=1)
    prev_tag = (first - timedelta(days=1)).strftime("%Y-%m")
    for s, ks in listing.items():
        if any(prev_tag in k for k in ks):
            d = first
            while d < today:
                jobs.append((s, f"{PFX}/daily/klines/{s}/1d/{s}-1d-{d.isoformat()}.zip"))
                d += timedelta(days=1)
    print(f"일봉 파일 {len(jobs)}개 받는 중", flush=True)

    def fetch(job):
        s, k = job
        b = get(f"{BASE}/{k}")
        return s, (list(rows_of(b)) if b else [])

    daily = {}
    with ThreadPoolExecutor(THREADS) as ex:
        for s, rows in ex.map(fetch, jobs):
            d = daily.setdefault(s, {})
            for r in rows:
                d[r[0]] = r
    with gzip.open(f"{out_dir}/daily.csv.gz", "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["sym", "t", "o", "h", "l", "c", "q"])
        n_daily = 0
        for s in sorted(daily):
            for t in sorted(daily[s]):
                w.writerow((s,) + daily[s][t]); n_daily += 1
    print(f"일봉 {n_daily}행 / 종목 {len(daily)}", flush=True)
    if n_daily < min_daily:
        raise SystemExit("일봉이 너무 적다 — 중단")

    num = {s: [(t,) + tuple(float(x) for x in r[1:]) for t, r in sorted(d.items())] for s, d in daily.items()}
    dev, bb = EVENT_DEV, EVENT_BB
    while True:
        ev = find_events(num, dev, bb)
        sd = event_symbol_days(ev)
        if len(sd) <= MAX_SYMBOL_DAYS:
            break
        dev += 0.05; bb += 0.02
    sd = sorted(x for x in sd if x[1] < datetime(today.year, today.month, today.day, tzinfo=timezone.utc).timestamp() * 1000)
    print(f"급등일 {len(ev)}건(5일선 +{dev:.0%} 또는 볼린저 +{bb:.0%}) → 5분봉 받을 종목·날짜 {len(sd)}개", flush=True)

    def fetch5(x):
        s, t = x
        day = datetime.fromtimestamp(t / 1000, timezone.utc).date().isoformat()
        b = get(f"{BASE}/{PFX}/daily/klines/{s}/5m/{s}-5m-{day}.zip")
        return s, day[:4], (list(rows_of(b)) if b else [])

    by_year, got, n5 = {}, 0, 0
    with ThreadPoolExecutor(THREADS) as ex:
        for s, y, rows in ex.map(fetch5, sd):
            if rows:
                got += 1; n5 += len(rows)
                by_year.setdefault(y, []).extend((s,) + r for r in rows)
    for y, rows in sorted(by_year.items()):
        rows.sort()
        with gzip.open(f"{out_dir}/ev5m_{y}.csv.gz", "wt", encoding="utf-8", newline="") as f:
            w = csv.writer(f); w.writerow(["sym", "t", "o", "h", "l", "c", "q"]); w.writerows(rows)
    print(f"5분봉 {n5}행 ({got}/{len(sd)} 파일)", flush=True)

    calib = []
    for k in range(CALIB_DAYS, 0, -1):
        day = (today - timedelta(days=k)).isoformat()
        b = get(f"{BASE}/{PFX}/daily/klines/{CALIB_SYMBOL}/1m/{CALIB_SYMBOL}-1m-{day}.zip")
        if b:
            calib += list(rows_of(b))
    with gzip.open(f"{out_dir}/calib_1m.csv.gz", "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(["t", "o", "h", "l", "c", "q"]); w.writerows(calib)

    meta = (f"symbols={len(daily)} daily_rows={n_daily} event_dev={dev:.2f} event_bb={bb:.2f} events={len(ev)} "
            f"symbol_days={len(sd)} files_5m={got} rows_5m={n5} calib_rows={len(calib)} fetched={today.isoformat()}")
    open(f"{out_dir}/meta.txt", "w").write(meta + "\n")
    print(meta, flush=True)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "out")
