"""채널 알림 시각 전후 1분봉 수집 (v2.75).

왜: 사용자 지시 2026-11-14 "5분봉기준말고 1분봉기준으로 다시 조사해봐". 급등 숏 채널(하이엔드 클래식 검색기)
알림을 5분봉으로 재면 알림 직후 몇 분의 순서(먼저 더 오르나, 먼저 내리나)를 못 가른다.

입력: tools/alert_days.csv (sym,date) — 채널 내보내기의 알림마다 알림일과 뒤 2일(UTC). 분석 세션에서 만든 목록.
출력: <out>/1m_YYYY-MM.csv.gz (sym,t,o,h,l,c,q), meta.txt. 원천은 data.binance.vision USDT 무기한 1분봉 일별 파일.
"""
import csv, gzip, os, sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from alt_history import BASE, PFX, THREADS, get, rows_of  # noqa: E402

LIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alert_days.csv")


def read_list(path=LIST):
    with open(path, encoding="utf-8") as f:
        return [(r["sym"], r["date"]) for r in csv.DictReader(f)]


def fetch(x):
    s, day = x
    b = get(f"{BASE}/{PFX}/daily/klines/{s}/1m/{s}-1m-{day}.zip")
    return s, day[:7], (list(rows_of(b)) if b else [])


def main(out_dir="out"):
    os.makedirs(out_dir, exist_ok=True)
    jobs = read_list()
    by_month, got, n, missing = {}, 0, 0, []
    with ThreadPoolExecutor(THREADS) as ex:
        for (s, m, rows), job in zip(ex.map(fetch, jobs), jobs):
            if rows:
                got += 1; n += len(rows)
                by_month.setdefault(m, []).extend((s,) + r for r in rows)
            else:
                missing.append(f"{job[0]}:{job[1]}")
    if got < len(jobs) * 0.5:
        raise SystemExit(f"1분봉 파일이 너무 적다({got}/{len(jobs)}) — 원천 문제. 커밋하지 않는다.")
    for m, rows in sorted(by_month.items()):
        rows.sort()
        with gzip.open(f"{out_dir}/1m_{m}.csv.gz", "wt", encoding="utf-8", newline="") as f:
            w = csv.writer(f); w.writerow(["sym", "t", "o", "h", "l", "c", "q"]); w.writerows(rows)
    open(f"{out_dir}/missing.txt", "w").write("\n".join(missing) + "\n")
    meta = f"jobs={len(jobs)} files={got} rows={n} missing={len(missing)}"
    open(f"{out_dir}/meta.txt", "w").write(meta + "\n")
    print(meta, flush=True)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "out")
