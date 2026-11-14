"""tools/alt_history.py — 가짜 원천으로 전체 흐름을 돌려 본다(실제 네트워크 없음)."""
import csv, gzip, io, sys, zipfile
from datetime import date, datetime, timedelta, timezone

import pytest

sys.path.insert(0, "tools")
import alt_history as ah

D = ah.D


def zip_of(rows, header=False):
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w") as z:
        t = io.StringIO(); w = csv.writer(t)
        if header:
            w.writerow(["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume"])
        for r in rows:
            w.writerow(r)
        z.writestr("x.csv", t.getvalue())
    return b.getvalue()


def ms(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def test_find_events_uses_running_five_day_mean_and_skips_gaps():
    flat = [(i * D, 1.0, 1.0, 1.0, 1.0, 0.0) for i in range(6)]
    pump = flat + [(6 * D, 1.0, 1.6, 1.0, 1.2, 0.0)]                 # 고가 1.6: 1.6/((4+1.6)/5) − 1 = +42.9%
    ev = ah.find_events({"AAAUSDT": pump}, 0.30)
    assert [(s, t) for s, t, _ in ev] == [("AAAUSDT", 6 * D)] and ev[0][2] == pytest.approx(1.6 / 1.12 - 1)
    assert ah.find_events({"AAAUSDT": pump}, 0.45) == []
    gap = flat[:5] + [(8 * D, 1.0, 1.6, 1.0, 1.2, 0.0)]                # 중간에 날짜가 빠짐 → 평균이 안 맞으니 건너뜀
    assert ah.find_events({"AAAUSDT": gap}, 0.30) == []
    assert ah.event_symbol_days([("A", 6 * D, 0.4)], 3) == {("A", 6 * D), ("A", 7 * D), ("A", 8 * D)}


def test_rows_of_handles_header_and_microseconds():
    rows = [[1_700_000_000_000_000, "1", "2", "0.5", "1.5", "9", 0, "123.4"], [1_700_000_300_000, "1", "2", "0.5", "1.5", "9", 0, "5"]]
    out = list(ah.rows_of(zip_of(rows, header=True)))
    assert out == [(1_700_000_000_000, "1", "2", "0.5", "1.5", "123.4"), (1_700_000_300_000, "1", "2", "0.5", "1.5", "5")]
    assert list(ah.rows_of(b"not a zip")) == []


def test_main_end_to_end_with_fake_source(tmp_path, monkeypatch):
    today = datetime.now(timezone.utc).date()
    first = today.replace(day=1)
    prev = (first - timedelta(days=1)).replace(day=1)
    syms = [f"S{i:03d}USDT" for i in range(120)] + ["DEADUSDT", "BTCUSDC", "ETHUSDT_260327"]
    days_prev = [(prev + timedelta(days=k)) for k in range((first - prev).days)]
    calls = []

    def fake_get(url, tries=3):
        calls.append(url)
        if "amazonaws" in url:
            pfx = url.split("prefix=")[1].split("&")[0]
            if pfx.endswith("/monthly/klines/"):
                return ("<Prefix>x</Prefix>" + "".join(f"<Prefix>{pfx}{s}/</Prefix>" for s in syms)).encode()
            sym = pfx.split("/")[-3]
            tag = "2020-01" if sym == "DEADUSDT" else prev.strftime("%Y-%m")       # 상폐 종목은 옛 파일만
            return f"<Prefix>x</Prefix><Key>{pfx}{sym}-1d-{tag}.zip</Key>".encode()
        name = url.rsplit("/", 1)[-1]
        sym = name.split("-")[0]
        if "/monthly/" in url:
            if sym == "DEADUSDT":
                return zip_of([[ms(date(2020, 1, 1)) + k * D, 1, 1, 1, 1, 0, 0, 10] for k in range(31)])
            rows = [[ms(d), 1, 1, 1, 1, 0, 0, 10] for d in days_prev]
            if sym == "S000USDT":
                rows[10] = [ms(days_prev[10]), 1, 2.0, 1, 1.3, 0, 0, 999]          # 급등일 하나
            return zip_of(rows)
        if "/1d/" in url:
            return zip_of([[ms(date.fromisoformat(name[-14:-4])), 1, 1, 1, 1, 0, 0, 10]])
        if "/5m/" in url:
            d0 = ms(date.fromisoformat(name[-14:-4]))
            return zip_of([[d0 + k * 300_000, 1, 1, 1, 1, 0, 0, 1] for k in range(288)])
        if "/1m/" in url:
            return None
        raise AssertionError(url)

    monkeypatch.setattr(ah, "get", fake_get)
    daily_rows = 120 * (len(days_prev) + (today - first).days) + 31
    monkeypatch.setattr(ah, "main", ah.main)
    # 실제 가드(일봉 10만 행)는 가짜 자료엔 너무 크다 — 가드가 걸리는 것 자체를 확인
    with pytest.raises(SystemExit, match="일봉이 너무 적다"):
        ah.main(str(tmp_path))
    rows = list(csv.reader(gzip.open(tmp_path / "daily.csv.gz", "rt")))
    assert len(rows) - 1 == daily_rows
    got = {r[0] for r in rows[1:]}
    assert "DEADUSDT" in got and "BTCUSDC" not in got and "ETHUSDT_260327" not in got   # 상폐 포함·USDT 무기한만
    assert not any("/daily/klines/DEADUSDT/1d/" in c for c in calls)                    # 죽은 종목은 이번 달 일별 조회 안 함
    # 가드를 풀고 끝까지: 급등일 + 뒤 2일의 5분봉만 받는다
    calls.clear()
    ah.main(str(tmp_path), min_daily=0)
    five = sorted(c.rsplit("/", 1)[-1] for c in calls if "/5m/" in c)
    want = sorted(f"S000USDT-5m-{(days_prev[10] + timedelta(days=k)).isoformat()}.zip" for k in range(3)
                  if days_prev[10] + timedelta(days=k) < today)
    assert five == want
    y = days_prev[10].strftime("%Y")
    ev = list(csv.reader(gzip.open(tmp_path / f"ev5m_{y}.csv.gz", "rt")))
    assert ev[0] == ["sym", "t", "o", "h", "l", "c", "q"] and ev[1][0] == "S000USDT" and len(ev) - 1 >= 288
    meta = (tmp_path / "meta.txt").read_text()
    assert "events=1 " in meta and "event_dev=0.30" in meta
