"""v2.82 짧은 코인 요약 — 오늘(11-15) 실제 숫자 모양으로."""
import coin_signal_track as st
import signal_tracker as tr

D = 86_400_000
NOW = 1_794_700_000_000


def pos(result, pct, adds=0, status="closed", age_d=1, pb=True, mae=0.3):
    return {"opened_ms": NOW - age_d * D, "closed_ms": NOW - age_d * D + 3_600_000, "status": status,
            "result": result if status == "closed" else None, "result_pct": pct if status == "closed" else None,
            "adds": [{}] * adds, "mae_pct": mae, "trimmed": False, "pullback_ok": pb, "tp": 1.0}


def track(touch2, age_d=1, pb=True, done=True):
    return {"start_ms": NOW - age_d * D, "pullback_ok": pb, "status": "done" if done else "open",
            "result": "hit_tp", "mae_pct": 0.5, "touch_2_ms": 1 if touch2 else None}


def sig(age_d=1):
    return {"bar_close_ms": NOW - age_d * D, "sent": True}


# 11-15 아침 요약의 7일 숫자: 추가 있음 8개 중 4 익절(평균 −0.70) · 없음 9개 다 익절(+0.46) · 보유 1
POS = ([pos("hit_tp", 0.5, adds=1)] * 4 + [pos("giveup", -1.9, adds=2)] * 4 + [pos("hit_tp", 0.46)] * 9
       + [pos(None, None, status="open")] + [pos("hit_tp", 0.5, age_d=20)] * 30)
TRACKS = [track(True)] * 53 + [track(False)] * 142


def test_brief_lines_and_colors():
    out = tr.format_brief([sig()] * 195, POS, NOW, tracks=TRACKS).split("\n")
    assert out[0].startswith("📊 코인 7일 (")
    assert out[1].startswith("🔴 봇 포지션 18개: 익절 13 · 포기 4 · 보유 1 (건당 −")   # 평균 −0.08%
    assert out[2] == "🔴 추가 진입한 건 8개 중 4개 익절 (추가 안 한 건 9개 중 9개)"
    assert out[3] == "🟡 신호 195개 · 그중 53개가 −2% 먼저 갔다 왔어"              # 27%
    assert out[4].startswith("30일: 봇 포지션 48개 · 익절 43")
    assert out[-1] == "자세히: <code>요약 자세히</code>"
    assert len(out) == 6 and "v1 가상" not in "\n".join(out) and "억제" not in "\n".join(out)


def test_brief_empty_and_thresholds():
    out = tr.format_brief([], [], NOW, tracks=[])
    assert "⚪ 봇 포지션 없음" in out and "⚪ 신호 0개" in out and "추가 진입" not in out and "30일" not in out
    good = [pos("hit_tp", 0.5)] * 3
    assert tr.format_brief([], good, NOW, tracks=[track(False)] * 10).split("\n")[1].startswith("🟢")
    assert "🔴 신호" in tr.format_brief([sig()] * 3, [], NOW, tracks=[track(True)] * 3 + [track(False)] * 7)   # 30%
    assert "🟢 신호" in tr.format_brief([sig()] * 10, [], NOW, tracks=[track(True)] + [track(False)] * 9)     # 10%


def test_daily_sends_brief_and_detail_keeps_old(tmp_path, monkeypatch):
    sent = []
    tr.daily_summary(sent.append, path=str(tmp_path / "s.json"), pos_path=str(tmp_path / "p.json"),
                     now_ms=NOW, track_path=str(tmp_path / "t.json"))
    assert sent[0].startswith("📊 코인 7일") and "자세히" in sent[0]
    d = tr.detail_text(path=str(tmp_path / "s.json"), pos_path=str(tmp_path / "p.json"), now_ms=NOW,
                       track_path=str(tmp_path / "t.json"))
    assert d.startswith("📊 코인 요약 (") and "[최근 7일]" in d and "v1 가상" not in d
    src = open("main.py", encoding="utf-8").read()
    assert 'if text in ("요약 자세히", "요약자세히", "자세히"):' in src and "signal_tracker.detail_text()" in src
