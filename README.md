# 얼마냐봇 v2.7 — 알림에 시장 게이트 삽입

## 선행 조건
**스캐너 v4.57 배포가 먼저.** `/api/market/gate` 응답 구조가 바뀌므로,
봇만 먼저 올리면 `gate_kr` / `gate_us` / `max_open_r_kr` 필드가 없어 헤더가 안 뜸
(에러는 안 나고 조용히 스킵됨).

## 적용 파일
`main.py` 하나.

## 작업

### 블록 A — 신규 추가
`_gate_last = {"suggest": None}` 선언 **아래**에:
- `_gate_cache`, `_GATE_CACHE_TTL`, `_GATE_EMOJI`
- `get_gate(force=False)`
- `_gate_line(ticker)`

`import time as _time`은 이미 있음 (`_pick_prev_close` 위).

### 블록 B — 교체
`check_market_gate()` **통째로 교체**. 4개 지수 표시 + 제거 규칙 내역 표시.

### 블록 C — 헤더 삽입 (4곳)
각 `send_telegram("\n".join(lines))` 직전에:
```python
_gl = _gate_line(ticker)
if _gl:
    lines = [_gl, ""] + lines
```

| 함수 | 알림 |
|---|---|
| `check_pivot_breakout()` | 🎯 목표가 도달 |
| `check_pivot_breakout()` | ⚡ 피벗 접근 |
| `check_pivot_breakout()` | 🚀 피벗 돌파 ← **가장 중요** |
| `check_ma_near()` | 🎯 이평 지지 접근 |

`🚀 피벗 돌파`의 기존 마지막 줄
`"시장 게이트 확인 후 진입 · 피벗 +2% 추격 금지"`
→ `"피벗 +2% 추격 금지"` 로 변경 (게이트가 헤더에 이미 있음).

## 게이트를 넣지 않는 곳
`check_positions()`의 🛑손절 / 💰+2R / 🏔마일스톤.

청산은 **게이트와 무관하게 무조건 실행**해야 하는 규칙임.
게이트를 보여주면 "게이트 좋으니 손절 미뤄도 되나" 하는 여지를 줌.

## 출력 예시
```
[시장] 🟡 조정 압박 · 코스피 분산3 · 코스닥 분산5
         신규 오픈 리스크 상한 1.5R

🚀 피벗 돌파! 진입 검토

종목: 삼성전자 (005930)
현재가: ₩74,500
피벗: ₩74,000 돌파 ✅
손절: ₩70,300

🟢 거래량 확증 (예상 180%)
→ 🟢면 진입 검토 · 🔴면 다음 기회
피벗 +2% 추격 금지
```

`분산?` 표시는 거래량 데이터가 없어 **판정 불가**라는 뜻 (0 아님).

## 캐시
게이트는 5분 캐시. 알림마다 API를 때리지 않음.
`check_market_gate()`는 30분 스케줄이며 `force=True`로 강제 갱신.
