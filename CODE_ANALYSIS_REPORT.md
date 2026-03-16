# 코드 분석 및 전략 검토 보고서

> 분석일: 2026-03-16 (소스 정리 후 재분석)
> 분석 대상: claude/code-analysis-strategy-review-s1qRY 브랜치
> 대상 파일: **19개 파일, 20,482줄**

---

## 목차

1. [프로젝트 현황](#1-프로젝트-현황)
2. [깨진 참조 분석 (CRITICAL)](#2-깨진-참조-분석)
3. [보안 취약점](#3-보안-취약점)
4. [리소스 누수](#4-리소스-누수)
5. [중복 코드 및 미사용 코드](#5-중복-코드-및-미사용-코드)
6. [에러 핸들링](#6-에러-핸들링)
7. [매매 전략 분석](#7-매매-전략-분석)
8. [개선 권고사항](#8-개선-권고사항)
9. [결론](#9-결론)

---

## 1. 프로젝트 현황

### 1.1 파일 목록 (라인 수 내림차순)

| # | 파일명 | 라인 수 | 역할 | 정상 작동? |
|---|--------|--------|------|-----------|
| 1 | terrabot.py | 8,656 | 메인 텔레그램 봇 | **X** (깨진 import) |
| 2 | reservebot.py | 3,209 | 예약매매 보조 봇 | **X** (깨진 import) |
| 3 | kis_holding_item.py | 2,073 | 보유종목 동기화 | O |
| 4 | kis_trading_trail_vol_state.py | 1,415 | 상태 기반 트레일링 스탑 | **X** (깨진 import) |
| 5 | kis_interest_item.py | 1,381 | 관심종목 동기화 | O |
| 6 | kis_auto_proc.py | 743 | 자동 매매 프로세스 | **X** (깨진 import) |
| 7 | kw_fast_stock_search.py | 634 | 키움 고속 종목검색 | O |
| 8 | kw_stock_search.py | 361 | 키움 종목검색 | O |
| 9 | kis_trading_save.py | 360 | 매매내역 저장 | **X** (깨진 import) |
| 10 | kis_trading_set.py | 353 | 매매 설정 | **X** (깨진 import) |
| 11 | fnguidePerformbot.py | 294 | FnGuide 성과 분석 | O |
| 12 | kis_trading_backup.py | 271 | 매매 백업 | **X** (깨진 import) |
| 13 | kis_stock_minute_save.py | 226 | 분봉 데이터 저장 | **X** (깨진 import) |
| 14 | kis_cash_proc.py | 223 | 현금 비중 관리 | **X** (깨진 import) |
| 15 | kis_subject_subtotal.py | 154 | 업종별 소계 | **X** (깨진 import) |
| 16 | kis_api_resp.py | 64 | API 응답 파서 | O |
| 17 | call_sync_total_item.py | 23 | 동기화 호출 | **X** (깨진 import) |
| 18 | call_sync_holding_item.py | 23 | 동기화 호출 | **X** (깨진 import) |
| 19 | call_upd_dly_stock_item.py | 19 | 일별 업데이트 호출 | **X** (깨진 import) |

**14개 파일이 깨진 import로 실행 불가 상태.**

---

## 2. 깨진 참조 분석

### 2.1 삭제된 파일을 import하는 코드 — CRITICAL

소스 정리로 `kis_api_prod*.py` 6개 파일이 삭제되었으나, 이를 import하는 코드가 14개 파일에 남아 있음.

**kis_api_prod (기본 모듈) — 삭제됨:**

| 파일 | 라인 | import 구문 |
|------|------|------------|
| kis_auto_proc.py | 14 | `import kis_api_prod as kb` |
| kis_cash_proc.py | 15 | `import kis_api_prod as kb` |
| kis_trading_trail_vol_state.py | 13 | `import kis_api_prod as kb` |
| kis_stock_minute_save.py | 13 | `import kis_api_prod as ka` |
| kis_trading_save.py | 14 | `import kis_api_prod as ka` |
| kis_trading_set.py | 13 | `import kis_api_prod as ka` |
| kis_trading_backup.py | 13 | `import kis_api_prod as ka` |
| kis_subject_subtotal.py | 14 | `import kis_api_prod as ka` |
| call_sync_holding_item.py | 4 | `import kis_api_prod as ka` |
| call_sync_total_item.py | 4 | `import kis_api_prod as ka` |
| call_upd_dly_stock_item.py | 4 | `import kis_api_prod as ka` |

**kis_api_prod_{계좌별} — 삭제됨:**

| 파일 | 라인 | import 구문 |
|------|------|------------|
| terrabot.py | 19-23 | `import kis_api_prod_phills75 as ka_phills75` 외 4개 |
| reservebot.py | 19-23 | `import kis_api_prod_phills75 as ka_phills75` 외 4개 |

**kis_stock_search_api 모듈도 삭제됨:**

| 파일 | 라인 | import 구문 |
|------|------|------------|
| terrabot.py | 22 | `import kis_stock_search_api as search` |

- terrabot.py:5643, 5657, 5671, 5685, 5699에서 `search.search()` 호출 → 런타임 에러
- 종목 검색 기능 완전 마비

**합계: 14개 파일에서 22건의 깨진 import. 이 파일들은 실행 시 즉시 `ModuleNotFoundError`로 종료됨.**

---

## 3. 보안 취약점

### 3.1 하드코딩 비밀번호 — CRITICAL

**2종의 DB 비밀번호가 소스코드에 직접 포함:**

| 비밀번호 | 용도 | 파일 수 |
|---------|------|--------|
| localhost 비밀번호 | localhost:5432 | **16개 파일** |
| 원격 DB 비밀번호 | 192.168.50.81:5432 | 1개 파일 (kis_trading_backup.py:40) |

해당 파일: terrabot.py:108, reservebot.py:104, kis_holding_item.py:46, kis_interest_item.py:44, kis_auto_proc.py:56, kis_cash_proc.py:40, kis_trading_trail_vol_state.py:40, kis_stock_minute_save.py:40, kis_trading_save.py:40, kis_trading_set.py:37, kis_trading_backup.py:38, kis_subject_subtotal.py:40, kw_stock_search.py:30, kw_fast_stock_search.py:25, fnguidePerformbot.py:18

### 3.2 SQL Injection — CRITICAL

**CLI 인자 직접 삽입 (가장 위험):**
```python
# kis_subject_subtotal.py:78
cur01.execute("... where nick_name = '" + arguments[1] + "'")
```

**f-string SQL (광범위):**

14개 파일에서 **150건+ SQL 쿼리**가 f-string 또는 문자열 연결 사용:
- terrabot.py: 447, 1066, 1260, 2150, 3400, 5000+ 등
- reservebot.py: 330, 580, 890, 1200+ 등
- kis_holding_item.py: 116, 153, 209, 374, 458, 580, 660+ 등
- kis_interest_item.py: 93, 127, 164, 280, 320+ 등
- kis_trading_trail_vol_state.py: 65, 100, 200, 350, 500+ 등
- 기타 9개 파일

### 3.3 SSL 검증 비활성화 — HIGH

**40건+ API 호출에서 `verify=False`:**
- terrabot.py: ~10건, reservebot.py: ~8건
- kis_auto_proc.py: ~5건, kis_trading_trail_vol_state.py: ~5건
- kw_fast_stock_search.py: ~5건, 기타: ~7건

### 3.4 Bare Except — HIGH

| 파일 | 라인 | 영향 |
|------|------|------|
| kis_api_resp.py | :44 | SystemExit/KeyboardInterrupt 삼킴, `return False` |
| terrabot.py | :119 | `except: pass` — 예외 무시 |
| reservebot.py | :120 | `except: pass` — 예외 무시 |
| kis_trading_save.py | :22 | 예외 무시 |

---

## 4. 리소스 누수

### 4.1 DB 커넥션 누수 — CRITICAL

**16개 파일**에서 글로벌 스코프로 `psycopg2.connect()` 호출, **`conn.close()` 없음**:

| 파일 | 라인 | 비고 |
|------|------|------|
| terrabot.py | 110-113 | 글로벌, 미종료 |
| reservebot.py | 106-109 | 글로벌, 미종료 |
| kis_holding_item.py | 48-50 | 글로벌, 미종료 |
| kis_interest_item.py | 46-48 | 글로벌, 미종료 |
| kis_auto_proc.py | 58-60 | 글로벌, 미종료 |
| kis_cash_proc.py | 42-44 | 글로벌, 미종료 |
| kis_trading_trail_vol_state.py | 42-44 | 글로벌, 미종료 |
| kis_stock_minute_save.py | 42-44 | 글로벌, 미종료 |
| kis_trading_save.py | 42-44 | 글로벌, 미종료 |
| kis_trading_set.py | 39-41 | 글로벌, 미종료 |
| kis_trading_backup.py | 40-43 | **2개 커넥션** (로컬+원격) 모두 미종료 |
| kis_subject_subtotal.py | 42-44 | 글로벌, 미종료 |
| kw_stock_search.py | 32-34 | 글로벌, 미종료 |
| kw_fast_stock_search.py | 27-29 | 글로벌, 미종료 |
| fnguidePerformbot.py | 20-22 | 글로벌, 미종료 |

**합계: 17개 커넥션 (16파일) 미종료**

### 4.2 API 타임아웃 미설정 — CRITICAL

**48건+ `requests.get/post` 호출**에 `timeout` 파라미터 없음:
- terrabot.py: ~15건, reservebot.py: ~10건
- kis_auto_proc.py: ~5건, kis_trading_trail_vol_state.py: ~5건
- kw_fast_stock_search.py: ~5건, 기타: ~8건

API 서버 행(hang) 시 프로세스 무한 대기 → 봇 무응답, 매매 신호 누락 가능.

---

## 5. 중복 코드 및 미사용 코드

### 5.1 중복 코드

| 패턴 | 파일 수 | 중복 라인 |
|------|--------|----------|
| auth() 함수 | 12개 | ~210줄 |
| account() 함수 | 12개 | ~360줄 |
| stock_balance() 함수 | 8개 | ~240줄 |
| format_number() 함수 | 3개 | ~15줄 |
| DB 연결 패턴 | 16개 | ~80줄 |
| **합계** | - | **~905줄 (전체의 4.4%)** |

**account() 중복 상세:**
kis_cash_proc.py:37-67, kis_auto_proc.py:37-67, kis_trading_set.py:32-64, kis_trading_backup.py:37-65, kis_trading_save.py:33-62, kis_stock_minute_save.py:35-66, kis_holding_item.py:38-68, kis_interest_item.py:38-68, kis_trading_trail_vol_state.py:43-68, terrabot.py:648-682, reservebot.py:326-359

**stock_balance() 중복 상세:**
kis_cash_proc.py:89-119, kis_auto_proc.py:111-141, kis_holding_item.py:90-120, kis_trading_set.py:67-100, kis_stock_minute_save.py:49-79, kis_trading_save.py:65-95, kis_trading_trail_vol_state.py:60-86, kis_interest_item.py 포함

### 5.2 미사용 import

| 파일 | 라인 | import | 상태 |
|------|------|--------|------|
| kis_auto_proc.py | 7 | `import asyncio` | 미사용 (await/async 없음) |
| kis_cash_proc.py | 7 | `import asyncio` | 미사용 |
| kis_holding_item.py | 7 | `import asyncio` | 미사용 |
| kis_interest_item.py | 7 | `import asyncio` | 미사용 |

### 5.3 비기능 스크립트 (실행 불가)

| 파일 | 라인 | 이유 |
|------|------|------|
| call_sync_holding_item.py | 23 | kis_api_prod import 깨짐 |
| call_sync_total_item.py | 23 | kis_api_prod import 깨짐 |
| call_upd_dly_stock_item.py | 19 | kis_api_prod import 깨짐 |

이 3개 파일은 삭제하거나 import를 수정해야 함.

---

## 6. 에러 핸들링

### 6.1 트랜잭션 관리

| 항목 | 건수 |
|------|------|
| conn.commit() | ~165건 |
| conn.rollback() | **0건** |
| **비율** | **165:0** |

**rollback이 단 한 건도 없음.** 예외 발생 시 불완전한 트랜잭션이 커밋될 수 있음.

참고: `kis_trading_set.py:303`에서 `ON CONFLICT DO NOTHING` 사용 (DB 레벨 중복 방지는 일부 존재)

### 6.2 중복 주문 방지 — CRITICAL (부재)

5개 핵심 매매 파일에 중복 주문 방지 메커니즘 **없음**:
- kis_trading_trail_vol_state.py: `order_cash()` 호출 전 기존 주문 확인 없음
- kis_auto_proc.py: 시그널 생성/주문 실행 시 중복 체크 없음
- kis_cash_proc.py, terrabot.py, reservebot.py: 동일

---

## 7. 매매 전략 분석

### 7.1 현재 남은 유일한 트레일링 전략: kis_trading_trail_vol_state.py

소스 정리로 `kis_trading_trail_vol.py`, `kis_trading_trail_vol_day.py`, 모든 `kis_simulation*.py`가 삭제됨.
**현재 남은 유일한 트레일링 스탑 전략이자 시뮬레이션 없이 운용 중.**

### 7.2 상태 전이 다이어그램

```
trail_tp='1' (신규 포지션, 돌파 대기)
  ├── 저가 ≤ 스탑가 → 전량 매도 (조기 종료)
  └── 고가 ≥ 목표가 → 10분 기준봉 생성 → trail_tp='2'

trail_tp='2' (돌파 후, 기준봉 활성)
  ├── 저가 < 기준봉 저가 → trail_plan에 따라 부분/전량 매도
  ├── 연속 하락 2회 → 매도
  └── 신규 고가 > 기준봉 고가 OR 신규 거래량 > 기준봉 거래량 → 기준봉 갱신 (트레일링)

trail_tp='L' (장기 보유)
  ├── 종가 ≤ 스탑가 AND 종가 < 전일저가 → 전량 매도
  └── 15:10 이후, 종가 < 전일저가 AND 거래량 > 전일 50% → 전량 매도

trade_tp='S': 종가 ≤ exit_price → 전량 매도
trade_tp='M': 종가 ≤ stop_price → 전량 매도
```

### 7.3 거래량 비율 체크 — 로직 버그 발견

**kis_trading_trail_vol_state.py:494-523**

```python
def volume_rate_chk(cur_time, acml_vol, prev_volume):
    if cur_time <= '10:00:00':           # 조건1: 10시 이전 → 50% 요구
        return acml_vol >= prev_volume * 0.5
    elif cur_time >= '09:00:00' and cur_time <= '09:20:00':   # 조건2: 09:00~09:20 → 20%
        return acml_vol >= prev_volume * 0.2
    elif cur_time >= '09:21:00' and cur_time <= '09:30:00':   # 조건3: 09:21~09:30 → 25%
        return acml_vol >= prev_volume * 0.25
    elif cur_time >= '15:00:00' and cur_time <= '15:30:00':   # 조건4: 15:00~15:30 → 25%
        return acml_vol >= prev_volume * 0.25
    else:
        return True                       # 조건5: 그 외 → 무조건 통과
```

**버그: 조건2, 조건3은 실행 불가능한 데드 코드(dead code)**

- 09:00~09:30은 `cur_time <= '10:00:00'` (조건1)에 먼저 걸림
- 따라서 조건1의 50% 기준이 적용되고, 조건2의 20%와 조건3의 25%는 **절대 실행되지 않음**
- 개발자 의도는 09:00~09:20에 20%, 09:21~09:30에 25%였겠지만 실제로는 10:00 이전 전체가 50%

**실제 동작:**
| 시간대 | 의도한 기준 | 실제 동작 |
|--------|-----------|----------|
| 09:00~09:20 | 20% | **50%** (조건1에 먼저 걸림) |
| 09:21~09:30 | 25% | **50%** (조건1에 먼저 걸림) |
| 09:31~10:00 | - | 50% (조건1) |
| 10:01~14:59 | - | **무조건 통과** (조건5) |
| 15:00~15:30 | 25% | 25% (조건4) |
| 15:31~ | - | **무조건 통과** (조건5) |

### 7.4 기준봉 갱신 — 설계 결함

**kis_trading_trail_vol_state.py:893**

```python
if int(new_high) > int(tenmin_state["base_high"]) or int(new_vol) > int(tenmin_state["base_vol"]):
    tenmin_state["base_low"] = new_low      # ← 문제
    tenmin_state["base_high"] = new_high
    tenmin_state["base_vol"] = new_vol
```

**결함:** 거래량만 증가하고(new_vol > base_vol) 가격은 하락한 경우(new_low < base_low), 기준봉이 갱신되면서 **트레일링 스탑(base_low)이 하향 이동**함. 트레일링 스탑의 핵심 원칙(스탑은 위로만 이동)을 위반.

**예시:**
```
기존 기준봉: base_low=10,000, base_high=10,500, base_vol=50,000
신규 10분봉: new_low=9,800, new_high=10,300, new_vol=60,000 (거래량만 증가)
→ 갱신 후: base_low=9,800 (스탑이 10,000에서 9,800으로 하락!)
```

### 7.5 자동매매 프로세스 (kis_auto_proc.py)

**매수 시그널 (trade_tp='B', line ~556):**
- 조건: `현재가 > 기준봉 고가` (10분봉 돌파)
- 포지션 사이징: `round(trade_sum / 현재가)` — 고정 금액, 변동성 무시
- 실행: 텔레그램 알림만 (수동 확인 필요)

**매도 시그널 (trade_tp='S', line ~606):**
- 조건: `현재가 < 기준봉 저가`
- 비율: as(100%), 66s(66%), 50s(50%), 33s(33%), 25s(25%), 20s(20%)
- AUTO_FUND_UP_SELL 표시 종목만 자동 실행

### 7.6 현금 비중 관리 (kis_cash_proc.py)

```
목표 현금 = 총평가 × (100 - market_ratio%) / 100
현금 부족분 = 목표 현금 - 현재 현금
매도 비율 = 현금 부족분 / 전체 포지션 가치
각 종목: 매도수량 = int(평가금액 × 매도비율 / 현재가)
```
- 전 포지션 동일 비율 축소 (승자/패자 구분 없음)
- `trading_plan IN ('i','h')` 종목 제외
- 텔레그램 명령 발송 (자동 실행 아님)

### 7.7 고정 파라미터 목록

| 파라미터 | 값 | 위치 | 시장 적응? |
|---------|---|------|----------|
| 기준봉 주기 | 10분 | trail_vol_state.py:~800 | X |
| 거래량 기준(<10:00) | 전일 50% | :~500 | X |
| 거래량 기준(15:00~30) | 전일 25% | :~518 | X |
| 안전마진 | 매수가 × 1.05 | :~1162 | X |
| 연속 하락 트리거 | 2회 | :~1185 | X |
| 장 초반 스킵 | 09:00~09:10 | :~750 | X |
| EOD 체크 시간 | 15:10 | :~659 | X |
| API 폴링 간격 | 0.5초 | 전체 | X |

### 7.8 전략 강점

1. **다층 청산 구조**: 스탑로스(trail_tp='1') + 트레일링(trail_tp='2') + EOD 체크(trail_tp='L') + 연속하락 카운터
2. **동적 트레일링**: 10분 기준봉이 가격/거래량에 따라 상향 조정 (단, 하향 이동 결함 있음)
3. **부분 익절**: 20%~100% 단계적 청산
4. **연속 하락 대응**: 저거래량 느린 하락에 2회 연속 하락 시 매도
5. **반자동 파이프라인**: 텔레그램 알림 → 수동 확인 → 실행의 안전장치
6. **기존 주문 취소**: `sell_order_cancel_proc()`로 신규 매도 전 기존 주문 정리

### 7.9 전략 약점

1. **시스템 현재 비기능**: 21건 깨진 import로 14개 파일 실행 불가
2. **volume_rate_chk 데드코드**: 09:00~09:30 기준이 실행되지 않아 의도보다 높은 50% 기준 적용
3. **기준봉 갱신 결함**: 거래량 조건만으로 갱신 시 스탑이 하향 이동 가능
4. **09:30~15:00 거래량 필터 없음**: 거래일 대부분(4.5시간)에서 거래량 무검증
5. **진입 신호 부재**: 시스템은 청산만 관리, 진입은 외부/수동
6. **시뮬레이션 삭제됨**: 전략 검증 수단 없음
7. **고정 파라미터 8개+**: 시장 레짐에 적응하지 못함
8. **시장 레짐 필터 없음**: KOSPI/KOSDAQ 지수 추세와 무관하게 매매
9. **중복 주문 방지 없음**: 스크립트 재실행 시 동일 주문 중복 가능
10. **슬리피지 무시**: 급락 시 지정가 주문 미체결 위험

### 7.10 시장을 해킹할 수 있는가?

**아니요.** 근본적인 이유:

1. **시스템이 현재 작동하지 않음**: kis_api_prod 삭제로 모든 API 호출, 주문 실행이 불가
2. **정보/속도 우위 없음**: REST API 0.5초 폴링은 기관/HFT 대비 수천 배 느림
3. **과밀 전략**: 10분봉 고가 돌파 매수는 모든 HTS/MTS에서 기본 제공
4. **전략 검증 불가**: 시뮬레이션 파일 전부 삭제되어 백테스트 불가
5. **코드 버그**: volume_rate_chk 데드코드와 기준봉 하향 이동 결함이 전략 신뢰성 저하
6. **리스크 관리 부족**: 포트폴리오 레벨 최대 손실 한도, 섹터 집중도 제한 없음

---

## 8. 개선 권고사항

### Phase 1: 시스템 복구 (즉시)

| # | 항목 | 심각도 | 영향 |
|---|------|--------|------|
| 1 | **kis_api_prod 모듈 복원 또는 재구성** — 14개 파일의 21건 깨진 import 해결 | CRITICAL | 시스템 작동 불가 |
| 2 | call_*.py 3개 파일 import 수정 또는 삭제 | CRITICAL | 비기능 스크립트 |
| 3 | volume_rate_chk() 조건 순서 수정 (조건2,3을 조건1 앞으로) | HIGH | 데드코드/로직 버그 |
| 4 | 기준봉 갱신 시 base_low 하향 방지 (`new_low = max(new_low, old_base_low)`) | HIGH | 트레일링 결함 |
| 5 | 중복 주문 방지 메커니즘 추가 | CRITICAL | 중복 주문 위험 |

### Phase 2: 보안/안정성 (단기)

| # | 항목 | 심각도 |
|---|------|--------|
| 6 | DB 비밀번호를 환경변수로 분리 (16개 파일) | CRITICAL |
| 7 | SQL Injection → 파라미터화 쿼리 전환 (150건+) | CRITICAL |
| 8 | API 호출에 `timeout=10` 추가 (48건+) | CRITICAL |
| 9 | DB 커넥션 `try/finally` + `conn.close()` (16개 파일) | CRITICAL |
| 10 | bare except → `except Exception as e:` + 로깅 (4건) | HIGH |
| 11 | .gitignore 생성 | HIGH |
| 12 | 트랜잭션 rollback 추가 (commit:rollback = **165:0**) | HIGH |
| 13 | 미사용 `import asyncio` 제거 (4개 파일) | LOW |

### Phase 3: 코드 품질 (중기)

| # | 항목 | 효과 |
|---|------|------|
| 14 | auth()/account()/stock_balance() 공통 모듈화 (kis_common.py) | ~810줄 절감 |
| 15 | DB 연결 패턴 공통화 (16개 파일) | ~80줄 절감 + 보안 |
| 16 | format_number() 공통화 (3개 파일) | ~15줄 절감 |

### Phase 4: 전략 개선 (중장기)

| # | 항목 | 기대 효과 |
|---|------|-----------|
| 15 | 시뮬레이션 프레임워크 재구축 | 전략 검증 가능 |
| 16 | KOSPI/KOSDAQ 지수 추세 필터 | 하락장 거짓 돌파 필터링 |
| 17 | ATR 기반 동적 스탑로스 | 변동성 적응형 리스크 관리 |
| 18 | 09:30~15:00 거래량 필터 보완 | 저거래량 허봉 방어 |
| 19 | 변동성 기반 포지션 사이징 | 리스크 조절 개선 |
| 20 | 슬리피지/수수료 모델링 | 현실적 백테스트 |

---

## 9. 결론

### 9.1 현재 상태

**소스 정리 후 시스템이 대부분 비기능 상태입니다.** 19개 파일 중 14개가 삭제된 `kis_api_prod` 모듈을 참조하여 실행 불가. 정상 동작하는 파일은 5개뿐 (kis_holding_item.py, kis_interest_item.py, kw_*.py, fnguidePerformbot.py, kis_api_resp.py).

### 9.2 핵심 수치

| 항목 | 수치 |
|------|------|
| 총 파일/라인 | 19개 / 20,482줄 |
| 깨진 import | **22건 (14개 파일)** |
| 하드코딩 비밀번호 | 16개 파일 (2종) |
| SQL Injection | 150건+ (14개 파일) |
| SSL verify=False | 49건 |
| DB 커넥션 누수 | 17개 (16파일) |
| API 타임아웃 미설정 | 49건 |
| commit vs rollback | **165:0** |
| 중복 주문 방지 | 없음 |
| 중복 코드 | ~905줄 (4.4%) |
| 미사용 import | 4건 (asyncio) |
| 전략 로직 버그 | 2건 (volume_rate_chk 데드코드, 기준봉 하향 이동) |

### 9.3 시장을 해킹하려면

1. **먼저 시스템을 복구** — kis_api_prod 모듈 복원이 최우선
2. **로직 버그 수정** — volume_rate_chk 데드코드, 기준봉 하향 이동 결함
3. **보안/안정성 확보** — DB 비밀번호, SQL Injection, 커넥션 누수, 타임아웃
4. **전략 검증 체계 구축** — 시뮬레이션 프레임워크 재구축
5. **진입 전략 고도화** — 시장 레짐 필터, 다양한 진입 시그널, 변동성 기반 사이징
