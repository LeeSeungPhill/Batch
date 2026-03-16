# 코드 분석 및 전략 검토 보고서

> 분석일: 2026-03-16 (재분석)
> 분석 대상: /home/user/Batch/ 전체 Python 코드베이스
> 대상 파일: 47개 파일, 35,969줄

---

## 목차

1. [프로젝트 개요](#1-프로젝트-개요)
2. [파일 구조 및 의존성](#2-파일-구조-및-의존성)
3. [중복 코드 분석](#3-중복-코드-분석)
4. [미사용 코드 분석](#4-미사용-코드-분석)
5. [보안 취약점](#5-보안-취약점)
6. [안정성 및 리소스 관리](#6-안정성-및-리소스-관리)
7. [매매 전략 분석](#7-매매-전략-분석)
8. [개선 권고사항](#8-개선-권고사항)
9. [결론](#9-결론)

---

## 1. 프로젝트 개요

### 1.1 파일 목록 (라인 수 내림차순)

| # | 파일명 | 라인 수 | 역할 |
|---|--------|--------|------|
| 1 | terrabot.py | 8,656 | 메인 텔레그램 봇 |
| 2 | reservebot.py | 3,209 | 예약매매 보조 봇 |
| 3 | reservebot_simulation.py | 2,710 | 시뮬레이션 봇 |
| 4 | kis_holding_item.py | 2,073 | 보유종목 동기화 |
| 5 | kis_trading_trail_vol_state.py | 1,415 | 상태 기반 트레일링 스탑 |
| 6 | kis_interest_item.py | 1,381 | 관심종목 동기화 |
| 7 | kis_trading_trail_vol_day.py | 1,032 | 일봉 트레일링 스탑 |
| 8 | kis_trading_trail_vol.py | 1,031 | 거래량 기반 트레일링 스탑 |
| 9 | kis_api_prod.py | 848 | KIS API 래퍼 (기본) |
| 10 | kis_api_prod_phills75.py | 848 | KIS API 래퍼 (phills75) |
| 11 | kis_api_prod_phills13.py | 848 | KIS API 래퍼 (phills13) |
| 12 | kis_api_prod_phills15.py | 848 | KIS API 래퍼 (phills15) |
| 13 | kis_api_prod_chichipa.py | 848 | KIS API 래퍼 (chichipa) |
| 14 | kis_api_prod_mama.py | 848 | KIS API 래퍼 (mama) |
| 15 | kis_auto_proc.py | 743 | 자동 매매 프로세스 |
| 16 | kis_simulation(volumn).py | 642 | 거래량 돌파 백테스트 |
| 17 | kw_fast_stock_search.py | 634 | 키움 고속 종목검색 |
| 18 | kis_simulation(돌파 후 이탈).py | 553 | 돌파 후 이탈 백테스트 |
| 19 | kis_stock_order_complete.py | 496 | 주문 체결 처리 |
| 20 | kis_simulation(돌파시 거래량).py | 492 | 돌파시 거래량 백테스트 |
| 21 | kis_simulation(돌파 후 오종전저).py | 486 | 전일종가 돌파 백테스트 |
| 22 | kis_trading_simulation.py | 486 | 매매 시뮬레이션 실행 |
| 23 | kis_stock_search.py | 483 | 종목 차트 검색 |
| 24 | kis_trading_simulation_day.py | 470 | 일자 고정 시뮬레이션 |
| 25 | kis_balance_save.py | 390 | 잔고 저장 (기본) |
| 26 | kis_cash_proc.py | 338 | 현금 비중 관리 |
| 27 | kis_stock_search_api.py | 258 | API 종목 검색 |
| 28 | kis_stock_minute_save.py | 241 | 분봉 데이터 저장 |
| 29 | kis_stock_search_title.py | 226 | 종목명 검색 |
| 30 | kis_trading_save.py | 221 | 매매내역 저장 |
| 31 | kis_trading_set.py | 208 | 매매 설정 |
| 32-36 | kis_balance_*_save.py (5개) | 193×5 | 계좌별 잔고 저장 |
| 37 | kis_trading_backup.py | 184 | 매매 백업 |
| 38 | backup_data.py | 178 | AWS RDS 백업 |
| 39 | main.py | 143 | 메인 진입점 |
| 40 | kis_api_resp.py | 142 | API 응답 파서 |
| 41 | kis_subject_subtotal.py | ? | 업종별 소계 |
| 42 | kw_stock_search.py | ? | 키움 종목검색 |
| 43 | fnguidePerformbot.py | ? | FnGuide 성과 분석 |
| 44 | matplotlib_dir.py | 5 | matplotlib 설정 확인 (미사용) |
| 45-47 | call_*.py (3개) | ? | 동기화 호출 스크립트 |

---

## 2. 파일 구조 및 의존성

### 2.1 의존성 그래프

```
라이브러리 모듈 (import 대상):
  kis_api_resp.py ← 8개 파일에서 import
  kis_api_prod.py ← kis_balance_save.py, kis_trading_trail_vol*.py, kis_simulation*.py 등
  kis_api_prod_{계좌}.py ← 대응 kis_balance_{계좌}_save.py

메인 실행 스크립트:
  terrabot.py ──→ kis_api_prod_phills75.py ──→ kis_api_resp.py
  reservebot.py ──→ kis_api_prod_phills75.py ──→ kis_api_resp.py
  kis_trading_trail_vol*.py ──→ kis_api_prod.py ──→ kis_api_resp.py
  kis_auto_proc.py ──→ kis_api_prod.py ──→ kis_api_resp.py
  kis_simulation*.py ──→ kis_api_prod.py ──→ kis_api_resp.py

독립 실행 스크립트 (내부 auth/account):
  kis_holding_item.py, kis_interest_item.py, kis_cash_proc.py,
  kis_stock_search*.py, kis_balance_*_save.py, backup_data.py 등

키움증권 계열 (KIS와 무관):
  kw_stock_search.py, kw_fast_stock_search.py
```

### 2.2 실행 진입점 vs 라이브러리

| 분류 | 파일 | 특징 |
|------|------|------|
| **라이브러리** | kis_api_resp.py | 공유 APIResp 클래스 |
| **라이브러리** | kis_api_prod*.py (6개) | KIS API 래퍼 |
| **메인 봇** | terrabot.py, reservebot.py | 상시 실행 |
| **자동매매** | kis_trading_trail_vol*.py (3개), kis_auto_proc.py | 장중 실행 |
| **데이터 수집** | kis_balance_*_save.py, kis_holding_item.py 등 | 정기 배치 |
| **백테스트** | kis_simulation*.py (4개), kis_trading_simulation*.py (2개) | 수동 실행 |

---

## 3. 중복 코드 분석

### 3.1 kis_api_prod*.py — 6개 파일, 5,088줄 중 4,240줄 중복

6개 파일이 **100% 동일**하며 **2곳만 다름**:

| 파일 | line 12: YAML 경로 | line 95: personalname |
|------|-------------------|---------------------|
| kis_api_prod.py | `kisdev_vi.yaml` | `'이승필'` |
| kis_api_prod_phills75.py | `kis_phills75.yaml` | `'phillseungkorea'` |
| kis_api_prod_phills13.py | `kis_phills13.yaml` | `'이재용'` |
| kis_api_prod_phills15.py | `kis_phills15.yaml` | `'이수지'` |
| kis_api_prod_chichipa.py | `kis_chichipa.yaml` | `'김미옥'` |
| kis_api_prod_mama.py | `kis_mama.yaml` | `'phillseungkorea'` |

**절감 가능:** 848줄 기본 + 설정파일 → **~4,000줄 절감 (78.7%)**

### 3.2 kis_balance_*_save.py — 6개 파일, ~1,355줄 중 865줄 중복

기본 `kis_balance_save.py` (390줄)과 5개 변형(193줄×5)이 존재. 차이점은 import하는 계좌 모듈만 다름.

**절감 가능:** ~865줄 (63.8%)

### 3.3 kis_trading_simulation.py vs _day.py — 99% 동일

| 파일 | line 21 차이점 |
|------|--------------|
| kis_trading_simulation.py | `today = datetime.now().strftime("%Y%m%d")` |
| kis_trading_simulation_day.py | `today = '20260213'` (하드코딩) |

나머지 코드 100% 동일. **절감 가능:** ~470줄

### 3.4 kis_trading_trail_vol.py vs _day.py — 85~90% 동일

시간대별 매도 조건에 약간의 차이. **절감 가능:** ~880줄

### 3.5 auth()/account() 패턴 — 30개+ 파일에서 반복

각 파일마다 30~40줄의 auth() + 30~40줄의 account() 함수가 독립적으로 정의됨.

**절감 가능:** ~2,100줄

### 3.6 중복 코드 요약

| 대상 | 중복 라인 | 비율 |
|------|----------|------|
| kis_api_prod*.py 통합 | ~4,000줄 | 11.1% |
| kis_balance_*_save.py 통합 | ~865줄 | 2.4% |
| trail_vol.py vs _day.py | ~880줄 | 2.4% |
| simulation.py vs _day.py | ~470줄 | 1.3% |
| auth()/account() 패턴 | ~2,100줄 | 5.8% |
| **합계** | **~8,315줄** | **23.1%** |

---

## 4. 미사용 코드 분석

### 4.1 미사용 파일

| 파일 | 라인 | 상태 |
|------|------|------|
| matplotlib_dir.py | 5줄 | 프로젝트 내 import/호출 0회. 디버깅 유틸리티 |

### 4.2 미사용 함수 (kis_api_prod.py 및 5개 변형, 총 6파일)

| 함수명 | 위치 | 프로젝트 전체 호출 | 추정 라인 |
|--------|------|------------------|----------|
| `do_sell()` | :426 | 0회 | ~10줄×6 |
| `do_buy()` | :434 | 0회 | ~10줄×6 |
| `get_orders()` | :442 | 0회 (do_cancel_all 내부만) | ~28줄×6 |
| `do_cancel_all()` | :519 | 0회 | ~12줄×6 |
| `get_current_price_OS()` | :734 | 0회 | ~22줄×6 |
| `get_stock_history_OS()` | :756 | 0회 | ~30줄×6 |
| `do_order_OS()` | :787 | 0회 | ~26줄×6 |

**절감 가능:** ~138줄 × 6파일 = **~828줄**

### 4.3 미사용 import

| 파일 | import | 상태 |
|------|--------|------|
| kis_trading_simulation.py:7 | `from psycopg2.extras import execute_values` | 미사용 |
| kis_trading_simulation_day.py:7 | `from psycopg2.extras import execute_values` | 미사용 |

---

## 5. 보안 취약점

### 5.1 하드코딩 비밀번호 — CRITICAL

**3종의 DB 비밀번호가 소스코드에 직접 포함:**

| 비밀번호 | 용도 | 파일 수 |
|---------|------|--------|
| 비밀번호1 | localhost:5432 | **37개 파일** |
| 비밀번호2 | 192.168.50.81:5432 | 2개 파일 |
| 비밀번호3 | AWS RDS | 1개 파일 |

주요 위치:
- kis_api_prod.py:78 (및 5개 변형 동일 라인)
- terrabot.py:108
- reservebot.py:104
- kis_auto_proc.py:56
- kis_balance_save.py:41 (및 5개 변형)
- 기타 25개+ 파일

### 5.2 SQL Injection — CRITICAL

**CLI 인자 직접 삽입 (가장 위험):**

```python
# kis_subject_subtotal.py:78
cur01.execute("... where nick_name = '" + arguments[1] + "'")

# reservebot_simulation.py:28, 309
"where nick_name = '" + arguments[1] + "'"
```

**f-string SQL (전체적으로 광범위):**

200개+ SQL 쿼리에서 f-string 또는 문자열 연결 사용:
- kis_holding_item.py: 116, 153, 209, 374, 458, 580, 610, 660 등
- kis_interest_item.py: 93, 127, 164, 280, 320 등
- kis_trading_trail_vol.py: 62-70, 100-130
- kis_trading_trail_vol_state.py: 65-75, 100-145
- terrabot.py: 500+, 700+, 1000+ 등 (텔레그램 콜백 데이터 → SQL 흐름 가능)
- reservebot.py: 유사 패턴

**권장:** 파라미터화된 쿼리 사용
```python
cur01.execute("... where nick_name = %s", (arguments[1],))
```

### 5.3 SSL 검증 비활성화 — HIGH

**130건+ API 호출에서 `verify=False`:**
- kis_api_prod*.py: ~13건 × 6파일 = ~78건
- terrabot.py: ~15건
- reservebot.py: ~10건
- 기타 파일: ~27건

### 5.4 .gitignore 부재 — HIGH

프로젝트 루트에 `.gitignore` 파일 없음. YAML 설정 파일(API 키/시크릿)과 DB 비밀번호가 포함된 소스코드가 git에 노출될 위험.

### 5.5 Bare Except 패턴 — HIGH

11개 인스턴스에서 `except:` (타입 미지정) 사용:

| 파일 | 라인 | 영향 |
|------|------|------|
| kis_api_resp.py | :44 | SystemExit/KeyboardInterrupt 삼킴 |
| kis_api_prod.py (×6) | :221 | API 실패 원인 추적 불가 |
| terrabot.py | :119 | 예외 무시 |
| reservebot.py | :120 | 예외 무시 |
| reservebot_simulation.py | :102 | 예외 무시 |
| kis_trading_save.py | :22 | 예외 무시 |

---

## 6. 안정성 및 리소스 관리

### 6.1 DB 커넥션 누수 — CRITICAL

**35개+ 파일**에서 `psycopg2.connect()`를 호출하고 `conn.close()` 없이 사용:

```python
# 전형적 패턴 (30개+ 파일)
conn = psycopg2.connect(...)  # 글로벌 스코프
cur01 = conn.cursor()
# ... 파일 끝까지 close() 없음
```

**특히 위험한 경우:**
- kis_trading_backup.py:40-43 — 2개 커넥션(로컬+원격) 모두 미종료
- backup_data.py:22-34 — 2개 커넥션(로컬+AWS) 모두 미종료
- kis_api_prod*.py의 auth() — 호출 시마다 새 커넥션 생성, 미종료 (토큰 갱신 시 누적)

### 6.2 API 타임아웃 미설정 — CRITICAL

**130건+ requests.get/post 호출**에 `timeout` 파라미터 없음:

- kis_api_prod*.py: ~13건 × 6파일 = ~78건
- terrabot.py: ~15건
- reservebot.py: ~10건
- 기타: ~27건

KIS API 서버 행(hang) 시 프로세스가 무한 대기하여:
- 봇 무응답
- 매매 신호 누락
- 좀비 프로세스 누적

### 6.3 중복 주문 방지 — CRITICAL (부재)

7개 핵심 매매 파일에 중복 주문 방지 메커니즘 **없음**:
- kis_trading_trail_vol.py, kis_trading_trail_vol_state.py, kis_trading_trail_vol_day.py
- kis_auto_proc.py, kis_cash_proc.py
- terrabot.py, reservebot.py

`kis_trading_trail_vol_state.py`의 `order_cash()` 호출 시 동일 종목에 이미 주문이 있는지 확인하지 않아, 스크립트 재실행이나 네트워크 오류로 동일 주문이 중복 제출될 수 있음.

### 6.4 트랜잭션 관리 — HIGH

| 항목 | 건수 |
|------|------|
| conn.commit() | ~200건 |
| conn.rollback() | ~7건 |
| **비율** | **28:1** |

대부분의 DB 쓰기에 에러 복구 없음. 쿼리 실패 시 부분 데이터가 커밋될 수 있음.

**위험 패턴 (kis_trading_trail_vol_state.py:440-537):**
```python
order_result = ka.order_cash(...)  # API 주문 호출
cur01.execute("UPDATE ...")         # DB 상태 업데이트
conn.commit()
# API 성공 → DB 실패 시 주문 추적 불가
```

---

## 7. 매매 전략 분석

### 7.1 전략 구조 요약

```
진입(Entry): kis_auto_proc.py
  └─ 10분봉 고가 돌파 시 매수 신호 (텔레그램)

청산(Exit): kis_trading_trail_vol*.py
  ├─ trail_tp='1': 돌파 전 스탑로스 이탈 → 전량 매도
  ├─ trail_tp='2': 10분 기준봉 저가 이탈 → 부분/전량 매도
  └─ trail_tp='L': 전일 저가 이탈 + 거래량 확인 → 전량 매도

리밸런싱: kis_cash_proc.py
  └─ 현금 비중 부족 시 전 포지션 동일비율 축소
```

### 7.2 상세 청산 로직

**trail_tp = '1' (신규 포지션, 돌파 전)**
| 조건 | 액션 | 위치 |
|------|------|------|
| 저가 ≤ 스탑가 | 전량 매도 (손절) | trail_vol.py:761-796 |
| 고가 ≥ 목표가 | 10분 기준봉 생성, '1'→'2' 전이 | trail_vol.py:799-837 |

**trail_tp = '2' (돌파 후, 기준봉 활성)**
| 조건 | 액션 | 위치 |
|------|------|------|
| 저가 < 기준봉 저가 | trail_plan에 따라 50%~100% 매도 | trail_vol.py:844-879 |
| 신규 10분봉 고가 > 기준봉 고가 OR 거래량 > 기준봉 거래량 | 기준봉 갱신 (트레일링) | trail_vol.py:884-917 |

**trail_tp = 'L' (장기 보유)**
| 조건 | 액션 | 위치 |
|------|------|------|
| 종가 ≤ 스탑가 AND 종가 < 전일저가 | 전량 매도 | trail_vol.py:619-654 |
| 15:10 이후, 종가 < 전일저가 AND 거래량 > 전일 50% | 전량 매도 | trail_vol.py:659-693 |

### 7.3 거래량 비율 체크 (volume_rate_chk)

| 시간대 | 요구 거래량 비율 | 비고 |
|--------|-----------------|------|
| ~10:00 | 전일 대비 50% 이상 | |
| 09:00~09:20 | 20% 이상 | |
| 09:21~09:30 | 25% 이상 | |
| 15:00~15:30 | 25% 이상 | |
| **09:30~15:00** | **무조건 통과** | **허점: 거래일 대부분(5.5시간)에서 필터 비활성** |

### 7.4 상태 기반 개선 (trail_vol_state.py)

trail_vol.py 대비 추가 기능:

| 기능 | 설명 | 위치 |
|------|------|------|
| 실제 주문 실행 | `order_cash()` 직접 호출 | :440-537 |
| 기존 주문 취소 | `sell_order_cancel_proc()` 호출 | :539-635 |
| 연속 하락 카운터 | 10분봉 저가가 기준봉 저가를 2회 연속 하회 시 매도 | :969, 1180-1186 |
| 피크 추적 | 모든 기준봉 갱신 시 최고가 기록 | :970 |
| 5% 안전마진 | `basic_price × 1.05` (일부 주석 처리) | :1162 |
| trade_tp='S' 종료 | `close_price ≤ exit_price` 시 청산 | :858-887 |
| trade_tp='M' 종료 | `close_price ≤ stop_price` 시 청산 | :890-919 |

### 7.5 자동매매 프로세스 (kis_auto_proc.py)

**Phase 1: 기준봉 탐색 (line 326-452)**
1. `trade_auto_proc` 테이블에서 당일 활성 종목 조회
2. 1분봉 → 10분봉 리샘플링, 최대 거래량 10분봉 탐색
3. 봉 몸통 분류: L(대형 >1.5x), M(중형), S(소형 <0.5x)

**Phase 2: 시그널 실행 (line 454-730)**
| 시그널 | 조건 | 포지션 사이징 | 실행 |
|--------|------|-------------|------|
| 매수 (trade_tp='B') | 현재가 > 기준봉 고가 | `round(trade_sum / 현재가)` | 텔레그램 알림 |
| 매도 (trade_tp='S') | 현재가 < 기준봉 저가 | as(100%), 66s, 50s, 33s, 25s, 20s | 텔레그램 또는 AUTO_FUND_UP_SELL시 자동실행 |

### 7.6 현금 비중 관리 (kis_cash_proc.py)

```
목표 현금 = 총평가 × (100 - market_ratio%) / 100
현금 부족분 = 목표 현금 - 현재 현금
매도 비율 = 현금 부족분 / 전체 포지션 가치
각 종목: 매도수량 = int(평가금액 × 매도비율 / 현재가)
```
- `trading_plan IN ('i','h')` 종목은 리밸런싱 제외
- 텔레그램 매도 명령 발송 (자동 실행 아님)

### 7.7 포지션 사이징

| 구분 | 방식 | 문제점 |
|------|------|--------|
| 매수 | 고정 금액(trade_sum) / 현재가 | 변동성 무시, ATR 미반영 |
| 매도 | trail_plan 비율 (20%~100%) | 적절 |
| 리밸런싱 | 전 포지션 동일 비율 축소 | 승자/패자 구분 없음 |

### 7.8 고정 파라미터 목록

| 파라미터 | 값 | 위치 | 시장 적응? |
|---------|---|------|----------|
| 기준봉 주기 | 10분 | trail_vol.py:~800 | X |
| 거래량 기준(~10:00) | 전일 50% | trail_vol.py:~500 | X |
| 거래량 기준(09:00~20) | 전일 20% | trail_vol.py:~510 | X |
| 거래량 기준(09:21~30) | 전일 25% | trail_vol.py:~515 | X |
| 거래량 기준(15:00~30) | 전일 25% | trail_vol.py:~518 | X |
| 안전마진 | 매수가 × 1.05 | trail_vol_state.py:~1162 | X |
| 연속 하락 트리거 | 2회 | trail_vol_state.py:~1185 | X |
| 장 초반 스킵 | 09:00~09:10 | trail_vol.py:~750 | X |
| API 폴링 간격 | 0.5초 | 전체 | X |
| EOD 체크 시간 | 15:10 | trail_vol.py:~659 | X |
| 봉 몸통 분류 | L>1.5x, S<0.5x | auto_proc.py:~400 | X |

### 7.9 시뮬레이션 vs 실전 차이점

| 구분 | 시뮬레이션 | 실전 |
|------|----------|------|
| 갭 하락 처리 | 시가 < 기준봉 저가 → 기준봉 무효화 | **미구현** |
| 거래량 필터 | 간소화 또는 미적용 | volume_rate_chk() 적용 |
| 주문 실행 | 없음 (추적만) | DB 업데이트 또는 실제 주문 |
| 슬리피지 | 미반영 | 미반영 |
| 수수료 | 미반영 | 미반영 |
| 주석 실험 코드 | lines 476-502 (과적합 징후) | 별도 실험 코드 |

**핵심 문제:** 갭 하락 방어 로직이 시뮬레이션에만 존재하여 백테스트 결과가 실전보다 낙관적.

### 7.10 전략 강점

1. **체계적 다층 청산**: 스탑로스 + 트레일링 + EOD 체크 + 거래량 게이트
2. **동적 트레일링**: 10분 기준봉이 가격/거래량에 따라 상향 조정
3. **부분 익절**: 20%~100% 단계적 청산으로 추가 상승 여지 보존
4. **연속 하락 대응** (state 버전): 저거래량 느린 하락에 대한 방어
5. **포트폴리오 현금 관리**: 개인투자자 시스템에서 드문 리밸런싱 체계
6. **반자동 파이프라인**: 텔레그램 알림 → 수동 확인 → 실행의 안전 장치

### 7.11 전략 약점

1. **진입 신호가 너무 단순**: `현재가 > 10분봉 고가` 돌파는 모든 HTS/MTS에서 기본 알림으로 제공하는 과밀(crowded) 전략
2. **정보/속도 우위 없음**: KIS REST API 0.5초 폴링은 기관/HFT 대비 수천 배 느림
3. **거래량 필터 09:30~15:00 허점**: 거래일 대부분에서 거래량 필터 비활성화
4. **시장 레짐 필터 없음**: KOSPI/KOSDAQ 지수 추세와 무관하게 돌파 매수 → 하락장에서 체계적 실패
5. **고정 파라미터**: 12개+ 하드코딩 값이 시장 변동성/레짐에 적응하지 못함
6. **슬리피지 무시**: 급락 시 종가 기준 지정가 주문 미체결 위험
7. **유동성 체크 없음**: 소형주 호가창 확인 없이 주문 → 미체결 위험
8. **갭 리스크**: 실전 코드에 갭 하락 방어 로직 없음
9. **단일 타임프레임**: 일봉/주봉 추세, 섹터 모멘텀 미고려
10. **포트폴리오 리스크 한도 없음**: 최대 손실, 상관관계, 섹터 집중도 제한 없음

---

## 8. 개선 권고사항

### 8.1 코드 안정성 (Phase 1 — 즉시)

| # | 항목 | 기대 효과 | 심각도 |
|---|------|-----------|--------|
| 1 | DB 비밀번호를 환경변수/시크릿으로 분리 | 보안 | CRITICAL |
| 2 | SQL Injection → 파라미터화 쿼리 전환 (200건+) | 보안 | CRITICAL |
| 3 | DB 커넥션 `try/finally` + `conn.close()` 추가 (35개+ 파일) | 안정성 | CRITICAL |
| 4 | API 호출에 `timeout=10` 추가 (130건+) | 안정성 | CRITICAL |
| 5 | 중복 주문 방지 메커니즘 추가 | 매매 신뢰성 | CRITICAL |
| 6 | .gitignore 생성 (*.yaml, *.pyc, __pycache__) | 보안 | HIGH |
| 7 | bare except → `except Exception as e:` + 로깅 전환 (11건) | 디버깅 | HIGH |
| 8 | 트랜잭션 rollback 추가 (commit:rollback = 200:7) | 데이터 무결성 | HIGH |

### 8.2 코드 품질 (Phase 2 — 단기)

| # | 항목 | 절감 효과 |
|---|------|-----------|
| 9 | kis_api_prod*.py 6파일 → 1파일 + 설정 파라미터화 | ~4,000줄 |
| 10 | kis_balance_*_save.py 6파일 → 1파일 | ~865줄 |
| 11 | kis_trading_simulation_day.py 제거 (날짜 파라미터화) | ~470줄 |
| 12 | kis_trading_trail_vol_day.py 제거 (조건 파라미터화) | ~880줄 |
| 13 | auth()/account() 공통 모듈화 | ~2,100줄 |
| 14 | 미사용 함수 제거 (do_sell, do_buy, do_cancel_all 등 7개×6파일) | ~828줄 |
| 15 | matplotlib_dir.py 삭제 | 미사용 파일 정리 |
| 16 | 미사용 import 제거 (execute_values 등) | 코드 정리 |

### 8.3 전략 개선 (Phase 3 — 중기)

| # | 항목 | 기대 효과 |
|---|------|-----------|
| 17 | KOSPI/KOSDAQ 지수 추세 필터 추가 | 하락장 거짓 돌파 필터링 |
| 18 | ATR 기반 동적 스탑로스 도입 | 변동성 적응형 리스크 관리 |
| 19 | 09:30~15:00 거래량 필터 허점 보완 | 저거래량 허봉 방어 |
| 20 | 시뮬레이션의 갭 하락 방어 로직을 실전 코드에 이식 | 갭 리스크 방어 |
| 21 | 변동성 기반 포지션 사이징 도입 | 리스크 조절 개선 |
| 22 | 시뮬레이션과 실전 로직 동기화 | 전략 검증 가능 |
| 23 | 슬리피지/수수료 모델링 추가 | 현실적 백테스트 |
| 24 | 매도 주문 시 슬리피지 버퍼 (지정가 하한 또는 시장가) | 체결률 향상 |
| 25 | Walk-Forward 검증 프레임워크 도입 | 과적합 방지 |
| 26 | 매도 실패 시 재시도 로직 추가 | 주문 누락 방지 |

---

## 9. 결론

### 9.1 현재 시스템의 본질

이 시스템은 **알파(초과수익)를 생성하는 전략이라기보다는, 규율 있는 매매 실행을 강제하는 리스크 관리 프레임워크**입니다. 개인투자자가 손실 종목을 끝까지 보유하거나 감정적으로 매매하는 것을 방지하는 데 실질적인 가치가 있습니다.

### 9.2 핵심 수치

| 항목 | 수치 |
|------|------|
| 총 파일/라인 | 47개 / 35,969줄 |
| 중복 코드 | ~8,315줄 (23.1%) |
| 미사용 함수 | ~828줄 (7개 함수 × 6파일) |
| CRITICAL 보안 이슈 | 5건 (비밀번호, SQL Injection, DB 누수, 타임아웃, 중복주문) |
| 고정 파라미터 | 12개+ (시장 비적응) |
| 시뮬레이션-실전 괴리 | 3건+ (갭 방어, 거래량 필터, 수수료) |

### 9.3 개선 로드맵

**Phase 1: 생존 (코드 안정성)** — 즉시
- DB 누수/비밀번호/SQL Injection/타임아웃/중복주문 해결
- 시스템이 예기치 않게 죽거나 잘못된 주문을 내지 않도록 보장

**Phase 2: 정리 (코드 품질)** — 1~2주
- kis_api_prod*.py, kis_balance_*_save.py, _day.py 변형 통합
- ~9,000줄 코드 절감, 유지보수성 대폭 향상

**Phase 3: 강화 (전략 개선)** — 중기
- 시장 레짐 필터 + 동적 파라미터 + Walk-Forward 검증
- 진입 신호 다양화, 포트폴리오 레벨 리스크 관리 도입
