# 코드 분석 및 전략 검토 보고서

> 분석일: 2026-03-16
> 분석 대상: /home/user/Batch/ 전체 Python 코드베이스 (44개 파일)

---

## 목차

1. [리소스 누수 (Resource Leak)](#1-리소스-누수-resource-leak)
2. [중복 코드 및 미사용 코드](#2-중복-코드-및-미사용-코드)
3. [보안 취약점 및 에러 처리](#3-보안-취약점-및-에러-처리)
4. [매매 전략 심층 분석](#4-매매-전략-심층-분석)
5. [종합 개선 권고사항](#5-종합-개선-권고사항)

---

## 1. 리소스 누수 (Resource Leak)

### 1.1 DB 커서 누수 — CRITICAL

대부분의 파일에서 psycopg2 cursor를 `try-finally`나 `with`문 없이 사용하고 있어, 예외 발생 시 커서가 닫히지 않습니다.

| 파일 | 심각도 | 문제 |
|------|--------|------|
| **backup_data.py** | CRITICAL | 예외 처리 전무. 16개 커서(cur100~cur107, cur200~cur207) 생성 후, 에러 시 모든 커서+연결 누수 |
| **kis_trading_backup.py:264-269** | CRITICAL | except 블록에서 remote_conn만 닫고, 5개 로컬 커서(cur1~cur5) 미정리 |
| **kis_stock_order_complete.py:199-487** | CRITICAL | 중첩 커서(cur101, cur302, cur401, cur600) try-finally 없음 |
| **kis_balance_save.py:378-386** | HIGH | except 블록에서 모든 커서 close() 시도하지만, 아직 생성되지 않은 커서 close() 시 NameError 발생 → 후속 커서/conn도 미정리 |
| **kis_holding_item.py:2015-2068** | HIGH | 중첩 커서 500/501이 except 핸들러에서 미정리 |
| **kis_interest_item.py:1305-1376** | HIGH | 동일한 패턴의 커서 누수 |
| **kis_auto_proc.py:317-738** | HIGH | 광범위한 try 블록 내 다수 커서, except에서 정리 없음 |
| **kis_trading_set.py:121-350** | HIGH | except에서 커서 정리 없음 |
| **kis_stock_minute_save.py:134-220** | HIGH | 루프 내 예외 시 cur1 미정리 |

**권장 수정:**
```python
# Before (문제)
cur = conn.cursor()
cur.execute(query)
cur.close()

# After (권장)
with conn.cursor() as cur:
    cur.execute(query)
```

### 1.2 API 타임아웃 미설정 — HIGH

**40개 이상**의 `requests.get()`/`requests.post()` 호출에서 `timeout` 파라미터가 누락되어 있습니다. 서버 무응답 시 봇이 무한 대기 상태에 빠집니다.

**해당 파일:** kis_api_prod*.py (6개 모두), kis_auto_proc.py, kis_cash_proc.py, kis_trading_trail_vol*.py 등

```python
# Before
res = requests.post(url, headers=headers, data=json.dumps(params), verify=False)

# After
res = requests.post(url, headers=headers, data=json.dumps(params), verify=False, timeout=10)
```

---

## 2. 중복 코드 및 미사용 코드

### 2.1 극심한 코드 중복 — CRITICAL

#### kis_api_prod*.py (6개 파일, 각 25KB)
- **99.9% 동일한 복사본**
- 차이점: **오직 2줄** (YAML 파일 경로 line 12, 이름 line 95)
- 총 **~150KB**의 중복 코드

| 파일 | 차이점 (line 12) | 차이점 (line 95) |
|------|------------------|------------------|
| kis_api_prod.py | `kisdev_vi.yaml` | `이승필` |
| kis_api_prod_chichipa.py | `kis_chichipa.yaml` | `김미옥` |
| kis_api_prod_mama.py | `kis_mama.yaml` | `phillseungkorea` |
| kis_api_prod_phills13.py | `kis_phills13.yaml` | `이재용` |
| kis_api_prod_phills15.py | `kis_phills15.yaml` | `이수지` |
| kis_api_prod_phills75.py | `kis_phills75.yaml` | `phillseungkorea` |

**권장:** 하나의 파일로 통합, 계좌 닉네임을 파라미터로 받아 YAML 동적 로드

#### kis_balance_*_save.py (6개 파일, 각 ~15KB)
- 계좌별 변형이지만 핵심 로직 동일
- kis_balance_save.py에만 종목검색 로직(192줄) 추가 포함

#### kis_trading_trail_vol.py vs _day.py
- 1031줄 vs 1032줄, 차이점: 날짜 하드코딩(`20260213`), 계좌 리스트만 다름
- _day.py는 테스트/디버깅 용도로 추정

#### kis_trading_simulation.py vs _day.py
- 474줄 vs 463줄, 유사한 차이 패턴
- _day.py에서 VOLUMN 필드 제거, JOIN 타입 변경(LEFT → FULL OUTER)

### 2.2 미사용 파일

| 파일 | 상태 | 설명 |
|------|------|------|
| **matplotlib_dir.py** | 미사용 | matplotlib 경로 출력용 디버그 스크립트. 어디에서도 import 안 됨 |
| **reservebot_simulation.py** | 독립실행 | 134KB, 어디에서도 import 안 됨. 단독 CLI 실행 전용 |

### 2.3 반복되는 auth()/account() 패턴

거의 모든 스크립트에 동일한 `auth()`, `account()` 함수가 반복 정의됨. 공통 모듈로 추출 가능.

---

## 3. 보안 취약점 및 에러 처리

### 3.1 하드코딩된 DB 비밀번호 — CRITICAL

| 파일 | 라인 | 비밀번호 |
|------|------|----------|
| main.py | 11 | `sktl2389!1` |
| kis_auto_proc.py | 14 | `sktl2389!1` |
| kis_interest_item.py | 15 | `sktl2389!1` |
| kis_trading_trail_vol.py | 18 | `asdf1234` (원격 DB) |
| backup_data.py | 8-9 | `sktl2389!1` (로컬) + `gr971499#1` (AWS RDS) |
| kis_balance_*_save.py | 7-9 | `sktl2389!1` |

**권장:** 환경변수 또는 별도 설정 파일(.env)로 분리, .gitignore에 추가

### 3.2 SQL Injection 취약점 — CRITICAL

**30개 이상**의 SQL 쿼리에서 문자열 연결(concatenation) 사용:

```python
# 가장 위험 (커맨드라인 인자 직접 삽입)
# kis_subject_subtotal.py:78
cur01.execute("... where nick_name = '" + arguments[1] + "'")

# reservebot_simulation.py:28, 309
"... where nick_name = '" + arguments[1] + "'"
```

**권장:** 파라미터화된 쿼리 사용
```python
cur01.execute("... where nick_name = %s", (arguments[1],))
```

### 3.3 Bare Except 패턴 — HIGH

11개 인스턴스에서 `except:` (타입 미지정) 사용:
- kis_api_resp.py:44, kis_api_prod*.py:221 (6개 모두)
- terrabot.py:119, reservebot.py:120, kis_trading_save.py:22, reservebot_simulation.py:102

SystemExit, KeyboardInterrupt까지 삼켜서 프로그램 종료 불가 상태 유발 가능.

### 3.4 트랜잭션 무결성 — HIGH

- **251개** `conn.commit()` 호출 vs **7개** `conn.rollback()` 호출
- 대부분의 commit이 에러 핸들링 없이 실행됨
- rollback이 있는 파일: terrabot.py, reservebot.py, reservebot_simulation.py만

### 3.5 중복 주문 방지 로직 부재 — CRITICAL

kis_trading_trail_vol.py, kis_auto_proc.py에서 **주문 중복 방지 로직이 없음**.
스크립트 크래시 후 재시작 시 동일 주문이 재전송될 위험.

---

## 4. 매매 전략 심층 분석

### 4.1 시스템 구조 요약

```
[진입 신호] kis_auto_proc.py → 10분봉 고가 돌파 시 매수
     ↓
[청산 관리] kis_trading_trail_vol.py → 10분봉 기반 트레일링 스탑
     ↓
[상태 관리] kis_trading_trail_vol_state.py → 상태 전이 기반 청산 (개선판)
     ↓
[현금 관리] kis_cash_proc.py → 포트폴리오 현금 비중 조절
```

### 4.2 진입 전략 (kis_auto_proc.py)

**매수 조건:**
- `현재가 > 기준 10분봉 고가` (line 556-603)
- 기준 봉은 최대 거래량 봉으로 동적 갱신 (line 388-390)
- 매수 수량: `round(trade_sum / current_price)`

**캔들 분류:** (line 395-401)
- L(Long): 캔들 실체 > 20일 평균의 1.5배
- S(Short): 캔들 실체 < 20일 평균의 0.5배
- M(Medium): 그 사이

### 4.3 청산 전략 (kis_trading_trail_vol.py)

**경로 A: trail_tp='L' (장기 보유 모니터링)**
1. **수익 후 이탈 매도** (line 619-654): 종가 ≤ 스탑가 AND 종가 < 전일 저가 → 전량 매도
2. **장 마감 일봉 이탈** (line 659-693): 15:10 이후, 종가 < 전일 저가 AND 누적거래량 > 전일의 50% → 전량 매도

**경로 B: trail_tp='1'/'2' (능동적 트레일링 스탑)**
3. **돌파 전 이탈** (line 761-796): 목표가 도달 전 스탑가 도달 → 100% 매도
4. **목표가 돌파 → 기준봉 생성** (line 798-837): 고가 > 목표가 시 현재 10분봉을 기준봉으로 설정 → trail_tp='1'→'2' 전이
5. **기준봉 저가 이탈 → 부분/전량 매도** (line 843-879): 이후 저가 < 기준봉 저가 시 매도. trail_plan에 따라 50%/100%
6. **기준봉 갱신 (트레일링)** (line 884-917): 새 10분봉의 고가 또는 거래량이 기준봉을 초과하면 기준봉 교체

**거래량 비율 체크** (line 494-523):
| 시간대 | 요구 거래량 비율 |
|--------|-----------------|
| ~10:00 | 전일 대비 50% 이상 |
| 09:00~09:20 | 20% 이상 |
| 09:21~09:30 | 25% 이상 |
| 15:00~15:30 | 25% 이상 |
| 기타 | 무조건 통과 |

### 4.4 상태 기반 개선 전략 (kis_trading_trail_vol_state.py)

trail_vol.py 대비 추가된 기능:
- **실제 주문 실행**: `order_cash()` 직접 호출 (trail_vol.py는 DB 갱신만)
- **연속 하락 카운터** (line 969-978): 10분봉 저가가 기준봉 저가를 2회 연속 하회 시 매도 (저거래량 느린 하락 대응)
- **피크 추적** (line 970): 모든 기준봉 갱신 시 최고가 기록
- **5% 안전마진** (line 1162): basic_price * 1.05 이하 하락 시 추가 청산 로직 (현재 일부 주석 처리)

### 4.5 현금 비중 관리 (kis_cash_proc.py)

- 목표 현금 부족분 = `(총평가 × (100 - 시장비율%) × 0.01) - 현재현금`
- 매도 비율 = `현금부족분 / 전체포지션가치`
- 모든 포지션을 **동일 비율**로 축소
- `trading_plan IN ('i','h')` 종목은 리밸런싱에서 제외

### 4.6 전략 강점

1. **체계적 청산 로직**: 10분봉 기반 트레일링 스탑 + 거래량 확인은 기술적으로 건전한 접근
2. **다층 방어**: 거래량 필터 + 시간대 필터 + 연속하락 카운터 + 일봉 이탈 체크
3. **부분 익절**: 50%/66%/100% 등 단계적 익절로 추가 상승 여지 보존
4. **포트폴리오 레벨 현금 관리**: 개인투자자 시스템에서 보기 드문 리스크 관리 체계
5. **텔레그램 연동**: 시그널 → 확인 → 실행의 반자동 파이프라인

### 4.7 전략 약점 및 취약점

#### 시장을 "해킹"하지 못하는 이유:

1. **진입 신호가 너무 단순**: `현재가 > 10분봉 고가` 돌파 매수는 가장 기본적이고 과밀(crowded)한 전략. 모든 HTS/MTS에서 기본 알림으로 제공하는 수준.

2. **정보/속도 우위 없음**: KIS API를 수초~수분 간격으로 폴링. 기관/HFT는 밀리초 단위. 이 시스템이 돌파를 감지할 때쯤 이미 빠른 참여자들이 가격을 움직인 후.

3. **고정 파라미터**: 20%, 25%, 50% 거래량 비율, 5% 안전마진, 10분봉 타임프레임 등이 모두 하드코딩. 시장 레짐(추세/횡보/고변동/저변동)에 따라 적응하지 못함.

4. **시뮬레이션-실전 괴리**: kis_simulation(volumn).py에서 트레일링 갱신 로직이 주석 처리되어 있어 백테스트와 실전 로직이 불일치. 전략 검증 불가.

5. **시장 레짐 필터 없음**: KOSPI/KOSDAQ 지수 추세와 무관하게 돌파 매수. 하락장에서 돌파 전략은 체계적으로 실패.

6. **비대칭 리스크**: 스탑로스가 기준봉 저가에 설정되어, 넓은 레인지의 10분봉은 큰 손실폭을 의미. 리스크/리워드 비율이 불리한 진입 가능.

7. **포지션 사이징 미흡**: 켈리 기준, 변동성 기반 사이징, 포트폴리오 레벨 리스크 예산 없이 단순 금액 기반 매수.

### 4.8 전략이 실패하는 시장 환경

| 시장 상황 | 실패 원인 |
|-----------|-----------|
| **하락장** | 돌파가 거짓 신호 → 스탑 연속 적중 → 손실 누적 |
| **갭다운** | 야간 뉴스로 스탑가 이하 시초가 → 스탑 무력화 |
| **저변동 횡보** | 돌파 발생 안 함 → 유휴 상태에서 보유 종목 느린 하락 |
| **HFT 조작** | 돌파 레벨 근처 스푸핑/레이어링 → 거짓 진입 유발 |

---

## 5. 종합 개선 권고사항

### 긴급 (보안/안정성)

| # | 항목 | 영향도 |
|---|------|--------|
| 1 | DB 비밀번호를 환경변수/.env로 분리 | CRITICAL |
| 2 | SQL 파라미터화 쿼리 전환 (30개+ 취약점) | CRITICAL |
| 3 | 중복 주문 방지 로직 추가 (주문번호 체크) | CRITICAL |
| 4 | DB 커서를 with문 또는 try-finally로 보호 | CRITICAL |
| 5 | API 호출에 timeout=10 추가 (40개+ 미설정) | HIGH |
| 6 | bare except → except Exception as e 전환 | HIGH |

### 코드 품질

| # | 항목 | 효과 |
|---|------|------|
| 7 | kis_api_prod*.py 6개 → 1개 통합 (파라미터화) | ~125KB 코드 삭제 |
| 8 | kis_balance_*_save.py 공통 모듈 추출 | ~60KB 중복 제거 |
| 9 | _day.py 변형 제거 (설정 파라미터화) | ~62KB 중복 제거 |
| 10 | auth()/account() 공통 모듈화 | 유지보수성 향상 |
| 11 | matplotlib_dir.py 삭제 | 미사용 파일 정리 |

### 전략 개선

| # | 항목 | 기대 효과 |
|---|------|-----------|
| 12 | KOSPI/KOSDAQ 지수 추세 필터 추가 | 하락장 거짓 돌파 필터링 |
| 13 | ATR 기반 동적 스탑로스 | 변동성 적응형 리스크 관리 |
| 14 | 거래량 비율 파라미터를 20일 이동평균 대비로 전환 | 레짐 적응형 거래량 필터 |
| 15 | 시뮬레이션 코드를 실전 로직과 동기화 | 전략 검증 가능 |
| 16 | 변동성 기반 포지션 사이징 도입 | 리스크 조절 개선 |
| 17 | 슬리피지/수수료 모델링 추가 | 현실적 백테스트 |

---

## 결론

현재 시스템은 **체계적인 청산 로직과 포트폴리오 현금 관리**라는 좋은 뼈대를 갖추고 있지만, **진입 신호가 너무 단순하고 시장 레짐에 적응하지 못하며, 코드 레벨의 보안/안정성 이슈가 심각**합니다.

"시장을 해킹"하기 위해서는:
1. 먼저 코드 안정성(DB 누수, 중복 주문, SQL injection)을 해결하여 **시스템이 죽지 않도록** 만들고
2. 시장 레짐 필터와 동적 파라미터를 도입하여 **적응형 전략**으로 진화시키며
3. 시뮬레이션과 실전 로직을 동기화하여 **전략 검증이 가능한 구조**를 만들어야 합니다

현재 가장 큰 리스크는 전략 자체보다 **코드 버그로 인한 의도치 않은 손실**(중복 주문, 커서 누수로 인한 시스템 다운, API 타임아웃 무한 대기 등)입니다.
