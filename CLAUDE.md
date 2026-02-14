# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

한국투자증권(KIS) Open API와 키움증권 API를 활용한 주식 자동매매 및 텔레그램 봇 시스템. 개발자 terra가 다수의 증권계좌(phills75, phills13, phills15, chichipa, mama 등)를 관리하며, 실시간 매매/모니터링/백업을 수행한다.

## Running Scripts

대부분의 스크립트는 계좌 닉네임을 인자로 실행:
```bash
python terrabot.py phills75
python reservebot.py phills75
python kis_trading_trail_vol.py mamalong
python kis_trading_simulation.py mamalong
python kis_holding_item.py phills75
python call_sync_holding_item.py phills75
```

`kis_api_prod.py` 계열은 직접 실행하지 않고, `main.py`나 `kis_stock_search.py`에서 모듈로 import하여 사용.

## Architecture

### 외부 의존성
- **KIS Open API** (`openapi.koreainvestment.com:9443`): 실전투자 REST API. OAuth2 토큰 인증
- **Kiwoom API** (`api.kiwoom.com`): 키움증권 REST + WebSocket (`kw_stock_search.py`)
- **PostgreSQL** (`fund_risk_mng` DB): 계좌정보, 매매내역, 종목데이터 저장. `psycopg2`로 직접 연결
  - 로컬: `localhost:5432`
  - 원격: `192.168.50.81:5432`
  - AWS RDS: `backup_data.py`에서 백업 대상
- **Telegram Bot API**: `python-telegram-bot` 라이브러리. 매매 알림 및 수동 주문 UI
- **KRX**: `kind.krx.co.kr`에서 상장법인 종목코드 목록 다운로드

### DB 테이블 (Django ORM 관리)
- `stockAccount_stock_account`: 계좌번호, API키, 토큰, 텔레그램봇 토큰 등 계정 마스터
- `stock_holiday`: 휴장일 관리

### 핵심 모듈 구조

**API 래퍼 (`kis_api_prod*.py`)**
- 계좌별 YAML 설정 파일(`kisdev_vi.yaml`, `kis_chichipa.yaml` 등)을 로드하여 인증/주문/조회 수행
- `kis_api_resp.py`의 `APIResp` 클래스로 응답 파싱 (namedtuple 기반)

**텔레그램 봇 (`terrabot.py`, `reservebot.py`)**
- `terrabot.py`(~500KB): 메인 봇. 차트 조회, 매수/매도 주문, 잔고 조회, 종목검색 등 전체 기능
- `reservebot.py`(~119KB): 예약매매/자동매매 관련 보조 봇

**자동매매 로직**
- `kis_trading_trail_vol.py`: 거래량 비율 기반 트레일링 스탑 매매
- `kis_trading_trail_vol_state.py`: 상태 기반(수익/이탈) 트레일링 스탑 매매
- `kis_trading_simulation.py`: 매매 시뮬레이션 실행
- `kis_auto_proc.py`: 자동 매매 프로세스 (조건 체크 → 주문)
- `kis_cash_proc.py`: 현금 비중 관리

**데이터 관리**
- `kis_holding_item.py`/`kis_interest_item.py`: 보유종목/관심종목 동기화 (KIS API → DB)
- `kis_trading_save.py`: 매매 내역 저장
- `kis_trading_backup.py`: 로컬 DB → 원격 DB 백업
- `kis_balance_save.py` + 계좌별 variants: 계좌 잔고 스냅샷 저장
- `kis_stock_minute_save.py`: 분봉 데이터 저장
- `backup_data.py`: 로컬 → AWS RDS 전체 백업

**시뮬레이션 (`kis_simulation*.py`)**
- 다양한 매매 전략을 과거 데이터로 백테스트
- 돌파, 거래량, 이탈 등 조건별 시뮬레이션

### 공통 패턴
- 모든 스크립트에 `auth()` + `account()` 함수가 반복 존재 (토큰 만료 시 자동 재발급)
- DB 테이블명이 Django 스타일 (`"stockAccount_stock_account"`) — 쌍따옴표 필수
- KIS API 호출 시 `tr_id`로 실전/모의 구분 (예: `TTTC8434R` 실전, `VTTC8434R` 모의)
- `time.sleep(0.5)` 등으로 API rate limit 대응

## YAML Config Structure

각 계좌 YAML 파일 구조 (`kis_*.yaml`, `kisdev_vi.yaml`):
```yaml
my_app: "실전 APP KEY"
my_sec: "실전 APP SECRET"
paper_app: "모의 APP KEY"
paper_sec: "모의 APP SECRET"
my_acct_stock: "계좌번호"
my_phone: "전화번호"
prod: "https://openapi.koreainvestment.com:9443"
vps: "https://openapivts.koreainvestment.com:29443"
```

## Language

코드 내 주석과 변수명은 한국어와 영어가 혼용됨. 커밋 메시지는 한국어로 작성.
