"""
trail_tp='L' 추적매매 전략 시뮬레이션
- 5가지 트레일링 스탑 전략을 과거 데이터로 백테스트
- 결과를 비교 테이블 및 CSV로 출력
"""

import warnings
warnings.filterwarnings('ignore')

import psycopg2 as db
import requests
import json
import time
import csv
import os
from datetime import datetime
from collections import defaultdict

# ============================================================
# 설정
# ============================================================
BASE_URL = "https://openapi.koreainvestment.com:9443"
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_OUTPUT = os.path.join(SCRIPT_DIR, "trail_tp_L_simulation_result.csv")

# ============================================================
# 인증 처리
# ============================================================
def auth(app_key, app_secret):
    """KIS API 토큰 발급"""
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecret": app_secret,
    }
    url = f"{BASE_URL}/oauth2/tokenP"
    res = requests.post(url, headers=headers, data=json.dumps(body), verify=False, timeout=10)
    return res.json()["access_token"]


# ============================================================
# 계좌 조회 (첫 번째 계좌)
# ============================================================
def get_first_account(conn):
    """DB에서 첫 번째 계좌 정보를 가져온다"""
    cur = conn.cursor()
    cur.execute("""
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date, nick_name
        FROM "stockAccount_stock_account"
        LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    if row is None:
        raise Exception("계좌 정보가 없습니다.")

    acct_no, access_token, app_key, app_secret, token_publ_date, nick_name = row
    today = datetime.now().strftime("%Y%m%d")

    # 토큰 유효성 확인 — 만료 시 재발급
    need_refresh = False
    try:
        valid_date = datetime.strptime(token_publ_date, '%Y%m%d%H%M%S')
        token_day = token_publ_date[:8]
        if (datetime.now() - valid_date).days >= 1 or token_day != today:
            need_refresh = True
    except Exception:
        need_refresh = True

    if need_refresh:
        access_token = auth(app_key, app_secret)
        token_publ_date = datetime.now().strftime('%Y%m%d%H%M%S')
        cur2 = conn.cursor()
        cur2.execute("""
            UPDATE "stockAccount_stock_account"
            SET access_token = %s, token_publ_date = %s, last_chg_date = %s
            WHERE acct_no = %s
        """, (access_token, token_publ_date, datetime.now(), acct_no))
        conn.commit()
        cur2.close()

    print(f"[계좌] {nick_name} ({acct_no})")
    return {
        "acct_no": acct_no,
        "access_token": access_token,
        "app_key": app_key,
        "app_secret": app_secret,
    }


# ============================================================
# 완료된 L-type 거래 조회
# ============================================================
def get_completed_L_trades(conn):
    """trail_tp IN ('3','4') 이면서 trade_result 있는 과거 L 거래 조회"""
    cur = conn.cursor()
    cur.execute("""
        SELECT acct_no, code, name, trail_day, trail_dtm,
               basic_price, basic_qty, stop_price, target_price,
               exit_price, trail_price, trail_qty, trail_rate,
               trade_tp, trade_result, trail_tp, order_price, proc_min
        FROM trading_trail
        WHERE trail_tp IN ('3', '4')
        ORDER BY trail_day DESC
    """)
    columns = [
        "acct_no", "code", "name", "trail_day", "trail_dtm",
        "basic_price", "basic_qty", "stop_price", "target_price",
        "exit_price", "trail_price", "trail_qty", "trail_rate",
        "trade_tp", "trade_result", "trail_tp", "order_price", "proc_min",
    ]
    rows = cur.fetchall()
    cur.close()
    trades = [dict(zip(columns, r)) for r in rows]
    print(f"[거래] 완료된 L-type 거래 {len(trades)}건 조회")
    return trades


# ============================================================
# 일별 OHLCV 조회 (최근 30거래일)
# ============================================================
def get_daily_ohlcv(stock_code, access_token, app_key, app_secret):
    """KIS API로 일봉(최근 30거래일) 조회"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKST01010400",
        "custtype": "P",
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_PERIOD_DIV_CODE": "D",
        "FID_ORG_ADJ_PRC": "1",
    }
    try:
        res = requests.get(url, headers=headers, params=params, verify=False, timeout=10)
        data = res.json()
        output = data.get("output", [])
        # 날짜 오름차순 정렬
        bars = []
        for item in output:
            bars.append({
                "date": item["stck_bsop_date"],
                "open": float(item["stck_oprc"]),
                "high": float(item["stck_hgpr"]),
                "low": float(item["stck_lwpr"]),
                "close": float(item["stck_clpr"]),
                "volume": float(item["acml_vol"]),
            })
        bars.sort(key=lambda x: x["date"])
        return bars
    except Exception as e:
        print(f"  [오류] {stock_code} 일봉 조회 실패: {e}")
        return []


# ============================================================
# ATR(14) 계산
# ============================================================
def calc_atr(bars, period=14):
    """True Range 기반 ATR 계산. bars는 날짜 오름차순 리스트."""
    if len(bars) < 2:
        return 0.0
    true_ranges = []
    for i in range(1, len(bars)):
        prev_close = bars[i - 1]["close"]
        cur = bars[i]
        tr = max(
            cur["high"] - cur["low"],
            abs(cur["high"] - prev_close),
            abs(cur["low"] - prev_close),
        )
        true_ranges.append(tr)

    if len(true_ranges) == 0:
        return 0.0

    # 단순 이동평균 ATR
    use = true_ranges[-period:] if len(true_ranges) >= period else true_ranges
    return sum(use) / len(use)


# ============================================================
# 전략 시뮬레이션 함수들
# ============================================================

def simulate_strategy_A(trade, bars_from_entry):
    """전략 A: 현행 — trade_tp별 고정 손절 + D+1 전일저가 기준"""
    basic_price = float(trade["basic_price"] or 0)
    stop_price = float(trade["stop_price"] or 0)
    exit_price = float(trade["exit_price"] or 0)
    trade_tp = trade["trade_tp"]

    if len(bars_from_entry) == 0:
        return None, None

    for i, bar in enumerate(bars_from_entry):
        close = bar["close"]

        # D+0: trade_tp별 기준
        if i == 0:
            if trade_tp == 'S' and exit_price > 0 and close <= exit_price:
                return bar["date"], close
            if trade_tp == 'M' and stop_price > 0 and close <= stop_price:
                return bar["date"], close
        else:
            # D+1 이후: 전일 저가 하회 & 거래량 조건
            prev_bar = bars_from_entry[i - 1]
            prev_low = prev_bar["low"]
            prev_vol = prev_bar["volume"]
            if close < prev_low and bar["volume"] > prev_vol / 2:
                return bar["date"], close

    # 기간 내 매도 안 됨 — 마지막 봉 종가로 처리
    last = bars_from_entry[-1]
    return last["date"], last["close"]


def simulate_strategy_B(trade, bars_from_entry, all_bars):
    """전략 B: ATR × 2.0 트레일링 스탑"""
    return _simulate_atr_trail(trade, bars_from_entry, all_bars, multiplier=2.0)


def simulate_strategy_C(trade, bars_from_entry, all_bars):
    """전략 C: ATR × 1.5 트레일링 스탑 (타이트)"""
    return _simulate_atr_trail(trade, bars_from_entry, all_bars, multiplier=1.5)


def _simulate_atr_trail(trade, bars_from_entry, all_bars, multiplier):
    """ATR 기반 트레일링 스탑 공통 로직"""
    basic_price = float(trade["basic_price"] or 0)
    stop_price = float(trade["stop_price"] or 0)

    if len(bars_from_entry) == 0:
        return None, None

    atr = calc_atr(all_bars)
    if atr == 0:
        atr = basic_price * 0.03  # 폴백: 매입가의 3%

    highest_close = basic_price
    for bar in bars_from_entry:
        close = bar["close"]
        highest_close = max(highest_close, close)
        dynamic_trail = highest_close - atr * multiplier
        # 원래 손절가 이하로는 내려가지 않음
        if stop_price > 0:
            dynamic_trail = max(dynamic_trail, stop_price)
        if close < dynamic_trail:
            return bar["date"], close

    last = bars_from_entry[-1]
    return last["date"], last["close"]


def simulate_strategy_D(trade, bars_from_entry):
    """전략 D: 수익 구간별 단계적 손절"""
    basic_price = float(trade["basic_price"] or 0)
    stop_price = float(trade["stop_price"] or 0)

    if len(bars_from_entry) == 0 or basic_price == 0:
        return None, None

    for bar in bars_from_entry:
        close = bar["close"]
        gain = (close - basic_price) / basic_price

        if gain >= 0.20:
            effective_stop = basic_price * 1.15
        elif gain >= 0.10:
            effective_stop = basic_price * 1.07
        else:
            effective_stop = stop_price if stop_price > 0 else basic_price * 0.95

        if close < effective_stop:
            return bar["date"], close

    last = bars_from_entry[-1]
    return last["date"], last["close"]


def simulate_strategy_E(trade, bars_from_entry, all_bars):
    """전략 E: 하이브리드 (ATR + 수익구간) — 추천 전략"""
    basic_price = float(trade["basic_price"] or 0)
    stop_price = float(trade["stop_price"] or 0)

    if len(bars_from_entry) == 0 or basic_price == 0:
        return None, None

    atr = calc_atr(all_bars)
    if atr == 0:
        atr = basic_price * 0.03

    highest_close = basic_price
    for bar in bars_from_entry:
        close = bar["close"]
        highest_close = max(highest_close, close)
        gain = (close - basic_price) / basic_price

        if gain >= 0.20:
            effective_stop = max(basic_price * 1.15, highest_close - atr * 1.5)
        elif gain >= 0.10:
            effective_stop = max(basic_price * 1.07, highest_close - atr * 2.0)
        elif gain >= 0.0:
            base_stop = stop_price if stop_price > 0 else basic_price * 0.95
            effective_stop = max(base_stop, highest_close - atr * 2.5)
        else:
            effective_stop = stop_price if stop_price > 0 else basic_price * 0.95

        if close < effective_stop:
            return bar["date"], close

    last = bars_from_entry[-1]
    return last["date"], last["close"]


# ============================================================
# 메인 시뮬레이션
# ============================================================
def main():
    print("=" * 60)
    print("trail_tp='L' 추적매매 전략 시뮬레이션")
    print("=" * 60)
    print()

    # DB 연결
    conn = db.connect(conn_string)
    print("[DB] 연결 성공")

    # 계좌 정보 (API 호출용)
    acct = get_first_account(conn)

    # 완료된 L-type 거래 조회
    trades = get_completed_L_trades(conn)
    if not trades:
        print("[종료] 시뮬레이션 대상 거래가 없습니다.")
        conn.close()
        return

    # 종목별 일봉 데이터 수집 (중복 호출 방지)
    unique_codes = list(set(t["code"] for t in trades))
    print(f"[데이터] 대상 종목 {len(unique_codes)}개 일봉 조회 중...")
    daily_data = {}  # code -> list of bars
    for idx, code in enumerate(unique_codes):
        print(f"  ({idx + 1}/{len(unique_codes)}) {code} 조회중...")
        bars = get_daily_ohlcv(code, acct["access_token"], acct["app_key"], acct["app_secret"])
        if bars:
            daily_data[code] = bars
        time.sleep(0.5)  # API 호출 제한 방지
    print(f"[데이터] {len(daily_data)}개 종목 일봉 수집 완료")
    print()

    # 전략별 결과 저장
    strategy_names = ["현행(A)", "ATR×2.0(B)", "ATR×1.5(C)", "수익구간(D)", "하이브리드(E)"]
    # 각 전략: list of { trade, sell_date, sell_price, return_pct }
    results = {name: [] for name in strategy_names}

    # CSV 상세 내역
    csv_rows = []

    skipped = 0
    for trade in trades:
        code = trade["code"]
        name = trade["name"]
        trail_day = str(trade["trail_day"])  # YYYYMMDD 형식
        basic_price = float(trade["basic_price"] or 0)
        basic_qty = float(trade["basic_qty"] or 0)

        if code not in daily_data or basic_price == 0:
            skipped += 1
            continue

        all_bars = daily_data[code]

        # trail_day 이후의 봉 필터링
        bars_from_entry = [b for b in all_bars if b["date"] >= trail_day]
        if len(bars_from_entry) == 0:
            # trail_day가 일봉 기간 밖인 경우 스킵
            skipped += 1
            continue

        # 각 전략 시뮬레이션
        sell_A_date, sell_A_price = simulate_strategy_A(trade, bars_from_entry)
        sell_B_date, sell_B_price = simulate_strategy_B(trade, bars_from_entry, all_bars)
        sell_C_date, sell_C_price = simulate_strategy_C(trade, bars_from_entry, all_bars)
        sell_D_date, sell_D_price = simulate_strategy_D(trade, bars_from_entry)
        sell_E_date, sell_E_price = simulate_strategy_E(trade, bars_from_entry, all_bars)

        sell_results = [
            (sell_A_date, sell_A_price),
            (sell_B_date, sell_B_price),
            (sell_C_date, sell_C_price),
            (sell_D_date, sell_D_price),
            (sell_E_date, sell_E_price),
        ]

        csv_row = {
            "code": code,
            "name": name,
            "trail_day": trail_day,
            "trade_tp": trade["trade_tp"],
            "basic_price": basic_price,
            "basic_qty": basic_qty,
            "stop_price": float(trade["stop_price"] or 0),
            "exit_price": float(trade["exit_price"] or 0),
            "target_price": float(trade["target_price"] or 0),
            "trade_result": trade["trade_result"],
        }

        for i, sname in enumerate(strategy_names):
            sell_date, sell_price = sell_results[i]
            if sell_date is None or sell_price is None:
                continue

            ret_pct = (sell_price - basic_price) / basic_price * 100 if basic_price > 0 else 0.0
            profit = (sell_price - basic_price) * basic_qty

            results[sname].append({
                "code": code,
                "name": name,
                "trail_day": trail_day,
                "basic_price": basic_price,
                "sell_date": sell_date,
                "sell_price": sell_price,
                "return_pct": ret_pct,
                "profit": profit,
            })

            csv_row[f"{sname}_sell_date"] = sell_date
            csv_row[f"{sname}_sell_price"] = sell_price
            csv_row[f"{sname}_return_pct"] = round(ret_pct, 2)

        csv_rows.append(csv_row)

    if skipped > 0:
        print(f"[참고] 데이터 부족으로 {skipped}건 스킵")
    print()

    # ============================================================
    # 결과 집계 및 출력
    # ============================================================
    # 기간 산출
    all_trail_days = [str(t["trail_day"]) for t in trades]
    date_min = min(all_trail_days) if all_trail_days else "N/A"
    date_max = max(all_trail_days) if all_trail_days else "N/A"
    # 날짜 포맷 변환
    try:
        date_min_fmt = f"{date_min[:4]}-{date_min[4:6]}-{date_min[6:8]}"
        date_max_fmt = f"{date_max[:4]}-{date_max[4:6]}-{date_max[6:8]}"
    except Exception:
        date_min_fmt = date_min
        date_max_fmt = date_max

    total_trades_count = len(csv_rows)

    print("=" * 72)
    print(f"  trail_tp='L' 전략 시뮬레이션 결과")
    print(f"  기간: {date_min_fmt} ~ {date_max_fmt} | 대상: {total_trades_count}건")
    print("=" * 72)
    print()
    header = f"{'전략':<16} | {'거래수':>6} | {'승률':>7} | {'평균수익률':>10} | {'최대손실':>9} | {'총수익률':>9}"
    print(header)
    print("-" * len(header.encode('euc-kr', errors='replace')))
    print("-" * 76)

    for sname in strategy_names:
        data = results[sname]
        n = len(data)
        if n == 0:
            print(f"{sname:<16} |    {'0':>3} |    N/A |       N/A |      N/A |      N/A")
            continue

        returns = [d["return_pct"] for d in data]
        wins = sum(1 for r in returns if r > 0)
        win_rate = wins / n * 100
        avg_ret = sum(returns) / n
        max_loss = min(returns)
        total_ret = sum(returns)

        # 부호 표시
        avg_sign = "+" if avg_ret >= 0 else ""
        total_sign = "+" if total_ret >= 0 else ""

        print(
            f"{sname:<16} | {n:>5} | {win_rate:>5.1f}% | {avg_sign}{avg_ret:>8.2f}% | {max_loss:>8.1f}% | {total_sign}{total_ret:>7.1f}%"
        )

    print()

    # ============================================================
    # CSV 출력
    # ============================================================
    if csv_rows:
        # CSV 컬럼 구성
        base_cols = [
            "code", "name", "trail_day", "trade_tp",
            "basic_price", "basic_qty", "stop_price", "exit_price",
            "target_price", "trade_result",
        ]
        strat_cols = []
        for sname in strategy_names:
            strat_cols.extend([
                f"{sname}_sell_date",
                f"{sname}_sell_price",
                f"{sname}_return_pct",
            ])
        all_cols = base_cols + strat_cols

        with open(CSV_OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
            writer.writeheader()
            for row in csv_rows:
                writer.writerow(row)
        print(f"[CSV] 상세 결과 저장: {CSV_OUTPUT}")
    else:
        print("[CSV] 출력할 데이터가 없습니다.")

    # ============================================================
    # 전략별 요약 추가 출력
    # ============================================================
    print()
    print("=" * 72)
    print("  전략별 상위/하위 종목 (수익률 기준)")
    print("=" * 72)
    for sname in strategy_names:
        data = results[sname]
        if not data:
            continue
        sorted_data = sorted(data, key=lambda x: x["return_pct"], reverse=True)
        print(f"\n[{sname}]")
        # 상위 3
        print("  상위:")
        for d in sorted_data[:3]:
            print(f"    {d['name']:<12} {d['trail_day']}  매입:{d['basic_price']:>8,.0f}  매도:{d['sell_price']:>8,.0f}  수익률:{d['return_pct']:>+7.2f}%")
        # 하위 3
        print("  하위:")
        for d in sorted_data[-3:]:
            print(f"    {d['name']:<12} {d['trail_day']}  매입:{d['basic_price']:>8,.0f}  매도:{d['sell_price']:>8,.0f}  수익률:{d['return_pct']:>+7.2f}%")

    conn.close()
    print()
    print("[완료] 시뮬레이션 종료")


if __name__ == "__main__":
    main()
