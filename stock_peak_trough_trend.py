"""
stock_peak_trough_trend.py
===========================
종목 일봉 데이터 기준 고점/저점(지그재그) 계산 및 추세(상승/하락/횡보) 판단

[로직]
  고점 : 종가가 전일 대비 상승 + 익일 대비 하락  (3봉 비교, 지그재그 방식)
  저점 : 종가가 전일 대비 하락 + 익일 대비 상승
  추세 : 마지막으로 형성된 고점을 종가가 재돌파 → Uptrend
         마지막으로 형성된 저점을 종가가 재이탈 → Downtrend
         그 외                                  → Sideways

일봉 조회는 market_trend_sim.py 의 KIS API 로직을 재사용한다.

사용법:
  python stock_peak_trough_trend.py [종목코드] [시작일YYYYMMDD] [종료일YYYYMMDD]
  예) python stock_peak_trough_trend.py 005930 20250101 20260403
  예) python stock_peak_trough_trend.py              ← 기본값 (005930, 최근 1년)
"""

import warnings
warnings.filterwarnings('ignore')

import os
import sys
import pandas as pd
import psycopg2 as db
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from market_trend_sim import get_first_account, get_daily_ohlcv, conn_string

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# ─────────────────────────────────────────────
# 일봉 OHLCV → DataFrame 변환
# ─────────────────────────────────────────────
def load_daily_df(stock_code, access_token, app_key, app_secret,
                   start_date="20230101", end_date=None):
    bars = get_daily_ohlcv(stock_code, access_token, app_key, app_secret,
                            start_date=start_date, end_date=end_date)
    df = pd.DataFrame(bars, columns=["date", "open", "high", "low", "close", "volume"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


# ─────────────────────────────────────────────
# 고점과 저점 계산 함수
# ─────────────────────────────────────────────
def calculate_peaks_and_troughs(data):
    highs = []
    lows = []

    for i in range(1, len(data) - 1):
        prev_high = data['high'].iloc[i - 1]
        curr_high = data['high'].iloc[i]
        next_high = data['high'].iloc[i + 1]
        prev_close = data['close'].iloc[i - 1]
        curr_close = data['close'].iloc[i]
        next_close = data['close'].iloc[i + 1]
        prev_low = data['low'].iloc[i - 1]
        curr_low = data['low'].iloc[i]
        next_low = data['low'].iloc[i + 1]

        # 고점: 상승 후 하락
        if curr_high > prev_high and curr_high > next_high:
        # if curr_close > prev_close and curr_close > next_close:            # 
            highs.append(curr_high)
            # highs.append(curr_close)
        else:
            highs.append(None)

        # 저점: 하락 후 상승
        if curr_low < prev_low and curr_low < next_low:
        # if curr_close < prev_close and curr_close < next_close:
            lows.append(curr_low)
            # lows.append(curr_close)
        else:
            lows.append(None)

    # 첫 번째와 마지막 값은 None 처리
    highs.insert(0, None)
    lows.insert(0, None)
    highs.append(None)
    lows.append(None)

    data['High Points'] = highs
    data['Low Points'] = lows
    return data


# ─────────────────────────────────────────────
# 추세 판단 함수
# ─────────────────────────────────────────────
def determine_trends(data):
    trend = []
    last_high = None
    last_low = None

    for i in range(len(data)):
        curr_close = data['close'].iloc[i]
        high_point = data['High Points'].iloc[i]
        low_point = data['Low Points'].iloc[i]

        if pd.notna(high_point):  # 고점 형성
            last_high = high_point

        if pd.notna(low_point):  # 저점 형성
            last_low = low_point

        # 상승 추세: 고점 재돌파
        if last_high and curr_close > last_high:
            print(f"last_high : {data['date'].iloc[i]}-{curr_close}-{last_high}")
            trend.append('Uptrend')

        # 하락 추세: 저점 재이탈
        elif last_low and curr_close < last_low:
            print(f"last_low : {data['date'].iloc[i]}-{curr_close}-{last_low}")
            trend.append('Downtrend')

        else:
            trend.append('Sideways')

    data['Trend'] = trend
    return data


# ─────────────────────────────────────────────
# 추세 구간 요약
# ─────────────────────────────────────────────
def build_segments(data):
    segments = []
    seg_trend = None
    seg_start_idx = None

    for i in range(len(data)):
        trend = data['Trend'].iloc[i]
        if trend != seg_trend:
            if seg_trend is not None:
                segments.append({
                    "trend": seg_trend,
                    "start": data['date'].iloc[seg_start_idx],
                    "end":   data['date'].iloc[i - 1],
                    "days":  i - seg_start_idx,
                })
            seg_trend = trend
            seg_start_idx = i

    if seg_trend is not None:
        segments.append({
            "trend": seg_trend,
            "start": data['date'].iloc[seg_start_idx],
            "end":   data['date'].iloc[len(data) - 1],
            "days":  len(data) - seg_start_idx,
        })
    return segments


# ─────────────────────────────────────────────
# 결과 출력
# ─────────────────────────────────────────────
def print_summary(data, stock_code, segments):
    total = len(data)
    up_d   = sum(s["days"] for s in segments if s["trend"] == "Uptrend")
    down_d = sum(s["days"] for s in segments if s["trend"] == "Downtrend")
    side_d = sum(s["days"] for s in segments if s["trend"] == "Sideways")

    print(f"\n{'='*70}")
    print(f"  고점/저점 & 추세 분석  [{stock_code}]  총 {total}일")
    print(f"  Uptrend {up_d}일({up_d/total*100:.1f}%)  "
          f"Downtrend {down_d}일({down_d/total*100:.1f}%)  "
          f"Sideways {side_d}일({side_d/total*100:.1f}%)")
    print(f"{'='*70}")

    print(f"\n[최근 30일 상세]")
    print(f"{'날짜':10} {'종가':>10} {'고점':>10} {'저점':>10} {'추세':10}")
    print("-" * 55)
    for _, r in data.tail(30).iterrows():
        def _fmt(v):
            return f"{v:,.0f}" if pd.notna(v) else "-"
        print(f"{r['date']:10} {r['close']:>10,.0f} "
              f"{_fmt(r['High Points']):>10} {_fmt(r['Low Points']):>10} {r['Trend']:10}")

    print(f"\n[추세 구간 요약]  (총 {len(segments)}개 구간)")
    print(f"{'추세':10} {'시작':10} {'종료':10} {'일수':>5}")
    print("-" * 40)
    for seg in segments:
        print(f"{seg['trend']:10} {seg['start']:10} {seg['end']:10} {seg['days']:>5}")
    print(f"{'='*70}\n")


def save_to_csv(data, filepath):
    out = data[["date", "open", "high", "low", "close", "volume",
                "High Points", "Low Points", "Trend"]]
    out.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"[저장] {filepath}")


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def main():
    args       = sys.argv[1:]
    stock_code = args[0] if len(args) > 0 else "005930"
    start_date = args[1] if len(args) > 1 else (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
    end_date   = args[2] if len(args) > 2 else datetime.now().strftime("%Y%m%d")

    print(f"[고점/저점 & 추세 분석]")
    print(f"  종목코드: {stock_code}")
    print(f"  분석기간: {start_date} ~ {end_date}")

    conn    = db.connect(conn_string)
    account = get_first_account(conn)
    conn.close()

    df = load_daily_df(
        stock_code,
        account["access_token"],
        account["app_key"],
        account["app_secret"],
        start_date=start_date,
        end_date=end_date,
    )

    if len(df) < 3:
        print("[경고] 데이터 부족 - 분석 불가 (최소 3봉 필요)")
        return

    df = calculate_peaks_and_troughs(df)
    df = determine_trends(df)
    segments = build_segments(df)

    print_summary(df, stock_code, segments)

    out_path = os.path.join(SCRIPT_DIR, f"peak_trough_trend_{stock_code}_{start_date}_{end_date}.csv")
    save_to_csv(df, out_path)


if __name__ == "__main__":
    main()
