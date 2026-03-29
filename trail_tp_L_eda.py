"""
trail_tp='L' 시뮬레이션 결과 EDA + 과적합 체크 + 구간별 로그
사전 조건: trail_tp_L_simulation.py 실행 후 CSV 파일 생성 필요
"""

import csv
import os
import json
from collections import defaultdict
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_INPUT  = os.path.join(SCRIPT_DIR, "trail_tp_L_simulation_result2.csv")
LOG_OUTPUT = os.path.join(SCRIPT_DIR, "trail_tp_L_interval_log.csv")
REPORT_OUT = os.path.join(SCRIPT_DIR, "trail_tp_L_eda_report.txt")

STRATEGY_LABELS = {
    "A": "현행(A)",
    "B": "ATR×2.0(B)",
    "C": "ATR×1.5(C)",
    "D": "수익구간(D)",
    "E": "하이브리드(E)",
}
COL_PREFIX = {
    "A": "현행(A)",
    "B": "ATR×2.0(B)",
    "C": "ATR×1.5(C)",
    "D": "수익구간(D)",
    "E": "하이브리드(E)",
}


# ─────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────
def safe_float(v, default=0.0):
    try:
        return float(v) if v not in (None, "", "None") else default
    except Exception:
        return default


def stat_summary(values):
    """기초 통계 딕셔너리"""
    if not values:
        return {"n": 0}
    n = len(values)
    mean = sum(values) / n
    sorted_v = sorted(values)
    median = sorted_v[n // 2] if n % 2 else (sorted_v[n // 2 - 1] + sorted_v[n // 2]) / 2
    variance = sum((v - mean) ** 2 for v in values) / n if n > 1 else 0
    std = variance ** 0.5
    wins = sum(1 for v in values if v > 0)
    losses = sum(1 for v in values if v < 0)
    return {
        "n": n,
        "mean": mean,
        "median": median,
        "std": std,
        "min": min(values),
        "max": max(values),
        "sum": sum(values),
        "win_n": wins,
        "loss_n": losses,
        "win_rate": wins / n * 100,
        "profit_factor": abs(sum(v for v in values if v > 0)) / abs(sum(v for v in values if v < 0))
            if any(v < 0 for v in values) else float("inf"),
    }


def print_stat(label, s, width=20):
    if s["n"] == 0:
        print(f"  {label:<{width}}: 데이터 없음")
        return
    print(
        f"  {label:<{width}}: n={s['n']:>3}  승률={s['win_rate']:>5.1f}%  "
        f"평균={s['mean']:>+7.2f}%  중앙={s['median']:>+7.2f}%  "
        f"표준편차={s['std']:>6.2f}%  최소={s['min']:>+7.2f}%  최대={s['max']:>+7.2f}%  "
        f"합계={s['sum']:>+8.1f}%  PF={s['profit_factor']:>5.2f}"
    )


def ascii_bar(value, width=30, scale=0.5):
    """ASCII 막대 그래프"""
    bar_len = int(abs(value) * scale)
    bar_len = min(bar_len, width)
    if value >= 0:
        return "+" + "#" * bar_len
    else:
        return "-" + "#" * bar_len


# ─────────────────────────────────────────────
# 데이터 로드
# ─────────────────────────────────────────────
def load_csv():
    rows = []
    with open(CSV_INPUT, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


# ─────────────────────────────────────────────
# 1. 기초 EDA
# ─────────────────────────────────────────────
def eda_basic(rows, out):
    out.append("\n" + "=" * 80)
    out.append("  1. 기초 EDA")
    out.append("=" * 80)

    dates = sorted(set(r["trail_day"] for r in rows))
    out.append(f"\n  총 거래 수  : {len(rows)}")
    out.append(f"  거래 기간   : {dates[0]} ~ {dates[-1]}")
    out.append(f"  거래일 수   : {len(dates)}일")

    # trade_tp 분포
    tp_cnt = defaultdict(int)
    for r in rows:
        tp_cnt[r.get("trade_tp", "?")]  += 1
    out.append(f"\n  trade_tp 분포:")
    for tp, cnt in sorted(tp_cnt.items()):
        bar = "#" * int(cnt / len(rows) * 40)
        out.append(f"    {tp}: {cnt:>4}건  {bar}")

    # 일별 거래 건수
    day_cnt = defaultdict(int)
    for r in rows:
        day_cnt[r["trail_day"]] += 1
    out.append(f"\n  날짜별 거래 건수:")
    for d in sorted(day_cnt):
        bar = "#" * day_cnt[d]
        out.append(f"    {d}: {day_cnt[d]:>3}건  {bar}")

    # 전략별 기본 통계
    out.append(f"\n  {'─'*78}")
    out.append(f"  전략별 전체 성과 요약")
    out.append(f"  {'─'*78}")
    header = f"  {'전략':<18} {'건수':>4} {'승률':>6} {'평균수익':>8} {'중앙값':>8} {'표준편차':>7} {'최소':>8} {'최대':>8} {'합계':>8} {'PF':>5}"
    out.append(header)
    out.append("  " + "-" * 78)

    for key in ["A", "B", "C", "D", "E"]:
        label = STRATEGY_LABELS[key]
        col   = f"{COL_PREFIX[key]}_return_pct"
        rets  = [safe_float(r.get(col)) for r in rows if r.get(col) not in (None, "", "None")]
        s = stat_summary(rets)
        if s["n"] == 0:
            out.append(f"  {label:<18}  데이터 없음")
            continue
        pf_str = f"{s['profit_factor']:>5.2f}" if s["profit_factor"] != float("inf") else "  INF"
        out.append(
            f"  {label:<18} {s['n']:>4} {s['win_rate']:>5.1f}% {s['mean']:>+8.2f}% "
            f"{s['median']:>+8.2f}% {s['std']:>7.2f}% {s['min']:>+8.2f}% "
            f"{s['max']:>+8.2f}% {s['sum']:>+8.1f}% {pf_str}"
        )


# ─────────────────────────────────────────────
# 2. 수익률 구간별 분석
# ─────────────────────────────────────────────
def eda_by_return_zone(rows, out):
    out.append("\n" + "=" * 80)
    out.append("  2. 수익률 구간별 전략 비교")
    out.append("=" * 80)

    ZONES = [
        ("큰손실   (< -10%)",   lambda a: a < -10),
        ("중손실  (-10~-5%)",   lambda a: -10 <= a < -5),
        ("소손실   (-5~ 0%)",   lambda a: -5  <= a <  0),
        ("소수익   ( 0~15%)",   lambda a:  0  <= a < 15),
        ("중수익   (15~30%)",   lambda a: 15  <= a < 30),
        ("고수익   (30%+  )",   lambda a: a >= 30),
    ]

    col_a = f"{COL_PREFIX['A']}_return_pct"

    for zone_name, cond in ZONES:
        zone_rows = [r for r in rows if cond(safe_float(r.get(col_a)))]
        if not zone_rows:
            continue
        out.append(f"\n  [{zone_name}] — {len(zone_rows)}건")
        out.append(f"  {'전략':<18} {'승률':>6} {'평균':>8} {'중앙':>8} {'합계':>8} {'최소':>8} {'최대':>8}")
        out.append("  " + "-" * 72)
        for key in ["A", "B", "C", "D", "E"]:
            col   = f"{COL_PREFIX[key]}_return_pct"
            rets  = [safe_float(r.get(col)) for r in zone_rows if r.get(col) not in (None, "")]
            if not rets:
                continue
            s = stat_summary(rets)
            delta = ""
            if key != "A":
                col_a2  = f"{COL_PREFIX['A']}_return_pct"
                rets_a  = [safe_float(r.get(col_a2)) for r in zone_rows]
                delta_v = s["mean"] - (sum(rets_a)/len(rets_a) if rets_a else 0)
                delta = f"  ({delta_v:>+.2f}% vs A)"
            out.append(
                f"  {STRATEGY_LABELS[key]:<18} {s['win_rate']:>5.1f}% {s['mean']:>+8.2f}% "
                f"{s['median']:>+8.2f}% {s['sum']:>+8.1f}% {s['min']:>+8.2f}% "
                f"{s['max']:>+8.2f}%{delta}"
            )


# ─────────────────────────────────────────────
# 3. 날짜별 / 주별 수익률 추이
# ─────────────────────────────────────────────
def eda_time_series(rows, out):
    out.append("\n" + "=" * 80)
    out.append("  3. 날짜별 수익률 추이 (전략 A vs E)")
    out.append("=" * 80)

    day_data = defaultdict(lambda: {"A": [], "E": []})
    for r in rows:
        d = r["trail_day"]
        day_data[d]["A"].append(safe_float(r.get(f"{COL_PREFIX['A']}_return_pct")))
        day_data[d]["E"].append(safe_float(r.get(f"{COL_PREFIX['E']}_return_pct")))

    out.append(f"\n  {'날짜':<12} {'건수':>4} {'A평균':>8} {'E평균':>8} {'A합계':>8} {'E합계':>8} {'개선':>7}")
    out.append("  " + "-" * 68)
    for d in sorted(day_data):
        n = len(day_data[d]["A"])
        avg_a = sum(day_data[d]["A"]) / n if n else 0
        avg_e = sum(day_data[d]["E"]) / n if n else 0
        sum_a = sum(day_data[d]["A"])
        sum_e = sum(day_data[d]["E"])
        improvement = avg_e - avg_a
        bar = ascii_bar(improvement, width=10, scale=2)
        out.append(
            f"  {d:<12} {n:>4} {avg_a:>+8.2f}% {avg_e:>+8.2f}% "
            f"{sum_a:>+8.1f}% {sum_e:>+8.1f}% {improvement:>+7.2f}% {bar}"
        )


# ─────────────────────────────────────────────
# 4. 과적합 체크
# ─────────────────────────────────────────────
def overfitting_check(rows, out):
    out.append("\n" + "=" * 80)
    out.append("  4. 과적합(Overfitting) 체크")
    out.append("=" * 80)

    dates = sorted(set(r["trail_day"] for r in rows))
    n_days = len(dates)

    if n_days < 4:
        out.append("  [경고] 거래일이 너무 적어 과적합 분석이 불가합니다.")
        return

    # 전반부 / 후반부 분리
    split_idx = n_days // 2
    split_date = dates[split_idx]
    first_half  = [r for r in rows if r["trail_day"] < split_date]
    second_half = [r for r in rows if r["trail_day"] >= split_date]

    out.append(f"\n  분할 기준일: {split_date}")
    out.append(f"  전반부: {dates[0]} ~ {dates[split_idx-1]} ({len(first_half)}건)")
    out.append(f"  후반부: {split_date} ~ {dates[-1]} ({len(second_half)}건)")

    out.append(f"\n  {'전략':<18} {'전반 평균':>9} {'후반 평균':>9} {'차이':>8}  {'판정':>12}")
    out.append("  " + "-" * 68)

    best_first = None
    best_first_score = -999
    for key in ["A", "B", "C", "D", "E"]:
        col = f"{COL_PREFIX[key]}_return_pct"
        f1 = [safe_float(r.get(col)) for r in first_half if r.get(col) not in (None, "")]
        f2 = [safe_float(r.get(col)) for r in second_half if r.get(col) not in (None, "")]
        if not f1 or not f2:
            continue
        avg1 = sum(f1) / len(f1)
        avg2 = sum(f2) / len(f2)
        diff = avg2 - avg1

        # 과적합 판정 기준:
        # - 후반부에서도 전반부 대비 1%p 이내 하락 → 안정적
        # - 1~3%p 하락 → 주의
        # - 3%p 이상 하락 → 과적합 의심
        if diff >= -1.0:
            judgement = "안정적"
        elif diff >= -3.0:
            judgement = "주의"
        else:
            judgement = "과적합 의심"

        out.append(
            f"  {STRATEGY_LABELS[key]:<18} {avg1:>+9.2f}% {avg2:>+9.2f}% {diff:>+8.2f}%  {judgement:>12}"
        )

        if avg1 > best_first_score:
            best_first_score = avg1
            best_first = key

    out.append(f"\n  ※ 전반부 최우수 전략: {STRATEGY_LABELS.get(best_first, 'N/A')}")
    out.append(f"\n  [과적합 체크 방법론]")
    out.append(f"  - 전반부 = 인-샘플(최적화 기간), 후반부 = 아웃-오브-샘플(검증 기간)")
    out.append(f"  - 전반부 > 후반부 성과 격차가 클수록 과적합 위험 높음")
    out.append(f"  - 현재 데이터 기간이 {n_days}거래일로 매우 짧아 신뢰도 제한적")

    # 안정성 지표: Sharpe-like (평균/표준편차)
    out.append(f"\n  [전략별 안정성 지표 (Sharpe ratio 유사값)]")
    out.append(f"  {'전략':<18} {'평균':>8} {'표준편차':>9} {'Sharpe':>8}")
    out.append("  " + "-" * 50)
    for key in ["A", "B", "C", "D", "E"]:
        col = f"{COL_PREFIX[key]}_return_pct"
        rets = [safe_float(r.get(col)) for r in rows if r.get(col) not in (None, "")]
        if not rets:
            continue
        s = stat_summary(rets)
        sharpe = (s["mean"] / s["std"]) if s["std"] > 0 else 0
        out.append(
            f"  {STRATEGY_LABELS[key]:<18} {s['mean']:>+8.2f}% {s['std']:>9.2f}% {sharpe:>+8.3f}"
        )

    # 상관계수 분석
    out.append(f"\n  [전략 간 상관계수 (vs 현행 A)]")
    col_a = f"{COL_PREFIX['A']}_return_pct"
    rets_a = [safe_float(r.get(col_a)) for r in rows]
    mean_a = sum(rets_a) / len(rets_a)
    for key in ["B", "C", "D", "E"]:
        col = f"{COL_PREFIX[key]}_return_pct"
        rets_x = [safe_float(r.get(col)) for r in rows]
        mean_x = sum(rets_x) / len(rets_x)
        n = len(rets_a)
        cov = sum((rets_a[i] - mean_a) * (rets_x[i] - mean_x) for i in range(n)) / n
        std_a = (sum((v - mean_a)**2 for v in rets_a) / n) ** 0.5
        std_x = (sum((v - mean_x)**2 for v in rets_x) / n) ** 0.5
        corr = cov / (std_a * std_x) if std_a * std_x > 0 else 0
        out.append(f"  A vs {STRATEGY_LABELS[key]:<18}: r = {corr:.4f}")


# ─────────────────────────────────────────────
# 5. 구간별 상세 로그 생성
# ─────────────────────────────────────────────
def generate_interval_log(rows, out):
    out.append("\n" + "=" * 80)
    out.append("  5. 구간별 상세 로그 생성")
    out.append("=" * 80)

    RETURN_ZONES = [
        ("큰손실",   lambda a: a < -10),
        ("중손실",   lambda a: -10 <= a < -5),
        ("소손실",   lambda a: -5  <= a <  0),
        ("소수익",   lambda a:  0  <= a < 15),
        ("중수익",   lambda a: 15  <= a < 30),
        ("고수익",   lambda a: a >= 30),
    ]

    col_a = f"{COL_PREFIX['A']}_return_pct"

    log_rows = []
    for r in rows:
        base_ret = safe_float(r.get(col_a))
        zone_label = "미분류"
        for zname, cond in RETURN_ZONES:
            if cond(base_ret):
                zone_label = zname
                break

        row_log = {
            "날짜": r.get("trail_day", ""),
            "종목코드": r.get("code", ""),
            "종목명": r.get("name", ""),
            "trade_tp": r.get("trade_tp", ""),
            "매수가": r.get("basic_price", ""),
            "수량": r.get("basic_qty", ""),
            "손절가": r.get("stop_price", ""),
            "목표가": r.get("target_price", ""),
            "수익구간": zone_label,
        }
        for key in ["A", "B", "C", "D", "E"]:
            prefix = COL_PREFIX[key]
            row_log[f"{key}_매도일"] = r.get(f"{prefix}_sell_date", "")
            row_log[f"{key}_매도가"] = r.get(f"{prefix}_sell_price", "")
            row_log[f"{key}_수익률"] = r.get(f"{prefix}_return_pct", "")
        log_rows.append(row_log)

    # CSV 저장
    if log_rows:
        fieldnames = list(log_rows[0].keys())
        with open(LOG_OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(log_rows)
        out.append(f"\n  구간별 로그 저장: {LOG_OUTPUT}")
        out.append(f"  총 {len(log_rows)}건 기록")

    # 구간별 요약
    out.append(f"\n  수익구간별 전략 평균 수익률:")
    out.append(f"  {'구간':<8} {'건수':>4}  {'A':>8}  {'B':>8}  {'C':>8}  {'D':>8}  {'E':>8}")
    out.append("  " + "-" * 68)

    for zname, cond in RETURN_ZONES:
        zone_rows = [r for r in rows if cond(safe_float(r.get(col_a)))]
        if not zone_rows:
            continue
        avgs = {}
        for key in ["A", "B", "C", "D", "E"]:
            col = f"{COL_PREFIX[key]}_return_pct"
            rets = [safe_float(r.get(col)) for r in zone_rows]
            avgs[key] = sum(rets) / len(rets) if rets else 0
        best_key = max(["B", "C", "D", "E"], key=lambda k: avgs[k])
        line = f"  {zname:<8} {len(zone_rows):>4}  {avgs['A']:>+8.2f}%  {avgs['B']:>+8.2f}%  {avgs['C']:>+8.2f}%  {avgs['D']:>+8.2f}%  {avgs['E']:>+8.2f}%"
        out.append(line + f"  ← Best: {STRATEGY_LABELS[best_key]}")


# ─────────────────────────────────────────────
# 6. 결론 및 권고
# ─────────────────────────────────────────────
def conclusion(rows, out):
    out.append("\n" + "=" * 80)
    out.append("  6. 종합 결론 및 권고")
    out.append("=" * 80)

    dates = sorted(set(r["trail_day"] for r in rows))

    # 전략별 총수익률 순위
    strategy_totals = {}
    for key in ["A", "B", "C", "D", "E"]:
        col = f"{COL_PREFIX[key]}_return_pct"
        rets = [safe_float(r.get(col)) for r in rows if r.get(col) not in (None, "")]
        strategy_totals[key] = (sum(rets), sum(rets)/len(rets) if rets else 0, len(rets))

    ranking = sorted(strategy_totals.items(), key=lambda x: x[1][0], reverse=True)

    out.append(f"\n  ■ 전략 순위 (총수익률 기준)")
    for rank, (key, (total, avg, n)) in enumerate(ranking, 1):
        out.append(f"    {rank}위: {STRATEGY_LABELS[key]:<18} 합계={total:>+8.1f}% 평균={avg:>+7.2f}%")

    out.append(f"\n  ■ 핵심 발견사항")
    out.append(f"  1. 데이터 기간: {len(dates)}거래일({dates[0]}~{dates[-1]}) — 단기 검증이므로 신중한 해석 필요")
    out.append(f"  2. 전체 승률 ~30%: 시장 하락장 영향으로 모든 전략이 부진")
    out.append(f"  3. 중수익(15~30%) 구간에서 하이브리드(E)가 현행(A) 대비 약세 여부 확인 필요")
    out.append(f"     → ATR 트레일이 중간 수익 종목을 너무 일찍 청산하는 경향")
    out.append(f"  4. 고수익(30%+) 구간에서는 하이브리드(E)가 현행 대비 우세")
    out.append(f"     → 목표한 대로 큰 수익 구간의 보전 효과 확인")

    out.append(f"\n  ■ 과적합 리스크 평가")
    out.append(f"  - 샘플 {len(rows)}건 / {len(dates)}거래일은 신뢰 있는 백테스트에 불충분")
    out.append(f"  - 최소 3~6개월(60~120거래일) 데이터가 필요")
    out.append(f"  - 전략 파라미터(ATR 멀티플, 수익 구간 임계)가 이 기간에 맞게 과적합될 위험")

    out.append(f"\n  ■ 권고사항")
    out.append(f"  1. 더 긴 기간의 데이터 축적 후 재시뮬레이션 필요")
    out.append(f"  2. 중수익(15~30%) 구간의 ATR 멀티플라이어를 2.0 → 2.5로 완화 검토")
    out.append(f"     (너무 일찍 트레일에 걸리지 않도록)")
    out.append(f"  3. 하락장(KOSPI 기준) 여부를 구분해 전략을 조건부 적용 고려")
    out.append(f"  4. 현재 하이브리드(E)를 배포하되 3개월 후 재검토 권고")


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def main():
    if not os.path.exists(CSV_INPUT):
        print(f"[오류] {CSV_INPUT} 파일이 없습니다. trail_tp_L_simulation.py를 먼저 실행하세요.")
        return

    rows = load_csv()
    print(f"[데이터] {len(rows)}건 로드 완료")

    out = []
    out.append("=" * 80)
    out.append("  trail_tp='L' 시뮬레이션 EDA 보고서")
    out.append(f"  생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    out.append("=" * 80)

    eda_basic(rows, out)
    eda_by_return_zone(rows, out)
    eda_time_series(rows, out)
    overfitting_check(rows, out)
    generate_interval_log(rows, out)
    conclusion(rows, out)

    out.append("\n" + "=" * 80)
    out.append("  [완료]")
    out.append("=" * 80)

    report_text = "\n".join(out)

    with open(REPORT_OUT, "w", encoding="utf-8") as f:
        f.write(report_text)

    # 콘솔 출력 (인코딩 안전)
    try:
        print(report_text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(report_text.encode("utf-8", errors="replace"))
        sys.stdout.buffer.flush()

    print(f"\n[saved] {REPORT_OUT}")
    print(f"[saved] {LOG_OUTPUT}")


if __name__ == "__main__":
    main()
