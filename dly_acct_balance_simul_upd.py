"""
trading_trail_simul (acct_no='SIMUL') 기반으로
dly_acct_balance_simul 의 acct='74346047' 에
20260401~20260416 일별 집계 수치를 설정한다.

컬럼 산출 기준:
  pchs_amt          = SUM(basic_price * basic_qty)  [trail_tp IN ('1','2','3','L','P')]
  prvs_excc_amt     = 초기자본금 - pchs_amt          (현금 추정)
  user_evlu_amt     = SUM(종가 * basic_qty)           [dly_stock_balance.current_price]
  tot_evlu_amt      = prvs_excc_amt + user_evlu_amt
  evlu_amt          = user_evlu_amt
  evlu_pfls_amt     = user_evlu_amt - pchs_amt
  ytdt_tot_evlu_amt = 전일 tot_evlu_amt
  asst_icdc_amt     = tot_evlu_amt - ytdt_tot_evlu_amt
  total_profit_loss = SUM((trail_price - basic_price) * trail_qty) [당일 매도완료 건]
  dnca_tot_amt      = 초기자본금  (고정)
  nass_amt          = tot_evlu_amt
  buy_psbl_amt      = prvs_excc_amt
"""
import psycopg2 as db
from datetime import datetime

SIMUL_ACCT_NO    = 'SIMUL'
TARGET_ACCT      = '74346047'
START_DT         = '20260406'
END_DT           = '20260416'
ACTIVE_TPS       = ['1', '2', '3', 'L', 'P']
INITIAL_CAPITAL  = 20_000_000

conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dly_acct_balance_simul (
    acct                  VARCHAR(20)  NOT NULL,
    dt                    VARCHAR(8)   NOT NULL,
    dnca_tot_amt          BIGINT       DEFAULT 0,
    prvs_excc_amt         BIGINT       DEFAULT 0,
    td_buy_amt            BIGINT       DEFAULT 0,
    td_sell_amt           BIGINT       DEFAULT 0,
    td_tex_amt            BIGINT       DEFAULT 0,
    user_evlu_amt         BIGINT       DEFAULT 0,
    tot_evlu_amt          BIGINT       DEFAULT 0,
    nass_amt              BIGINT       DEFAULT 0,
    pchs_amt              BIGINT       DEFAULT 0,
    evlu_amt              BIGINT       DEFAULT 0,
    evlu_pfls_amt         BIGINT       DEFAULT 0,
    ytdt_tot_evlu_amt     BIGINT       DEFAULT 0,
    asst_icdc_amt         BIGINT       DEFAULT 0,
    total_profit_loss_amt BIGINT       DEFAULT 0,
    buy_psbl_amt          BIGINT       DEFAULT 0,
    cash_rate             INTEGER      DEFAULT 0,
    market_ratio          INTEGER      DEFAULT 0,
    last_chg_date         TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (acct, dt)
);
"""

UPSERT_SQL = """
INSERT INTO dly_acct_balance_simul (
    acct, dt,
    dnca_tot_amt, prvs_excc_amt,
    td_buy_amt, td_sell_amt, td_tex_amt,
    user_evlu_amt, tot_evlu_amt, nass_amt,
    pchs_amt, evlu_amt, evlu_pfls_amt,
    ytdt_tot_evlu_amt, asst_icdc_amt,
    total_profit_loss_amt, buy_psbl_amt,
    last_chg_date
) VALUES (
    %s, %s,
    %s, %s,
    0, 0, 0,
    %s, %s, %s,
    %s, %s, %s,
    0, %s,
    %s, %s,
    %s
)
ON CONFLICT (acct, dt) DO UPDATE SET
    dnca_tot_amt          = EXCLUDED.dnca_tot_amt,
    prvs_excc_amt         = EXCLUDED.prvs_excc_amt,
    user_evlu_amt         = EXCLUDED.user_evlu_amt,
    tot_evlu_amt          = EXCLUDED.tot_evlu_amt,
    nass_amt              = EXCLUDED.nass_amt,
    pchs_amt              = EXCLUDED.pchs_amt,
    evlu_amt              = EXCLUDED.evlu_amt,
    evlu_pfls_amt         = EXCLUDED.evlu_pfls_amt,
    asst_icdc_amt         = EXCLUDED.asst_icdc_amt,
    total_profit_loss_amt = EXCLUDED.total_profit_loss_amt,
    buy_psbl_amt          = EXCLUDED.buy_psbl_amt,
    last_chg_date         = EXCLUDED.last_chg_date
"""

conn = db.connect(conn_string)
try:
    cur = conn.cursor()

    # 테이블 생성 (없으면)
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    # 영업일 목록 조회
    cur.execute("""
        SELECT TO_CHAR(dt::date, 'YYYYMMDD')
        FROM generate_series(%s::date, %s::date, '1 day') dt
        WHERE EXTRACT(DOW FROM dt) NOT IN (0, 6)
          AND TO_CHAR(dt::date, 'YYYYMMDD') NOT IN (
              SELECT holiday FROM stock_holiday
          )
        ORDER BY dt
    """, (f"{START_DT[:4]}-{START_DT[4:6]}-{START_DT[6:]}",
          f"{END_DT[:4]}-{END_DT[4:6]}-{END_DT[6:]}"))
    biz_days = [row[0] for row in cur.fetchall()]
    print(f"처리 영업일: {biz_days}")

    print(f"초기자본금(고정): {INITIAL_CAPITAL:,}")

    # 시작일 이전 누적 실현손익 (prvs_excc_amt 기준점)
    cur.execute("""
        SELECT COALESCE(SUM(total_profit_loss_amt), 0)
        FROM dly_acct_balance_simul
        WHERE acct = %s AND dt < %s
    """, (TARGET_ACCT, START_DT))
    cum_profit = int(cur.fetchone()[0])

    # 첫날 이전 tot_evlu_amt (asst_icdc_amt 기준)
    cur.execute("""
        SELECT tot_evlu_amt FROM dly_acct_balance_simul
        WHERE acct = %s AND dt < %s
        ORDER BY dt DESC LIMIT 1
    """, (TARGET_ACCT, biz_days[0]))
    prev_row = cur.fetchone()
    prev_tot = int(prev_row[0]) if prev_row else None

    for day in biz_days:
        # 활성 포지션 조회
        cur.execute("""
            SELECT code, basic_price, basic_qty
            FROM trading_trail_simul
            WHERE acct_no = %s AND trail_day = %s AND trail_tp = ANY(%s)
        """, (SIMUL_ACCT_NO, day, ACTIVE_TPS))
        positions = cur.fetchall()

        pchs_amt = sum(r[1] * r[2] for r in positions)

        # 종가 조회 (dly_stock_balance)
        codes = [r[0] for r in positions]
        if codes:
            cur.execute("""
                SELECT DISTINCT ON (code) code, current_price
                FROM dly_stock_balance
                WHERE dt = %s AND code = ANY(%s)
                ORDER BY code
            """, (day, codes))
            price_map = {r[0]: int(r[1] or 0) for r in cur.fetchall()}
        else:
            price_map = {}

        user_evlu_amt = sum(
            price_map.get(r[0], r[1]) * r[2] for r in positions
        )

        # 당일 매도완료 건의 실현손익
        cur.execute("""
            SELECT COALESCE(SUM((trail_price - basic_price) * trail_qty), 0)
            FROM trading_trail_simul
            WHERE acct_no = %s AND trail_day = %s
              AND trail_price > 0 AND trail_qty > 0
              AND trail_tp IN ('3', '4')
        """, (SIMUL_ACCT_NO, day))
        total_profit_loss_amt = int(cur.fetchone()[0])
        cum_profit += total_profit_loss_amt

        prvs_excc_amt = INITIAL_CAPITAL - pchs_amt + cum_profit
        tot_evlu_amt  = prvs_excc_amt + user_evlu_amt
        evlu_pfls_amt = user_evlu_amt - pchs_amt
        asst_icdc_amt = (tot_evlu_amt - prev_tot) if prev_tot is not None else 0

        cur.execute(UPSERT_SQL, (
            TARGET_ACCT, day,
            INITIAL_CAPITAL, prvs_excc_amt,
            user_evlu_amt, tot_evlu_amt, tot_evlu_amt,
            pchs_amt, user_evlu_amt, evlu_pfls_amt,
            asst_icdc_amt,
            total_profit_loss_amt, prvs_excc_amt,
            datetime.now()
        ))

        missing = [c for c in codes if c not in price_map]
        miss_str = f" [종가없음:{missing}]" if missing else ""
        print(
            f"[{day}] {len(positions)}종목 "
            f"pchs:{pchs_amt:,} cash:{prvs_excc_amt:,} "
            f"evlu:{user_evlu_amt:,} tot:{tot_evlu_amt:,} "
            f"pfls:{evlu_pfls_amt:,} profit:{total_profit_loss_amt:,}"
            f"{miss_str}"
        )
        prev_tot = tot_evlu_amt

    conn.commit()
    print(f"\n완료: {len(biz_days)}일 처리됨 (acct={TARGET_ACCT})")

except Exception as e:
    conn.rollback()
    import traceback
    traceback.print_exc()
    print(f"오류: {e}")
finally:
    conn.close()
