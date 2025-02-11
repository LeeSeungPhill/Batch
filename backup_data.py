from datetime import datetime
import psycopg2 as db
import sys

arguments = sys.argv

# PostgreSQL 연결 설정
conn_string1 = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string2 = "dbname='fund_risk_mng' host='fund-risk-mng-phills2.cdwam6448jv5.ap-northeast-2.rds.amazonaws.com' port='5432' user='postgres' password='gr971499#1'"

# DB 연결
conn1 = db.connect(conn_string1)
conn2 = db.connect(conn_string2)

today = datetime.now().strftime("%Y%m%d")
#today = '20240703'

cur100 = conn1.cursor()
cur101 = conn1.cursor()
cur102 = conn1.cursor()
cur103 = conn1.cursor()
cur104 = conn1.cursor()
cur105 = conn1.cursor()
cur106 = conn1.cursor()
cur107 = conn1.cursor()
cur200 = conn2.cursor()
cur201 = conn2.cursor()
cur202 = conn2.cursor()
cur203 = conn2.cursor()
cur204 = conn2.cursor()
cur205 = conn2.cursor()
cur206 = conn2.cursor()
cur207 = conn2.cursor()

cur100.execute(f"""select 
                        acct,
                        dt,
                        dnca_tot_amt,
                        prvs_excc_amt,
                        td_buy_amt,
                        td_sell_amt,
                        td_tex_amt,
                        user_evlu_amt,
                        tot_evlu_amt,
                        nass_amt,
                        pchs_amt,
                        evlu_amt,
                        evlu_pfls_amt,
                        ytdt_tot_evlu_amt,
                        asst_icdc_amt,
                        last_chg_date
                   from dly_acct_balance where dt = '{today}'""")

result_100 = cur100.fetchall()

cur200.execute(f"""DELETE FROM dly_acct_balance WHERE dt = '{today}'""")

for row in result_100:
    cur200.execute("""
        INSERT INTO dly_acct_balance(acct, dt, dnca_tot_amt, prvs_excc_amt, td_buy_amt, td_sell_amt, td_tex_amt, 
            user_evlu_amt, tot_evlu_amt, nass_amt, pchs_amt, evlu_amt, evlu_pfls_amt, ytdt_tot_evlu_amt, asst_icdc_amt, last_chg_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, row)

cur101.execute(f"""select
                        acct,
	                    order_no,
	                    org_order_no,
                        order_type,
                        order_dt,
                        order_tmd,
                        "name",
                        order_price,
                        order_qty,
                        total_complete_qty,
                        remain_qty,
                        cncl_yn,
                        cncl_qty,
                        complete_avg_price,
                        total_complete_amt,
                        last_chg_date
                    from dly_order_completion where order_dt = '{today}'""")  

result_101 = cur101.fetchall()

cur201.execute(f"""DELETE FROM dly_order_completion WHERE order_dt = '{today}'""")

for row in result_101:
    cur201.execute("""
        INSERT INTO dly_order_completion(acct, order_no, org_order_no, order_type, order_dt, order_tmd, name, 
            order_price, order_qty, total_complete_qty, remain_qty, cncl_yn, cncl_qty, complete_avg_price, total_complete_amt, last_chg_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, row)

cur102.execute(f"""select
                        acct,
                        dt,
                        code,
                        "name",
                        buy_qty,
                        sell_qty,
                        purchase_price,
                        purchase_qty,
                        purchase_amt,
                        current_price,
                        eval_sum,
                        earnings_rate,
                        valuation_sum,
                        open_price,
                        high_price,
                        low_price,
                        volumn,
                        volumn_rate,
                        dly_signal_code,
                        last_chg_date,
                        sign_resist_price,
                        sign_support_price,
                        end_loss_price,
                        end_target_price
                    from dly_stock_balance where dt = '{today}'""")

result_102 = cur102.fetchall()

cur202.execute(f"""DELETE FROM dly_stock_balance WHERE dt = '{today}'""")

for row in result_102:
    cur202.execute("""
        INSERT INTO dly_stock_balance(acct, dt, code, name, buy_qty, sell_qty, purchase_price, purchase_qty, purchase_amt, current_price,
            eval_sum, earnings_rate, valuation_sum, open_price, high_price, low_price, volumn, volumn_rate, dly_signal_code, last_chg_date,
            sign_resist_price, sign_support_price, end_loss_price, end_target_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, row)

cur103.execute(f"""select
                        acct,
                        dt,
                        code,
                        "name",
                        current_price,
                        open_price,
                        high_price,
                        low_price,
                        volumn,
                        volumn_rate,
                        dly_signal_code,
                        through_price,
                        leave_price,
                        resist_price,
                        support_price,
                        trend_high_price,
                        trend_low_price,
                        last_chg_date
                    from dly_stock_interest where dt = '{today}'""")

result_103 = cur103.fetchall()

cur203.execute(f"""DELETE FROM dly_stock_interest WHERE dt = '{today}'""")

for row in result_103:
    cur203.execute("""
        INSERT INTO dly_stock_interest(acct, dt, code, name, current_price, open_price, high_price, low_price, volumn,
            volumn_rate, dly_signal_code, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, last_chg_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, row)

cur104.execute(f"""select 
                        dt,
	                    code,
	                    "name",
	                    current_price,
	                    open_price,
	                    high_price,
	                    low_price,
	                    volumn,
	                    last_chg_date
                   from stock_minute_info where substr(dt,1,8) = '{today}'""")

result_104 = cur104.fetchall()

cur204.execute(f"""DELETE FROM stock_minute_info WHERE substr(dt,1,8) = '{today}'""")

for row in result_104:
    cur204.execute("""
        INSERT INTO stock_minute_info(
                        dt,
	                    code,
	                    "name",
	                    current_price,
	                    open_price,
	                    high_price,
	                    low_price,
	                    volumn,
	                    last_chg_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, row)

cur105.execute(f"""select 
                        search_day,
	                    search_time,
	                    search_name,
	                    code,
	                    "name",
	                    low_price,
	                    high_price,
	                    current_price,
	                    day_rate,
	                    volumn,
	                    volumn_rate,
	                    market_total_sum,
	                    cdate
                   from stock_search_form where search_day = '{today}'""")

result_105 = cur105.fetchall()

cur205.execute(f"""DELETE FROM stock_search_form WHERE search_day = '{today}'""")

for row in result_105:
    cur205.execute("""
        INSERT INTO stock_search_form(
                        search_day,
	                    search_time,
	                    search_name,
	                    code,
	                    "name",
	                    low_price,
	                    high_price,
	                    current_price,
	                    day_rate,
	                    volumn,
	                    volumn_rate,
	                    market_total_sum,
	                    cdate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, row)

cur106.execute(f"""select 
                        acct,
                        trail_day,
                        trail_time,
                        trail_signal_code,
                        trail_signal_name,
                        code,
                        "name",
                        current_price,
                        high_price,
                        low_price,
                        volumn,
                        volumn_rate,
                        cdate,
                        sell_plan_amt,
                        sell_plan_qty,
                        buy_plan_amt,
                        buy_plan_qty        
                   from trail_signal where trail_day = '{today}'""")

result_106 = cur106.fetchall()

cur206.execute(f"""DELETE FROM trail_signal WHERE trail_day = '{today}'""")

for row in result_106:
    cur206.execute("""
        INSERT INTO trail_signal(
                        acct,
                        trail_day,
                        trail_time,
                        trail_signal_code,
                        trail_signal_name,
                        code,
                        "name",
                        current_price,
                        high_price,
                        low_price,
                        volumn,
                        volumn_rate,
                        cdate,
                        sell_plan_amt,
                        sell_plan_qty,
                        buy_plan_amt,
                        buy_plan_qty)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, row)

cur107.execute(f"""select 
                        acct,
                        trail_day,
                        trail_time,
                        trail_signal_code,
                        trail_signal_name,
                        code,
                        "name",
                        current_price,
                        high_price,
                        low_price,
                        volumn,
                        volumn_rate,
                        cdate,
                        sell_plan_amt,
                        sell_plan_qty,
                        buy_plan_amt,
                        buy_plan_qty
                   from trail_signal_hist where trail_day = '{today}'""")

result_107 = cur107.fetchall()

cur207.execute(f"""DELETE FROM trail_signal_hist WHERE trail_day = '{today}'""")

for row in result_107:
    cur207.execute("""
        INSERT INTO trail_signal_hist(
                        acct,
                        trail_day,
                        trail_time,
                        trail_signal_code,
                        trail_signal_name,
                        code,
                        "name",
                        current_price,
                        high_price,
                        low_price,
                        volumn,
                        volumn_rate,
                        cdate,
                        sell_plan_amt,
                        sell_plan_qty,
                        buy_plan_amt,
                        buy_plan_qty)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, row)

conn2.commit()

cur100.close()
cur101.close()
cur102.close()
cur103.close()
cur104.close()
cur105.close()
cur106.close()
cur107.close()
cur200.close()
cur201.close()
cur202.close()
cur203.close()
cur204.close()
cur205.close()
cur206.close()
cur207.close()
conn1.close()
conn2.close()