import psycopg2 as db
import requests
import json
from datetime import datetime

# 기본 DB 연결 정보
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
remote_conn_string = "dbname='fund_risk_mng' host='192.168.50.248' port='5432' user='postgres' password='asdf1234'"

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# DB 연결
conn = db.connect(conn_string)
remote_conn = db.connect(remote_conn_string)

today = datetime.now().strftime("%Y%m%d")

def auth(APP_KEY, APP_SECRET):
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False)
    return res.json()["access_token"]

def account(nickname):
    cur01 = conn.cursor()
    cur01.execute("""
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day
        FROM "stockAccount_stock_account"
        WHERE nick_name = %s
    """, (nickname,))
    result_two = cur01.fetchone()
    cur01.close()

    acct_no, access_token, app_key, app_secret, token_publ_date, token_day = result_two
    validTokenDate = datetime.strptime(token_publ_date, '%Y%m%d%H%M%S')
    if (datetime.now() - validTokenDate).days >= 1 or token_day != today:
        access_token = auth(app_key, app_secret)
        token_publ_date = datetime.now().strftime('%Y%m%d%H%M%S')
        cur02 = conn.cursor()
        cur02.execute("""
            UPDATE "stockAccount_stock_account"
            SET access_token = %s, token_publ_date = %s, last_chg_date = %s
            WHERE acct_no = %s
        """, (access_token, token_publ_date, datetime.now(), acct_no))
        conn.commit()
        cur02.close()

    return {
        'acct_no': acct_no,
        'access_token': access_token,
        'app_key': app_key,
        'app_secret': app_secret
    }

nickname_list = ['chichipa', 'phills2', 'phills75', 'yh480825', 'phills13', 'phills15']
start_dt = datetime.now().strftime('%Y%m%d')
end_dt = datetime.now().strftime('%Y%m%d')
# start_dt = "20250101"
# end_dt = "20250526"

for nick in nickname_list:
    try:
        ac = account(nick)
        acct_no = ac['acct_no']

        cur1 = conn.cursor()
        cur1.execute("""
            SELECT 
                acct_no, order_no, org_order_no, order_type, order_dt, order_tmd, name, order_price, order_amount, total_complete_qty, remain_qty,
                total_complete_amt, hold_price, hold_vol, profit_loss_rate, profit_loss_amt, paid_tax, paid_fee, last_chg_date
            FROM \"stockOrderComplete_stock_order_complete\"
            WHERE acct_no = %s AND order_dt BETWEEN %s AND %s
        """, (acct_no, start_dt, end_dt))
        stock_order_complete_result = cur1.fetchall()
        cur1.close()

        if not stock_order_complete_result:
            print(f"[{nick}] No order data found.")

        remote_cur1 = remote_conn.cursor()

        insert_query1 = """
            INSERT INTO \"stockOrderComplete_stock_order_complete\" (
                acct_no, order_no, org_order_no, order_type, order_dt, order_tmd, name, order_price, order_amount, total_complete_qty, remain_qty,
                total_complete_amt, hold_price, hold_vol, profit_loss_rate, profit_loss_amt, paid_tax, paid_fee, last_chg_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (acct_no, order_dt, order_no) DO NOTHING
        """

        for row in stock_order_complete_result:
            acct_no, order_no, org_order_no, order_type, order_dt, order_tmd, name, order_price, order_amount, total_complete_qty, remain_qty, total_complete_amt, hold_price, hold_vol, profit_loss_rate, profit_loss_amt, paid_tax, paid_fee, last_chg_date = row
            try:
                remote_cur1.execute(insert_query1, (
                    acct_no, order_no, org_order_no, order_type, order_dt, order_tmd, name, order_price, order_amount, total_complete_qty, remain_qty, total_complete_amt, hold_price, hold_vol, profit_loss_rate, profit_loss_amt, paid_tax, paid_fee, last_chg_date
                ))
            except Exception as e:
                print(f"[{nick}] Error order inserting row {row}: {e}")

        remote_conn.commit()
        remote_cur1.close()
        print(f"[{nick}] Insert order completed. ({len(stock_order_complete_result)} rows processed)")

        cur2 = conn.cursor()
        cur2.execute("""
            SELECT 
                acct, dt, dnca_tot_amt, prvs_excc_amt, td_buy_amt, td_sell_amt, td_tex_amt, user_evlu_amt, tot_evlu_amt, nass_amt, 
                pchs_amt, evlu_amt, evlu_pfls_amt, ytdt_tot_evlu_amt, asst_icdc_amt, last_chg_date
            FROM dly_acct_balance
            WHERE acct = %s AND dt BETWEEN %s AND %s
        """, (str(acct_no), start_dt, end_dt))
        acc_balance_result = cur2.fetchall()
        cur2.close()

        if not acc_balance_result:
            print(f"[{nick}] No dly_acct_balance data found.")

        remote_cur2 = remote_conn.cursor()

        insert_query2 = """
            INSERT INTO dly_acct_balance (
                acct, dt, dnca_tot_amt, prvs_excc_amt, td_buy_amt, td_sell_amt, td_tex_amt, user_evlu_amt, tot_evlu_amt, nass_amt, pchs_amt, evlu_amt, evlu_pfls_amt, ytdt_tot_evlu_amt, asst_icdc_amt, last_chg_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (acct, dt) DO NOTHING
        """

        for row in acc_balance_result:
            acct, dt, dnca_tot_amt, prvs_excc_amt, td_buy_amt, td_sell_amt, td_tex_amt, user_evlu_amt, tot_evlu_amt, nass_amt, pchs_amt, evlu_amt, evlu_pfls_amt, ytdt_tot_evlu_amt, asst_icdc_amt, last_chg_date = row
            try:
                remote_cur2.execute(insert_query2, (
                    acct, dt, dnca_tot_amt, prvs_excc_amt, td_buy_amt, td_sell_amt, td_tex_amt, user_evlu_amt, tot_evlu_amt, nass_amt, pchs_amt, evlu_amt, evlu_pfls_amt, ytdt_tot_evlu_amt, asst_icdc_amt, last_chg_date
                ))
            except Exception as e:
                print(f"[{nick}] Error dly_acct_balance inserting row {row}: {e}")

        remote_conn.commit()
        remote_cur2.close()
        print(f"[{nick}] Insert dly_acct_balance completed. ({len(acc_balance_result)} rows processed)")

        cur3 = conn.cursor()
        cur3.execute("""
            SELECT 
                acct, dt, code, name, buy_qty, sell_qty, purchase_price, purchase_qty, purchase_amt, current_price, 
                eval_sum, earnings_rate, valuation_sum, open_price, high_price, low_price, volumn, volumn_rate, dly_signal_code, sign_resist_price, 
                sign_support_price, end_loss_price, end_target_price, trading_plan, limit_price, limit_amt, last_chg_date
            FROM dly_stock_balance
            WHERE acct = %s AND dt BETWEEN %s AND %s
        """, (str(acct_no), start_dt, end_dt))
        stock_balance_result = cur3.fetchall()
        cur3.close()

        if not stock_balance_result:
            print(f"[{nick}] No dly_stock_balance data found.")

        remote_cur3 = remote_conn.cursor()

        insert_query3 = """
            INSERT INTO dly_stock_balance (
                acct, dt, code, name, buy_qty, sell_qty, purchase_price, purchase_qty, purchase_amt, current_price, eval_sum, earnings_rate, valuation_sum, open_price, high_price, low_price, volumn, volumn_rate, dly_signal_code, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, limit_price, limit_amt, last_chg_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (acct, dt, code) DO NOTHING
        """

        for row in stock_balance_result:
            acct, dt, code, name, buy_qty, sell_qty, purchase_price, purchase_qty, purchase_amt, current_price, eval_sum, earnings_rate, valuation_sum, open_price, high_price, low_price, volumn, volumn_rate, dly_signal_code, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, limit_price, limit_amt, last_chg_date = row
            try:
                remote_cur3.execute(insert_query3, (
                    acct, dt, code, name, buy_qty, sell_qty, purchase_price, purchase_qty, purchase_amt, current_price, eval_sum, earnings_rate, valuation_sum, open_price, high_price, low_price, volumn, volumn_rate, dly_signal_code, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, limit_price, limit_amt, last_chg_date
                ))
            except Exception as e:
                print(f"[{nick}] Error dly_stock_balance inserting row {row}: {e}")

        remote_conn.commit()
        remote_cur3.close()
        print(f"[{nick}] Insert dly_stock_balance completed. ({len(stock_balance_result)} rows processed)")

        cur4 = conn.cursor()
        cur4.execute("""
            SELECT 
                acct, dt, code, name, current_price, open_price, high_price, low_price, volumn, volumn_rate, 
                dly_signal_code, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, last_chg_date
            FROM dly_stock_interest
            WHERE acct = %s AND dt BETWEEN %s AND %s
        """, (str(acct_no), start_dt, end_dt))
        stock_interest_result = cur4.fetchall()
        cur4.close()

        if not stock_interest_result:
            print(f"[{nick}] No dly_stock_interest data found.")

        remote_cur4 = remote_conn.cursor()

        insert_query4 = """
            INSERT INTO dly_stock_interest (
                acct, dt, code, name, current_price, open_price, high_price, low_price, volumn, volumn_rate, dly_signal_code, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, last_chg_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (acct, dt, code) DO NOTHING
        """

        for row in stock_interest_result:
            acct, dt, code, name, current_price, open_price, high_price, low_price, volumn, volumn_rate, dly_signal_code, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, last_chg_date = row
            try:
                remote_cur4.execute(insert_query4, (
                    acct, dt, code, name, current_price, open_price, high_price, low_price, volumn, volumn_rate, dly_signal_code, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, last_chg_date
                ))
            except Exception as e:
                print(f"[{nick}] Error dly_stock_interest inserting row {row}: {e}")

        remote_conn.commit()
        remote_cur4.close()
        print(f"[{nick}] Insert dly_stock_interest completed. ({len(stock_interest_result)} rows processed)")

        cur5 = conn.cursor()
        cur5.execute("""
            SELECT 
                search_day, search_time, search_name, code, name, low_price, high_price, current_price, day_rate, volumn, volumn_rate, market_total_sum, cdate
            FROM stock_search_form
            WHERE search_day BETWEEN %s AND %s
        """, (start_dt, end_dt))
        stock_search_result = cur5.fetchall()
        cur5.close()

        if not stock_search_result:
            print(f"[{nick}] No stock_search_form data found.")

        remote_cur5 = remote_conn.cursor()

        insert_query5 = """
            INSERT INTO stock_search_form (
                search_day, search_time, search_name, code, name, low_price, high_price, current_price, day_rate, volumn, volumn_rate, market_total_sum, cdate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (search_day, search_name, code) DO NOTHING
        """

        for row in stock_search_result:
            search_day, search_time, search_name, code, name, low_price, high_price, current_price, day_rate, volumn, volumn_rate, market_total_sum, cdate = row
            try:
                remote_cur5.execute(insert_query5, (
                    search_day, search_time, search_name, code, name, low_price, high_price, current_price, day_rate, volumn, volumn_rate, market_total_sum, cdate
                ))
            except Exception as e:
                print(f"[{nick}] Error stock_search_form inserting row {row}: {e}")

        remote_conn.commit()
        remote_cur5.close()
        print(f"[{nick}] Insert stock_search_form completed. ({len(stock_search_result)} rows processed)")

    except Exception as e:
        print(f"[{nick}] Error trading backup : {e}")

# 연결 종료
conn.close()
remote_conn.close()
