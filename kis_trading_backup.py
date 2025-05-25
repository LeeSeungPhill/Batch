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
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date
        FROM "stockAccount_stock_account"
        WHERE nick_name = %s
    """, (nickname,))
    result_two = cur01.fetchone()
    cur01.close()

    acct_no, access_token, app_key, app_secret, token_publ_date = result_two
    validTokenDate = datetime.strptime(token_publ_date, '%Y%m%d%H%M%S')
    if (datetime.now() - validTokenDate).days >= 1:
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

nickname_list = ['phills2', 'phills75', 'yh480825', 'phills13', 'phills15']
today_str = datetime.now().strftime('%Y%m%d')

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
            WHERE acct_no = %s AND order_dt = %s
        """, (acct_no, today_str))
        stock_order_complete_result = cur1.fetchall()
        cur1.close()

        if not stock_order_complete_result:
            print(f"[{nick}] No order data found.")
            continue

        remote_cur1 = remote_conn.cursor()

        insert_query = """
            INSERT INTO \"stockOrderComplete_stock_order_complete\" (
                acct_no, order_no, org_order_no, order_type, order_dt, order_tmd, name, order_price, order_amount, total_complete_qty, remain_qty,
                total_complete_amt, hold_price, hold_vol, profit_loss_rate, profit_loss_amt, paid_tax, paid_fee, last_chg_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (acct_no, order_dt, order_no) DO NOTHING
        """

        for row in stock_order_complete_result:
            acct_no, order_no, org_order_no, order_type, order_dt, order_tmd, name, order_price, order_amount, total_complete_qty, remain_qty, total_complete_amt, hold_price, hold_vol, profit_loss_rate, profit_loss_amt, paid_tax, paid_fee, last_chg_date = row
            try:
                remote_cur1.execute(insert_query, (
                    acct_no, order_no, org_order_no, order_type, order_dt, order_tmd, name, order_price, order_amount, total_complete_qty, remain_qty, total_complete_amt, hold_price, hold_vol, profit_loss_rate, profit_loss_amt, paid_tax, paid_fee, last_chg_date
                ))
            except Exception as e:
                print(f"[{nick}] Error inserting row {row}: {e}")

        remote_conn.commit()
        remote_cur1.close()
        print(f"[{nick}] Insert completed. ({len(stock_order_complete_result)} rows processed)")

    except Exception as e:
        print(f"[{nick}] Error processing account: {e}")

# 연결 종료
conn.close()
remote_conn.close()
