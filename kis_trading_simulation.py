import psycopg2 as db
import requests
import json
from datetime import datetime, timedelta

# 기본 DB 연결 정보
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# DB 연결
conn = db.connect(conn_string)

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

def post_business_day_char(business_day:str):
    cur100 = conn.cursor()
    cur100.execute("select post_business_day_char('"+business_day+"'::date)")
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

def get_previous_business_day(day):
    cur100 = conn.cursor()
    cur100.execute("select prev_business_day_char(%s)", (day,))
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

nickname_list = ['chichipa', 'phills2', 'phills75', 'yh480825', 'phills13', 'phills15', 'mamalong']

for nick in nickname_list:
    try:
        ac = account(nick)
        acct_no = ac['acct_no']

        business_day = datetime.now().strftime("%Y-%m-%d")
        trail_day = post_business_day_char(business_day)
        prev_date = get_previous_business_day((datetime.strptime(business_day, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d"))

        cur1 = conn.cursor()
        cur1.execute("""
            SELECT
                A.acct_no,
                A.name,
                A.code,
                %s,
                CASE WHEN A.trade_day = %s THEN A.trade_dtm ELSE %s END,
                CASE WHEN A.proc_yn = 'L' THEN 'L' ELSE '1' END AS trail_tp,
                A.buy_price,
                CASE WHEN A.proc_yn <> 'Y' THEN COALESCE(C.stop_price, A.loss_price) ELSE A.loss_price END AS loss_price,
                CASE WHEN A.proc_yn <> 'Y' THEN COALESCE(C.target_price, A.profit_price) ELSE A.profit_price END AS profit_price,
                now(),
                now()
            FROM tradng_simulation A JOIN (
                SELECT
                    acct_no,
                    code,
                    trade_tp,
                    MAX(trade_day) AS max_trade_day
                FROM tradng_simulation
                GROUP BY acct_no, code, trade_tp
            ) B
            ON A.acct_no = B.acct_no AND A.code = B.code AND A.trade_tp = B.trade_tp AND A.trade_day = B.max_trade_day
            LEFT JOIN trading_trail C
            ON C.acct_no = A.acct_no
            AND C.code = A.code
            AND C.trail_day = %s
            AND C.trail_tp IN ('1', '2', '3', 'L')
            WHERE A.trade_tp = '1'
            AND A.acct_no = %s
            AND A.proc_yn IN ('N', 'C', 'L')
            AND A.trade_day <= %s
            AND NOT EXISTS (
                SELECT 1
                FROM trading_trail T
                WHERE T.acct_no = A.acct_no
                AND T.code = A.code
                AND T.trail_day = %s
                AND T.trail_dtm = CASE WHEN A.trade_day = %s THEN A.trade_dtm ELSE %s END
                AND T.trail_tp IN ('1', '2', '3', 'L')
            )
        """, (trail_day, trail_day, '090000', prev_date, acct_no, business_day, trail_day, trail_day, '090000'))
        trading_trail_create_list = cur1.fetchall()
        cur1.close()

        if not trading_trail_create_list:
            print(f"[{nick}] No trading simulation data found.")

        insert_query1 = """
            INSERT INTO trading_trail (
                acct_no,
                name,
                code,
                trail_day,
                trail_dtm,
                trail_tp,
                basic_price,
                stop_price,
                target_price,
                crt_dt,
                mod_dt
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (acct_no, code, trail_day, trail_dtm, trail_tp) DO NOTHING
        """
        cur2 = conn.cursor()
        for row in trading_trail_create_list:
            acct_no, name, code, trail_day, trail_dtm, trail_tp, basic_price, stop_price, target_price, crt_dt, mod_dt = row
            try:
                cur2.execute(insert_query1, (
                    acct_no, name, code, trail_day, trail_dtm, trail_tp, basic_price, stop_price, target_price, crt_dt, mod_dt
                ))
            except Exception as e:
                print(f"[{nick}] Error trading_trail inserting row {row}: {e}")

        conn.commit()
        cur2.close()
        print(f"[{nick}] Insert trading_trail completed. ({len(trading_trail_create_list)} rows processed)")

    except Exception as e:
        print(f"[{nick}] Error trading_trail Insert : {e}")

# 연결 종료
conn.close()
