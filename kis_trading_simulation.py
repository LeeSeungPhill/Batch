import psycopg2 as db
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import kis_api_resp as resp
from psycopg2.extras import execute_values
from telegram import Bot
from telegram.ext import Updater
import traceback

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
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day, bot_token1, bot_token2, chat_id
        FROM "stockAccount_stock_account"
        WHERE nick_name = %s
    """, (nickname,))
    result_two = cur01.fetchone()
    cur01.close()

    acct_no, access_token, app_key, app_secret, token_publ_date, token_day, bot_token1, bot_token2, chat_id = result_two
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
        'app_secret': app_secret,
        'bot_token1': bot_token1,
        'bot_token2': bot_token2,
        'chat_id': chat_id
    }

# 계좌잔고 조회
def stock_balance(access_token, app_key, app_secret, acct_no, rtFlag):
   
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8434R"}            # tr_id : TTTC8434R[실전투자], VTTC8434R[모의투자]
    params = {
                "CANO": acct_no,
                'ACNT_PRDT_CD': '01',
                'AFHR_FLPR_YN': 'N',
                'OFL_YN': '',                   # 오프라인여부 : 공란(Default)
                'INQR_DVSN': '02',              # 조회구분 : 01 대출일별, 02 종목별
                'UNPR_DVSN': '01',              # 단가구분 : 01 기본값
                'FUND_STTL_ICLD_YN': 'N',       # 펀드결제분포함여부 : Y 포함, N 포함하지 않음
                'FNCG_AMT_AUTO_RDPT_YN': 'N',   # 융자금액자동상환여부 : N 기본값
                'PRCS_DVSN': '01',              # 처리구분 : 00 전일매매포함, 01 전일매매미포함
                'CTX_AREA_FK100': '',
                'CTX_AREA_NK100': ''
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
   
    if rtFlag == "all" and ar.isOK():
        output = ar.getBody().output2
    else:    
        output = ar.getBody().output1

    if isinstance(output, list):
        return pd.DataFrame(output)
    else:
        return pd.DataFrame([])
    
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

nickname_list = ['chichipa', 'phills2', 'phills75', 'yh480825', 'phills13', 'phills15', 'mamalong', 'honeylong']

for nick in nickname_list:
    try:
        ac = account(nick)
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']
        token = ac['bot_token2']
        chat_id = ac['chat_id']

        updater = Updater(token=token, use_context=True)
        bot = updater.bot

        business_day = datetime.now().strftime("%Y-%m-%d")
        trail_day = post_business_day_char(business_day)
        prev_date = get_previous_business_day((datetime.strptime(business_day, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"))

        # 계좌잔고 조회
        c = stock_balance(access_token, app_key, app_secret, acct_no, "")
        
        cur199 = conn.cursor()
        balance_rows = []
        
        #  일별 매매 잔고 현행화
        for i in range(len(c)):
            if int(c['hldg_qty'][i]) >  0:

                insert_query199 = """
                    INSERT INTO dly_trading_balance (
                        acct_no,
                        code,
                        name,
                        balance_day,
                        balance_price,
                        balance_qty,
                        balance_amt,
                        value_rate,
                        value_amt,
                        mod_dt
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (acct_no, code, balance_day)
                    DO UPDATE SET
                        balance_price = EXCLUDED.balance_price,
                        balance_qty   = EXCLUDED.balance_qty,
                        balance_amt   = EXCLUDED.balance_amt,
                        value_rate    = EXCLUDED.value_rate,
                        value_amt     = EXCLUDED.value_amt,
                        mod_dt        = EXCLUDED.mod_dt;
                """
                record_to_insert199 = (
                    acct_no,
                    c['pdno'][i],
                    c['prdt_name'][i],
                    business_day.replace('-', ''),
                    float(c['pchs_avg_pric'][i]),
                    int(c['hldg_qty'][i]),
                    int(c['pchs_amt'][i]),
                    float(c['evlu_pfls_rt'][i]),
                    int(c['evlu_pfls_amt'][i]),
                    datetime.now()
                )
                cur199.execute(insert_query199, record_to_insert199)
                conn.commit()

                balance_rows.append((
                    acct_no,
                    c['pdno'][i],                   # code
                    c['prdt_name'][i],              # name
                    float(c['pchs_avg_pric'][i]),   # purchase_price
                    int(c['hldg_qty'][i])           # purchase_qty
                ))

        cur199.close()

        if len(balance_rows) > 0:
            balance_sql = f"""
            WITH balance(acct_no, code, name, purchase_price, purchase_qty) AS (
                VALUES %s
            ),
            sim AS (
                SELECT *
                FROM (
                    SELECT
                        A.acct_no,
                        A.name,
                        A.code,
                        A.trade_day,
                        A.trade_dtm,
                        A.buy_price,
                        A.buy_qty,
                        COALESCE(B.stop_price, A.loss_price) AS loss_price,
                        COALESCE(B.target_price, A.profit_price) AS profit_price,
                        A.proc_yn,
                        ROW_NUMBER() OVER (
                            PARTITION BY A.acct_no, A.code
                            ORDER BY A.trade_day DESC, A.trade_dtm DESC, A.crt_dt DESC
                        ) AS rn
                    FROM tradng_simulation A
                    LEFT JOIN trading_trail B
                        ON B.acct_no = A.acct_no
                        AND B.code = A.code
                        AND B.trail_day = '{prev_date}'
                        AND B.trail_dtm = A.trade_dtm
                        AND B.trail_tp IN ('1','2','3','L')
                    WHERE A.trade_tp = '1'
                    AND A.acct_no = {acct_no}
                    AND A.proc_yn IN ('N','C','L')
                    AND SUBSTR(COALESCE(A.proc_dtm,'{prev_date}'), 1, 8) < '{trail_day}'
                    AND A.trade_day <= replace('{business_day}', '-', '')
                ) t
                WHERE rn = 1
            )
            SELECT
                COALESCE(BAL.acct_no, S.acct_no) AS acct_no,
                COALESCE(S.name, BAL.name) AS name,
                COALESCE(BAL.code, S.code) AS code,
                '{trail_day}' AS trail_day,
                CASE WHEN S.trade_day = '{trail_day}' THEN S.trade_dtm ELSE '090000' END AS trail_dtm,
                CASE WHEN BAL.acct_no IS NOT NULL AND S.acct_no IS NULL THEN 'L' WHEN S.proc_yn = 'L' THEN 'L' ELSE '1' END AS trail_tp,
                COALESCE(BAL.purchase_price, S.buy_price) AS basic_price,
                COALESCE(BAL.purchase_qty, S.buy_qty) AS basic_qty,
                COALESCE(S.loss_price, 0) AS stop_price,
                COALESCE(S.profit_price, 0) AS target_price,
                CASE WHEN S.trade_day = '{trail_day}' THEN S.trade_dtm ELSE '090000' END AS proc_min,
                now(),
                now()
            FROM balance BAL
            FULL OUTER JOIN sim S ON S.acct_no = BAL.acct_no AND S.code = BAL.code
            WHERE NOT EXISTS (
                SELECT 1
                FROM trading_trail T
                WHERE T.acct_no = COALESCE(BAL.acct_no, S.acct_no)
                AND T.code = COALESCE(BAL.code, S.code)
                AND T.trail_day = '{trail_day}'
                AND T.trail_dtm = CASE WHEN S.trade_day = '{trail_day}' THEN S.trade_dtm ELSE '090000' END
                AND T.trail_tp IN ('1','2','3','L')
            );
            """
            cur200 = conn.cursor()

            execute_values(
                cur200,
                balance_sql,
                balance_rows,
                template="(%s, %s, %s, %s, %s)"
            )

            trading_trail_create_list = cur200.fetchall()
            cur200.close()

            inserted_count = 0
            if not trading_trail_create_list or len(trading_trail_create_list) < 1:
                print(f"[{nick}] No trading simulation data found.")
            else:
                insert_query1 = """
                    INSERT INTO trading_trail (
                        acct_no,
                        name,
                        code,
                        trail_day,
                        trail_dtm,
                        trail_tp,
                        basic_price,
                        basic_qty,
                        basic_amt,
                        stop_price,
                        target_price,
                        proc_min,
                        crt_dt,
                        mod_dt
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (acct_no, code, trail_day, trail_dtm, trail_tp) DO NOTHING
                """
                
                cur201 = conn.cursor()
                for row in trading_trail_create_list:
                    acct_no, name, code, trail_day, trail_dtm, trail_tp, basic_price, basic_qty, stop_price, target_price, proc_min, crt_dt, mod_dt = row
                    try:
                        cur201.execute(insert_query1, (
                            acct_no, name, code, trail_day, trail_dtm, trail_tp, basic_price, 0 if basic_qty is None else basic_qty, 0 if basic_qty is None else basic_price*basic_qty, stop_price, target_price, proc_min, crt_dt, mod_dt
                        ))
                        inserted_count += cur201.rowcount
                    except Exception as e:
                        print(f"[{nick}] Error trading_trail inserting row {row}: {e}")

                conn.commit()
                cur201.close()

            skipped_count = len(trading_trail_create_list) - inserted_count
            
            message = (
                f"[{today}-{nick}] trading_trail 생성 \n"
                f"(전체 : {len(trading_trail_create_list)}건, "
                f"생성 : {inserted_count}건, "
                f"미생성 : {skipped_count}건)"
            )
            print(message)
            bot.send_message(
                chat_id=chat_id,
                text=message
            )

    except Exception as e:
        error_msg = (
            f"[{today}-{nick}] trading_trail 생성 에러\n\n"
            f"Error: {str(e)}\n\n"
            f"Traceback:\n{traceback.format_exc()}"
        )
        print(error_msg)
        try:
            if bot is not None:
                bot.send_message(
                    chat_id=chat_id,
                    text=error_msg[:4000]  # Telegram 메시지 길이 제한 대비
                )
        except Exception as te:
            print(f"[{nick}] Telegram error send failed: {te}")

# 연결 종료
conn.close()
