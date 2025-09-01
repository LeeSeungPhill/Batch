import psycopg2 as db
from datetime import datetime, timedelta
import kis_api_resp as resp
import requests
import json
import telegram
import asyncio
import pandas as pd
import time

#URL_BASE = "https://openapivts.koreainvestment.com:29443"   # 모의투자서비스
URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# PostgreSQL 연결 설정
#conn_string = "dbname='my_develop' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")
cur_time = datetime.now().strftime("%H%M")
second = datetime.now().strftime("%H%M%S")

# 인증처리
def auth(APP_KEY, APP_SECRET):

    # 인증처리
    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
            "appkey":APP_KEY,
            "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False)
    ACCESS_TOKEN = res.json()["access_token"]

    return ACCESS_TOKEN

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

# 주식현재가 시세
def inquire_price(access_token, app_key, app_secret, code):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "FHKST01010100"}
    params = {
            'FID_COND_MRKT_DIV_CODE': "J",
            'FID_INPUT_ISCD': code
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)

    return ar.getBody().output

# 계좌잔고 조회
def stock_balance(access_token, app_key, app_secret, acct_no, rtFlag):
    
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8434R"}    # tr_id : TTTC8434R[실전투자], VTTC8434R[모의투자]
    params = {
                "CANO": acct_no,
                'ACNT_PRDT_CD': '01',
                'AFHR_FLPR_YN': 'N',
                'FNCG_AMT_AUTO_RDPT_YN': 'N',
                'FUND_STTL_ICLD_YN': 'N',
                'INQR_DVSN': '01',
                'OFL_YN': 'N',
                'PRCS_DVSN': '01',
                'UNPR_DVSN': '01',
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

    return pd.DataFrame(output)

async def main(telegram_text):
    chat_id = "2147256258"
    bot = telegram.Bot(token=token)
    await bot.send_message(chat_id=chat_id, text=telegram_text, parse_mode='HTML')

# 휴일정보 조회
cur0 = conn.cursor()
cur0.execute("select name from stock_holiday where holiday = '"+today+"'")
result_one = cur0.fetchone()
cur0.close()

nickname_list = ['chichipa', 'phills2', 'phills75', 'yh480825', 'phills13', 'phills15']

# 휴일이 아닌 경우
if result_one == None:

    for nick in nickname_list:
        try:
            # 텔레그램봇 사용할 token
            if nick == 'chichipa':
                token = "6353758449:AAG6LVdzgSRDSspoSzSJZVGnGw1SGHlAgi4"
            elif nick == 'phills13':
                token = "5721274603:AAHiwtuara7M-I-MIzcrt3E8TZBCRUpBUB4"
            elif nick == 'phills15':
                token = "6376313566:AAFPYOKj5_yyZ5jZJJ4JXJPqpyZXXo3fZ4M"
            elif nick == 'phills2':
                token = "5458112774:AAGwNnfjuC75WdK2ZYm_mttmXajzkhyvaHc"
            elif nick == 'phills75':
                token = "7242807146:AAH9fbu34tKKNaDDtJ2ew6zYPhzXkVvc9KA"
            elif nick == 'yh480825':
                token = "8143915544:AAEF-wVvqg9XZFKkVF4zUjm5LYC648OSWOg"    
            else:
                token = "6008784254:AAGYG-ZqwsJ4EKeidhzxn2EaYNLLFOPRMBI"  

            ac = account(nick)
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']

            # 자산정보 현금화 대상 금액 조회
            cur31 = conn.cursor()

            sql = """
                SELECT 
                    (SELECT SUM(eval_sum) FROM public."stockBalance_stock_balance" A WHERE A.acct_no = B.acct_no AND trading_plan NOT IN ('i', 'h') AND proc_yn = 'Y') - prvs_rcdl_excc_amt
                    , ((SELECT SUM(eval_sum) FROM public."stockBalance_stock_balance" A WHERE A.acct_no = B.acct_no AND A.trading_plan NOT IN ('i', 'h') AND proc_yn = 'Y') * (100 - market_ratio::numeric) * 0.01)::int - prvs_rcdl_excc_amt
                FROM public."stockFundMng_stock_fund_mng" B
                WHERE B.acct_no = %s
            """

            cur31.execute(sql, (str(acct_no),))
            result_three_one = cur31.fetchall()
            cur31.close()

            cash_transfer_rate = 0
            cash_transfer_amt = 0
            for i in result_three_one:

                # 현금화 대상 금액이 존재한 경우
                if int(i[1]) > 0:
                    cash_transfer_rate = round((int(i[1]) / int(i[0])), 4)
                    cash_transfer_amt = int(i[1])

            if cash_transfer_amt > 0:

                # 보유종목 조회
                cur32 = conn.cursor()

                sql = """
                    SELECT 
                        sb.code,
                        sb.name,
                        sb.purchase_price,
                        sb.purchase_amount,
                        sb.eval_sum
                    FROM public."stockBalance_stock_balance" sb
                    WHERE sb.proc_yn = 'Y'
                    AND sb.trading_plan NOT IN ('i', 'h')
                    AND sb.acct_no = %s
                """

                cur32.execute(sql, (str(acct_no),))
                result_three_two = cur32.fetchall()
                cur32.close()

                a = ""
                cash_rate_amt = 0
                
                for j in result_three_two:
                    try:
                        time.sleep(0.3)  # 초당 3건 이하로 제한
                        a = inquire_price(access_token, app_key, app_secret, j[0])

                        hold_price = float(j[2])
                        hold_qty = int(j[3])
                        current_price = int(a['stck_prpr'])
                        cash_rate_amt = int(int(j[4]) * cash_transfer_rate)
                        cash_rate_qty = int(cash_rate_amt / current_price)

                        if cash_rate_qty > 0:
                            sell_command = f"/HoldingSell_{j[0]}_{cash_rate_qty}"
                            telegram_text = (f"[{round(cash_transfer_rate * 100, 2)}% 현금비중]{j[1]}[<code>{j[0]}</code>] 현재가 : {format(current_price, ',d')}원, 매도량 : {format(cash_rate_qty, ',d')}주, 매도금액 : {format(cash_rate_amt, ',d')}원 => {sell_command}")
                            # 텔레그램 메시지 전송
                            asyncio.run(main(telegram_text))

                    except Exception as ex:
                        print(f"현재가 시세 에러 : [{j[0]}] {ex}")    

        except Exception as e:
            print(f"[{nick}] Error kis_cash_proc : {e}")      
    
    conn.close()

else:
    conn.close()
    print("Today is Holiday")