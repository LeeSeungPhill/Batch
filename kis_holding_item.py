import psycopg2 as db
from datetime import datetime
import kis_api_resp as resp
import requests
import json
import telegram
import asyncio
import sys
import pandas as pd

#URL_BASE = "https://openapivts.koreainvestment.com:29443"   # 모의투자서비스
URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

arguments = sys.argv

# PostgreSQL 연결 설정
#conn_string = "dbname='my_develop' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")
time = datetime.now().strftime("%H%M")

# 텔레그램봇 사용할 token
if arguments[1] == 'chichipa':
    token = "6353758449:AAG6LVdzgSRDSspoSzSJZVGnGw1SGHlAgi4"
elif arguments[1] == 'phills13':
    token = "5721274603:AAHiwtuara7M-I-MIzcrt3E8TZBCRUpBUB4"
elif arguments[1] == 'phills15':
    token = "6376313566:AAFPYOKj5_yyZ5jZJJ4JXJPqpyZXXo3fZ4M"
elif arguments[1] == 'phills2':
    token = "5458112774:AAGwNnfjuC75WdK2ZYm_mttmXajzkhyvaHc"
elif arguments[1] == 'phills75':
    token = "7242807146:AAH9fbu34tKKNaDDtJ2ew6zYPhzXkVvc9KA"
elif arguments[1] == 'yh480825':
    token = "8143915544:AAEF-wVvqg9XZFKkVF4zUjm5LYC648OSWOg"    
else:
    token = "6008784254:AAEcJaePafd6Bh0riGL57OjhZ_ZoFxe6Fw0"    

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

# 잔고정보 처리
def balance_proc():
    # 계좌잔고 조회
    b = stock_balance(access_token, app_key, app_secret, acct_no, "all")

    for i, name in enumerate(b.index):
        u_tot_evlu_amt = int(b['tot_evlu_amt'][i])                  # 총평가금액
        u_dnca_tot_amt = int(b['dnca_tot_amt'][i])                  # 예수금총금액
        u_nass_amt = int(b['nass_amt'][i])                          # 순자산금액(세금비용 제외)
        u_prvs_rcdl_excc_amt = int(b['prvs_rcdl_excc_amt'][i])      # 가수도 정산 금액
        u_scts_evlu_amt = int(b['scts_evlu_amt'][i])                # 유저 평가 금액
        u_asst_icdc_amt = int(b['asst_icdc_amt'][i])                # 자산 증감액

    print("총평가금액 : " + format(int(u_tot_evlu_amt), ',d'))       
    print("예수금총금액 : " + format(int(u_dnca_tot_amt), ',d'))
    print("순자산금액(세금비용 제외) : " + format(int(u_nass_amt), ',d'))
    print("가수도 정산 금액 : " + format(int(u_prvs_rcdl_excc_amt), ',d'))
    print("유저 평가 금액 : " + format(int(u_scts_evlu_amt), ',d'))
    print("자산 증감액 : " + format(int(u_asst_icdc_amt), ',d'))

    # 자산정보 조회
    cur100 = conn.cursor()
    cur100.execute("select asset_num, cash_rate, tot_evlu_amt, prvs_rcdl_excc_amt, sell_plan_amt from \"stockFundMng_stock_fund_mng\" where acct_no = '" + str(acct_no) + "'")
    result_one00 = cur100.fetchall()
    cur100.close()

    asset_num = 0
    sell_plan_amt = 0 

    for i in result_one00:
        asset_num = i[0]
        sell_plan_amt = i[4]
        print("자산번호 : " + str(asset_num))   
        print("매도예정금액 : " + str(sell_plan_amt))

    # 자산정보 변경
    cur200 = conn.cursor()
    update_query200 = "update \"stockFundMng_stock_fund_mng\" set tot_evlu_amt = %s, dnca_tot_amt = %s, prvs_rcdl_excc_amt = %s, nass_amt = %s, scts_evlu_amt = %s, asset_icdc_amt = %s, last_chg_date = %s where asset_num = %s and acct_no = %s"
    # update 인자값 설정
    record_to_update200 = ([u_tot_evlu_amt, u_dnca_tot_amt, u_prvs_rcdl_excc_amt, u_nass_amt, u_scts_evlu_amt, u_asst_icdc_amt, datetime.now(), asset_num, acct_no])
    # DB 연결된 커서의 쿼리 수행
    cur200.execute(update_query200, record_to_update200)
    conn.commit()
    cur200.close()

    # 계좌종목 조회
    c = stock_balance(access_token, app_key, app_secret, acct_no, "")

    # 잔고정보 변경
    cur300 = conn.cursor()
    update_query300 = "update \"stockBalance_stock_balance\" set proc_yn = 'N', last_chg_date = %s where acct_no = %s and proc_yn = 'Y'"
    # update 인자값 설정
    record_to_update300 = ([datetime.now(), acct_no])
    # DB 연결된 커서의 쿼리 수행
    cur300.execute(update_query300, record_to_update300)
    conn.commit()
    cur300.close()

    for i, name in enumerate(c.index):
        e_code = c['pdno'][i]
        e_name = c['prdt_name'][i]
        e_purchase_price = c['pchs_avg_pric'][i]
        e_purchase_amount = int(c['hldg_qty'][i])
        e_purchase_sum = int(c['pchs_amt'][i])
        e_current_price = int(c['prpr'][i])
        e_eval_sum = int(c['evlu_amt'][i])
        e_earnings_rate = c['evlu_pfls_rt'][i]
        e_valuation_sum = int(c['evlu_pfls_amt'][i])

        # 자산번호의 매도예정자금이 존재하는 경우, 보유종목 비중별 매도가능금액 및 매도가능수량 계산
        if sell_plan_amt > 0:
            # 종목 매입금액 비중 = 평가금액 / 총평가금액(예수금총금액 + 유저평가금액) * 100
            item_eval_gravity = e_eval_sum / u_tot_evlu_amt * 100
            print("종목 매입금액 비중 : " + format(int(item_eval_gravity), ',d'))
            # 종목 매도가능금액 = 매도예정자금 * 종목 매입금액 비중 * 0.01
            e_sell_plan_sum = sell_plan_amt * item_eval_gravity * 0.01

            # 종목 매도가능수량 = 종목 매도가능금액 / 현재가
            e_sell_plan_amount = e_sell_plan_sum / e_current_price

            cur301 = conn.cursor()
            update_query301 = "with upsert as (update \"stockBalance_stock_balance\" set purchase_price = %s, purchase_amount = %s, purchase_sum = %s, current_price = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, sell_plan_sum = %s, sell_plan_amount = %s, proc_yn = 'Y', last_chg_date = %s where acct_no = %s and code = %s and asset_num = %s returning * ) insert into \"stockBalance_stock_balance\"(acct_no, code, name, purchase_price, purchase_amount, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, asset_num, sell_plan_sum, sell_plan_amount, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, proc_yn, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select A.sign_resist_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_resist_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.sign_support_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_support_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_loss_price from(select row_number() over(order by last_chg_date desc) as num, b.end_loss_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_target_price from(select row_number() over(order by last_chg_date desc) as num, b.end_target_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.trading_plan from(select row_number() over(order by last_chg_date desc) as num, b.trading_plan from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), %s, %s where not exists(select * from upsert)";
            # update 인자값 설정
            record_to_update301 = ([e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, e_sell_plan_sum, e_sell_plan_amount, datetime.now(), acct_no, e_code, asset_num,
                                    acct_no, e_code, e_name, e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, asset_num, e_sell_plan_sum, e_sell_plan_amount, 
                                    acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code,
                                    'Y', datetime.now()])
            # DB 연결된 커서의 쿼리 수행
            cur301.execute(update_query301, record_to_update301)
            conn.commit()
            cur301.close()

        else:
            cur301 = conn.cursor()
            update_query301 = "with upsert as (update \"stockBalance_stock_balance\" set purchase_price = %s, purchase_amount = %s, purchase_sum = %s, current_price = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, proc_yn = 'Y', last_chg_date = %s where acct_no = %s and code = %s and asset_num = %s returning * ) insert into \"stockBalance_stock_balance\"(acct_no, code, name, purchase_price, purchase_amount, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, asset_num, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, proc_yn, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select A.sign_resist_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_resist_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.sign_support_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_support_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_loss_price from(select row_number() over(order by last_chg_date desc) as num, b.end_loss_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_target_price from(select row_number() over(order by last_chg_date desc) as num, b.end_target_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.trading_plan from(select row_number() over(order by last_chg_date desc) as num, b.trading_plan from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), %s, %s where not exists(select * from upsert)";
            # update 인자값 설정
            record_to_update301 = ([e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, datetime.now(), acct_no, e_code, asset_num,
                                    acct_no, e_code, e_name, e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, asset_num, 
                                    acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code,
                                    'Y', datetime.now()])
            # DB 연결된 커서의 쿼리 수행
            cur301.execute(update_query301, record_to_update301)
            conn.commit()
            cur301.close()

async def main(telegram_text):
    chat_id = "2147256258"
    bot = telegram.Bot(token=token)
    await bot.send_message(chat_id, telegram_text)


# 휴일정보 조회
cur0 = conn.cursor()
cur0.execute("select name from stock_holiday where holiday = '"+today+"'")
result_one = cur0.fetchone()
cur0.close()

# 휴일이 아닌 경우
if result_one == None:

    # 계정정보 조회
    cur01 = conn.cursor()
    cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date from \"stockAccount_stock_account\" where nick_name = '"+arguments[1]+"'")
    result_two = cur01.fetchone()
    cur01.close()

    acct_no = result_two[0]
    access_token = result_two[1]
    app_key = result_two[2]
    app_secret = result_two[3]

    YmdHMS = datetime.now()
    validTokenDate = datetime.strptime(result_two[4], '%Y%m%d%H%M%S')
    diff = YmdHMS - validTokenDate
    #print("diff : " + str(diff.days))
    if diff.days >= 1:  # 토큰 유효기간(1일) 만료 재발급
        access_token = auth(result_two[2], result_two[3])
        token_publ_date = datetime.now().strftime("%Y%m%d%H%M%S")
        print("new access_token : " + access_token)
        # 계정정보 토큰값 변경
        cur02 = conn.cursor()
        update_query = "update \"stockAccount_stock_account\" set access_token = %s, token_publ_date = %s, last_chg_date = %s where acct_no = %s"
        # update 인자값 설정
        record_to_update = ([access_token, token_publ_date, datetime.now(), acct_no])
        # DB 연결된 커서의 쿼리 수행
        cur02.execute(update_query, record_to_update)
        conn.commit()
        cur02.close()

    # 잔고정보 호출
    balance_proc()
    #params = {'acct_no': str(acct_no), 'app_key': app_key, 'app_secret': app_secret, 'access_token': access_token}
    #url = 'http://phills2.gonetis.com:8000/stockBalance/balanceList'   # Django URL 주소
    #url = 'http://localhost:8000/stockBalance/balanceList'  # Django URL 주소
    #response = requests.get(url, params=params)

    #if response.status_code == 200:
        # 요청이 성공했을 때의 처리
    #    print(response.text)
    #else:
        # 요청이 실패했을 때의 처리
    #    print('요청이 실패했습니다. 상태 코드:', response.status_code)

    # 보유정보 조회
    cur03 = conn.cursor()
    cur03.execute("select code, name, sign_resist_price, sign_support_price, end_target_price, end_loss_price, purchase_amount, (select 1 from trail_signal_recent where acct_no = '"+str(acct_no)+"' and trail_day = TO_CHAR(now(), 'YYYYMMDD') and code = '0001' and trail_signal_code = '04') as market_dead, (select 1 from trail_signal_recent where acct_no = '"+str(acct_no)+"' and trail_day = TO_CHAR(now(), 'YYYYMMDD') and code = '0001' and trail_signal_code = '06') as market_over, case when cast(A.earnings_rate as INTEGER) > 0 then (select B.low_price from dly_stock_balance B where A.code = B.code and A.acct_no = cast(B.acct as INTEGER)	and B.dt = TO_CHAR(get_previous_business_day(now()::date), 'YYYYMMDD')) else null end as low_price from \"stockBalance_stock_balance\" A where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y'")
    result_three = cur03.fetchall()
    cur03.close()

    # 보유종목 최종이탈가, 최종목표가를 각각 실시간 종목시세의 최고가와 최저가 비교
    for i in result_three:
        print("종목코드 : " + i[0])
        print("종목명 : " + i[1])

        a = inquire_price(access_token, app_key, app_secret, i[0])
        print("현재가 : " + format(int(a['stck_prpr']), ',d'))  # 현재가
        print("최고가 : " + format(int(a['stck_hgpr']), ',d'))  # 최고가
        print("최저가 : " + format(int(a['stck_lwpr']), ',d'))  # 최저가
        print("누적거래량 : " + format(int(a['acml_vol']), ',d'))  # 누적거래량
        print("전일대비거래량비율 : " + a['prdy_vrss_vol_rate'])  # 전일대비거래량비율

        trail_signal_code = ""
        trail_signal_name = ""
        sell_plan_amount = ""
        n_sell_amount = 0
        n_sell_sum = 0
        if i[2] != None:
            if i[2] > 0:
                print("저항가 : " + format(int(i[2]), ',d'))
                if int(a['stck_prpr']) > i[2]:
                    print("저항가 돌파")
                    trail_signal_code = "07"
                    trail_signal_name = format(int(i[2]), ',d') + "원 {저항가 돌파}"

        if i[3] != None:
            if i[3] > 0:
                print("지지가 : " + format(int(i[3]), ',d'))
                if int(a['stck_prpr']) < i[3]:
                    print("지지가 이탈")
                    trail_signal_code = "08"
                    trail_signal_name = format(int(i[3]), ',d') + "원 {지지가 이탈}"

                    if i[6] != None:
                        n_sell_amount = round(i[6]/2)
                        n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                        sell_plan_amount = format(int(n_sell_amount), ',d')

        if i[4] != None:
            if i[4] > 0:
                print("최종목표가 : " + format(int(i[4]), ',d'))
                if int(a['stck_hgpr']) > i[4]:
                    print("최종목표가 돌파")
                    trail_signal_code = "09"
                    trail_signal_name = format(int(i[4]), ',d') + "원 {최종목표가 돌파}"

        if i[5] != None:
            if i[5] > 0:
                print("최종이탈가 : " + format(int(i[5]), ',d'))
                if int(a['stck_lwpr']) < i[5]:
                    print("최종이탈가 이탈")
                    trail_signal_code = "10"
                    trail_signal_name = format(int(i[5]), ',d') + "원 {최종이탈가 이탈}"

                    if i[6] != None:
                        n_sell_amount = i[6]
                        n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                        sell_plan_amount = format(int(n_sell_amount), ',d')

        if i[7] != None:
            print("지지선이탈 : " + str(i[7]))  # 지지선이탈
            trail_signal_code = "11"
            trail_signal_name = "시장 지지선 이탈[지지가 : " + format(int(i[3]), ',d') + "원]"

            if i[6] != None:
                n_sell_amount = round(i[6]/2)
                n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                sell_plan_amount = format(int(n_sell_amount), ',d')

        if i[8] != None:
            print("추세선이탈 : " + str(i[8]))  # 추세선이탈
            trail_signal_code = "12"
            trail_signal_name = "시장 추세선 이탈[지지가 : " + format(int(i[3]), ',d') + "원]"

            if i[6] != None:
                n_sell_amount = i[6]
                n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                sell_plan_amount = format(int(n_sell_amount), ',d')

        if time > '1510':
            if i[9] != None:
                print("수익률 0 이상 보유종목 대상 전일 저가 : " + str(i[9])) # 수익률 0 이상 보유종목 대상 전일 저가 
             
                if int(a['stck_prpr']) < i[9]:
                    trail_signal_code = "13"
                    trail_signal_name = format(int(i[9]), ',d') +"원 {전일 저가 이탈}"
        
        # 추적정보 조회(현재일 종목코드 기준)
        cur04 = conn.cursor()
        cur04.execute("select TS.trail_signal_code from trail_signal TS where TS.acct = '" + str(acct_no) + "' and TS.code = '" + i[0] + "' and TS.trail_day = TO_CHAR(now(), 'YYYYMMDD') and trail_signal_code = '" + trail_signal_code + "'")
        result_four = cur04.fetchall()
        cur04.close()

        if len(result_four) > 0:
            for j in result_four:
                print("trail_signal_code1 : " + j[0])
                if trail_signal_code != "":
                    if trail_signal_code != j[0]:
                        if n_sell_amount > 0:
                            sell_command = f"/BalanceSell_{i[0]}_{i[6]}"
                            # telegram_text = i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 매도량 : " + sell_plan_amount + "주, 매도금액 : " + format(int(n_sell_sum), ',d') +"원"
                            telegram_text = (f"{i[1]}[{i[0]}] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매도량 : {sell_plan_amount}주, 매도금액 : {format(int(n_sell_sum), ',d')}원 => {sell_command}")
                        else:
                            telegram_text = i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate']                            
                        # 텔레그램 메시지 전송
                        asyncio.run(main(telegram_text))

                        # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                        cur20 = conn.cursor()
                        insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, cdate = %s, sell_plan_qty = %s, sell_plan_amt = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                        # insert 인자값 설정
                        record_to_insert0 = ([time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                        # DB 연결된 커서의 쿼리 수행
                        cur20.execute(insert_query0, record_to_insert0)

                        # 추적신호이력 정보 생성
                        cur2 = conn.cursor()
                        insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        # insert 인자값 설정
                        record_to_insert = ([acct_no, today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                        # DB 연결된 커서의 쿼리 수행
                        cur2.execute(insert_query, record_to_insert)
                        conn.commit()
                        cur20.close()
                        cur2.close()
        else:
            print("trail_signal_code2 : " + trail_signal_code)
            if trail_signal_code != "":
                if n_sell_amount > 0:
                    sell_command = f"/BalanceSell_{i[0]}_{i[6]}"
                    # telegram_text = i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 매도량 : " + sell_plan_amount + "주, 매도금액 : " + format(int(n_sell_sum), ',d') +"원"
                    telegram_text = (f"{i[1]}[{i[0]}] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매도량 : {sell_plan_amount}주, 매도금액 : {format(int(n_sell_sum), ',d')}원 => {sell_command}")

                else:
                    telegram_text = i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate']                            
                # 텔레그램 메시지 전송
                asyncio.run(main(telegram_text))

                # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                cur20 = conn.cursor()
                insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, cdate = %s, sell_plan_qty = %s, sell_plan_amt = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                # insert 인자값 설정
                record_to_insert0 = ([time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                # DB 연결된 커서의 쿼리 수행
                cur20.execute(insert_query0, record_to_insert0)

                # 추적신호이력 정보 생성
                cur2 = conn.cursor()
                insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                # insert 인자값 설정
                record_to_insert = ([acct_no, today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                # DB 연결된 커서의 쿼리 수행
                cur2.execute(insert_query, record_to_insert)
                conn.commit()
                cur20.close()
                cur2.close()

    conn.close()

else:
    conn.close()
    print("Today is Holiday")