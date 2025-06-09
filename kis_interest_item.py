import psycopg2 as db
from datetime import datetime
import kis_api_resp as resp
import requests
import json
import telegram
import asyncio
import math
import pandas as pd

#URL_BASE = "https://openapivts.koreainvestment.com:29443"   # 모의투자서비스
URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# PostgreSQL 연결 설정
#conn_string = "dbname='my_develop' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")
time = datetime.now().strftime("%H%M")

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

# 국내주식업종기간별시세
def inquire_daily_indexchartprice(access_token, app_key, app_secret, market, stock_day):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "FHKUP03500100",
               "custtype": "P"}
    params = {
        'FID_COND_MRKT_DIV_CODE': "U",  # 시장 분류 코드(J : 주식, ETF, ETN U: 업종)
        'FID_INPUT_ISCD': market,
        'FID_INPUT_DATE_1': stock_day,
        'FID_INPUT_DATE_2': stock_day,
        'FID_PERIOD_DIV_CODE': 'D'}
    PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)

    return ar.getBody().output1

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

# 자산정보 및 시장레벨정보 처리
def fund_marketLevel_proc(access_token, app_key, app_secret, acct_no):
    # 계좌잔고 조회
    b = stock_balance(access_token, app_key, app_secret, acct_no, "all")

    for i, name in enumerate(b.index):
        u_tot_evlu_amt = int(b['tot_evlu_amt'][i])                  # 총평가금액
        u_dnca_tot_amt = int(b['dnca_tot_amt'][i])                  # 예수금총금액
        u_nass_amt = int(b['nass_amt'][i])                          # 순자산금액(세금비용 제외)
        u_prvs_rcdl_excc_amt = int(b['prvs_rcdl_excc_amt'][i])      # 가수도 정산 금액
        u_scts_evlu_amt = int(b['scts_evlu_amt'][i])                # 유저 평가 금액
        u_asst_icdc_amt = int(b['asst_icdc_amt'][i])                # 자산 증감액

    # 자산정보 조회
    cur100 = conn.cursor()
    cur100.execute("select asset_num, cash_rate, tot_evlu_amt, prvs_rcdl_excc_amt from \"stockFundMng_stock_fund_mng\" where acct_no = '" + str(acct_no) + "'")
    result_one00 = cur100.fetchall()
    cur100.close()

    asset_num = 0 

    for i in result_one00:
        asset_num = i[0]

    # 자산정보 변경
    cur200 = conn.cursor()
    update_query200 = "update \"stockFundMng_stock_fund_mng\" set tot_evlu_amt = %s, dnca_tot_amt = %s, prvs_rcdl_excc_amt = %s, nass_amt = %s, scts_evlu_amt = %s, asset_icdc_amt = %s, last_chg_date = %s where asset_num = %s and acct_no = %s"
    # update 인자값 설정
    record_to_update200 = ([u_tot_evlu_amt, u_dnca_tot_amt, u_prvs_rcdl_excc_amt, u_nass_amt, u_scts_evlu_amt, u_asst_icdc_amt, datetime.now(), asset_num, acct_no])
    # DB 연결된 커서의 쿼리 수행
    cur200.execute(update_query200, record_to_update200)
    conn.commit()
    cur200.close()

    # 시장레벨정보 조회
    cur300 = conn.cursor()
    cur300.execute("select asset_risk_num, market_level_num from \"stockMarketMng_stock_market_mng\" where acct_no = '" + str(acct_no) + "' and aply_end_dt = '99991231'")
    result_one01 = cur300.fetchall()
    cur300.close()

    asset_risk_num = 0 

    for i in result_one01:

        asset_risk_num = i[0]
        print("자산리스크번호 : " + str(asset_risk_num))   
        if i[1] == "1":   # 하락 지속 후, 기술적 반등
            n_asset_sum = u_prvs_rcdl_excc_amt * 30 * 0.01
            if n_asset_sum < 10000000:
                n_asset_sum = 10000000
                n_risk_rate = 2
                n_stock_num = 2
            elif n_asset_sum > 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 2
                n_stock_num = 4
            else:
                n_risk_rate = 1.8
                n_stock_num = 3
        elif i[1] == "2": # 단기 추세 전환 후, 기술적 반등
            n_asset_sum = u_prvs_rcdl_excc_amt * 30 * 0.01
            if n_asset_sum < 20000000:
                n_asset_sum = 20000000
                n_risk_rate = 3
                n_stock_num = 4
            elif n_asset_sum > 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 4
                n_stock_num = 6
            else:
                n_risk_rate = 3.5
                n_stock_num = 5
        elif i[1] == "3": # 패턴내에서 기술적 반등
            n_asset_sum = u_prvs_rcdl_excc_amt * 50 * 0.01
            if n_asset_sum < 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 4
                n_stock_num = 6
            elif n_asset_sum > 50000000:
                n_asset_sum = 50000000
                n_risk_rate = 4
                n_stock_num = 8
            else:
                n_risk_rate = 2.8
                n_stock_num = 5
        elif i[1] == "4": # 일봉상 추세 전환 후, 눌림구간에서 반등
            n_asset_sum = u_prvs_rcdl_excc_amt * 70 * 0.01
            if n_asset_sum < 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 5.5
                n_stock_num = 8
            elif n_asset_sum > 70000000:
                n_asset_sum = 70000000
                n_risk_rate = 3.5
                n_stock_num = 10
            else:
                n_risk_rate = 5
                n_stock_num = 10
        elif i[1] == "5": # 상승 지속 후, 패턴내에서 기술적 반등
            n_asset_sum = u_prvs_rcdl_excc_amt * 50 * 0.01
            if n_asset_sum < 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 4
                n_stock_num = 6
            elif n_asset_sum > 50000000:
                n_asset_sum = 50000000
                n_risk_rate = 4
                n_stock_num = 8
            else:
                n_risk_rate = 2.8
                n_stock_num = 5
        else:
            n_asset_sum = u_prvs_rcdl_excc_amt * 30 * 0.01
            if n_asset_sum < 10000000:
                n_asset_sum = 10000000
                n_risk_rate = 2
                n_stock_num = 2
            elif n_asset_sum > 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 2
                n_stock_num = 4
            else:
                n_risk_rate = 1.8
                n_stock_num = 3

    n_risk_sum = n_asset_sum * n_risk_rate * 0.01

    # 시장레벨정보 변경
    cur400 = conn.cursor()
    update_query400 = "update \"stockMarketMng_stock_market_mng\" set total_asset = %s, risk_rate = %s, risk_sum = %s, item_number = %s where asset_risk_num = %s and acct_no = %s and aply_end_dt = '99991231'"
    # update 인자값 설정
    record_to_update400 = ([n_asset_sum, n_risk_rate, n_risk_sum, n_stock_num, asset_risk_num, acct_no])
    # DB 연결된 커서의 쿼리 수행
    cur400.execute(update_query400, record_to_update400)
    conn.commit()
    cur400.close()

def fundTrail_proc():
    # 관심종목 코스피, 코스닥 미존재시 생성
    cur100 = conn.cursor()
    insert_query100 = "with A as (select * from \"interestItem_interest_item\" where acct_no = %s and code = '0001') insert into \"interestItem_interest_item\"(acct_no, code, name, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, buy_expect_sum, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from A);"
    record_to_insert100 = ([acct_no, acct_no, '0001', '코스피', 0, 0, 0, 0, 0, 0, 0, datetime.now()])
    cur100.execute(insert_query100, record_to_insert100)
    conn.commit()
    cur100.close()

    cur200 = conn.cursor()
    insert_query200 = "with A as (select * from \"interestItem_interest_item\" where acct_no = %s and code = '1001') insert into \"interestItem_interest_item\"(acct_no, code, name, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, buy_expect_sum, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from A);"
    record_to_insert200 = ([acct_no, acct_no, '1001', '코스닥', 0, 0, 0, 0, 0, 0, 0, datetime.now()])
    cur200.execute(insert_query200, record_to_insert200)
    conn.commit()
    cur200.close()

    # 추적신호 조회(코스피) : 추적신호코드별 총평가금액 기준 현금비중금액 설정, 매도예정자금 설정(총평가금액 기준 현금비중금액 - 가수도정산금액), 매수예정자금 설정(가수도정산금액 - 총평가금액 기준 현금비중금액)
    cur300 = conn.cursor()
    cur300.execute("select trail_signal_code, tot_evlu_amt, prvs_rcdl_excc_amt, asset_num from (select row_number() over(order by trail_day desc, trail_time desc) as num, A.trail_signal_code, B.tot_evlu_amt, B.prvs_rcdl_excc_amt, B.asset_num from trail_signal_recent A, \"stockFundMng_stock_fund_mng\" B where cast(A.acct_no as INTEGER) = B.acct_no and code = '0001' and A.acct_no = '" + str(acct_no) + "') T where num = 1")
    result_one100 = cur300.fetchall()
    cur300.close()

    for i in result_one100:

        trail_signal_result1 = i[0]
        tot_evlu_amt = i[1]
        prvs_rcdl_excc_amt = i[2]
        asset_num = i[3]
        print("코스피 추적 신호 : " + str(trail_signal_result1))   
        print("총평가금액 : " + str(tot_evlu_amt))
        print("가수도정산금액 : " + str(prvs_rcdl_excc_amt))
        print("자산번호 : " + str(asset_num))
            
        # 시장 신호 정보 변경 기준 현금 비율 변경
        if trail_signal_result1 == '03': # 저항가 돌파
            kospi_ratio = "H"  # 시장 상승
            cash_rate = 30 # 전체금액의 30% 미만 현금 비중 설정
            cash_rate_amt = round(tot_evlu_amt * cash_rate * 0.01, 0)  # 총평가금액 기준 현금 비중 금액
        elif trail_signal_result1 == '04': # 지지가 이탈
            kospi_ratio = "D"  # 시장 하락
            cash_rate = 70 # 전체금액의 70% 이상 현금 비중 설정
            cash_rate_amt = round(tot_evlu_amt * cash_rate * 0.01, 0)  # 총평가금액 기준 현금 비중 금액
        elif trail_signal_result1 == '05': # 추세상단가 돌파
            kospi_ratio = "H"  # 시장 상승
            cash_rate = 10 # 전체금액의 10% 미만 현금 비중 설정
            cash_rate_amt = round(tot_evlu_amt * cash_rate * 0.01, 0)  # 총평가금액 기준 현금 비중 금액
        elif trail_signal_result1 == '06': # 추세하단가 이탈
            kospi_ratio = "D"  # 시장 하락
            cash_rate = 90 # 전체금액의 90% 이상 현금 비중 설정
            cash_rate_amt = round(tot_evlu_amt * cash_rate * 0.01, 0)  # 총평가금액 기준 현금 비중 금액
        elif trail_signal_result1 == '01': # 돌파가 돌파
            kospi_ratio = "H"  # 시장 상승
            remain_cash_rate = 30 # 남은 현금기준 비중 30% 미만 현금 비중 설정
            cash_rate_amt = round(prvs_rcdl_excc_amt * remain_cash_rate * 0.01, 0)  # 가수도정산금액 기준 현금 비중 금액
            cash_rate = 100 - (tot_evlu_amt/(tot_evlu_amt + prvs_rcdl_excc_amt - cash_rate_amt)) * 100
        elif trail_signal_result1 == '02': # 이탈가 이탈
            kospi_ratio = "D"  # 시장 하락
            remain_cash_rate = 70 # 남은 현금기준 비중 70% 이상 현금 비중 설정
            cash_rate_amt = round(prvs_rcdl_excc_amt * remain_cash_rate * 0.01, 0)  # 가수도정산금액 기준 현금 비중 금액
            cash_rate = 100 - (tot_evlu_amt / (tot_evlu_amt + prvs_rcdl_excc_amt - cash_rate_amt)) * 100

        print("현금비중 : " + format(int(cash_rate), ',d'))
        print("현금비중금액 : " + format(int(cash_rate_amt), ',d'))
        sell_plan_amt = cash_rate_amt - prvs_rcdl_excc_amt  # 매도예정자금(총평가금액 기준 현금비중금액 - 가수도 정산금액)
        if sell_plan_amt < 0:
            sell_plan_amt = 0

        buy_plan_amt = prvs_rcdl_excc_amt - cash_rate_amt  # 매수예정자금(가수도 정산금액 - 총평가금액 기준 현금비중금액)
        if buy_plan_amt < 0:
            buy_plan_amt = 0

        print("매도예정자금 : " + format(int(sell_plan_amt), ',d'))
        print("매수예정자금 : " + format(int(buy_plan_amt), ',d'))

        # 자산정보 변경
        cur400 = conn.cursor()
        update_query100 = "update \"stockFundMng_stock_fund_mng\" set cash_rate = %s, cash_rate_amt = %s, sell_plan_amt = %s, buy_plan_amt = %s, last_chg_date = %s where asset_num = %s and acct_no = %s"
        # update 인자값 설정
        record_to_update100 = ([cash_rate, cash_rate_amt, sell_plan_amt, buy_plan_amt, datetime.now(), asset_num, acct_no])
        # DB 연결된 커서의 쿼리 수행
        cur400.execute(update_query100, record_to_update100)
        conn.commit()
        cur400.close()

    # 추적신호 조회(코스닥) : 추적신호코드별 시장 흐름 설정, 코스피, 코스닥 조합 시장승률 설정
    cur500 = conn.cursor()
    cur500.execute("select trail_signal_code, asset_num from (select row_number() over(order by trail_day desc, trail_time desc) as num, A.trail_signal_code, B.asset_num from trail_signal_recent A, \"stockFundMng_stock_fund_mng\" B where cast(A.acct_no as INTEGER) = B.acct_no and code = '1001' and A.acct_no = '" + str(acct_no) + "') T where num = 1")
    result_one200 = cur500.fetchall()
    cur500.close()

    for i in result_one200:

        trail_signal_result2 = i[0]
        asset_num = i[1]
        print("코스닥 추적 신호 : " + str(trail_signal_result2))   
        print("자산번호 : " + str(asset_num))
            
        if trail_signal_result2 == '03':  # 저항가 돌파
            kosdak_ratio = "H"  # 시장 상승
        elif trail_signal_result2 == '04':  # 지지가 이탈
            kosdak_ratio = "D"  # 시장 하락
        elif trail_signal_result2 == '05':  # 추세상단가 돌파
            kosdak_ratio = "H"  # 시장 상승
        elif trail_signal_result2 == '06':  # 추세하단가 이탈
            kosdak_ratio = "D"  # 시장 하락
        elif trail_signal_result2 == '01':  # 돌파가 돌파
            kosdak_ratio = "H"  # 시장 상승
        elif trail_signal_result2 == '02':  # 이탈가 이탈
            kosdak_ratio = "D"  # 시장 하락

        # 시장 승률정보 저장처리
        if kospi_ratio == "H" and kosdak_ratio == "H":    # 코스피 강세 & 코스닥 강세
            market_ratio = 90
        elif kospi_ratio == "H" and kosdak_ratio == "D":  # 코스피 강세 & 코스닥 약세
            market_ratio = 70
        elif kospi_ratio == "D" and kosdak_ratio == "H":  # 코스피 약세 & 코스닥 강세
            market_ratio = 50
        elif kospi_ratio == "D" and kosdak_ratio == "D":  # 코스피 약세 & 코스닥 약세
            market_ratio = 30

        print("시장 승률 : " + str(market_ratio))

        # 자산정보 변경
        cur600 = conn.cursor()
        update_query200 = "update \"stockFundMng_stock_fund_mng\" set market_ratio = %s, last_chg_date = %s where asset_num = %s and acct_no = %s"
        # update 인자값 설정
        record_to_update200 = ([market_ratio, datetime.now(), asset_num, acct_no])
        # DB 연결된 커서의 쿼리 수행
        cur600.execute(update_query200, record_to_update200)
        conn.commit()
        cur600.close()
        
        # 자산번호와 시장승률이 다를 경우 자산정보 변경 처리
        if str(asset_num)[0:2] != str(market_ratio):
            
            # 자산번호 생성
            new_asset_num = str(market_ratio) + today
            print("신규 자산번호 : " + new_asset_num)

            if int(new_asset_num) != asset_num:

                # 자산정보 생성
                cur601 = conn.cursor()
                insert_query001 = "insert into stockFundMngHist(asset_num, acct_no, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt, last_chg_date, market_ratio) select asset_num, acct_no, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt, now(), market_ratio from \"stockFundMng_stock_fund_mng\" where acct_no = %s and asset_num = %s"
                # insert 인자값 설정
                record_to_insert001 = ([acct_no, asset_num])
                # DB 연결된 커서의 쿼리 수행
                cur601.execute(insert_query001, record_to_insert001)
                conn.commit()
                cur601.close()

                # 자산정보 이력 생성
                cur602 = conn.cursor()
                insert_query002 = "insert into \"stockFundMng_stock_fund_mng\"(asset_num, acct_no, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt, last_chg_date, market_ratio) select %s, acct_no, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt, now(), market_ratio from \"stockFundMng_stock_fund_mng\" where acct_no = %s and asset_num = %s"
                # insert 인자값 설정
                record_to_insert002 = ([int(new_asset_num), acct_no, asset_num])
                # DB 연결된 커서의 쿼리 수행
                cur602.execute(insert_query002, record_to_insert002)
                conn.commit()
                cur602.close()

                # 자산정보 삭제
                cur603 = conn.cursor()
                delete_query001 = "delete from \"stockFundMng_stock_fund_mng\" where acct_no = %s and asset_num = %s"
                # insert 인자값 설정
                record_to_delete001 = ([acct_no, asset_num])
                # DB 연결된 커서의 쿼리 수행
                cur603.execute(delete_query001, record_to_delete001)
                conn.commit()
                cur603.close()

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
                token = "6008784254:AAEcJaePafd6Bh0riGL57OjhZ_ZoFxe6Fw0"  

            ac = account(nick)
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']

            # 자산정보 및 시장레벨정보 처리
            fund_marketLevel_proc(access_token, app_key, app_secret, acct_no)

            # 관심정보 조회
            cur03 = conn.cursor()
            cur03.execute("select code, name, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, ROUND(B.buy_avail_cash*C.risk_rate*0.01/C.item_number, 0) as item_loss_sum from \"interestItem_interest_item\" A, (select row_number() over (order by id desc) as ROWNUM, prvs_rcdl_excc_amt, acct_no, case when cash_rate > COALESCE(market_ratio, 0) then ROUND(cash_rate*prvs_rcdl_excc_amt*0.01, 0) else ROUND(COALESCE(market_ratio, 0)*prvs_rcdl_excc_amt*0.01, 0) end as buy_avail_cash from \"stockFundMng_stock_fund_mng\" where acct_no = '"+str(acct_no)+"') B, (select acct_no, risk_rate, item_number from \"stockMarketMng_stock_market_mng\" where acct_no = '"+str(acct_no)+"' and aply_end_dt = '99991231') C where A.acct_no = B.acct_no and A.acct_no = C.acct_no and A.acct_no = '"+str(acct_no)+"' and B.rownum = 1")
            result_three = cur03.fetchall()
            cur03.close()

            # 관심종목 이탈가, 돌파가, 지지가, 저항가, 추세하단가, 추세상단가를 각각 실시간 종목시세의 최고가와 최저가 비교
            for i in result_three:
                print("종목명 : " + i[1])

                trail_signal_code = ""
                trail_signal_name = ""
                vol_appear = 0

                if len(i[0]) == 6:

                    a = inquire_price(access_token, app_key, app_secret, i[0])

                    if time > '0900' and time < '0910':
                        if round(float(a['prdy_vrss_vol_rate'])) > 10:
                            vol_appear = 1
                    if time > '0910' and time < '0920':
                        if round(float(a['prdy_vrss_vol_rate'])) > 20:
                            vol_appear = 1
                    if time > '0920' and time < '0930':
                        if round(float(a['prdy_vrss_vol_rate'])) > 30:
                            vol_appear = 1
                    if time > '0930' and time < '1000':
                        if round(float(a['prdy_vrss_vol_rate'])) > 50:
                            vol_appear = 1
                    if time > '1000' and time < '1430':
                        if round(float(a['prdy_vrss_vol_rate'])) > 100:
                            vol_appear = 1
                    vol_appear = 1 # 전일대비거래량비율 체크 제외(20250406)        
                    print("vol_appear : " + str(vol_appear))

                    n_buy_amount = 0
                    n_buy_sum = 0

                    if time > '1430' and time < '1520':     # 장종료 1시간전 현재가 기준 돌파, 이탈 설정

                        if int(a['stck_prpr']) > i[2]:
                            print("돌파가 돌파")
                            trail_signal_code = "01"
                            trail_signal_name = format(int(i[2]), ',d') + "원 {돌파가 돌파}"
                            # 손절금액
                            loss_price = int(i[3])
                            # 종목손실금액
                            item_loss_sum = i[8]
                            print("종목손실금액 : " + format(int(item_loss_sum), ',d'))
                            # 매수량
                            # n_buy_amount = item_loss_sum / (int(a['stck_prpr']) - loss_price)
                            n_buy_amount = round(2000000 / int(a['stck_prpr']))
                            print("매수량 : " + format(int(round(n_buy_amount)), ',d'))
                            # 매수금액
                            n_buy_sum = int(a['stck_prpr']) * round(n_buy_amount)
                            print("매수금액 : " + format(int(n_buy_sum), ',d'))
                        if int(a['stck_prpr']) < i[3]:
                            print("이탈가 이탈")
                            trail_signal_code = "02"
                            trail_signal_name = format(int(i[3]), ',d') + "원 {이탈가 이탈}"
                        if int(a['stck_prpr']) > i[4]:
                            print("저항가 돌파")
                            trail_signal_code = "03"
                            trail_signal_name = format(int(i[4]), ',d') + "원 {저항가 돌파}"
                            # 손절금액
                            loss_price = int(i[3])
                            # 종목손실금액
                            item_loss_sum = i[8]
                            print("종목손실금액 : " + format(int(item_loss_sum), ',d'))
                            # 매수량
                            # n_buy_amount = item_loss_sum / (int(a['stck_prpr']) - loss_price)
                            n_buy_amount = round(2000000 / int(a['stck_prpr']))
                            print("매수량 : " + format(int(round(n_buy_amount)), ',d'))
                            # 매수금액
                            n_buy_sum = int(a['stck_prpr']) * round(n_buy_amount)                    
                        if int(a['stck_prpr']) < i[5]:
                            print("지지가 이탈")
                            trail_signal_code = "04"
                            trail_signal_name = format(int(i[5]), ',d') + "원 {지지가 이탈}"
                        if int(a['stck_prpr']) > i[6]:
                            print("추세상단가 돌파")
                            trail_signal_code = "05"
                            trail_signal_name = format(int(i[6]), ',d') + "원 {추세상단가 돌파}"
                            # 손절금액
                            loss_price = int(i[3])
                            # 종목손실금액
                            item_loss_sum = i[8]
                            print("종목손실금액 : " + format(int(item_loss_sum), ',d'))
                            # 매수량
                            # n_buy_amount = item_loss_sum / (int(a['stck_prpr']) - loss_price)
                            n_buy_amount = round(2000000 / int(a['stck_prpr']))
                            print("매수량 : " + format(int(round(n_buy_amount)), ',d'))
                            # 매수금액
                            n_buy_sum = int(a['stck_prpr']) * round(n_buy_amount)
                        if int(a['stck_prpr']) < i[7]:
                            print("추세하단가 이탈")
                            trail_signal_code = "06"
                            trail_signal_name = format(int(i[7]), ',d') + "원 {추세하단가 이탈}"

                    else:   # 돌파시 최고가, 이탈시 최저가 기준 설정

                        if int(a['stck_hgpr']) > i[2]:
                            print("돌파가 돌파")
                            trail_signal_code = "01"
                            trail_signal_name = format(int(i[2]), ',d') + "원 {돌파가 돌파}"
                            # 손절금액
                            loss_price = int(i[3])
                            # 종목손실금액
                            item_loss_sum = i[8]
                            print("종목손실금액 : " + format(int(item_loss_sum), ',d'))
                            # 매수량
                            # n_buy_amount = item_loss_sum / (int(a['stck_prpr']) - loss_price)
                            n_buy_amount = round(2000000 / int(a['stck_prpr']))
                            print("매수량 : " + format(int(round(n_buy_amount)), ',d'))
                            # 매수금액
                            n_buy_sum = int(a['stck_prpr']) * round(n_buy_amount)                    
                        if int(a['stck_lwpr']) < i[3]:
                            print("이탈가 이탈")
                            trail_signal_code = "02"
                            trail_signal_name = format(int(i[3]), ',d') + "원 {이탈가 이탈}"
                        if int(a['stck_hgpr']) > i[4]:
                            print("저항가 돌파")
                            trail_signal_code = "03"
                            trail_signal_name = format(int(i[4]), ',d') + "원 {저항가 돌파}"
                            # 손절금액
                            loss_price = int(i[3])
                            # 종목손실금액
                            item_loss_sum = i[8]
                            print("종목손실금액 : " + format(int(item_loss_sum), ',d'))
                            # 매수량
                            # n_buy_amount = item_loss_sum / (int(a['stck_prpr']) - loss_price)
                            n_buy_amount = round(2000000 / int(a['stck_prpr']))
                            print("매수량 : " + format(int(round(n_buy_amount)), ',d'))
                            # 매수금액
                            n_buy_sum = int(a['stck_prpr']) * round(n_buy_amount)                    
                        if int(a['stck_lwpr']) < i[5]:
                            print("지지가 이탈")
                            trail_signal_code = "04"
                            trail_signal_name = format(int(i[5]), ',d') + "원 {지지가 이탈}"
                        if int(a['stck_hgpr']) > i[6]:
                            print("추세상단가 돌파")
                            trail_signal_code = "05"
                            trail_signal_name = format(int(i[6]), ',d') + "원 {추세상단가 돌파}"
                            # 손절금액
                            loss_price = int(i[3])
                            # 종목손실금액
                            item_loss_sum = i[8]
                            print("종목손실금액 : " + format(int(item_loss_sum), ',d'))
                            # 매수량
                            # n_buy_amount = item_loss_sum / (int(a['stck_prpr']) - loss_price)
                            n_buy_amount = round(2000000 / int(a['stck_prpr']))
                            print("매수량 : " + format(int(round(n_buy_amount)), ',d'))
                            # 매수금액
                            n_buy_sum = int(a['stck_prpr']) * round(n_buy_amount)                    
                        if int(a['stck_lwpr']) < i[7]:
                            print("추세하단가 이탈")
                            trail_signal_code = "06"
                            trail_signal_name = format(int(i[7]), ',d') + "원 {추세하단가 이탈}"

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

                                    if vol_appear > 0:
                                        # 자산관리정보 조회
                                        cur041 = conn.cursor()
                                        cur041.execute("select case when market_ratio = 0 then 100 - cash_rate else market_ratio end as market_ratio from (select row_number() over (order by id desc) as ROWNUM, cash_rate, COALESCE(market_ratio, 0) as market_ratio from  \"stockFundMng_stock_fund_mng\" where acct_no = '" + str(acct_no) + "') A where A.ROWNUM = 1")
                                        result_fourone = cur041.fetchall()
                                        cur041.close()

                                        for k in result_fourone:
                                            print("시장승률 : "+str(k[0]))
                                            if k[0] >= 50:   # 시장 상승인 경우
                                                if n_buy_amount > 0:
                                                    buy_command = f"/InterestBuy_{i[0]}_{a['stck_prpr']}"
                                                    # telegram_text = "[시장상승-거래증가]" + i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 매수량 : " +  format(int(round(n_buy_amount)), ',d') + "주, 매수금액 : " + format(int(n_buy_sum), ',d') + "원, 손절가 : " + format(int(loss_price), ',d') + "원, 손절금액 : " + format(int(item_loss_sum), ',d') + "원"
                                                    telegram_text = (f"[시장상승]{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매수량 : {format(int(round(n_buy_amount)), ',d')}주, 매수금액 : {format(int(n_buy_sum), ',d')}원, 손절가 : {format(int(loss_price), ',d')}, 손절금액 : {format(int(item_loss_sum), ',d')}원 => {buy_command}")
                                                else:
                                                    # telegram_text = "[시장상승-거래증가]" + i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                                                    telegram_text = "[시장상승]" + i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                                                # 텔레그램 메시지 전송
                                                asyncio.run(main(telegram_text))

                                                # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                                                cur20 = conn.cursor()
                                                insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, buy_plan_qty = %s, buy_plan_amt = %s, cdate = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                                                # insert 인자값 설정
                                                record_to_insert0 = ([time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], int(round(n_buy_amount)), int(n_buy_sum), datetime.now(), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                                # DB 연결된 커서의 쿼리 수행
                                                cur20.execute(insert_query0, record_to_insert0)

                                                # 추적신호이력 정보 생성
                                                cur2 = conn.cursor()
                                                insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                                # insert 인자값 설정
                                                record_to_insert = ([acct_no, today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                                # DB 연결된 커서의 쿼리 수행
                                                cur2.execute(insert_query, record_to_insert)
                                                conn.commit()
                                                cur20.close()
                                                cur2.close()
                                            else:
                                                if n_buy_amount > 0:
                                                    buy_command = f"/InterestBuy_{i[0]}_{a['stck_prpr']}"
                                                    # telegram_text = "[시장하락-거래증가]" + i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 매수량 : " +  format(int(round(n_buy_amount)), ',d') + "주, 매수금액 : " + format(int(n_buy_sum), ',d') + "원, 손절가 : " + format(int(loss_price), ',d') + "원, 손절금액 : " + format(int(item_loss_sum), ',d') + "원"
                                                    telegram_text = (f"[시장하락]{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매수량 : {format(int(round(n_buy_amount)), ',d')}주, 매수금액 : {format(int(n_buy_sum), ',d')}원, 손절가 : {format(int(loss_price), ',d')}, 손절금액 : {format(int(item_loss_sum), ',d')}원 => {buy_command}")
                                                else:
                                                    # telegram_text = "[시장하락-거래증가]" + i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                                                    telegram_text = "[시장하락]" + i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                                                # 텔레그램 메시지 전송
                                                asyncio.run(main(telegram_text))

                                                # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                                                cur20 = conn.cursor()
                                                insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, buy_plan_qty = %s, buy_plan_amt = %s, cdate = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                                                # insert 인자값 설정
                                                record_to_insert0 = ([time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], int(round(n_buy_amount)), int(n_buy_sum), datetime.now(), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                                # DB 연결된 커서의 쿼리 수행
                                                cur20.execute(insert_query0, record_to_insert0)

                                                # 추적신호이력 정보 생성
                                                cur2 = conn.cursor()
                                                insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                                # insert 인자값 설정
                                                record_to_insert = ([acct_no, today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                                # DB 연결된 커서의 쿼리 수행
                                                cur2.execute(insert_query, record_to_insert)
                                                conn.commit()
                                                cur20.close()
                                                cur2.close()
                                    else:
                                        if time > '1430' and time < '1520':
                                            if n_buy_amount > 0:
                                                buy_command = f"/InterestBuy_{i[0]}_{a['stck_prpr']}"
                                                # telegram_text = "[장마감전]" + i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 매수량 : " +  format(int(round(n_buy_amount)), ',d') + "주, 매수금액 : " + format(int(n_buy_sum), ',d') + "원, 손절가 : " + format(int(loss_price), ',d') + "원, 손절금액 : " + format(int(item_loss_sum), ',d') + "원"
                                                telegram_text = (f"[장마감전]{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매수량 : {format(int(round(n_buy_amount)), ',d')}주, 매수금액 : {format(int(n_buy_sum), ',d')}원, 손절가 : {format(int(loss_price), ',d')}, 손절금액 : {format(int(item_loss_sum), ',d')}원 => {buy_command}")
                                            else:
                                                telegram_text = "[장마감전]" + i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                                            # 텔레그램 메시지 전송
                                            asyncio.run(main(telegram_text))

                                            # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                                            cur20 = conn.cursor()
                                            insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, buy_plan_qty = %s, buy_plan_amt = %s, cdate = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                                            # insert 인자값 설정
                                            record_to_insert0 = ([time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], int(round(n_buy_amount)), int(n_buy_sum), datetime.now(), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                            # DB 연결된 커서의 쿼리 수행
                                            cur20.execute(insert_query0, record_to_insert0)

                                            # 추적신호이력 정보 생성
                                            cur2 = conn.cursor()
                                            insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                            # insert 인자값 설정
                                            record_to_insert = ([acct_no, today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                            # DB 연결된 커서의 쿼리 수행
                                            cur2.execute(insert_query, record_to_insert)
                                            conn.commit()
                                            cur20.close()
                                            cur2.close()

                    else:
                        print("trail_signal_code2 : " + trail_signal_code)

                        if trail_signal_code != "":

                            if vol_appear > 0:
                                # 자산관리정보 조회
                                cur041 = conn.cursor()
                                cur041.execute("select case when market_ratio = 0 then 100 - cash_rate else market_ratio end as market_ratio from (select row_number() over (order by id desc) as ROWNUM, cash_rate, COALESCE(market_ratio, 0) as market_ratio from  \"stockFundMng_stock_fund_mng\" where acct_no = '" + str(acct_no) + "') A where A.ROWNUM = 1")
                                result_fourone = cur041.fetchall()
                                cur041.close()

                                for k in result_fourone:
                                    print("시장승률 : "+str(k[0]))
                                    if k[0] >= 50:  # 시장 상승인 경우
                                        if n_buy_amount > 0:
                                            buy_command = f"/InterestBuy_{i[0]}_{a['stck_prpr']}"
                                            # telegram_text = "[시장상승-거래증가]" + i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 매수량 : " +  format(int(round(n_buy_amount)), ',d') + "주, 매수금액 : " + format(int(n_buy_sum), ',d') + "원, 손절가 : " + format(int(loss_price), ',d') + "원, 손절금액 : " + format(int(item_loss_sum), ',d') + "원"
                                            telegram_text = (f"[시장상승]{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매수량 : {format(int(round(n_buy_amount)), ',d')}주, 매수금액 : {format(int(n_buy_sum), ',d')}원, 손절가 : {format(int(loss_price), ',d')}, 손절금액 : {format(int(item_loss_sum), ',d')}원 => {buy_command}")
                                        else:
                                            # telegram_text = "[시장상승-거래증가]" + i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                                            telegram_text = "[시장상승]" + i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                                        # 텔레그램 메시지 전송
                                        asyncio.run(main(telegram_text))

                                        # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                                        cur20 = conn.cursor()
                                        insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, buy_plan_qty = %s, buy_plan_amt = %s, cdate = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                                        # insert 인자값 설정
                                        record_to_insert0 = ([time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], int(round(n_buy_amount)), int(n_buy_sum), datetime.now(), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                        # DB 연결된 커서의 쿼리 수행
                                        cur20.execute(insert_query0, record_to_insert0)

                                        # 추적신호이력 정보 생성
                                        cur2 = conn.cursor()
                                        insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                        # insert 인자값 설정
                                        record_to_insert = ([acct_no, today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                        # DB 연결된 커서의 쿼리 수행
                                        cur2.execute(insert_query, record_to_insert)
                                        conn.commit()
                                        cur20.close()
                                        cur2.close()
                                    else:
                                        if n_buy_amount > 0:
                                            buy_command = f"/InterestBuy_{i[0]}_{a['stck_prpr']}"
                                            # telegram_text = "[시장하락-거래증가]" + i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 매수량 : " +  format(int(round(n_buy_amount)), ',d') + "주, 매수금액 : " + format(int(n_buy_sum), ',d') + "원, 손절가 : " + format(int(loss_price), ',d') + "원, 손절금액 : " + format(int(item_loss_sum), ',d') + "원"
                                            telegram_text = (f"[시장하락]{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매수량 : {format(int(round(n_buy_amount)), ',d')}주, 매수금액 : {format(int(n_buy_sum), ',d')}원, 손절가 : {format(int(loss_price), ',d')}, 손절금액 : {format(int(item_loss_sum), ',d')}원 => {buy_command}")
                                        else:
                                            # telegram_text = "[시장하락-거래증가]" + i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                                            telegram_text = "[시장하락]" + i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                                        # 텔레그램 메시지 전송
                                        asyncio.run(main(telegram_text))

                                        # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                                        cur20 = conn.cursor()
                                        insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, buy_plan_qty = %s, buy_plan_amt = %s, cdate = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                                        # insert 인자값 설정
                                        record_to_insert0 = ([time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], int(round(n_buy_amount)), int(n_buy_sum), datetime.now(), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                        # DB 연결된 커서의 쿼리 수행
                                        cur20.execute(insert_query0, record_to_insert0)

                                        # 추적신호이력 정보 생성
                                        cur2 = conn.cursor()
                                        insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                        # insert 인자값 설정
                                        record_to_insert = ([acct_no, today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                        # DB 연결된 커서의 쿼리 수행
                                        cur2.execute(insert_query, record_to_insert)
                                        conn.commit()
                                        cur20.close()
                                        cur2.close()
                            else:
                                if time > '1430' and time < '1520':
                                    if n_buy_amount > 0:
                                        buy_command = f"/InterestBuy_{i[0]}_{a['stck_prpr']}"
                                        # telegram_text = "[장마감전]" + i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 매수량 : " +  format(int(round(n_buy_amount)), ',d') + "주, 매수금액 : " + format(int(n_buy_sum), ',d') + "원, 손절가 : " + format(int(loss_price), ',d') + "원, 손절금액 : " + format(int(item_loss_sum), ',d') + "원"
                                        telegram_text = (f"[장마감전]{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매수량 : {format(int(round(n_buy_amount)), ',d')}주, 매수금액 : {format(int(n_buy_sum), ',d')}원, 손절가 : {format(int(loss_price), ',d')}, 손절금액 : {format(int(item_loss_sum), ',d')}원 => {buy_command}")
                                    else:
                                        telegram_text = "[장마감전]" + i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                                    # 텔레그램 메시지 전송
                                    asyncio.run(main(telegram_text))

                                    # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                                    cur20 = conn.cursor()
                                    insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, buy_plan_qty = %s, buy_plan_amt = %s, cdate = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                                    # insert 인자값 설정
                                    record_to_insert0 = ([time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], int(round(n_buy_amount)), int(n_buy_sum), datetime.now(), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                    # DB 연결된 커서의 쿼리 수행
                                    cur20.execute(insert_query0, record_to_insert0)

                                    # 추적신호이력 정보 생성
                                    cur2 = conn.cursor()
                                    insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                    # insert 인자값 설정
                                    record_to_insert = ([acct_no, today, time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                                    # DB 연결된 커서의 쿼리 수행
                                    cur2.execute(insert_query, record_to_insert)
                                    conn.commit()
                                    cur20.close()
                                    cur2.close()

                elif len(i[0]) == 4:
                    
                    b = inquire_daily_indexchartprice(access_token, app_key, app_secret, i[0], today)
                    # print("현재포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_prpr']), ',f'))  # 현재포인트
                    # print("최고포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_hgpr']), ',f'))  # 최고포인트
                    # print("최저포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_lwpr']), ',f'))  # 최저포인트
                    # print("누적거래량 : " + format(int(b['acml_vol']), ',d'))  # 누적거래량

                    # 시장레벨정보 조회
                    cur05 = conn.cursor()
                    cur05.execute("select asset_risk_num, market_level_num from \"stockMarketMng_stock_market_mng\" where acct_no = '" + str(acct_no) + "' and aply_end_dt = '99991231'")
                    result_five = cur05.fetchall()
                    cur05.close()

                    market_level_num = ""
                    risk_rate = 0
                    item_number = 0

                    if math.ceil(float(b['bstp_nmix_prpr'])) > i[2]:
                        print("돌파포인트 돌파")
                        trail_signal_code = "01"
                        trail_signal_name = format(int(i[2]), ',d') + " {돌파포인트 돌파}"

                        if i[0] == "0001":
                            if len(result_five) > 0:
                                for k in result_five:
                                    # 기존 시장레벨번호가 단기 추세 전환 후, 기술적 반등보다 쟉은 경우(market_level_num='1')
                                    if int(k[1]) < 2:
                                        market_level_num = "2"  # 단기 추세 전환 후, 기술적 반등
                                        risk_rate = 3
                                        item_number = 4

                    if math.ceil(float(b['bstp_nmix_prpr'])) < i[3]:
                        print("이탈포인트 이탈")
                        trail_signal_code = "02"
                        trail_signal_name = format(int(i[3]), ',d') + " {이탈포인트 이탈}"

                        if i[0] == "0001":
                            if len(result_five) > 0:
                                for k in result_five:
                                    # 기존 시장레벨번호가 패턴내에서 기술적 반등보다 작은 경우(market_level_num='1', market_level_num='2')
                                    if int(k[1]) < 3:
                                        market_level_num = "1"  # 하락 지속 후, 기술적 반등
                                        risk_rate = 2
                                        item_number = 2


                    if math.ceil(float(b['bstp_nmix_prpr'])) > i[4]:
                        print("저항포인트 돌파")
                        trail_signal_code = "03"
                        trail_signal_name = format(int(i[4]), ',d') + " {저항포인트 돌파}"

                        if i[0] == "0001":
                            if len(result_five) > 0:
                                for k in result_five:
                                    # 기존 시장레벨번호가 일봉상 추세 전환 후, 눌림구간에서 반등보다 작은 경우(market_level_num='1', market_level_num='2', market_level_num='3')                            if int(k[1]) < 4:
                                        market_level_num = "4"  # 일봉상 추세 전환 후, 눌림구간에서 반등
                                        risk_rate = 5.5
                                        item_number = 8

                    if math.ceil(float(b['bstp_nmix_prpr'])) < i[5]:
                        print("지지포인트 이탈")
                        trail_signal_code = "04"
                        trail_signal_name = format(int(i[5]), ',d') + " {지지포인트 이탈}"

                        if i[0] == "0001":
                            for k in result_five:
                                if len(result_five) > 0:
                                    # 기존 시장레벨번호가 일봉상 추세 전환 후, 눌림구간에서 반등보다 작은 경우(market_level_num='1', market_level_num='2', market_level_num='3', market_level_num='4')
                                    if int(k[1]) < 5:
                                        market_level_num = "1"  # 하락 지속 후, 기술적 반등
                                        risk_rate = 2
                                        item_number = 2

                    if math.ceil(float(b['bstp_nmix_prpr'])) > i[6]:
                        print("추세상단포인트 돌파")
                        trail_signal_code = "05"
                        trail_signal_name = format(int(i[6]), ',d') + " {추세상단포인트 돌파}"

                        if i[0] == "0001":
                            if len(result_five) > 0:
                                market_level_num = "5"  # 상승 지속 후, 패턴내에서 기술적 반등
                                risk_rate = 4
                                item_number = 6

                    if math.ceil(float(b['bstp_nmix_prpr'])) < i[7]:
                        print("추세하단포인트 이탈")
                        trail_signal_code = "06"
                        trail_signal_name = format(int(i[7]), ',d') + " {추세하단포인트 이탈}"

                        if i[0] == "0001":
                            if len(result_five) > 0:
                                market_level_num = "1"  # 하락 지속 후, 기술적 반등
                                risk_rate = 2
                                item_number = 2

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
                                    telegram_text = i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 최고포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_hgpr']), ',f') + ", 최저포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_lwpr']), ',f') + ", 현재포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_prpr']), ',f') + ", 거래량 : " + format(int(b['acml_vol']), ',d') + "주" 
                                    # 텔레그램 메시지 전송
                                    asyncio.run(main(telegram_text))

                                    if i[0] == "0001":
                                        if len(result_five) > 0:
                                            for k in result_five:
                                                # 시장레벨번호가 존재하는 경우
                                                if len(market_level_num) > 0:
                                                    # 시장레벨정보에 시장레벨번호가 미존재한 경우
                                                    if k[0] != int(market_level_num + today):
                                                        # 시장레벨정보 변경
                                                        cur200 = conn.cursor()
                                                        update_query200 = "update \"stockMarketMng_stock_market_mng\" set aply_end_dt = TO_CHAR(now(), 'YYYYMMDD') where acct_no = %s and aply_end_dt = '99991231'"
                                                        # update 인자값 설정
                                                        record_to_update200 = ([acct_no])
                                                        # DB 연결된 커서의 쿼리 수행
                                                        cur200.execute(update_query200, record_to_update200)

                                                        # 시장레벨정보 생성
                                                        cur201 = conn.cursor()
                                                        insert_query201 = "insert into \"stockMarketMng_stock_market_mng\"(asset_risk_num, acct_no, market_level_num, total_asset, risk_rate, risk_sum, item_number, aply_start_dt, aply_end_dt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                                        # update 인자값 설정
                                                        record_to_insert201 = ([int(market_level_num + today), acct_no, market_level_num, 0, risk_rate, 0, item_number, today, '99991231'])
                                                        # DB 연결된 커서의 쿼리 수행
                                                        cur201.execute(insert_query201, record_to_insert201)

                                    # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                                    cur20 = conn.cursor()
                                    insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, cdate = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, cdate) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                                    # insert 인자값 설정
                                    record_to_insert0 = ([time, i[1], math.ceil(float(b['bstp_nmix_prpr'])), math.ceil(float(b['bstp_nmix_hgpr'])), math.floor(float(b['bstp_nmix_lwpr'])), int(b['acml_vol']), datetime.now(), str(acct_no), today, trail_signal_code, i[0], str(acct_no), today, time, trail_signal_code, trail_signal_name, i[0], i[1], math.ceil(float(b['bstp_nmix_prpr'])), math.ceil(float(b['bstp_nmix_hgpr'])), math.floor(float(b['bstp_nmix_lwpr'])), int(b['acml_vol']), datetime.now()])
                                    # DB 연결된 커서의 쿼리 수행
                                    cur20.execute(insert_query0, record_to_insert0)

                                    # 추적신호이력 정보 생성
                                    cur2 = conn.cursor()
                                    insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, cdate) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                    # insert 인자값 설정
                                    record_to_insert = ([acct_no, today, time, trail_signal_code, trail_signal_name, i[0], i[1], math.ceil(float(b['bstp_nmix_prpr'])), math.ceil(float(b['bstp_nmix_hgpr'])), math.floor(float(b['bstp_nmix_lwpr'])), int(b['acml_vol']), datetime.now()])
                                    # DB 연결된 커서의 쿼리 수행
                                    cur2.execute(insert_query, record_to_insert)
                                    conn.commit()
                                    cur20.close()
                                    cur2.close()

                                    # 코스피, 코스닥 추적신호정보 기준 자산관리정보 현금비중, 승률, 매수예정자금, 매도예정자금 변경 처리 호출
                                    fundTrail_proc()

                    else:
                        print("trail_signal_code2 : " + trail_signal_code)

                        if trail_signal_code != "":
                            telegram_text = i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 최고포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_hgpr']), ',f') + ", 최저포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_lwpr']), ',f') + ", 현재포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_prpr']), ',f') + ", 거래량 : " + format(int(b['acml_vol']), ',d') + "주"
                            # 텔레그램 메시지 전송
                            asyncio.run(main(telegram_text))

                            if i[0] == "0001":
                                if len(result_five) > 0:
                                    for k in result_five:
                                        # 시장레벨번호가 존재하는 경우
                                        if len(market_level_num) > 0:
                                            # 시장레벨정보에 시장레벨번호가 미존재한 경우
                                            if k[0] != int(market_level_num + today):
                                                # 시장레벨정보 변경
                                                cur200 = conn.cursor()
                                                update_query200 = "update \"stockMarketMng_stock_market_mng\" set aply_end_dt = TO_CHAR(now(), 'YYYYMMDD') where acct_no = %s and aply_end_dt = '99991231'"
                                                # update 인자값 설정
                                                record_to_update200 = ([acct_no])
                                                # DB 연결된 커서의 쿼리 수행
                                                cur200.execute(update_query200, record_to_update200)

                                                # 시장레벨정보 생성
                                                cur201 = conn.cursor()
                                                insert_query201 = "insert into \"stockMarketMng_stock_market_mng\"(asset_risk_num, acct_no, market_level_num, total_asset, risk_rate, risk_sum, item_number, aply_start_dt, aply_end_dt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                                # update 인자값 설정
                                                record_to_insert201 = (
                                                [int(market_level_num + today), acct_no, market_level_num, 0, risk_rate, 0, item_number, today, '99991231'])
                                                # DB 연결된 커서의 쿼리 수행
                                                cur201.execute(insert_query201, record_to_insert201)

                            # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                            cur20 = conn.cursor()
                            insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, cdate = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, cdate) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                            # insert 인자값 설정
                            record_to_insert0 = ([time, i[1], math.ceil(float(b['bstp_nmix_prpr'])), math.ceil(float(b['bstp_nmix_hgpr'])), math.floor(float(b['bstp_nmix_lwpr'])), int(b['acml_vol']), datetime.now(), str(acct_no), today, trail_signal_code, i[0], str(acct_no), today, time, trail_signal_code, trail_signal_name, i[0], i[1], math.ceil(float(b['bstp_nmix_prpr'])), math.ceil(float(b['bstp_nmix_hgpr'])), math.floor(float(b['bstp_nmix_lwpr'])), int(b['acml_vol']), datetime.now()])
                            # DB 연결된 커서의 쿼리 수행
                            cur20.execute(insert_query0, record_to_insert0)

                            # 추적신호이력 정보 생성
                            cur2 = conn.cursor()
                            insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, cdate) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            # insert 인자값 설정
                            record_to_insert = ([acct_no, today, time, trail_signal_code, trail_signal_name, i[0], i[1], math.ceil(float(b['bstp_nmix_prpr'])), math.ceil(float(b['bstp_nmix_hgpr'])), math.floor(float(b['bstp_nmix_lwpr'])), int(b['acml_vol']), datetime.now()])
                            # DB 연결된 커서의 쿼리 수행
                            cur2.execute(insert_query, record_to_insert)
                            conn.commit()
                            cur20.close()
                            cur2.close()

                            # 코스피, 코스닥 추적신호정보 기준 자산관리정보 현금비중, 승률, 매수예정자금, 매도예정자금 변경 처리 호출
                            fundTrail_proc()

        except Exception as e:
            print(f"[{nick}] Error interest item : {e}")      
    
    conn.close()

else:
    conn.close()
    print("Today is Holiday")