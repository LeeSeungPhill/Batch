import psycopg2 as db
from datetime import datetime
import requests
import json
import kis_api_resp as resp
import pandas as pd
import time

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
#conn_string = "dbname='my_develop' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")

def format_number(value):
    try:
        return f"{float(value):,.2f}" if isinstance(value, float) else f"{int(value):,}"
    except:
        return str(value)
    
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

    return pd.DataFrame(output)

# 주식현재가 시세
def inquire_price(access_token, app_key, app_secret, code):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "FHKST01010100"}
    params = {
                'FID_COND_MRKT_DIV_CODE': "J",  # J:KRX, NX:NXT, UN:통합
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

# 기간별매매손익현황 합산조회
def inquire_period_trade_profit_sum(access_token, app_key, app_secret, acct_no, strt_dt, end_dt):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8715R",
               "custtype": "P"}
    params = {
            'CANO': acct_no,            # 종합계좌번호
            'SORT_DVSN': "01",          # 00: 최근 순, 01: 과거 순, 02: 최근 순
            'ACNT_PRDT_CD': "01",
            'CBLC_DVSN': "00",
            'PDNO': "",                 # ""공란입력 시, 전체
            'INQR_STRT_DT': strt_dt,    # 조회시작일(8자리) 
            'INQR_END_DT': end_dt,      # 조회종료일(8자리)
            'CTX_AREA_NK100': "",
            'CTX_AREA_FK100': "" 
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-period-trade-profit"
    URL = f"{URL_BASE}/{PATH}"

    try:
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)

        # 응답에 output2이 있는지 확인
        body = ar.getBody()
        if hasattr(body, 'output2'):
            return body.output2['tot_rlzt_pfls']
        else:
            print("기간별매매손익현황 합산조회 응답이 없습니다.")
            return []  # 혹은 None

    except Exception as e:
        print("기간별매매손익현황 합산조회 중 오류 발생:", e)
        return []

# 매수 가능(현금) 조회
def inquire_psbl_order(access_token, app_key, app_secret, acct_no):
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8908R"}    # tr_id : TTTC8908R[실전투자], VTTC8908R[모의투자]
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "PDNO": "",                     # 종목번호(6자리)
               "ORD_UNPR": "0",                # 1주당 가격
               "ORD_DVSN": "02",               # 02 : 조건부지정가
               "CMA_EVLU_AMT_ICLD_YN": "Y",    # CMA평가금액포함여부
               "OVRS_ICLD_YN": "N"             # 해외포함여부
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-psbl-order"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)

    return ar.getBody().output['nrcvb_buy_amt']

# 일별 매매정보 처리
def trading_proc(access_token, app_key, app_secret, acct_no):
    # 계좌잔고 조회
    d = stock_balance(access_token, app_key, app_secret, acct_no, "all")

    cur1 = conn.cursor() 
    for i, name in enumerate(d.index):
        u_dnca_tot_amt = int(d['dnca_tot_amt'][i])  # 예수금총금액
        u_prvs_rcdl_excc_amt = int(d['prvs_rcdl_excc_amt'][i])  # 가수도 정산 금액
        u_thdt_buy_amt = int(d['thdt_buy_amt'][i])  # 금일 매수 금액
        u_thdt_sll_amt = int(d['thdt_sll_amt'][i])  # 금일 매도 금액
        u_thdt_tlex_amt = int(d['thdt_tlex_amt'][i])  # 금일 제비용 금액
        u_scts_evlu_amt = int(d['scts_evlu_amt'][i])  # 유저 평가 금액
        u_tot_evlu_amt = int(d['tot_evlu_amt'][i])  # 총평가금액
        u_nass_amt = int(d['nass_amt'][i])  # 순자산금액(세금비용 제외)
        u_pchs_amt_smtl_amt = int(d['pchs_amt_smtl_amt'][i])  # 매입금액 합계금액
        u_evlu_amt_smtl_amt = int(d['evlu_amt_smtl_amt'][i])  # 평가금액 합계금액
        u_evlu_pfls_smtl_amt = int(d['evlu_pfls_smtl_amt'][i])  # 평가손익 합계금액
        u_bfdy_tot_asst_evlu_amt = int(d['bfdy_tot_asst_evlu_amt'][i])  # 전일총자산 평가금액
        u_asst_icdc_amt = int(d['asst_icdc_amt'][i])  # 자산 증감액

    # 총실현손익
    result1 = inquire_period_trade_profit_sum(access_token, app_key, app_secret, acct_no, today, today)

    # 매수 가능(현금) 조회
    result2 = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
    
    insert_query1 = "with upsert as (update dly_acct_balance set dnca_tot_amt = %s, prvs_excc_amt = %s, td_buy_amt = %s, td_sell_amt = %s, td_tex_amt = %s, user_evlu_amt = %s, tot_evlu_amt = %s, nass_amt = %s, pchs_amt = %s, evlu_amt = %s, evlu_pfls_amt = %s, ytdt_tot_evlu_amt = %s, asst_icdc_amt = %s, total_profit_loss_amt = %s, buy_psbl_amt = %s, last_chg_date = %s where dt = %s and acct = %s returning * ) insert into dly_acct_balance(acct, dt, dnca_tot_amt, prvs_excc_amt, td_buy_amt, td_sell_amt, td_tex_amt, user_evlu_amt, tot_evlu_amt, nass_amt, pchs_amt, evlu_amt, evlu_pfls_amt, ytdt_tot_evlu_amt, asst_icdc_amt, total_profit_loss_amt, buy_psbl_amt, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
    # insert 인자값 설정
    record_to_insert1 = ([u_dnca_tot_amt, u_prvs_rcdl_excc_amt, u_thdt_buy_amt, u_thdt_sll_amt, u_thdt_tlex_amt, u_scts_evlu_amt, u_tot_evlu_amt, u_nass_amt, u_pchs_amt_smtl_amt, u_evlu_amt_smtl_amt, u_evlu_pfls_smtl_amt, u_bfdy_tot_asst_evlu_amt, u_asst_icdc_amt, int(result1), int(result2), datetime.now(), today, str(acct_no),
        str(acct_no), today, u_dnca_tot_amt, u_prvs_rcdl_excc_amt, u_thdt_buy_amt, u_thdt_sll_amt, u_thdt_tlex_amt, u_scts_evlu_amt, u_tot_evlu_amt, u_nass_amt, u_pchs_amt_smtl_amt, u_evlu_amt_smtl_amt, u_evlu_pfls_smtl_amt, u_bfdy_tot_asst_evlu_amt, u_asst_icdc_amt, int(result1), int(result2), datetime.now()])
    # DB 연결된 커서의 쿼리 수행
    cur1.execute(insert_query1, record_to_insert1)
    conn.commit()

    cur11 = conn.cursor()
    # 보유잔고 정보 proc_yn = 'N' 대상 삭제 처리
    delete_query1 = "delete from \"stockBalance_stock_balance\" where proc_yn = 'N' and acct_no = %s and TO_CHAR(last_chg_date, 'YYYYMMDD') < %s"
    # insert 인자값 설정
    record_to_delete1 = ([acct_no, today])
    # DB 연결된 커서의 쿼리 수행
    cur11.execute(delete_query1, record_to_delete1)
    conn.commit()

    # 계좌종목 조회
    e = stock_balance(access_token, app_key, app_secret, acct_no, "")
    
    cur2 = conn.cursor()
    for i, name in enumerate(e.index):
        e_code = e['pdno'][i]                           # 종목코드
        e_name = e['prdt_name'][i]                      # 종목명
        e_buy_qty = int(e['thdt_buyqty'][i])            # 금일매수수량
        e_sell_qty = int(e['thdt_sll_qty'][i])          # 금일매도수량
        e_purchase_price = e['pchs_avg_pric'][i]        # 매입단가
        e_purchase_qty = int(e['hldg_qty'][i])          # 보유수량
        e_purchase_amt = int(e['pchs_amt'][i])          # 매입금액
        e_current_price = int(e['prpr'][i])             # 현재가
        e_eval_amt = int(e['evlu_amt'][i])              # 평가금액
        e_earnings_rate = e['evlu_pfls_rt'][i]          # 수익율
        e_valuation_amt = int(e['evlu_pfls_amt'][i])    # 평가손익금액
        f = ""

        try:
            time.sleep(0.3)  # 초당 3건 이하로 제한
            f = inquire_price(access_token, app_key, app_secret, e_code)
            f_open_price = int(f['stck_oprc'])              # 시가
            f_high_price = int(f['stck_hgpr'])              # 최고가
            f_low_price = int(f['stck_lwpr'])               # 최저가
            f_volumn = int(f['acml_vol'])                   # 누적거래량
            f_volumn_rate = float(f['prdy_vrss_vol_rate'])  # 전일대비거래량비율
        except Exception as ex:
            print(f"현재가 시세 에러 : [{e_code}] {ex}")

        print(f"[{acct_no}-{e_name}] 보유가 : {format_number(e_purchase_price)}, 보유량 : {format_number(e_purchase_qty)}, 현재가 : {format_number(e_current_price)}, 평가손익 : {format_number(e_valuation_amt)}, 처리일시 : {datetime.now()}")

        insert_query2 = "with upsert as (update dly_stock_balance set buy_qty = %s, sell_qty = %s, purchase_price = %s, purchase_qty = %s, purchase_amt = %s, current_price = %s, open_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, last_chg_date = %s, sign_resist_price = (select COALESCE(sign_resist_price, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), sign_support_price = (select COALESCE(sign_support_price, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), end_target_price = (select COALESCE(end_target_price, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), end_loss_price = (select COALESCE(end_loss_price, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), trading_plan = (select COALESCE(trading_plan, '') from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), limit_price = (select COALESCE(limit_price, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), limit_amt = (select COALESCE(limit_amt, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), safe_margin_sum = (select COALESCE(safe_margin_sum, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"') where dt = %s and code = %s and acct = %s returning * ) insert into dly_stock_balance(acct, dt, name, code, buy_qty, sell_qty, purchase_price, purchase_qty, purchase_amt, current_price, open_price, high_price, low_price, volumn, volumn_rate, eval_sum, earnings_rate, valuation_sum, last_chg_date, sign_resist_price, sign_support_price, end_target_price, end_loss_price, trading_plan, limit_price, limit_amt, safe_margin_sum) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select COALESCE(sign_resist_price, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), (select COALESCE(sign_support_price, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), (select COALESCE(end_target_price, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), (select COALESCE(end_loss_price, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), (select COALESCE(trading_plan, '') from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), (select COALESCE(limit_price, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), (select COALESCE(limit_amt, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"'), (select COALESCE(safe_margin_sum, 0) from \"stockBalance_stock_balance\" where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and code = '"+e_code+"') where not exists(select * from upsert)"
        # insert 인자값 설정
        record_to_insert2 = ([e_buy_qty, e_sell_qty, round(float(e_purchase_price)), e_purchase_qty, e_purchase_amt, e_current_price, f_open_price, f_high_price, f_low_price, f_volumn, float(f_volumn_rate), e_eval_amt, float(e_earnings_rate), e_valuation_amt, datetime.now(), today, e_code[:6], str(acct_no),
            str(acct_no), today, e_name, e_code[:6], e_buy_qty, e_sell_qty, round(float(e_purchase_price)), e_purchase_qty, e_purchase_amt, e_current_price, f_open_price, f_high_price, f_low_price, f_volumn, float(f_volumn_rate), e_eval_amt, float(e_earnings_rate), e_valuation_amt, datetime.now()])
        # DB 연결된 커서의 쿼리 수행
        cur2.execute(insert_query2, record_to_insert2)
        conn.commit()

    # 관심정보 조회
    cur21 = conn.cursor()
    cur21.execute("select code, name, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price from \"interestItem_interest_item\" where acct_no = '"+str(acct_no)+"'")
    result = cur21.fetchall()
    cur21.close()
    cur22 = conn.cursor()

    # 관심종목 이탈가, 돌파가, 지지가, 저항가, 추세하단가, 추세상단가를 각각 현재시세의 최고가 최저가 비교
    for i in result:
        print(f"[{acct_no}-{i[1]}] 돌파가 : {format_number(i[2])}, 이탈가 : {format_number(i[3])}, 저항가 : {format_number(i[4])}, 지지가 : {format_number(i[5])}, 추세상단가 : {format_number(i[6])}, 추세하단가 : {format_number(i[7])}")
        code = i[0]                                         # 종목코드
        name = i[1]                                         # 종목명
        b = ""
        f = ""

        if len(i[0]) == 4:
            b = inquire_daily_indexchartprice(access_token, app_key, app_secret, i[0], today)
            open_price = int(float(b['bstp_nmix_oprc']))    # 시가포인트
            high_price = int(float(b['bstp_nmix_hgpr']))    # 최고포인트
            low_price = int(float(b['bstp_nmix_lwpr']))     # 최저포인트
            current_price = int(float(b['bstp_nmix_prpr'])) # 현재포인트
            volumn = int(b['acml_vol'])                     # 누적거래량
            volumn_rate = float(int(b['acml_vol']) / int(b['prdy_vol']) * 100)   # 전일대비거래량비율

        else:
            try:
                time.sleep(0.3)  # 초당 3건 이하로 제한
                f = inquire_price(access_token, app_key, app_secret, code)
                open_price = int(f['stck_oprc'])                # 시가
                high_price = int(f['stck_hgpr'])                # 최고가
                low_price = int(f['stck_lwpr'])                 # 최저가
                current_price = int(f['stck_prpr'])             # 종가
                volumn = int(f['acml_vol'])                     # 누적거래량
                volumn_rate = float(f['prdy_vrss_vol_rate'])    # 전일대비거래량비율  
            except Exception as ex:
                print(f"현재가 시세 에러 : [{code}] {ex}")          
        
        insert_query22 = "with upsert as (update dly_stock_interest set current_price = %s, open_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, through_price = %s, leave_price = %s, resist_price = %s, support_price = %s, trend_high_price = %s, trend_low_price = %s, last_chg_date = %s where dt = %s and code = %s and acct = %s returning * ) insert into dly_stock_interest(acct, dt, name, code, current_price, open_price, high_price, low_price, volumn, volumn_rate, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert)"
        # insert 인자값 설정
        record_to_insert22 = ([current_price, open_price, high_price, low_price, volumn, float(volumn_rate), int(i[2]), int(i[3]), int(i[4]), int(i[5]), int(i[6]), int(i[7]), datetime.now(), today, code, str(acct_no),
            str(acct_no), today, name, code, current_price, open_price, high_price, low_price, volumn, float(volumn_rate), int(i[2]), int(i[3]), int(i[4]), int(i[5]), int(i[6]), int(i[7]), datetime.now()])
        # DB 연결된 커서의 쿼리 수행
        cur22.execute(insert_query22, record_to_insert22)
        conn.commit()    

    cur1.close()
    cur11.close()
    cur2.close()
    cur22.close()

cur0 = conn.cursor()
cur0.execute("select name from stock_holiday where holiday = '"+today+"'")
result_one = cur0.fetchone()
cur0.close()

nickname_list = ['chichipa', 'phills2', 'phills75', 'yh480825', 'phills13', 'phills15', 'mamalong']

if result_one == None:

    for nick in nickname_list:
        try:
            ac = account(nick)
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']

            # 일별 매매정보 처리
            trading_proc(access_token, app_key, app_secret, acct_no)

        except Exception as ex:
            print('일별 매매정보 처리 에러 : ', ex)

    conn.close()

else:
    conn.close()    
    print("Today is Holiday")
