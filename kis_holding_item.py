import psycopg2 as db
from datetime import datetime, timedelta
import kis_api_resp as resp
import requests
import json
import telegram
import asyncio
import pandas as pd
from decimal import Decimal
import time

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# PostgreSQL 연결 설정
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

# 일별주문체결조회
def get_my_complete(access_token, app_key, app_secret, acct_no):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC0081R",
               "custtype": "P"}
    params = {
            'CANO': acct_no,                                    # 종합계좌번호 계좌번호 체계(8-2)의 앞 8자리
            'ACNT_PRDT_CD':"01",                                # 계좌상품코드 계좌번호 체계(8-2)의 뒤 2자리
            'SORT_DVSN': "01",                                  # 00: 최근 순, 01: 과거 순, 02: 최근 순
            'INQR_STRT_DT': datetime.now().strftime('%Y%m%d'),  # 조회시작일(8자리) 
            'INQR_END_DT': datetime.now().strftime('%Y%m%d'),   # 조회종료일(8자리)
            # 'INQR_STRT_DT': "20250522",  # 조회시작일(8자리) 
            # 'INQR_END_DT': "20250522",   # 조회종료일(8자리)
            'SLL_BUY_DVSN_CD': "00",                            # 매도매수구분코드 00 : 전체 / 01 : 매도 / 02 : 매수
            'PDNO': "",                                         # 종목번호(6자리) ""공란입력 시, 전체
            'ORD_GNO_BRNO': "",                                 # 주문채번지점번호 ""공란입력 시, 전체
            'ODNO': "",                                         # 주문번호 ""공란입력 시, 전체
            'CCLD_DVSN': "00",                                  # 체결구분 00 전체, 01 체결, 02 미체결
            'INQR_DVSN': "01",                                  # 조회구분 00 역순, 01 정순
            'INQR_DVSN_1': "",                                  # 조회구분1 없음: 전체, 1: ELW, 2: 프리보드
            'INQR_DVSN_3': "00",                                # 조회구분3 00 전체, 01 현금, 02 신용, 03 담보, 04 대주, 05 대여, 06 자기융자신규/상환, 07 유통융자신규/상환
            'EXCG_ID_DVSN_CD': "KRX",                           # 거래소ID구분코드 KRX : KRX, NXT : NXT
            'CTX_AREA_NK100': "",
            'CTX_AREA_FK100': "" 
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    URL = f"{URL_BASE}/{PATH}"

    try:
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)

        # 응답에 output1이 있는지 확인
        body = ar.getBody()
        return body.output1 if hasattr(body, 'output1') else []

    except Exception as e:
        print("일별주문체결조회 중 오류 발생:", e)
        return []

# 기간별손익일별합산조회
def inquire_period_profit_loss(access_token, app_key, app_secret, code, strt_dt, end_dt):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8708R",
               "custtype": "P"}
    params = {
            'CANO': acct_no,            # 종합계좌번호
            'SORT_DVSN': "01",          # 00: 최근 순, 01: 과거 순, 02: 최근 순
            'INQR_DVSN': "00",
            'ACNT_PRDT_CD':"01",
            'CBLC_DVSN': "00",
            'PDNO': code,               # ""공란입력 시, 전체
            'INQR_STRT_DT': strt_dt,    # 조회시작일(8자리) 
            'INQR_END_DT': end_dt,      # 조회종료일(8자리)
            'CTX_AREA_NK100': "",
            'CTX_AREA_FK100': "" 
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-period-profit"
    URL = f"{URL_BASE}/{PATH}"

    try:
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)

        # 응답에 output1이 있는지 확인
        body = ar.getBody()
        if hasattr(body, 'output1'):
            return body.output1
        else:
            print("기간별손익일별합산조회 응답이 없습니다.")
            return []  # 혹은 None

    except Exception as e:
        print("기간별손익일별합산조회 중 오류 발생:", e)
        return []

def fetch_candles_with_base(access_token, app_key, app_secret, code, base_dtm):
    """
    1분봉을 조회한 뒤 10분봉으로 리샘플링하여 base_dtm 포함 여부 확인.
    필요하면 과거 데이터를 추가 조회.
    최종 반환값은 원본 API 결과(dict 리스트).
    """

    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": app_key,
        "appSecret": app_secret,
        "tr_id": "FHKST03010200",
        "custtype": "P"
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
    URL = f"{URL_BASE}/{PATH}"

    def request_candles(start_time):
        """API 호출 → 원본 dict 리스트 반환"""
        params = {
            'FID_COND_MRKT_DIV_CODE': "J",
            'FID_INPUT_ISCD': code,
            'FID_INPUT_HOUR_1': start_time,  # 기준 시각
            'FID_PW_DATA_INCU_YN': 'N',
            'FID_ETC_CLS_CODE': ""
        }
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)
        return ar.getBody().output2  # 원본 dict 리스트

    def convert_to_df(candle_list):
        """dict 리스트 → DataFrame(1분봉)"""
        minute_list = []
        for item in candle_list:
            minute_list.append({
                'timestamp': pd.to_datetime(item['stck_cntg_hour'], format='%H%M%S'),
                'open': float(item['stck_oprc']),
                'high': float(item['stck_hgpr']),
                'low': float(item['stck_lwpr']),
                'close': float(item['stck_prpr']),
                'volume': float(item['cntg_vol'])
            })
        return pd.DataFrame(minute_list).sort_values('timestamp').reset_index(drop=True)

    def resample_to_10min(df):
        """1분봉 → 10분봉 변환"""
        df = df.set_index('timestamp')
        df_10 = df.resample('10T').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna().reset_index()
        return df_10

    # 1차 조회
    cur_time = datetime.now().strftime("%H%M%S")
    candle_list = request_candles(cur_time)
    df_1m = convert_to_df(candle_list)
    df_10m = resample_to_10min(df_1m)

    # base_dtm이 속한 봉 시작시간
    base_candle_start = base_dtm.replace(minute=(base_dtm.minute // 10) * 10, second=0)

    included = any(df_10m['timestamp'] == base_candle_start)

    # 포함 안 되면 과거 조회 반복
    while not included:
        oldest_time = df_1m['timestamp'].min() - timedelta(minutes=1)
        start_time_str = oldest_time.strftime("%H%M%S")

        extra_candles = request_candles(start_time_str)
        if not extra_candles:  # 더 이상 데이터 없음
            break

        extra_df = convert_to_df(extra_candles)
        df_1m = pd.concat([extra_df, df_1m]).drop_duplicates().sort_values('timestamp').reset_index(drop=True)
        df_10m = resample_to_10min(df_1m)

        # 원본도 합쳐줌
        candle_list = extra_candles + candle_list

        included = any(df_10m['timestamp'] == base_candle_start)

    return candle_list

# 잔고정보 처리
def balance_proc(access_token, app_key, app_secret, acct_no):
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
    cur100.execute("select asset_num, cash_rate, tot_evlu_amt, prvs_rcdl_excc_amt, sell_plan_amt, (select (risk_sum / item_number)::int from public.\"stockMarketMng_stock_market_mng\" where acct_no = A.acct_no and aply_end_dt = '99991231') as risk_amt from \"stockFundMng_stock_fund_mng\" A where acct_no = '" + str(acct_no) + "'")
    result_one00 = cur100.fetchall()
    cur100.close()

    asset_num = 0
    sell_plan_amt = 0
    risk_amt = 0 

    for i in result_one00:
        asset_num = i[0]
        sell_plan_amt = i[4]
        risk_amt = i[5]

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

    balance_list = []

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
        e_ord_psbl_qty = int(c['ord_psbl_qty'][i])

        # 보유종목 손실금액 조회
        cur101 = conn.cursor()
        cur101.execute("""
            SELECT limit_amt
            FROM "stockBalance_stock_balance"
            WHERE acct_no = %s AND code = %s
        """, (acct_no, e_code))

        row = cur101.fetchone()
        cur101.close()

        limit_price = 0

        if e_purchase_amount > 0:
            if row and row[0] is not None:
                try:
                    # 공백 제거 후 정수로 변환 (양수/음수 모두 지원)
                    limit_amt = int(str(row[0]).strip())
                    limit_price = int((e_purchase_sum + limit_amt) / e_purchase_amount)
                except (ValueError, TypeError):
                    # 정수 변환 불가한 경우 예외 처리
                    limit_price = int((e_purchase_sum - int(risk_amt)) / e_purchase_amount)
            else:
                # row가 없거나 limit_amt가 NULL인 경우
                limit_price = int((e_purchase_sum - int(risk_amt)) / e_purchase_amount)

        balance_list.append({
            '계좌번호': str(acct_no),
            '종목코드': e_code,
            '종목명': e_name,
            '보유단가': e_purchase_price,
            '보유수량': e_purchase_amount,
            '현재가': e_current_price,
        })

        # 자산번호의 매도예정자금이 존재하는 경우, 보유종목 비중별 매도가능금액 및 매도가능수량 계산
        if sell_plan_amt > 0:
            # 종목 매입금액 비중 = 평가금액 / 총평가금액(예수금총금액 + 유저평가금액) * 100
            item_eval_gravity = e_eval_sum / u_tot_evlu_amt * 100
            # print("종목 매입금액 비중 : " + format(int(item_eval_gravity), ',d'))
            # 종목 매도가능금액 = 매도예정자금 * 종목 매입금액 비중 * 0.01
            e_sell_plan_sum = sell_plan_amt * item_eval_gravity * 0.01

            # 종목 매도가능수량 = 종목 매도가능금액 / 현재가
            e_sell_plan_amount = e_sell_plan_sum / e_current_price

            cur301 = conn.cursor()
            update_query301 = "with upsert as (update \"stockBalance_stock_balance\" set purchase_price = %s, purchase_amount = %s, purchase_sum = %s, current_price = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, sell_plan_sum = %s, sell_plan_amount = %s, avail_amount = %s, limit_price = %s, proc_yn = 'Y', last_chg_date = %s where acct_no = %s and code = %s and asset_num = %s returning * ) insert into \"stockBalance_stock_balance\"(acct_no, code, name, purchase_price, purchase_amount, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, asset_num, sell_plan_sum, sell_plan_amount, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, limit_price, proc_yn, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select A.sign_resist_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_resist_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.sign_support_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_support_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_loss_price from(select row_number() over(order by last_chg_date desc) as num, b.end_loss_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_target_price from(select row_number() over(order by last_chg_date desc) as num, b.end_target_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.trading_plan from(select row_number() over(order by last_chg_date desc) as num, b.trading_plan from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.limit_price from (select row_number() over(order by last_chg_date desc) as num, b.limit_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num = 1), %s, %s where not exists(select * from upsert)";
            # update 인자값 설정
            record_to_update301 = ([e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, e_sell_plan_sum, e_sell_plan_amount, e_ord_psbl_qty, limit_price, datetime.now(), acct_no, e_code, asset_num,
                                    acct_no, e_code, e_name, e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, asset_num, e_sell_plan_sum, e_sell_plan_amount, 
                                    acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code,
                                    'Y', datetime.now()])
            # DB 연결된 커서의 쿼리 수행
            cur301.execute(update_query301, record_to_update301)
            conn.commit()
            cur301.close()

        else:
            cur301 = conn.cursor()
            update_query301 = "with upsert as (update \"stockBalance_stock_balance\" set purchase_price = %s, purchase_amount = %s, purchase_sum = %s, current_price = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, avail_amount = %s, limit_price = %s, proc_yn = 'Y', last_chg_date = %s where acct_no = %s and code = %s and asset_num = %s returning * ) insert into \"stockBalance_stock_balance\"(acct_no, code, name, purchase_price, purchase_amount, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, asset_num, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, limit_price, proc_yn, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select A.sign_resist_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_resist_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.sign_support_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_support_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_loss_price from(select row_number() over(order by last_chg_date desc) as num, b.end_loss_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_target_price from(select row_number() over(order by last_chg_date desc) as num, b.end_target_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.trading_plan from(select row_number() over(order by last_chg_date desc) as num, b.trading_plan from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.limit_price from (select row_number() over(order by last_chg_date desc) as num, b.limit_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where	A.num = 1), %s, %s where not exists(select * from upsert)";
            # update 인자값 설정
            record_to_update301 = ([e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, e_ord_psbl_qty, limit_price, datetime.now(), acct_no, e_code, asset_num,
                                    acct_no, e_code, e_name, e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, asset_num, 
                                    acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code,
                                    'Y', datetime.now()])
            # DB 연결된 커서의 쿼리 수행
            cur301.execute(update_query301, record_to_update301)
            conn.commit()
            cur301.close()

    # 잔고정보 맵 설정 : 계좌번호, 종목명
    balance_map = {
        (item['계좌번호'], item['종목명']): item
        for item in balance_list
    }
    
    # 일별 주문 체결 조회
    order_complete_output = get_my_complete(access_token, app_key, app_secret, acct_no)

    order_complete_list = []
    today_str = datetime.now().strftime('%Y%m%d')
    # today_str = "20250522"

    if order_complete_output:
    
        for item in order_complete_output:
            odno = item['odno']
            orgn_odno = item['orgn_odno']
            pfls_rate = 0
            pfls_amt = 0
            paid_tax = 0
            paid_fee = 0
            
            if float(item['tot_ccld_qty']) > 0:

                # 현재일 최근 체결된 해당종목의 일별체결정보 조회
                cur302 = conn.cursor()
                cur302.execute("select paid_fee, profit_loss_amt, paid_tax from \"stockOrderComplete_stock_order_complete\" A where acct_no = '" + str(acct_no) + "' and name = '" + item['prdt_name'] + "' and order_dt = '" + today_str + "' and total_complete_qty::int > 0")
                result_one32 = cur302.fetchall()
                cur302.close()

                if len(result_one32) > 0:
                    last_paid_fee = 0
                    last_pfls_amt = 0
                    last_paid_tax = 0

                    for comp_info in result_one32:
                        # 기존 수수료 존재시
                        if comp_info[0] != None:
                            last_paid_fee += int(comp_info[0])  # 수수료 설정
                        # 기존 수익손실금 존재시
                        if comp_info[1] != None:                            
                            last_pfls_amt += int(comp_info[1])  # 수익손실금 설정
                        # 기존 세금 존재시
                        if comp_info[2] != None:    
                            last_paid_tax += int(comp_info[2])  # 세금 설정

                    # 기간별손익일별합산조회
                    period_profit_loss_sum_output = inquire_period_profit_loss(access_token, app_key, app_secret, item['pdno'], today_str, today_str)

                    for item2 in period_profit_loss_sum_output:
            
                        pfls_rate = float(item2['pfls_rt'])
                        pfls_amt = float(item2['rlzt_pfls']) - last_pfls_amt
                        paid_tax = float(item2['tl_tax']) - last_paid_tax
                        paid_fee = float(item2['fee']) - last_paid_fee

                else:
                    # 기간별손익일별합산조회
                    period_profit_loss_sum_output = inquire_period_profit_loss(access_token, app_key, app_secret, item['pdno'], today_str, today_str)

                    for item2 in period_profit_loss_sum_output:
                    
                        pfls_rate = float(item2['pfls_rt'])
                        pfls_amt = float(item2['rlzt_pfls'])
                        paid_tax = float(item2['tl_tax'])
                        paid_fee = float(item2['fee'])

            order_complete_list.append({
                '계좌번호': str(acct_no),
                '주문일자': item['ord_dt'],
                '주문시각': item['ord_tmd'],
                '종목명': item['prdt_name'],
                '주문번호': float(odno) if odno != "" else "",
                '원주문번호': float(orgn_odno) if orgn_odno != "" else "",
                '체결금액': float(item['tot_ccld_amt']),
                '주문유형': item['sll_buy_dvsn_cd_name'],
                '주문단가': float(item['ord_unpr']),
                '주문수량': float(item['ord_qty']),
                '체결단가': float(item['avg_prvs']),
                '체결수량': float(item['tot_ccld_qty']),
                '잔여수량': float(item['rmn_qty']),
                'pfls_rate': pfls_rate,
                'pfls_amt': pfls_amt,
                'paid_tax': paid_tax,
                'paid_fee': paid_fee,
            })        

    cur400 = conn.cursor()

    # 일별주문체결정보 조회
    cur400.execute("""
        SELECT 
            order_no, org_order_no, total_complete_qty, remain_qty
        FROM \"stockOrderComplete_stock_order_complete\"
        WHERE acct_no = %s 
        AND order_dt = %s
    """
    , (str(acct_no), today_str))
    result_400 = cur400.fetchall()
    cur400.close()

    # 읿별주문체결정보 맵 설정 : 계좌번호, 주문일자, 주문번호, 원주문번호의 체결량, 잔여량 
    order_commplete_map = {
        (str(acct_no), today_str, str(int(row[0])), str(int(row[1])) if row[1] != "" else ""): (int(row[2]), int(row[3]))
        for row in result_400
    }

    for item in order_complete_list:
        key1 = (item['계좌번호'], item['종목명'])

        # 잔고정보 맵의 해당하는 일별 주문 체결 조회의 계좌번호, 종목명이 존재하는 경우 : 보유단가, 보유수량 설정
        if key1 in balance_map:
            item['보유단가'] = balance_map[key1]['보유단가']
            item['보유수량'] = balance_map[key1]['보유수량']
        else:   # 잔고정보 맵의 해당하는 일별 주문 체결 조회의 계좌번호, 종목명이 미존재하는 경우
            item['보유단가'] = 0
            item['보유수량'] = 0

        key2 = (str(item['계좌번호']), item['주문일자'], str(int(item['주문번호'])), str(int(item['원주문번호'])) if item['원주문번호'] != "" else "")
        new_complete_qty = int(item['체결수량'])
        new_remain_qty = int(item['잔여수량'])
        
        cur600 = conn.cursor()
        
        # 읿별주문체결정보의 계좌번호, 주문일자, 주문번호, 원주문번호와 일별 주문 체결 조회의 계좌번호, 주문일자, 주문번호, 원주문번호가 미존재하는 경우
        if key2 not in order_commplete_map:
            # 미존재시 INSERT 처리
            if float(item['보유단가']) > 0 and int(item['보유수량']) > 0:
                cur600.execute("""
                    INSERT INTO \"stockOrderComplete_stock_order_complete\" (
                        acct_no, 
                        order_dt,
                        order_tmd, 
                        name, 
                        order_no, 
                        org_order_no,
                        total_complete_amt, 
                        order_type, 
                        order_price, 
                        order_amount,
                        total_complete_qty, 
                        remain_qty, 
                        hold_price, 
                        hold_vol,
                        profit_loss_rate,
                        profit_loss_amt,
                        paid_tax, 
                        paid_fee,
                        last_chg_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                """
                , (
                    acct_no, 
                    item['주문일자'], 
                    item['주문시각'], 
                    item['종목명'], 
                    str(int(item['주문번호'])),
                    str(int(item['원주문번호'])) if item['원주문번호'] != "" else "",
                    int(item['체결금액']),
                    item['주문유형'], 
                    int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']), 
                    int(item['주문수량']),
                    new_complete_qty, 
                    new_remain_qty, 
                    item['보유단가'], 
                    item['보유수량'],
                    Decimal(item['pfls_rate']),
                    int(item['pfls_amt']),
                    int(item['paid_tax']),
                    int(item['paid_fee'])
                ))

                # 단기매매내역정보 조회
                cur402 = conn.cursor()
                cur402.execute("select code, order_type, tr_proc, sh_trading_num from short_trading_detail where acct_no = %s and tr_day = %s and order_no = %s", (str(acct_no), item['주문일자'], str(int(item['주문번호']))))
                result_one01 = cur402.fetchone()
                cur402.close()

                if result_one01:

                    # UPDATE
                    cur403 = conn.cursor()

                    cur403.execute("""
                        UPDATE short_trading_detail
                        SET
                            total_complete_qty = %s,
                            remain_qty = %s,
                            hold_price = %s,
                            hold_vol = %s,
                            profit_loss_rate = %s,
                            profit_loss_amt = %s,
                            chgr_id = %s,
                            chg_date = %s
                        WHERE acct_no = %s 
                        AND tr_day = %s
                        AND order_no = %s 
                    """
                    , (
                        new_complete_qty, 
                        new_remain_qty,
                        item['보유단가'], 
                        item['보유수량'],
                        Decimal(item['pfls_rate']),
                        int(item['pfls_amt']),
                        'holding_item',
                        datetime.now(),
                        str(acct_no), 
                        item['주문일자'], 
                        str(int(item['주문번호']))
                    ))
                    
                    cur403.close()  

                elif item['원주문번호'] != "":

                    # 주문정정대상 단기매매내역정보 조회
                    cur4021 = conn.cursor()
                    cur4021.execute("select code, order_type, tr_proc, sh_trading_num from short_trading_detail where acct_no = %s and tr_day = %s and order_no = %s", (str(acct_no), item['주문일자'], str(int(item['원주문번호']))))
                    result_one02 = cur4021.fetchone()
                    cur4021.close()

                    if result_one02:
                    
                        # UPDATE
                        cur403 = conn.cursor()

                        cur403.execute("""
                            UPDATE short_trading_detail
                            SET
                                order_price = %s,
                                tr_qty = %s,
                                tr_amt = %s,                                       
                                total_complete_qty = %s,
                                remain_qty = %s,
                                hold_price = %s,
                                hold_vol = %s,
                                profit_loss_rate = %s,
                                profit_loss_amt = %s,
                                order_no = %s,
                                org_order_no = %s,                                  
                                chgr_id = %s,
                                chg_date = %s
                            WHERE acct_no = %s 
                            AND tr_day = %s
                            AND order_no = %s 
                        """
                        , (
                            int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']), 
                            int(item['주문수량']),
                            int(item['체결금액']),
                            new_complete_qty, 
                            new_remain_qty,
                            item['보유단가'], 
                            item['보유수량'],
                            Decimal(item['pfls_rate']),
                            int(item['pfls_amt']),
                            str(int(item['주문번호'])),
                            str(int(item['원주문번호'])),
                            'holding_item',
                            datetime.now(),
                            str(acct_no), 
                            item['주문일자'], 
                            str(int(item['원주문번호']))
                        ))
                        
                        cur403.close() 

                order_type = item.get('주문유형', '')

                if new_complete_qty > 0 and '매도' in order_type:
                
                    # 매매처리 미처리된 매수체결 단기매매내역정보 조회
                    cur404 = conn.cursor()
                    cur404.execute("select sh_trading_num, name, code, tr_day, tr_dtm, order_price, total_complete_qty from short_trading_detail where acct_no = %s and name = %s and order_type like %s and total_complete_qty::int > 0 and tr_proc is null", (str(acct_no), item['종목명'], '%매수%'))
                    result_one404 = cur404.fetchall()
                    cur404.close()

                    cur405 = conn.cursor()
                    for item404 in result_one404:
                        sh_trading_num = item404[0]
                        # 매수가보다 매도가 더 큰 경우 : SM(안전마진), 매수가보다 매도가 낮은 경우 : LC(손절매도)
                        if int(item['체결단가']) > 0:
                            tr_proc = "SM" if int(item404[5]) < int(item['체결단가']) else "LC"
                        else:
                            tr_proc = "SM" if int(item404[5]) < int(item['주문단가']) else "LC"

                        # 단기매매내역의 매수체결 대상 변경(매매처리 : tr_proc)
                        update_query404 = "UPDATE short_trading_detail SET tr_proc = %s, chgr_id = %s, chg_date = %s WHERE acct_no = %s AND name = %s AND order_type LIKE %s AND total_complete_qty::int > 0 AND sh_trading_num = %s"
                        record_to_update404 = ([tr_proc, 'tr_proc', datetime.now(), str(acct_no), item['종목명'], '%매수%', sh_trading_num])
                        cur405.execute(update_query404, record_to_update404)
                        
                    cur405.close()

                conn.commit()                        

            else:
                # 주문(주문정정) 생성 후, 주문체결정보 현행화(1분단위)되기전에 전량 체결되어 잔고정보의 보유단가와 보유수량이 0 인 경우, 
                # 보유단가 = 체결금액 - 수익금액(세금 및 수수료 포함) / 체결수량
                if new_complete_qty > 0:
                    if item['원주문번호'] != "":
                        # 원주문번호의 일별체결정보 조회
                        cur401 = conn.cursor()
                        cur401.execute("select hold_price from \"stockOrderComplete_stock_order_complete\" A where acct_no = '" + str(acct_no) + "' and order_no = '" + item['원주문번호'] + "' and order_dt = '" + today_str + "'")
                        result_one41 = cur401.fetchone()
                        cur401.close()

                        # 주문정정인 경우, 이전 주문체결정보의 보유단가로 설정
                        if result_one41:
                            hold_price = float(result_one41[0])
                        else:
                            hold_price = 0        
                    else:    
                        hold_price = round((int(item['체결금액']) - int(item['pfls_amt']) - int(item['paid_tax']) - int(item['paid_fee'])) / new_complete_qty)

                    cur600.execute("""
                        INSERT INTO \"stockOrderComplete_stock_order_complete\" (
                            acct_no, 
                            order_dt,
                            order_tmd, 
                            name, 
                            order_no, 
                            org_order_no,
                            total_complete_amt, 
                            order_type, 
                            order_price, 
                            order_amount,
                            total_complete_qty, 
                            remain_qty, 
                            hold_price, 
                            hold_vol,
                            profit_loss_rate,
                            profit_loss_amt,
                            paid_tax, 
                            paid_fee,
                            last_chg_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    """
                    , (
                        acct_no, 
                        item['주문일자'], 
                        item['주문시각'], 
                        item['종목명'], 
                        str(int(item['주문번호'])),
                        str(int(item['원주문번호'])) if item['원주문번호'] != "" else "",
                        int(item['체결금액']),
                        item['주문유형'], 
                        int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']), 
                        int(item['주문수량']),
                        new_complete_qty, 
                        new_remain_qty, 
                        hold_price, 
                        new_complete_qty,
                        Decimal(item['pfls_rate']),
                        int(item['pfls_amt']),
                        int(item['paid_tax']),
                        int(item['paid_fee'])
                    ))

                    # 총체결수량이 존재하는 단기매매내역정보 조회
                    cur402 = conn.cursor()
                    cur402.execute("select code, order_type, tr_proc, sh_trading_num from short_trading_detail where acct_no = %s and tr_day = %s and order_no = %s", (str(acct_no), item['주문일자'], str(int(item['주문번호']))))
                    result_one01 = cur402.fetchone()
                    cur402.close()

                    if result_one01:

                        # UPDATE
                        cur403 = conn.cursor()

                        cur403.execute("""
                            UPDATE short_trading_detail
                            SET
                                total_complete_qty = %s,
                                remain_qty = %s,
                                hold_price = %s,
                                hold_vol = %s,
                                profit_loss_rate = %s,
                                profit_loss_amt = %s,
                                chgr_id = %s,
                                chg_date = %s
                            WHERE acct_no = %s 
                            AND tr_day = %s
                            AND order_no = %s 
                        """
                        , (
                            new_complete_qty, 
                            new_remain_qty,
                            hold_price,
                            new_complete_qty,
                            Decimal(item['pfls_rate']),
                            int(item['pfls_amt']),
                            'holding_item',
                            datetime.now(),
                            str(acct_no), 
                            item['주문일자'], 
                            str(int(item['주문번호']))
                        ))
                        
                        cur403.close()  

                    elif item['원주문번호'] != "":
                        # 주문정정대상 단기매매내역정보 조회
                        cur4021 = conn.cursor()
                        cur4021.execute("select code, order_type, tr_proc, sh_trading_num from short_trading_detail where acct_no = %s and tr_day = %s and order_no = %s", (str(acct_no), item['주문일자'], str(int(item['원주문번호']))))
                        result_one02 = cur4021.fetchone()
                        cur4021.close()

                        if result_one02:
                        
                            # UPDATE
                            cur403 = conn.cursor()

                            cur403.execute("""
                                UPDATE short_trading_detail
                                SET
                                    order_price = %s,
                                    tr_qty = %s,
                                    tr_amt = %s,                                       
                                    total_complete_qty = %s,
                                    remain_qty = %s,
                                    hold_price = %s,
                                    hold_vol = %s,
                                    profit_loss_rate = %s,
                                    profit_loss_amt = %s,
                                    order_no = %s,
                                    org_order_no = %s,                                  
                                    chgr_id = %s,
                                    chg_date = %s
                                WHERE acct_no = %s 
                                AND tr_day = %s
                                AND order_no = %s 
                            """
                            , (
                                int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']), 
                                int(item['주문수량']),
                                int(item['체결금액']),
                                new_complete_qty, 
                                new_remain_qty,
                                hold_price,
                                new_complete_qty,
                                Decimal(item['pfls_rate']),
                                int(item['pfls_amt']),
                                str(int(item['주문번호'])),
                                str(int(item['원주문번호'])),
                                'holding_item',
                                datetime.now(),
                                str(acct_no), 
                                item['주문일자'], 
                                str(int(item['원주문번호']))
                            ))
                            
                            cur403.close()     

                    order_type = item.get('주문유형', '')

                    if '매도' in order_type:
                    
                        # 매매처리 미처리된 매수체결 단기매매내역정보 조회
                        cur404 = conn.cursor()
                        cur404.execute("select sh_trading_num, name, code, tr_day, tr_dtm, order_price, total_complete_qty from short_trading_detail where acct_no = %s and name = %s and order_type like %s and total_complete_qty::int > 0 and tr_proc is null", (str(acct_no), item['종목명'], '%매수%'))
                        result_one404 = cur404.fetchall()
                        cur404.close()

                        cur405 = conn.cursor()
                        for item404 in result_one404:
                            sh_trading_num = item404[0]
                            # 매수가보다 매도가 더 큰 경우 : SM(안전마진), 매수가보다 매도가 낮은 경우 : LC(손절매도)
                            if int(item['체결단가']) > 0:
                                tr_proc = "SM" if int(item404[5]) < int(item['체결단가']) else "LC"
                            else:
                                tr_proc = "SM" if int(item404[5]) < int(item['주문단가']) else "LC"

                            # 단기매매내역의 매수체결 대상 변경(매매처리 : tr_proc)
                            update_query404 = "UPDATE short_trading_detail SET tr_proc = %s, chgr_id = %s, chg_date = %s WHERE acct_no = %s AND name = %s AND order_type LIKE %s AND total_complete_qty::int > 0 AND sh_trading_num = %s"
                            record_to_update404 = ([tr_proc, 'tr_proc', datetime.now(), str(acct_no), item['종목명'], '%매수%', sh_trading_num])
                            cur405.execute(update_query404, record_to_update404)
                        
                        cur405.close()

                    conn.commit()                            

        else:   # 읿별주문체결정보의 계좌번호, 주문일자, 주문번호, 원주문번호와 일별 주문 체결 조회의 계좌번호, 주문일자, 주문번호, 원주문번호가 존재하는 경우
            old_complete_qty, old_remain_qty = order_commplete_map[key2]
            #  읿별주문체결정보의 체결수량, 잔여수량과 일별 주문 체결 조회의 체결수량, 잔여수량이 다른 경우 UPDATE 처리
            if new_complete_qty != int(old_complete_qty) or new_remain_qty != int(old_remain_qty):

                if float(item['보유단가']) > 0 and int(item['보유수량']) > 0:
                    # UPDATE
                    cur600.execute("""
                        UPDATE \"stockOrderComplete_stock_order_complete\"
                        SET
                            order_price = %s,       
                            total_complete_qty = %s,
                            remain_qty = %s,
                            total_complete_amt = %s,
                            hold_price = %s, 
                            hold_vol = %s,
                            profit_loss_rate = %s,
                            profit_loss_amt = %s,
                            paid_tax = %s,
                            paid_fee = %s,
                            last_chg_date = now()
                        WHERE acct_no = %s 
                        AND order_dt = %s
                        AND order_no = %s 
                    """
                    , (
                        int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']), 
                        new_complete_qty, 
                        new_remain_qty,
                        int(item['체결금액']),
                        item['보유단가'], 
                        item['보유수량'],
                        Decimal(item['pfls_rate']),
                        int(item['pfls_amt']),
                        int(item['paid_tax']),
                        int(item['paid_fee']),
                        acct_no, 
                        item['주문일자'], 
                        str(int(item['주문번호']))
                    ))
                else:
                    # UPDATE
                    cur600.execute("""
                        UPDATE \"stockOrderComplete_stock_order_complete\"
                        SET
                            order_price = %s,       
                            total_complete_qty = %s,
                            remain_qty = %s,
                            total_complete_amt = %s,
                            profit_loss_rate = %s,
                            profit_loss_amt = %s,
                            paid_tax = %s,
                            paid_fee = %s,
                            last_chg_date = now()
                        WHERE acct_no = %s 
                        AND order_dt = %s
                        AND order_no = %s 
                    """
                    , (
                        int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']), 
                        new_complete_qty, 
                        new_remain_qty,
                        int(item['체결금액']),
                        Decimal(item['pfls_rate']),
                        int(item['pfls_amt']),
                        int(item['paid_tax']),
                        int(item['paid_fee']),
                        acct_no, 
                        item['주문일자'], 
                        str(int(item['주문번호']))
                    ))    

                # 총체결수량이 다른 단기매매내역정보 조회
                cur402 = conn.cursor()
                cur402.execute("select code, order_type, tr_proc, sh_trading_num from short_trading_detail where acct_no = %s and tr_day = %s and order_no = %s", (str(acct_no), item['주문일자'], str(int(item['주문번호']))))
                result_one01 = cur402.fetchone()
                cur402.close()

                if result_one01:

                    # UPDATE
                    cur403 = conn.cursor()

                    if float(item['보유단가']) > 0 and int(item['보유수량']) > 0:
                        cur403.execute("""
                            UPDATE short_trading_detail
                            SET
                                total_complete_qty = %s,
                                remain_qty = %s,
                                hold_price = %s,
                                hold_vol = %s,
                                profit_loss_rate = %s,
                                profit_loss_amt = %s,
                                chgr_id = %s,
                                chg_date = %s
                            WHERE acct_no = %s 
                            AND tr_day = %s
                            AND order_no = %s 
                        """
                        , (
                            new_complete_qty, 
                            new_remain_qty,
                            item['보유단가'], 
                            item['보유수량'],
                            Decimal(item['pfls_rate']),
                            int(item['pfls_amt']),
                            'holding_item',
                            datetime.now(),
                            str(acct_no), 
                            item['주문일자'], 
                            str(int(item['주문번호']))
                        ))
                    
                    else: 
                        cur403.execute("""
                            UPDATE short_trading_detail
                            SET
                                total_complete_qty = %s,
                                remain_qty = %s,
                                profit_loss_rate = %s,
                                profit_loss_amt = %s,
                                chgr_id = %s,
                                chg_date = %s
                            WHERE acct_no = %s 
                            AND tr_day = %s
                            AND order_no = %s 
                        """
                        , (
                            new_complete_qty, 
                            new_remain_qty,
                            Decimal(item['pfls_rate']),
                            int(item['pfls_amt']),
                            'holding_item',
                            datetime.now(),
                            str(acct_no), 
                            item['주문일자'], 
                            str(int(item['주문번호']))
                        ))
                    
                    cur403.close()    
                    
                order_type = item.get('주문유형', '')

                if '매도' in order_type:
                
                    # 매매처리 미처리된 매수체결 단기매매내역정보 조회
                    cur404 = conn.cursor()
                    cur404.execute("select sh_trading_num, name, code, tr_day, tr_dtm, order_price, total_complete_qty from short_trading_detail where acct_no = %s and name = %s and order_type like %s and total_complete_qty::int > 0 and tr_proc is null", (str(acct_no), item['종목명'], '%매수%'))
                    result_one404 = cur404.fetchall()
                    cur404.close()

                    cur405 = conn.cursor()
                    for item404 in result_one404:
                        sh_trading_num = item404[0]
                        # 매수가보다 매도가 더 큰 경우 : SM(안전마진), 매수가보다 매도가 낮은 경우 : LC(손절매도)
                        if int(item['체결단가']) > 0:
                            tr_proc = "SM" if int(item404[5]) < int(item['체결단가']) else "LC"
                        else:
                            tr_proc = "SM" if int(item404[5]) < int(item['주문단가']) else "LC"

                        # 단기매매내역의 매수체결 대상 변경(매매처리 : tr_proc)
                        update_query404 = "UPDATE short_trading_detail SET tr_proc = %s, chgr_id = %s, chg_date = %s WHERE acct_no = %s AND name = %s AND order_type LIKE %s AND total_complete_qty::int > 0 AND sh_trading_num = %s"
                        record_to_update404 = ([tr_proc, 'tr_proc', datetime.now(), str(acct_no), item['종목명'], '%매수%', sh_trading_num])
                        cur405.execute(update_query404, record_to_update404)
                    
                    cur405.close()
                    
                conn.commit()    
    
        cur600.close()

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

            # 잔고정보 처리
            balance_proc(access_token, app_key, app_secret, acct_no)
    
            # 보유정보 조회
            cur03 = conn.cursor()
            cur03.execute("select code, name, sign_resist_price, sign_support_price, end_target_price, end_loss_price, purchase_amount, (select 1 from trail_signal_recent where acct_no = '"+str(acct_no)+"' and trail_day = TO_CHAR(now(), 'YYYYMMDD') and code = '0001' and trail_signal_code = '04') as market_dead, (select 1 from trail_signal_recent where acct_no = '"+str(acct_no)+"' and trail_day = TO_CHAR(now(), 'YYYYMMDD') and code = '0001' and trail_signal_code = '06') as market_over, case when cast(A.earnings_rate as INTEGER) > 0 then (select B.low_price from dly_stock_balance B where A.code = B.code and A.acct_no = cast(B.acct as INTEGER)    and B.dt = TO_CHAR(get_previous_business_day(now()::date), 'YYYYMMDD')) else null end as low_price from \"stockBalance_stock_balance\" A where acct_no = '"+str(acct_no)+"' and proc_yn = 'Y' and trading_plan not in ('h','i')")
            result_three = cur03.fetchall()
            cur03.close()

            # 보유종목 최종이탈가, 최종목표가를 각각 실시간 종목시세의 최고가와 최저가 비교
            for i in result_three:
                # print("종목명 : " + i[1])
                a = ""
                try:
                    time.sleep(0.3)  # 초당 3건 이하로 제한
                    a = inquire_price(access_token, app_key, app_secret, i[0])
                

                    trail_signal_code = ""
                    trail_signal_name = ""
                    sell_plan_amount = ""
                    n_sell_amount = 0
                    n_sell_sum = 0
                    if i[2] != None:
                        if i[2] > 0:
                            if int(a['stck_prpr']) > i[2]:
                                # print("저항가 돌파 : " + format(int(i[2]), ',d'))
                                trail_signal_code = "07"
                                trail_signal_name = format(int(i[2]), ',d') + "원 {저항가 돌파}"

                                if i[6] != None:
                                    n_sell_amount = i[6]
                                    n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                                    sell_plan_amount = format(int(n_sell_amount), ',d')

                    if i[3] != None:
                        if i[3] > 0:
                            if int(a['stck_prpr']) < i[3]:
                                # print("지지가 이탈 : " + format(int(i[3]), ',d'))
                                trail_signal_code = "08"
                                trail_signal_name = format(int(i[3]), ',d') + "원 {지지가 이탈}"

                                if i[6] != None:
                                    n_sell_amount = i[6]
                                    n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                                    sell_plan_amount = format(int(n_sell_amount), ',d')

                    if i[4] != None:
                        if i[4] > 0:
                            if int(a['stck_hgpr']) > i[4]:
                                # print("최종목표가 돌파 : " + format(int(i[4]), ',d'))
                                trail_signal_code = "09"
                                trail_signal_name = format(int(i[4]), ',d') + "원 {최종목표가 돌파}"

                                if i[6] != None:
                                    n_sell_amount = i[6]
                                    n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                                    sell_plan_amount = format(int(n_sell_amount), ',d')

                    if i[5] != None:
                        if i[5] > 0:
                            if int(a['stck_lwpr']) < i[5]:
                                # print("최종이탈가 이탈 : " + format(int(i[5]), ',d'))
                                trail_signal_code = "10"
                                trail_signal_name = format(int(i[5]), ',d') + "원 {최종이탈가 이탈}"

                                if i[6] != None:
                                    n_sell_amount = i[6]
                                    n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                                    sell_plan_amount = format(int(n_sell_amount), ',d')

                    if i[7] != None:
                        # print("지지선이탈 : " + str(i[7]))  # 지지선이탈
                        trail_signal_code = "11"
                        trail_signal_name = "시장 지지선 이탈[지지가 : " + format(int(i[3]), ',d') + "원]"

                        if i[6] != None:
                            n_sell_amount = i[6]
                            n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                            sell_plan_amount = format(int(n_sell_amount), ',d')

                    if i[8] != None:
                        # print("추세선이탈 : " + str(i[8]))  # 추세선이탈
                        trail_signal_code = "12"
                        trail_signal_name = "시장 추세선 이탈[지지가 : " + format(int(i[3]), ',d') + "원]"

                        if i[6] != None:
                            n_sell_amount = i[6]
                            n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                            sell_plan_amount = format(int(n_sell_amount), ',d')

                    if cur_time > '1510':
                        if i[9] != None:
                            if int(a['stck_prpr']) < i[9]:
                                trail_signal_code = "13"
                                trail_signal_name = format(int(i[9]), ',d') +"원 {전일 저가 이탈}"
                                # print("수익률 0 이상 보유종목 대상 전일 저가 이탈 : " + str(i[9])) 
                    
                    # 추적정보 조회(현재일 종목코드 기준)
                    cur04 = conn.cursor()
                    cur04.execute("select TS.trail_signal_code, TS.trail_time from trail_signal TS where TS.acct = '" + str(acct_no) + "' and TS.code = '" + i[0] + "' and TS.trail_day = TO_CHAR(now(), 'YYYYMMDD') and trail_signal_code = '" + trail_signal_code + "'")
                    result_four = cur04.fetchall()
                    cur04.close()

                    if len(result_four) > 0:
                        for j in result_four:
                            # print("trail_signal_code1 : " + j[0])
                            if trail_signal_code != "":
                                if trail_signal_code != j[0]:
                                    print("종목명 : " + i[1] + "추적정보 대상 : " + trail_signal_name)
                                    if n_sell_amount > 0:
                                        sell_command = f"/HoldingSell_{i[0]}_{i[6]}"
                                        # telegram_text = i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 매도량 : " + sell_plan_amount + "주, 매도금액 : " + format(int(n_sell_sum), ',d') +"원"
                                        telegram_text = (f"{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매도량 : {sell_plan_amount}주, 매도금액 : {format(int(n_sell_sum), ',d')}원 => {sell_command}")
                                    else:
                                        telegram_text = i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate']                            
                                    # 텔레그램 메시지 전송
                                    asyncio.run(main(telegram_text))

                                    # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                                    cur20 = conn.cursor()
                                    insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, cdate = %s, sell_plan_qty = %s, sell_plan_amt = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                                    # insert 인자값 설정
                                    record_to_insert0 = ([cur_time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                                    # DB 연결된 커서의 쿼리 수행
                                    cur20.execute(insert_query0, record_to_insert0)

                                    # 추적신호이력 정보 생성
                                    cur2 = conn.cursor()
                                    insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                    # insert 인자값 설정
                                    record_to_insert = ([acct_no, today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                                    # DB 연결된 커서의 쿼리 수행
                                    cur2.execute(insert_query, record_to_insert)
                                    conn.commit()
                                    cur20.close()
                                    cur2.close()

                                    # 저항가 돌파, 지지가 이탈, 최종목표가 돌파, 최종이탈가 이탈시 
                                    if trail_signal_code == '07' or trail_signal_code == '08' or trail_signal_code == '09' or trail_signal_code == '10':
                                    
                                        # base_dtm datetime 변환
                                        base_dtm = datetime.strptime(today + j[1] + '00', '%Y%m%d%H%M%S')
                                        
                                        # 주식당일분봉조회
                                        candle_list = fetch_candles_with_base(access_token, app_key, app_secret, i[0], base_dtm)

                                        minute_list = []
                                        for item in candle_list:
                                            minute_list.append({
                                                '체결시간': item['stck_cntg_hour'],
                                                '종가': item['stck_prpr'],
                                                '시가': item['stck_oprc'],
                                                '고가': item['stck_hgpr'],
                                                '저가': item['stck_lwpr'],
                                                '거래량': item['cntg_vol']
                                            })

                                        df = pd.DataFrame(minute_list)
                                        df['체결시간'] = pd.to_datetime(df['체결시간'], format='%H%M%S')
                                        df = df.sort_values('체결시간').reset_index(drop=True)
                                        df.rename(columns={
                                            '종가': 'close',
                                            '시가': 'open',
                                            '고가': 'high',
                                            '저가': 'low',
                                            '거래량': 'volume',
                                            '체결시간': 'timestamp'
                                        }, inplace=True)

                                        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
                                        df['body'] = (df['close'] - df['open']).abs()

                                        # 1분봉 df → 10분봉 리샘플링
                                        df_10m = df.resample('10T', on='timestamp', label='left', closed='left').agg({
                                            'open': 'first',
                                            'high': 'max',
                                            'low': 'min',
                                            'close': 'last',
                                            'volume': 'sum'
                                        }).reset_index()
                                        # 10분봉 몸통(body) 계산
                                        df_10m['body'] = (df_10m['close'] - df_10m['open']).abs()
                                        기준봉 = df_10m.loc[df_10m['volume'].idxmax()]
                                        avg_body = df_10m['body'].rolling(20).mean().iloc[-1] if len(df_10m) >= 20 else df_10m['body'].mean()

                                        # 몸통 유형 구분
                                        body_value = 기준봉['body']
                                        if body_value > avg_body * 1.5:
                                            candle_body = "L"   # 장봉
                                        elif body_value < avg_body * 0.5:
                                            candle_body = "S"   # 단봉
                                        else:
                                            candle_body = "M"   # 보통

                                        # 매매자동처리 insert
                                        cur500 = conn.cursor()
                                        insert_query = """
                                            INSERT INTO trade_auto_proc (
                                                acct_no, name, code, base_day, base_dtm, trade_tp, open_price, high_price, low_price, close_price, vol, candle_body, trade_sum, proc_yn, regr_id, reg_date, chgr_id, chg_date
                                            )       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                            ON CONFLICT (acct_no, code, base_day, base_dtm, trade_tp) DO NOTHING
                                        """
                                        # insert 인자값 설정
                                        cur500.execute(insert_query, (
                                            acct_no, i[1], i[0], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S"), "S", 기준봉['open'], 기준봉['high'], 기준봉['low'], 기준봉['close'], 기준봉['volume'], candle_body, '100', 'Y', 'AUTO_SELL', datetime.now(), 'AUTO_SELL', datetime.now()
                                        ))

                                        was_inserted = cur500.rowcount == 1

                                        conn.commit()
                                        cur500.close()

                                        if was_inserted:
                                            # 매매자동처리 update
                                            cur501 = conn.cursor()
                                            update_query = """
                                                UPDATE trade_auto_proc
                                                SET
                                                    proc_yn = 'N'
                                                    , chgr_id = 'AUTO_UP_SELL'
                                                    , chg_date = %s
                                                WHERE acct_no = %s
                                                AND code = %s
                                                AND base_day = %s
                                                AND base_dtm <> %s
                                                AND trade_tp = 'S'
                                                AND proc_yn = 'Y'
                                            """

                                            # update 인자값 설정
                                            cur501.execute(update_query, (
                                                datetime.now(), str(acct_no), i[0], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S")
                                            ))

                                            conn.commit()
                                            cur501.close()

                    else:
                        # print("trail_signal_code2 : " + trail_signal_code)
                        if trail_signal_code != "":
                            print("종목명 : " + i[1] + "추적신호 : " + trail_signal_name)
                            if n_sell_amount > 0:
                                sell_command = f"/HoldingSell_{i[0]}_{i[6]}"
                                # telegram_text = i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 매도량 : " + sell_plan_amount + "주, 매도금액 : " + format(int(n_sell_sum), ',d') +"원"
                                telegram_text = (f"{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매도량 : {sell_plan_amount}주, 매도금액 : {format(int(n_sell_sum), ',d')}원 => {sell_command}")

                            else:
                                telegram_text = i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate']                            
                            # 텔레그램 메시지 전송
                            asyncio.run(main(telegram_text))

                            # 추적신호 정보 미존재 대상 신규생성 또는 변경(현재일 종목 기준)
                            cur20 = conn.cursor()
                            insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, cdate = %s, sell_plan_qty = %s, sell_plan_amt = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                            # insert 인자값 설정
                            record_to_insert0 = ([cur_time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                            # DB 연결된 커서의 쿼리 수행
                            cur20.execute(insert_query0, record_to_insert0)

                            # 추적신호이력 정보 생성
                            cur2 = conn.cursor()
                            insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            # insert 인자값 설정
                            record_to_insert = ([acct_no, today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                            # DB 연결된 커서의 쿼리 수행
                            cur2.execute(insert_query, record_to_insert)
                            conn.commit()
                            cur20.close()
                            cur2.close()

                            if trail_signal_code == '07' or trail_signal_code == '08' or trail_signal_code == '09' or trail_signal_code == '10':
                                    
                                # base_dtm datetime 변환
                                base_dtm = datetime.strptime(today + cur_time + '00', '%Y%m%d%H%M%S')
                                
                                # 주식당일분봉조회
                                candle_list = fetch_candles_with_base(access_token, app_key, app_secret, i[0], base_dtm)

                                minute_list = []
                                for item in candle_list:
                                    minute_list.append({
                                        '체결시간': item['stck_cntg_hour'],
                                        '종가': item['stck_prpr'],
                                        '시가': item['stck_oprc'],
                                        '고가': item['stck_hgpr'],
                                        '저가': item['stck_lwpr'],
                                        '거래량': item['cntg_vol']
                                    })

                                df = pd.DataFrame(minute_list)
                                df['체결시간'] = pd.to_datetime(df['체결시간'], format='%H%M%S')
                                df = df.sort_values('체결시간').reset_index(drop=True)
                                df.rename(columns={
                                    '종가': 'close',
                                    '시가': 'open',
                                    '고가': 'high',
                                    '저가': 'low',
                                    '거래량': 'volume',
                                    '체결시간': 'timestamp'
                                }, inplace=True)

                                df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
                                df['body'] = (df['close'] - df['open']).abs()

                                # 1분봉 df → 10분봉 리샘플링
                                df_10m = df.resample('10T', on='timestamp', label='left', closed='left').agg({
                                    'open': 'first',
                                    'high': 'max',
                                    'low': 'min',
                                    'close': 'last',
                                    'volume': 'sum'
                                }).reset_index()
                                # 10분봉 몸통(body) 계산
                                df_10m['body'] = (df_10m['close'] - df_10m['open']).abs()
                                기준봉 = df_10m.loc[df_10m['volume'].idxmax()]
                                avg_body = df_10m['body'].rolling(20).mean().iloc[-1] if len(df_10m) >= 20 else df_10m['body'].mean()

                                # 몸통 유형 구분
                                body_value = 기준봉['body']
                                if body_value > avg_body * 1.5:
                                    candle_body = "L"   # 장봉
                                elif body_value < avg_body * 0.5:
                                    candle_body = "S"   # 단봉
                                else:
                                    candle_body = "M"   # 보통

                                # 매매자동처리 insert
                                cur500 = conn.cursor()
                                insert_query = """
                                    INSERT INTO trade_auto_proc (
                                        acct_no, name, code, base_day, base_dtm, trade_tp, open_price, high_price, low_price, close_price, vol, candle_body, trade_sum, proc_yn, regr_id, reg_date, chgr_id, chg_date
                                    )       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (acct_no, code, base_day, base_dtm, trade_tp) DO NOTHING
                                """
                                # insert 인자값 설정
                                cur500.execute(insert_query, (
                                    acct_no, i[1], i[0], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S"), "S", 기준봉['open'], 기준봉['high'], 기준봉['low'], 기준봉['close'], 기준봉['volume'], candle_body, '100', 'Y', 'AUTO_SELL', datetime.now(), 'AUTO_SELL', datetime.now()
                                ))

                                was_inserted = cur500.rowcount == 1

                                conn.commit()
                                cur500.close()

                                if was_inserted:
                                    # 매매자동처리 update
                                    cur501 = conn.cursor()
                                    update_query = """
                                        UPDATE trade_auto_proc
                                        SET
                                            proc_yn = 'N'
                                            , chgr_id = 'AUTO_UP_SELL'
                                            , chg_date = %s
                                        WHERE acct_no = %s
                                        AND code = %s
                                        AND base_day = %s
                                        AND base_dtm <> %s
                                        AND trade_tp = 'S'
                                        AND proc_yn = 'Y'
                                    """

                                    # update 인자값 설정
                                    cur501.execute(update_query, (
                                        datetime.now(), str(acct_no), i[0], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S")
                                    ))

                                    conn.commit()
                                    cur501.close()
                                
                except Exception as ex:
                    print(f"현재가 시세 에러 : [{i[0]}] {ex}")    

            time.sleep(3)                                        

        except Exception as e:
            print(f"[{nick}] Error holding item : {e}")               

    conn.close()

else:
    conn.close()
    print("Today is Holiday")