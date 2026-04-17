import psycopg2 as db
from datetime import datetime, timedelta
import kis_api_resp as resp
import requests
import json
import telegram
import pandas as pd
from decimal import Decimal
import time

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")

# 인증처리
def auth(APP_KEY, APP_SECRET):

    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
            "appkey":APP_KEY,
            "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False, timeout=10)
    ACCESS_TOKEN = res.json()["access_token"]

    return ACCESS_TOKEN

def account(nickname):
    cur01 = conn.cursor()
    cur01.execute("""
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day, bot_token1
        FROM "stockAccount_stock_account"
        WHERE nick_name = %s
    """, (nickname,))
    result_two = cur01.fetchone()
    cur01.close()

    acct_no, access_token, app_key, app_secret, token_publ_date, token_day, bot_token1 = result_two
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
        'bot_token1': bot_token1
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
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)

    return ar.getBody().output

# 계좌잔고 조회
def stock_balance(access_token, app_key, app_secret, acct_no, rtFlag):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8434R"}
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
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)

    if rtFlag == "all" and ar.isOK():
        output = ar.getBody().output2
    else:
        output = ar.getBody().output1

    return pd.DataFrame(output)

# 일별주문체결조회
def get_my_complete(access_token, app_key, app_secret, acct_no, code, order_no):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC0081R",
               "custtype": "P"}
    params = {
            'CANO': acct_no,
            'ACNT_PRDT_CD':"01",
            'SORT_DVSN': "01",
            'INQR_STRT_DT': datetime.now().strftime('%Y%m%d'),
            'INQR_END_DT': datetime.now().strftime('%Y%m%d'),
            'SLL_BUY_DVSN_CD': "00",
            'PDNO': code,
            'ORD_GNO_BRNO': "",
            'ODNO': order_no,
            'CCLD_DVSN': "00",
            'INQR_DVSN': "01",
            'INQR_DVSN_1': "",
            'INQR_DVSN_3': "00",
            'EXCG_ID_DVSN_CD': "ALL",
            'CTX_AREA_NK100': "",
            'CTX_AREA_FK100': ""
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    URL = f"{URL_BASE}/{PATH}"

    try:
        res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
        ar = resp.APIResp(res)
        body = ar.getBody()
        return body.output1 if hasattr(body, 'output1') else []
    except Exception as e:
        print("일별주문체결조회 중 오류 발생:", e)
        return []

# 주식주문(정정취소)
def order_cancel_revice(access_token, app_key, app_secret, acct_no, cncl_dv, order_no, order_qty, order_price):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC0013U",
               "custtype": "P"
    }
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "KRX_FWDG_ORD_ORGNO": "06010",
               "ORGN_ODNO": order_no,
               "ORD_DVSN": "00" if int(order_price) > 0 else "01",
               "RVSE_CNCL_DVSN_CD": cncl_dv,
               "ORD_QTY": str(order_qty),
               "ORD_UNPR": str(order_price),
               "QTY_ALL_ORD_YN": "Y"
    }
    PATH = "uapi/domestic-stock/v1/trading/order-rvsecncl"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
    ar = resp.APIResp(res)
    return ar.getBody().output

# 기간별손익일별합산조회
def inquire_period_profit_loss(access_token, app_key, app_secret, code, strt_dt, end_dt, acct_no):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8708R",
               "custtype": "P"}
    params = {
            'CANO': acct_no,
            'SORT_DVSN': "01",
            'INQR_DVSN': "00",
            'ACNT_PRDT_CD':"01",
            'CBLC_DVSN': "00",
            'PDNO': code,
            'INQR_STRT_DT': strt_dt,
            'INQR_END_DT': end_dt,
            'CTX_AREA_NK100': "",
            'CTX_AREA_FK100': ""
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-period-profit"
    URL = f"{URL_BASE}/{PATH}"

    try:
        res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
        ar = resp.APIResp(res)
        body = ar.getBody()
        if hasattr(body, 'output1'):
            return body.output1
        else:
            print("기간별손익일별합산조회 응답이 없습니다.")
            return []
    except Exception as e:
        print("기간별손익일별합산조회 중 오류 발생:", e)
        return []

# 매도 주문정보 존재시 취소 처리
def sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, code):

    result_msgs = []

    try:
        output1 = get_my_complete(access_token, app_key, app_secret, acct_no, code, '')

        if len(output1) > 0:

            tdf = pd.DataFrame(output1)
            tdf.set_index('odno')
            d = tdf[['pdno', 'odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]
            order_no = 0

            for i, name in enumerate(d.index):

                if d['sll_buy_dvsn_cd'][i] == "01":

                    if int(d['rmn_qty'][i]) > 0:
                        order_no = int(d['odno'][i])

                        c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "02", str(order_no), "0", "0")
                        if c['ODNO'] != "":
                            print("매도주문취소 완료")
                        else:
                            print("매도주문취소 실패")
                            msg = f"[{d['prdt_name'][i]}] 매도주문취소 실패"
                            result_msgs.append(msg)

    except Exception as e:
        print('매도주문취소 오류.', e)
        msg = f"[{code}] 매도주문취소 오류 - {str(e)}"
        result_msgs.append(msg)

    final_message = result_msgs if result_msgs else "success"

    return final_message

# 주식주문(현금)
def order_cash(buy_flag, access_token, app_key, app_secret, acct_no, stock_code, ord_dvsn, order_qty, order_price, cndt_price=None):

    if buy_flag:
        tr_id = "TTTC0012U"
    else:
        tr_id = "TTTC0011U"

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": tr_id,
               "custtype": "P"
    }
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "PDNO": stock_code,
               "ORD_DVSN": ord_dvsn,
               "ORD_QTY": order_qty,
               "ORD_UNPR": order_price
    }
    if ord_dvsn == "22":
        params["CNDT_PRIC"] = str(cndt_price)

    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
    ar = resp.APIResp(res)
    return ar.getBody().output

def fetch_candles_with_base(access_token, app_key, app_secret, code, base_dtm):
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
        params = {
            'FID_COND_MRKT_DIV_CODE': "J",
            'FID_INPUT_ISCD': code,
            'FID_INPUT_HOUR_1': start_time,
            'FID_PW_DATA_INCU_YN': 'N',
            'FID_ETC_CLS_CODE': ""
        }
        res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
        ar = resp.APIResp(res)
        return ar.getBody().output2

    def convert_to_df(candle_list):
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
        df = df.set_index('timestamp')
        df_10 = df.resample('10T').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna().reset_index()
        return df_10

    cur_time_str = datetime.now().strftime("%H%M%S")
    candle_list = request_candles(cur_time_str)
    df_1m = convert_to_df(candle_list)
    df_10m = resample_to_10min(df_1m)

    base_candle_start = base_dtm.replace(minute=(base_dtm.minute // 10) * 10, second=0)
    included = any(df_10m['timestamp'] == base_candle_start)

    while not included:
        oldest_time = df_1m['timestamp'].min() - timedelta(minutes=1)
        start_time_str = oldest_time.strftime("%H%M%S")

        extra_candles = request_candles(start_time_str)
        if not extra_candles:
            break

        extra_df = convert_to_df(extra_candles)
        df_1m = pd.concat([extra_df, df_1m]).drop_duplicates().sort_values('timestamp').reset_index(drop=True)
        df_10m = resample_to_10min(df_1m)
        candle_list = extra_candles + candle_list
        included = any(df_10m['timestamp'] == base_candle_start)

    return candle_list

# 잔고정보 처리 (balance_map 반환)
def balance_proc(access_token, app_key, app_secret, acct_no):
    b = stock_balance(access_token, app_key, app_secret, acct_no, "all")

    for i, name in enumerate(b.index):
        u_tot_evlu_amt = int(b['tot_evlu_amt'][i])
        u_dnca_tot_amt = int(b['dnca_tot_amt'][i])
        u_nass_amt = int(b['nass_amt'][i])
        u_prvs_rcdl_excc_amt = int(b['prvs_rcdl_excc_amt'][i])
        u_scts_evlu_amt = int(b['scts_evlu_amt'][i])
        u_asst_icdc_amt = int(b['asst_icdc_amt'][i])

    cur100 = conn.cursor()
    cur100.execute("""
        SELECT asset_num, cash_rate, tot_evlu_amt, prvs_rcdl_excc_amt, sell_plan_amt,
               (SELECT (risk_sum / item_number)::int FROM public."stockMarketMng_stock_market_mng"
                WHERE acct_no = A.acct_no AND aply_end_dt = '99991231') AS risk_amt
        FROM "stockFundMng_stock_fund_mng" A
        WHERE acct_no = %s
    """, (str(acct_no),))
    result_one00 = cur100.fetchall()
    cur100.close()

    asset_num = 0
    sell_plan_amt = 0
    risk_amt = 0

    for i in result_one00:
        asset_num = i[0]
        sell_plan_amt = i[4]
        risk_amt = i[5]

    cur200 = conn.cursor()
    update_query200 = "update \"stockFundMng_stock_fund_mng\" set tot_evlu_amt = %s, dnca_tot_amt = %s, prvs_rcdl_excc_amt = %s, nass_amt = %s, scts_evlu_amt = %s, asset_icdc_amt = %s, last_chg_date = %s where asset_num = %s and acct_no = %s"
    record_to_update200 = ([u_tot_evlu_amt, u_dnca_tot_amt, u_prvs_rcdl_excc_amt, u_nass_amt, u_scts_evlu_amt, u_asst_icdc_amt, datetime.now(), asset_num, acct_no])
    cur200.execute(update_query200, record_to_update200)
    conn.commit()
    cur200.close()

    c = stock_balance(access_token, app_key, app_secret, acct_no, "")

    cur300 = conn.cursor()
    update_query300 = "update \"stockBalance_stock_balance\" set proc_yn = 'N', last_chg_date = %s where acct_no = %s and proc_yn = 'Y'"
    record_to_update300 = ([datetime.now(), acct_no])
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
                    limit_amt = int(str(row[0]).strip())
                    limit_price = int((e_purchase_sum + limit_amt) / e_purchase_amount)
                except (ValueError, TypeError):
                    limit_price = int((e_purchase_sum - int(risk_amt)) / e_purchase_amount)
            else:
                limit_price = int((e_purchase_sum - int(risk_amt)) / e_purchase_amount)

        balance_list.append({
            '계좌번호': str(acct_no),
            '종목코드': e_code,
            '종목명': e_name,
            '보유단가': e_purchase_price,
            '보유수량': e_purchase_amount,
            '현재가': e_current_price,
        })

        if sell_plan_amt > 0:
            item_eval_gravity = e_eval_sum / u_tot_evlu_amt * 100
            e_sell_plan_sum = sell_plan_amt * item_eval_gravity * 0.01
            e_sell_plan_amount = e_sell_plan_sum / e_current_price

            cur301 = conn.cursor()
            update_query301 = "with upsert as (update \"stockBalance_stock_balance\" set purchase_price = %s, purchase_amount = %s, purchase_sum = %s, current_price = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, sell_plan_sum = %s, sell_plan_amount = %s, avail_amount = %s, limit_price = %s, proc_yn = 'Y', last_chg_date = %s where acct_no = %s and code = %s and asset_num = %s returning * ) insert into \"stockBalance_stock_balance\"(acct_no, code, name, purchase_price, purchase_amount, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, asset_num, sell_plan_sum, sell_plan_amount, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, limit_price, proc_yn, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select A.sign_resist_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_resist_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.sign_support_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_support_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_loss_price from(select row_number() over(order by last_chg_date desc) as num, b.end_loss_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_target_price from(select row_number() over(order by last_chg_date desc) as num, b.end_target_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.trading_plan from(select row_number() over(order by last_chg_date desc) as num, b.trading_plan from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.limit_price from (select row_number() over(order by last_chg_date desc) as num, b.limit_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num = 1), %s, %s where not exists(select * from upsert)"
            record_to_update301 = ([e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, e_sell_plan_sum, e_sell_plan_amount, e_ord_psbl_qty, limit_price, datetime.now(), acct_no, e_code, asset_num,
                                    acct_no, e_code, e_name, e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, asset_num, e_sell_plan_sum, e_sell_plan_amount,
                                    acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code,
                                    'Y', datetime.now()])
            cur301.execute(update_query301, record_to_update301)
            conn.commit()
            cur301.close()

        else:
            cur301 = conn.cursor()
            update_query301 = "with upsert as (update \"stockBalance_stock_balance\" set purchase_price = %s, purchase_amount = %s, purchase_sum = %s, current_price = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, avail_amount = %s, limit_price = %s, proc_yn = 'Y', last_chg_date = %s where acct_no = %s and code = %s and asset_num = %s returning * ) insert into \"stockBalance_stock_balance\"(acct_no, code, name, purchase_price, purchase_amount, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, asset_num, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, limit_price, proc_yn, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select A.sign_resist_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_resist_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.sign_support_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_support_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_loss_price from(select row_number() over(order by last_chg_date desc) as num, b.end_loss_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_target_price from(select row_number() over(order by last_chg_date desc) as num, b.end_target_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.trading_plan from(select row_number() over(order by last_chg_date desc) as num, b.trading_plan from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.limit_price from (select row_number() over(order by last_chg_date desc) as num, b.limit_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where	A.num = 1), %s, %s where not exists(select * from upsert)"
            record_to_update301 = ([e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, e_ord_psbl_qty, limit_price, datetime.now(), acct_no, e_code, asset_num,
                                    acct_no, e_code, e_name, e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, asset_num,
                                    acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code,
                                    'Y', datetime.now()])
            cur301.execute(update_query301, record_to_update301)
            conn.commit()
            cur301.close()

    # 잔고정보 맵 설정
    balance_map = {
        (item['계좌번호'], item['종목명']): item
        for item in balance_list
    }

    # 주문체결 처리
    order_complete_proc(access_token, app_key, app_secret, acct_no, balance_map)


# 주문체결정보 처리 (NXT 시간대에서도 단독 호출 가능)
def order_complete_proc(access_token, app_key, app_secret, acct_no, balance_map=None):
    if balance_map is None:
        balance_map = {}

    today_str = datetime.now().strftime('%Y%m%d')

    order_complete_output = get_my_complete(access_token, app_key, app_secret, acct_no, "", "")

    order_complete_list = []

    if order_complete_output:

        for item in order_complete_output:
            odno = item['odno']
            orgn_odno = item['orgn_odno']
            pfls_rate = 0
            pfls_amt = 0
            paid_tax = 0
            paid_fee = 0

            if float(item['tot_ccld_qty']) > 0:

                cur302 = conn.cursor()
                cur302.execute("""
                    SELECT paid_fee, profit_loss_amt, paid_tax
                    FROM "stockOrderComplete_stock_order_complete" A
                    WHERE acct_no = %s AND name = %s AND order_dt = %s AND total_complete_qty::int > 0
                """, (str(acct_no), item['prdt_name'], today_str))
                result_one32 = cur302.fetchall()
                cur302.close()

                if len(result_one32) > 0:
                    last_paid_fee = 0
                    last_pfls_amt = 0
                    last_paid_tax = 0

                    for comp_info in result_one32:
                        if comp_info[0] != None:
                            last_paid_fee += int(comp_info[0])
                        if comp_info[1] != None:
                            last_pfls_amt += int(comp_info[1])
                        if comp_info[2] != None:
                            last_paid_tax += int(comp_info[2])

                    period_profit_loss_sum_output = inquire_period_profit_loss(access_token, app_key, app_secret, item['pdno'], today_str, today_str, acct_no)

                    for item2 in period_profit_loss_sum_output:
                        pfls_rate = float(item2['pfls_rt'])
                        pfls_amt = float(item2['rlzt_pfls']) - last_pfls_amt
                        paid_tax = float(item2['tl_tax']) - last_paid_tax
                        paid_fee = float(item2['fee']) - last_paid_fee

                else:
                    period_profit_loss_sum_output = inquire_period_profit_loss(access_token, app_key, app_secret, item['pdno'], today_str, today_str, acct_no)

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
    cur400.execute("""
        SELECT
            order_no, org_order_no, total_complete_qty, remain_qty
        FROM "stockOrderComplete_stock_order_complete"
        WHERE acct_no = %s
        AND order_dt = %s
    """, (str(acct_no), today_str))
    result_400 = cur400.fetchall()
    cur400.close()

    order_commplete_map = {
        (str(acct_no), today_str, str(int(row[0])), str(int(row[1])) if row[1] != "" else ""): (int(row[2]), int(row[3]))
        for row in result_400
    }

    for item in order_complete_list:
        key1 = (item['계좌번호'], item['종목명'])

        if key1 in balance_map:
            item['보유단가'] = balance_map[key1]['보유단가']
            item['보유수량'] = balance_map[key1]['보유수량']
        else:
            item['보유단가'] = 0
            item['보유수량'] = 0

        key2 = (str(item['계좌번호']), item['주문일자'], str(int(item['주문번호'])), str(int(item['원주문번호'])) if item['원주문번호'] != "" else "")
        new_complete_qty = int(item['체결수량'])
        new_remain_qty = int(item['잔여수량'])

        cur600 = conn.cursor()

        if key2 not in order_commplete_map:
            if float(item['보유단가']) > 0 and int(item['보유수량']) > 0:
                cur600.execute("""
                    INSERT INTO "stockOrderComplete_stock_order_complete" (
                        acct_no, order_dt, order_tmd, name, order_no, org_order_no,
                        total_complete_amt, order_type, order_price, order_amount,
                        total_complete_qty, remain_qty, hold_price, hold_vol,
                        profit_loss_rate, profit_loss_amt, paid_tax, paid_fee, last_chg_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                """, (
                    acct_no, item['주문일자'], item['주문시각'], item['종목명'],
                    str(int(item['주문번호'])),
                    str(int(item['원주문번호'])) if item['원주문번호'] != "" else "",
                    int(item['체결금액']), item['주문유형'],
                    int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']),
                    int(item['주문수량']), new_complete_qty, new_remain_qty,
                    item['보유단가'], item['보유수량'],
                    Decimal(item['pfls_rate']), int(item['pfls_amt']),
                    int(item['paid_tax']), int(item['paid_fee'])
                ))

                # cur402 = conn.cursor()
                # cur402.execute("select code, order_type, tr_proc, sh_trading_num from short_trading_detail where acct_no = %s and tr_day = %s and order_no = %s", (str(acct_no), item['주문일자'], str(int(item['주문번호']))))
                # result_one01 = cur402.fetchone()
                # cur402.close()

                # if result_one01:
                #     cur403 = conn.cursor()
                #     cur403.execute("""
                #         UPDATE short_trading_detail
                #         SET total_complete_qty = %s, remain_qty = %s, hold_price = %s, hold_vol = %s,
                #             profit_loss_rate = %s, profit_loss_amt = %s, chgr_id = %s, chg_date = %s
                #         WHERE acct_no = %s AND tr_day = %s AND order_no = %s
                #     """, (
                #         new_complete_qty, new_remain_qty, item['보유단가'], item['보유수량'],
                #         Decimal(item['pfls_rate']), int(item['pfls_amt']),
                #         'holding_item', datetime.now(),
                #         str(acct_no), item['주문일자'], str(int(item['주문번호']))
                #     ))
                #     cur403.close()

                # elif item['원주문번호'] != "":
                #     cur4021 = conn.cursor()
                #     cur4021.execute("select code, order_type, tr_proc, sh_trading_num from short_trading_detail where acct_no = %s and tr_day = %s and order_no = %s", (str(acct_no), item['주문일자'], str(int(item['원주문번호']))))
                #     result_one02 = cur4021.fetchone()
                #     cur4021.close()

                #     if result_one02:
                #         cur403 = conn.cursor()
                #         cur403.execute("""
                #             UPDATE short_trading_detail
                #             SET order_price = %s, tr_qty = %s, tr_amt = %s,
                #                 total_complete_qty = %s, remain_qty = %s, hold_price = %s, hold_vol = %s,
                #                 profit_loss_rate = %s, profit_loss_amt = %s,
                #                 order_no = %s, org_order_no = %s, chgr_id = %s, chg_date = %s
                #             WHERE acct_no = %s AND tr_day = %s AND order_no = %s
                #         """, (
                #             int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']),
                #             int(item['주문수량']), int(item['체결금액']),
                #             new_complete_qty, new_remain_qty, item['보유단가'], item['보유수량'],
                #             Decimal(item['pfls_rate']), int(item['pfls_amt']),
                #             str(int(item['주문번호'])), str(int(item['원주문번호'])),
                #             'holding_item', datetime.now(),
                #             str(acct_no), item['주문일자'], str(int(item['원주문번호']))
                #         ))
                #         cur403.close()

                # order_type = item.get('주문유형', '')

                # if new_complete_qty > 0 and '매도' in order_type:

                #     cur404 = conn.cursor()
                #     cur404.execute("select sh_trading_num, name, code, tr_day, tr_dtm, order_price, total_complete_qty from short_trading_detail where acct_no = %s and name = %s and order_type like %s and total_complete_qty::int > 0 and tr_proc is null", (str(acct_no), item['종목명'], '%매수%'))
                #     result_one404 = cur404.fetchall()
                #     cur404.close()

                #     cur405 = conn.cursor()
                #     for item404 in result_one404:
                #         sh_trading_num = item404[0]
                #         if int(item['체결단가']) > 0:
                #             tr_proc = "SM" if int(item404[5]) < int(item['체결단가']) else "LC"
                #         else:
                #             tr_proc = "SM" if int(item404[5]) < int(item['주문단가']) else "LC"

                #         update_query404 = "UPDATE short_trading_detail SET tr_proc = %s, chgr_id = %s, chg_date = %s WHERE acct_no = %s AND name = %s AND order_type LIKE %s AND total_complete_qty::int > 0 AND sh_trading_num = %s"
                #         record_to_update404 = ([tr_proc, 'tr_proc', datetime.now(), str(acct_no), item['종목명'], '%매수%', sh_trading_num])
                #         cur405.execute(update_query404, record_to_update404)

                #     cur405.close()

                #     cur406 = conn.cursor()
                #     cur406.execute("select COALESCE(safe_margin_sum, 0) from \"stockBalance_stock_balance\" where acct_no = %s and name = %s and proc_yn = 'Y'", (acct_no, item['종목명']))
                #     result_one406 = cur406.fetchall()
                #     cur406.close()

                #     cur407 = conn.cursor()
                #     safe_margin_sum = 0
                #     for item406 in result_one406:
                #         if int(item['체결단가']) > 0:
                #             safe_margin_sum = Decimal(item406[0]) + (Decimal(item['체결단가']) - Decimal(item['보유단가'])) * new_complete_qty
                #         else:
                #             safe_margin_sum = Decimal(item406[0]) + (Decimal(item['주문단가']) - Decimal(item['보유단가'])) * new_complete_qty

                #         update_query406 = "UPDATE \"stockBalance_stock_balance\" SET safe_margin_sum = %s WHERE acct_no = %s AND name = %s AND proc_yn = 'Y'"
                #         record_to_update406 = ([int(safe_margin_sum), acct_no, item['종목명']])
                #         cur407.execute(update_query406, record_to_update406)

                #     cur407.close()

                conn.commit()

            else:
                if new_complete_qty > 0:
                    if item['원주문번호'] != "":
                        cur401 = conn.cursor()
                        cur401.execute("""
                            SELECT hold_price FROM "stockOrderComplete_stock_order_complete" A
                            WHERE acct_no = %s AND order_no = %s AND order_dt = %s
                        """, (str(acct_no), item['원주문번호'], today_str))
                        result_one41 = cur401.fetchone()
                        cur401.close()

                        if result_one41:
                            hold_price = float(result_one41[0])
                        else:
                            hold_price = 0
                    else:
                        hold_price = round((int(item['체결금액']) - int(item['pfls_amt']) - int(item['paid_tax']) - int(item['paid_fee'])) / new_complete_qty)

                    cur600.execute("""
                        INSERT INTO "stockOrderComplete_stock_order_complete" (
                            acct_no, order_dt, order_tmd, name, order_no, org_order_no,
                            total_complete_amt, order_type, order_price, order_amount,
                            total_complete_qty, remain_qty, hold_price, hold_vol,
                            profit_loss_rate, profit_loss_amt, paid_tax, paid_fee, last_chg_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    """, (
                        acct_no, item['주문일자'], item['주문시각'], item['종목명'],
                        str(int(item['주문번호'])),
                        str(int(item['원주문번호'])) if item['원주문번호'] != "" else "",
                        int(item['체결금액']), item['주문유형'],
                        int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']),
                        int(item['주문수량']), new_complete_qty, new_remain_qty,
                        hold_price, new_complete_qty,
                        Decimal(item['pfls_rate']), int(item['pfls_amt']),
                        int(item['paid_tax']), int(item['paid_fee'])
                    ))

                    # cur402 = conn.cursor()
                    # cur402.execute("select code, order_type, tr_proc, sh_trading_num from short_trading_detail where acct_no = %s and tr_day = %s and order_no = %s", (str(acct_no), item['주문일자'], str(int(item['주문번호']))))
                    # result_one01 = cur402.fetchone()
                    # cur402.close()

                    # if result_one01:
                    #     cur403 = conn.cursor()
                    #     cur403.execute("""
                    #         UPDATE short_trading_detail
                    #         SET total_complete_qty = %s, remain_qty = %s, hold_price = %s, hold_vol = %s,
                    #             profit_loss_rate = %s, profit_loss_amt = %s, chgr_id = %s, chg_date = %s
                    #         WHERE acct_no = %s AND tr_day = %s AND order_no = %s
                    #     """, (
                    #         new_complete_qty, new_remain_qty, hold_price, new_complete_qty,
                    #         Decimal(item['pfls_rate']), int(item['pfls_amt']),
                    #         'holding_item', datetime.now(),
                    #         str(acct_no), item['주문일자'], str(int(item['주문번호']))
                    #     ))
                    #     cur403.close()

                    # elif item['원주문번호'] != "":
                    #     cur4021 = conn.cursor()
                    #     cur4021.execute("select code, order_type, tr_proc, sh_trading_num from short_trading_detail where acct_no = %s and tr_day = %s and order_no = %s", (str(acct_no), item['주문일자'], str(int(item['원주문번호']))))
                    #     result_one02 = cur4021.fetchone()
                    #     cur4021.close()

                    #     if result_one02:
                    #         cur403 = conn.cursor()
                    #         cur403.execute("""
                    #             UPDATE short_trading_detail
                    #             SET order_price = %s, tr_qty = %s, tr_amt = %s,
                    #                 total_complete_qty = %s, remain_qty = %s, hold_price = %s, hold_vol = %s,
                    #                 profit_loss_rate = %s, profit_loss_amt = %s,
                    #                 order_no = %s, org_order_no = %s, chgr_id = %s, chg_date = %s
                    #             WHERE acct_no = %s AND tr_day = %s AND order_no = %s
                    #         """, (
                    #             int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']),
                    #             int(item['주문수량']), int(item['체결금액']),
                    #             new_complete_qty, new_remain_qty, hold_price, new_complete_qty,
                    #             Decimal(item['pfls_rate']), int(item['pfls_amt']),
                    #             str(int(item['주문번호'])), str(int(item['원주문번호'])),
                    #             'holding_item', datetime.now(),
                    #             str(acct_no), item['주문일자'], str(int(item['원주문번호']))
                    #         ))
                    #         cur403.close()

                    # order_type = item.get('주문유형', '')

                    # if '매도' in order_type:

                    #     cur404 = conn.cursor()
                    #     cur404.execute("select sh_trading_num, name, code, tr_day, tr_dtm, order_price, total_complete_qty from short_trading_detail where acct_no = %s and name = %s and order_type like %s and total_complete_qty::int > 0 and tr_proc is null", (str(acct_no), item['종목명'], '%매수%'))
                    #     result_one404 = cur404.fetchall()
                    #     cur404.close()

                    #     cur405 = conn.cursor()
                    #     for item404 in result_one404:
                    #         sh_trading_num = item404[0]
                    #         if int(item['체결단가']) > 0:
                    #             tr_proc = "SM" if int(item404[5]) < int(item['체결단가']) else "LC"
                    #         else:
                    #             tr_proc = "SM" if int(item404[5]) < int(item['주문단가']) else "LC"

                    #         update_query404 = "UPDATE short_trading_detail SET tr_proc = %s, chgr_id = %s, chg_date = %s WHERE acct_no = %s AND name = %s AND order_type LIKE %s AND total_complete_qty::int > 0 AND sh_trading_num = %s"
                    #         record_to_update404 = ([tr_proc, 'tr_proc', datetime.now(), str(acct_no), item['종목명'], '%매수%', sh_trading_num])
                    #         cur405.execute(update_query404, record_to_update404)

                    #     cur405.close()

                    #     cur406 = conn.cursor()
                    #     cur406.execute("select COALESCE(safe_margin_sum, 0) from \"stockBalance_stock_balance\" where acct_no = %s and name = %s and proc_yn = 'Y'", (acct_no, item['종목명']))
                    #     result_one406 = cur406.fetchall()
                    #     cur406.close()

                    #     cur407 = conn.cursor()
                    #     safe_margin_sum = 0
                    #     for item406 in result_one406:
                    #         if int(item['체결단가']) > 0:
                    #             safe_margin_sum = Decimal(item406[0]) + (Decimal(item['체결단가']) - Decimal(item['보유단가'])) * new_complete_qty
                    #         else:
                    #             safe_margin_sum = Decimal(item406[0]) + (Decimal(item['주문단가']) - Decimal(item['보유단가'])) * new_complete_qty

                    #         update_query406 = "UPDATE \"stockBalance_stock_balance\" SET safe_margin_sum = %s WHERE acct_no = %s AND name = %s AND proc_yn = 'Y'"
                    #         record_to_update406 = ([int(safe_margin_sum), acct_no, item['종목명']])
                    #         cur407.execute(update_query406, record_to_update406)

                    #     cur407.close()

                    conn.commit()

        else:
            old_complete_qty, old_remain_qty = order_commplete_map[key2]
            if new_complete_qty != int(old_complete_qty) or new_remain_qty != int(old_remain_qty):

                if float(item['보유단가']) > 0 and int(item['보유수량']) > 0:
                    cur600.execute("""
                        UPDATE "stockOrderComplete_stock_order_complete"
                        SET order_price = %s, total_complete_qty = %s, remain_qty = %s,
                            total_complete_amt = %s, hold_price = %s, hold_vol = %s,
                            profit_loss_rate = %s, profit_loss_amt = %s,
                            paid_tax = %s, paid_fee = %s, last_chg_date = now()
                        WHERE acct_no = %s AND order_dt = %s AND order_no = %s
                    """, (
                        int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']),
                        new_complete_qty, new_remain_qty, int(item['체결금액']),
                        item['보유단가'], item['보유수량'],
                        Decimal(item['pfls_rate']), int(item['pfls_amt']),
                        int(item['paid_tax']), int(item['paid_fee']),
                        acct_no, item['주문일자'], str(int(item['주문번호']))
                    ))
                else:
                    cur600.execute("""
                        UPDATE "stockOrderComplete_stock_order_complete"
                        SET order_price = %s, total_complete_qty = %s, remain_qty = %s,
                            total_complete_amt = %s, profit_loss_rate = %s, profit_loss_amt = %s,
                            paid_tax = %s, paid_fee = %s, last_chg_date = now()
                        WHERE acct_no = %s AND order_dt = %s AND order_no = %s
                    """, (
                        int(item['체결단가']) if int(item['체결단가']) > 0 else int(item['주문단가']),
                        new_complete_qty, new_remain_qty, int(item['체결금액']),
                        Decimal(item['pfls_rate']), int(item['pfls_amt']),
                        int(item['paid_tax']), int(item['paid_fee']),
                        acct_no, item['주문일자'], str(int(item['주문번호']))
                    ))

                # cur402 = conn.cursor()
                # cur402.execute("select code, order_type, tr_proc, sh_trading_num from short_trading_detail where acct_no = %s and tr_day = %s and order_no = %s", (str(acct_no), item['주문일자'], str(int(item['주문번호']))))
                # result_one01 = cur402.fetchone()
                # cur402.close()

                # if result_one01:
                #     cur403 = conn.cursor()

                #     if float(item['보유단가']) > 0 and int(item['보유수량']) > 0:
                #         cur403.execute("""
                #             UPDATE short_trading_detail
                #             SET total_complete_qty = %s, remain_qty = %s, hold_price = %s, hold_vol = %s,
                #                 profit_loss_rate = %s, profit_loss_amt = %s, chgr_id = %s, chg_date = %s
                #             WHERE acct_no = %s AND tr_day = %s AND order_no = %s
                #         """, (
                #             new_complete_qty, new_remain_qty, item['보유단가'], item['보유수량'],
                #             Decimal(item['pfls_rate']), int(item['pfls_amt']),
                #             'holding_item', datetime.now(),
                #             str(acct_no), item['주문일자'], str(int(item['주문번호']))
                #         ))
                #     else:
                #         cur403.execute("""
                #             UPDATE short_trading_detail
                #             SET total_complete_qty = %s, remain_qty = %s,
                #                 profit_loss_rate = %s, profit_loss_amt = %s, chgr_id = %s, chg_date = %s
                #             WHERE acct_no = %s AND tr_day = %s AND order_no = %s
                #         """, (
                #             new_complete_qty, new_remain_qty,
                #             Decimal(item['pfls_rate']), int(item['pfls_amt']),
                #             'holding_item', datetime.now(),
                #             str(acct_no), item['주문일자'], str(int(item['주문번호']))
                #         ))

                #     cur403.close()

                # order_type = item.get('주문유형', '')

                # if '매도' in order_type:

                #     cur404 = conn.cursor()
                #     cur404.execute("select sh_trading_num, name, code, tr_day, tr_dtm, order_price, total_complete_qty from short_trading_detail where acct_no = %s and name = %s and order_type like %s and total_complete_qty::int > 0 and tr_proc is null", (str(acct_no), item['종목명'], '%매수%'))
                #     result_one404 = cur404.fetchall()
                #     cur404.close()

                #     cur405 = conn.cursor()
                #     for item404 in result_one404:
                #         sh_trading_num = item404[0]
                #         if int(item['체결단가']) > 0:
                #             tr_proc = "SM" if int(item404[5]) < int(item['체결단가']) else "LC"
                #         else:
                #             tr_proc = "SM" if int(item404[5]) < int(item['주문단가']) else "LC"

                #         update_query404 = "UPDATE short_trading_detail SET tr_proc = %s, chgr_id = %s, chg_date = %s WHERE acct_no = %s AND name = %s AND order_type LIKE %s AND total_complete_qty::int > 0 AND sh_trading_num = %s"
                #         record_to_update404 = ([tr_proc, 'tr_proc', datetime.now(), str(acct_no), item['종목명'], '%매수%', sh_trading_num])
                #         cur405.execute(update_query404, record_to_update404)

                #     cur405.close()

                #     cur406 = conn.cursor()
                #     cur406.execute("select COALESCE(safe_margin_sum, 0) from \"stockBalance_stock_balance\" where acct_no = %s and name = %s and proc_yn = 'Y'", (acct_no, item['종목명']))
                #     result_one406 = cur406.fetchall()
                #     cur406.close()

                #     cur407 = conn.cursor()
                #     safe_margin_sum = 0
                #     for item406 in result_one406:
                #         if int(item['체결단가']) > 0:
                #             safe_margin_sum = Decimal(item406[0]) + (Decimal(item['체결단가']) - Decimal(item['보유단가'])) * new_complete_qty
                #         else:
                #             safe_margin_sum = Decimal(item406[0]) + (Decimal(item['주문단가']) - Decimal(item['보유단가'])) * new_complete_qty

                #         update_query406 = "UPDATE \"stockBalance_stock_balance\" SET safe_margin_sum = %s WHERE acct_no = %s AND name = %s AND proc_yn = 'Y'"
                #         record_to_update406 = ([int(safe_margin_sum), acct_no, item['종목명']])
                #         cur407.execute(update_query406, record_to_update406)

                #     cur407.close()

                conn.commit()

        cur600.close()


def main(telegram_text):
    chat_id = "2147256258"
    bot = telegram.Bot(token=token)
    bot.send_message(chat_id=chat_id, text=telegram_text, parse_mode='HTML')


# 휴일정보 조회
cur0 = conn.cursor()
cur0.execute("SELECT name FROM stock_holiday WHERE holiday = %s", (today,))
result_one = cur0.fetchone()
cur0.close()

nickname_list = ['chichipa', 'phills2', 'phills75', 'yh480825', 'phills13', 'phills15', 'mamalong', 'honeylong', 'worry106']

# 휴일이 아닌 경우
if result_one == None:

    for nick in nickname_list:
        try:
            ac = account(nick)
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']
            token = ac['bot_token1']

            cur_time = datetime.now().strftime("%H%M")

            if '0800' <= cur_time < '0900':
                # NXT 장전 : 주문체결 처리만 수행
                order_complete_proc(access_token, app_key, app_secret, acct_no)

            elif '0900' <= cur_time < '1530':
                # KRX 정규시장 : 잔고정보 처리(내부에서 주문체결 처리 포함) + trail signal 처리
                balance_proc(access_token, app_key, app_secret, acct_no)

                # 보유정보 조회
                cur03 = conn.cursor()
                cur03.execute("""
                    SELECT code, name, sign_resist_price, sign_support_price, end_target_price, end_loss_price, purchase_amount,
                        (SELECT 1 FROM trail_signal_recent WHERE acct_no = %s AND trail_day = TO_CHAR(now(), 'YYYYMMDD') AND code = '0001' AND trail_signal_code = '02') AS market_dead,
                        (SELECT 1 FROM trail_signal_recent WHERE acct_no = %s AND trail_day = TO_CHAR(now(), 'YYYYMMDD') AND code = '0001' AND trail_signal_code = '04') AS market_over,
                        CASE WHEN cast(A.purchase_amount AS INTEGER) > 0
                             THEN (SELECT B.low_price FROM dly_stock_balance B WHERE A.code = B.code AND A.acct_no = cast(B.acct AS INTEGER) AND B.dt = TO_CHAR(get_previous_business_day(now()::date), 'YYYYMMDD'))
                             ELSE null END AS low_price,
                        (SELECT 1 FROM trail_signal_recent WHERE acct_no = %s AND trail_day = TO_CHAR(now(), 'YYYYMMDD') AND code = A.code AND trail_signal_code = '07') AS regist_over,
                        (SELECT 1 FROM trail_signal_recent WHERE acct_no = %s AND trail_day = TO_CHAR(now(), 'YYYYMMDD') AND code = A.code AND trail_signal_code = '09') AS target_over,
                        COALESCE(NULLIF(trading_plan, ''), 'as'), COALESCE(safe_margin_sum, 0)
                    FROM "stockBalance_stock_balance" A
                    WHERE acct_no = %s AND proc_yn = 'Y' AND (trading_plan IS NULL OR trading_plan NOT IN ('i'))
                """, (str(acct_no), str(acct_no), str(acct_no), str(acct_no), str(acct_no)))
                result_three = cur03.fetchall()
                cur03.close()

                for i in result_three:
                    a = ""
                    try:
                        time.sleep(0.3)
                        a = inquire_price(access_token, app_key, app_secret, i[0])
                    except Exception as ex:
                        print(f"현재가 시세 에러 : [{i[0]}] {ex}")
                    if not a:
                        continue

                    trail_signal_code = ""
                    trail_signal_name = ""
                    sell_plan_amount = ""
                    n_sell_amount = 0
                    n_sell_sum = 0
                    if i[2] != None:
                        if i[2] > 0:
                            if int(a['stck_prpr']) > i[2]:
                                trail_signal_code = "07"
                                trail_signal_name = format(int(i[2]), ',d') + "원 {저항가 돌파}"

                                if i[6] != None:
                                    n_sell_amount = i[6]
                                    n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                                    sell_plan_amount = format(int(n_sell_amount), ',d')

                    if i[3] != None:
                        if i[3] > 0:
                            if int(a['stck_prpr']) < i[3]:
                                trail_signal_code = "08"
                                trail_signal_name = format(int(i[3]), ',d') + "원 {지지가 이탈}"

                                if i[6] != None:
                                    n_sell_amount = i[6]
                                    n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                                    sell_plan_amount = format(int(n_sell_amount), ',d')

                    if i[4] != None:
                        if i[4] > 0:
                            if int(a['stck_hgpr']) > i[4]:
                                trail_signal_code = "09"
                                trail_signal_name = format(int(i[4]), ',d') + "원 {최종목표가 돌파}"

                                if i[6] != None:
                                    n_sell_amount = i[6]
                                    n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                                    sell_plan_amount = format(int(n_sell_amount), ',d')

                    if i[5] != None:
                        if i[5] > 0:
                            if int(a['stck_lwpr']) < i[5]:
                                trail_signal_code = "10"
                                trail_signal_name = format(int(i[5]), ',d') + "원 {최종이탈가 이탈}"

                                if i[6] != None:
                                    n_sell_amount = i[6]
                                    n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                                    sell_plan_amount = format(int(n_sell_amount), ',d')

                    if i[7] != None:
                        trail_signal_code = "11"
                        trail_signal_name = "시장 지지선 이탈[지지가 : " + format(int(i[3]), ',d') + "원]"

                        if i[6] != None:
                            n_sell_amount = i[6]
                            n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                            sell_plan_amount = format(int(n_sell_amount), ',d')

                    if i[8] != None:
                        trail_signal_code = "12"
                        trail_signal_name = "시장 추세선 이탈[지지가 : " + format(int(i[3]), ',d') + "원]"

                        if i[6] != None:
                            n_sell_amount = i[6]
                            n_sell_sum = int(a['stck_prpr']) * n_sell_amount
                            sell_plan_amount = format(int(n_sell_amount), ',d')

                    # if cur_time > '1510':
                    #     if i[9] != None:
                    #         if int(a['stck_prpr']) < i[9]:
                    #             if i[7] != None:
                    #                 trail_signal_code = "13"
                    #                 trail_signal_name = "시장 이탈하고 전일 저가 " + format(int(i[9]), ',d') +"원 이탈"

                    #                 trading_plan_dic = {"as":"100", "66s":"66", "50s":"50", "33s":"33", "25s":"25", "20s":"20"}

                    #                 for key, value in trading_plan_dic.items():
                    #                     if key == i[11]:
                    #                         sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, i[0])

                    #                 c = stock_balance(access_token, app_key, app_secret, acct_no, "")
                    #                 result_msgs = []

                    #                 for j, name in enumerate(c.index):
                    #                     J_code = c['pdno'][j]
                    #                     j_hldg_qty = int(c['hldg_qty'][j])
                    #                     j_ord_psbl_qty = int(c['ord_psbl_qty'][j])

                    #                     sell_rate = 0
                    #                     sell_amount = 0
                    #                     sell_sum = 0
                    #                     sell_price = 0

                    #                     if J_code == i[0]:
                    #                         sell_price = int(a['stck_oprc'])

                    #                         for key, value in trading_plan_dic.items():
                    #                             if key == i[11]:
                    #                                 sell_rate = int(value)

                    #                         if sell_rate > 0:
                    #                             n_sell_amount = round(j_ord_psbl_qty * (sell_rate / 100))
                    #                             n_sell_sum = n_sell_amount * sell_price
                    #                             sell_plan_amount = format(int(n_sell_amount), ',d')

                    #                             if j_ord_psbl_qty > 0:
                    #                                 try:
                    #                                     c = order_cash(False, access_token, app_key, app_secret, str(acct_no), J_code, "00", str(n_sell_amount), str(sell_price))

                    #                                     if c['ODNO'] != "":
                    #                                         output1 = get_my_complete(access_token, app_key, app_secret, acct_no, J_code, c['ODNO'])
                    #                                         tdf = pd.DataFrame(output1)
                    #                                         tdf.set_index('odno')
                    #                                         d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                    #                                         for k, name in enumerate(d.index):
                    #                                             d_order_no = int(d['odno'][k])
                    #                                             d_order_type = d['sll_buy_dvsn_cd_name'][k]
                    #                                             d_order_dt = d['ord_dt'][k]
                    #                                             d_order_tmd = d['ord_tmd'][k]
                    #                                             d_name = d['prdt_name'][k]
                    #                                             d_order_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                    #                                             d_order_amount = d['ord_qty'][k]
                    #                                             d_total_complete_qty = d['tot_ccld_qty'][k]
                    #                                             d_remain_qty = d['rmn_qty'][k]
                    #                                             d_total_complete_amt = d['tot_ccld_amt'][k]

                    #                                             print("매도주문 완료")
                    #                                             msg = f"[시장 이탈하고 전일 저가 이탈 시작가 자동처리 매도-{d_name}] 매도가 : {int(d_order_price):,}원, 매도체결량 : {int(d_total_complete_qty):,}주, 매도체결금액 : {int(d_total_complete_amt):,}원 주문 완료, 주문번호 : <code>{d_order_no}</code>"
                    #                                             result_msgs.append(msg)

                    #                                         cur13 = conn.cursor()
                    #                                         update_query13 = "UPDATE \"stockBalance_stock_balance\" A SET trading_plan = 'h' where acct_no = %s and proc_yn = 'Y' and code = %s"
                    #                                         record_to_update404 = ([str(acct_no), i[0]])
                    #                                         cur13.execute(update_query13, record_to_update404)
                    #                                         cur13.close()
                    #                                         conn.commit()

                    #                                     else:
                    #                                         print("매도주문 실패")
                    #                                         msg = f"[시장 이탈하고 전일 저가 이탈 시작가 자동처리 매도-{i[1]}] 매도가 : {int(sell_price):,}원, 매도량 : {int(n_sell_amount):,}주 매도주문 실패"
                    #                                         result_msgs.append(msg)

                    #                                 except Exception as e:
                    #                                     print('매도주문 오류.', e)
                    #                                     msg = f"[시장 이탈하고 전일 저가 이탈 시작가 자동처리 매도-{i[1]}] 매도가 : {int(sell_price):,}원, 매도량 : {int(n_sell_amount):,}주 [매도주문 오류] - {str(e)}"
                    #                                     result_msgs.append(msg)

                    #                                 final_message = "\n".join(result_msgs) if result_msgs else "대상이 존재하지 않습니다."
                    #                                 main(final_message)

                    #             elif i[8] != None:
                    #                 trail_signal_code = "14"
                    #                 trail_signal_name = "시장 지지선 이탈하고 전일 저가 " + format(int(i[9]), ',d') +"원 이탈"

                    #                 trading_plan_dic = {"as":"100", "66s":"66", "50s":"50", "33s":"33", "25s":"25", "20s":"20", "1b":"66", "2b":"50", "3b":"33", "4b":"25", "5b":"20"}

                    #                 for key, value in trading_plan_dic.items():
                    #                     if key == i[11]:
                    #                         sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, i[0])

                    #                 c = stock_balance(access_token, app_key, app_secret, acct_no, "")
                    #                 result_msgs = []

                    #                 for j, name in enumerate(c.index):
                    #                     J_code = c['pdno'][j]
                    #                     j_hldg_qty = int(c['hldg_qty'][j])
                    #                     j_ord_psbl_qty = int(c['ord_psbl_qty'][j])

                    #                     sell_rate = 0
                    #                     sell_amount = 0
                    #                     sell_sum = 0
                    #                     sell_price = 0

                    #                     if J_code == i[0]:
                    #                         sell_price = int(a['stck_prpr'])

                    #                         for key, value in trading_plan_dic.items():
                    #                             if key == i[11]:
                    #                                 sell_rate = int(value)

                    #                         if sell_rate > 0:
                    #                             n_sell_amount = round(j_ord_psbl_qty * (sell_rate / 100))
                    #                             n_sell_sum = n_sell_amount * sell_price
                    #                             sell_plan_amount = format(int(n_sell_amount), ',d')

                    #                             if j_ord_psbl_qty > 0:
                    #                                 try:
                    #                                     c = order_cash(False, access_token, app_key, app_secret, str(acct_no), J_code, "00", str(n_sell_amount), str(sell_price))

                    #                                     if c['ODNO'] != "":
                    #                                         output1 = get_my_complete(access_token, app_key, app_secret, acct_no, J_code, c['ODNO'])
                    #                                         tdf = pd.DataFrame(output1)
                    #                                         tdf.set_index('odno')
                    #                                         d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                    #                                         for k, name in enumerate(d.index):
                    #                                             d_order_no = int(d['odno'][k])
                    #                                             d_order_type = d['sll_buy_dvsn_cd_name'][k]
                    #                                             d_order_dt = d['ord_dt'][k]
                    #                                             d_order_tmd = d['ord_tmd'][k]
                    #                                             d_name = d['prdt_name'][k]
                    #                                             d_order_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                    #                                             d_order_amount = d['ord_qty'][k]
                    #                                             d_total_complete_qty = d['tot_ccld_qty'][k]
                    #                                             d_remain_qty = d['rmn_qty'][k]
                    #                                             d_total_complete_amt = d['tot_ccld_amt'][k]

                    #                                             print("매도주문 완료")
                    #                                             msg = f"[시장 지지선 이탈하고 전일 저가 이탈 현재가 자동처리 매도-{d_name}] 매도가 : {int(d_order_price):,}원, 매도체결량 : {int(d_total_complete_qty):,}주, 매도체결금액 : {int(d_total_complete_amt):,}원 주문 완료, 주문번호 : <code>{d_order_no}</code>"
                    #                                             result_msgs.append(msg)

                    #                                         cur13 = conn.cursor()
                    #                                         update_query13 = "UPDATE \"stockBalance_stock_balance\" A SET trading_plan = 'h' where acct_no = %s and proc_yn = 'Y' and code = %s"
                    #                                         record_to_update404 = ([str(acct_no), i[0]])
                    #                                         cur13.execute(update_query13, record_to_update404)
                    #                                         cur13.close()
                    #                                         conn.commit()

                    #                                     else:
                    #                                         print("매도주문 실패")
                    #                                         msg = f"[시장 지지선 이탈하고 전일 저가 이탈 현재가 자동처리 매도-{i[1]}] 매도가 : {int(sell_price):,}원, 매도량 : {int(n_sell_amount):,}주 매도주문 실패"
                    #                                         result_msgs.append(msg)

                    #                                 except Exception as e:
                    #                                     print('매도주문 오류.', e)
                    #                                     msg = f"[시장 지지선 이탈하고 전일 저가 이탈 현재가 자동처리 매도-{i[1]}] 매도가 : {int(sell_price):,}원, 매도량 : {int(n_sell_amount):,}주 [매도주문 오류] - {str(e)}"
                    #                                     result_msgs.append(msg)

                    #                                 final_message = "\n".join(result_msgs) if result_msgs else "대상이 존재하지 않습니다."
                    #                                 main(final_message)

                    #             elif i[10] != None:
                    #                 trail_signal_code = "15"
                    #                 trail_signal_name = "저항가 돌파하고 전일 저가 "+format(int(i[9]), ',d') +"원 이탈"

                    #                 trading_plan_dic = {"as":"100", "66s":"66", "50s":"50", "33s":"33", "25s":"25", "20s":"20"}

                    #                 for key, value in trading_plan_dic.items():
                    #                     if key == i[11]:
                    #                         sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, i[0])

                    #                 c = stock_balance(access_token, app_key, app_secret, acct_no, "")
                    #                 result_msgs = []

                    #                 for j, name in enumerate(c.index):
                    #                     J_code = c['pdno'][j]
                    #                     j_hldg_qty = int(c['hldg_qty'][j])
                    #                     j_ord_psbl_qty = int(c['ord_psbl_qty'][j])

                    #                     sell_rate = 0
                    #                     sell_amount = 0
                    #                     sell_sum = 0
                    #                     sell_price = 0

                    #                     if J_code == i[0]:
                    #                         sell_price = int(a['stck_oprc'])

                    #                         for key, value in trading_plan_dic.items():
                    #                             if key == i[11]:
                    #                                 sell_rate = int(value)

                    #                         if sell_rate > 0:
                    #                             n_sell_amount = round(j_ord_psbl_qty * (sell_rate / 100))
                    #                             n_sell_sum = n_sell_amount * sell_price
                    #                             sell_plan_amount = format(int(n_sell_amount), ',d')

                    #                             if j_ord_psbl_qty > 0:
                    #                                 try:
                    #                                     c = order_cash(False, access_token, app_key, app_secret, str(acct_no), J_code, "00", str(n_sell_amount), str(sell_price))

                    #                                     if c['ODNO'] != "":
                    #                                         output1 = get_my_complete(access_token, app_key, app_secret, acct_no, J_code, c['ODNO'])
                    #                                         tdf = pd.DataFrame(output1)
                    #                                         tdf.set_index('odno')
                    #                                         d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                    #                                         for k, name in enumerate(d.index):
                    #                                             d_order_no = int(d['odno'][k])
                    #                                             d_order_type = d['sll_buy_dvsn_cd_name'][k]
                    #                                             d_order_dt = d['ord_dt'][k]
                    #                                             d_order_tmd = d['ord_tmd'][k]
                    #                                             d_name = d['prdt_name'][k]
                    #                                             d_order_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                    #                                             d_order_amount = d['ord_qty'][k]
                    #                                             d_total_complete_qty = d['tot_ccld_qty'][k]
                    #                                             d_remain_qty = d['rmn_qty'][k]
                    #                                             d_total_complete_amt = d['tot_ccld_amt'][k]

                    #                                             print("매도주문 완료")
                    #                                             msg = f"[저항가 돌파하고 전일 저가 이탈 시작가 자동처리 매도-{d_name}] 매도가 : {int(d_order_price):,}원, 매도체결량 : {int(d_total_complete_qty):,}주, 매도체결금액 : {int(d_total_complete_amt):,}원 주문 완료, 주문번호 : <code>{d_order_no}</code>"
                    #                                             result_msgs.append(msg)

                    #                                         cur13 = conn.cursor()
                    #                                         update_query13 = "UPDATE \"stockBalance_stock_balance\" A SET trading_plan = 'h' where acct_no = %s and proc_yn = 'Y' and code = %s"
                    #                                         record_to_update404 = ([str(acct_no), i[0]])
                    #                                         cur13.execute(update_query13, record_to_update404)
                    #                                         cur13.close()
                    #                                         conn.commit()

                    #                                     else:
                    #                                         print("매도주문 실패")
                    #                                         msg = f"[저항가 돌파하고 전일 저가 이탈 시작가 자동처리 매도-{i[1]}] 매도가 : {int(sell_price):,}원, 매도량 : {int(n_sell_amount):,}주 매도주문 실패"
                    #                                         result_msgs.append(msg)

                    #                                 except Exception as e:
                    #                                     print('매도주문 오류.', e)
                    #                                     msg = f"[저항가 돌파하고 전일 저가 이탈 시작가 자동처리 매도-{i[1]}] 매도가 : {int(sell_price):,}원, 매도량 : {int(n_sell_amount):,}주 [매도주문 오류] - {str(e)}"
                    #                                     result_msgs.append(msg)

                    #                                 final_message = "\n".join(result_msgs) if result_msgs else "대상이 존재하지 않습니다."
                    #                                 main(final_message)

                    #             elif i[11] != None:
                    #                 trail_signal_code = "16"
                    #                 trail_signal_name = "최종목표가 돌파하고 전일 저가 "+format(int(i[9]), ',d') +"원 이탈"

                    #                 trading_plan_dic = {"as":"100", "66s":"66", "50s":"50", "33s":"33", "25s":"25", "20s":"20", "1b":"66", "2b":"50", "3b":"33", "4b":"25", "5b":"20"}

                    #                 for key, value in trading_plan_dic.items():
                    #                     if key == i[11]:
                    #                         sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, i[0])

                    #                 c = stock_balance(access_token, app_key, app_secret, acct_no, "")
                    #                 result_msgs = []

                    #                 for j, name in enumerate(c.index):
                    #                     J_code = c['pdno'][j]
                    #                     j_hldg_qty = int(c['hldg_qty'][j])
                    #                     j_ord_psbl_qty = int(c['ord_psbl_qty'][j])

                    #                     sell_rate = 0
                    #                     sell_amount = 0
                    #                     sell_sum = 0
                    #                     sell_price = 0

                    #                     if J_code == i[0]:
                    #                         sell_price = int(a['stck_prpr'])

                    #                         for key, value in trading_plan_dic.items():
                    #                             if key == i[11]:
                    #                                 sell_rate = int(value)

                    #                         if sell_rate > 0:
                    #                             n_sell_amount = round(j_ord_psbl_qty * (sell_rate / 100))
                    #                             n_sell_sum = n_sell_amount * sell_price
                    #                             sell_plan_amount = format(int(n_sell_amount), ',d')

                    #                             if j_ord_psbl_qty > 0:
                    #                                 try:
                    #                                     c = order_cash(False, access_token, app_key, app_secret, str(acct_no), J_code, "00", str(n_sell_amount), str(sell_price))

                    #                                     if c['ODNO'] != "":
                    #                                         output1 = get_my_complete(access_token, app_key, app_secret, acct_no, J_code, c['ODNO'])
                    #                                         tdf = pd.DataFrame(output1)
                    #                                         tdf.set_index('odno')
                    #                                         d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                    #                                         for k, name in enumerate(d.index):
                    #                                             d_order_no = int(d['odno'][k])
                    #                                             d_order_type = d['sll_buy_dvsn_cd_name'][k]
                    #                                             d_order_dt = d['ord_dt'][k]
                    #                                             d_order_tmd = d['ord_tmd'][k]
                    #                                             d_name = d['prdt_name'][k]
                    #                                             d_order_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                    #                                             d_order_amount = d['ord_qty'][k]
                    #                                             d_total_complete_qty = d['tot_ccld_qty'][k]
                    #                                             d_remain_qty = d['rmn_qty'][k]
                    #                                             d_total_complete_amt = d['tot_ccld_amt'][k]

                    #                                             print("매도주문 완료")
                    #                                             msg = f"[최종목표가 돌파하고 전일 저가 이탈 현재가 자동처리 매도-{d_name}] 매도가 : {int(d_order_price):,}원, 매도체결량 : {int(d_total_complete_qty):,}주, 매도체결금액 : {int(d_total_complete_amt):,}원 주문 완료, 주문번호 : <code>{d_order_no}</code>"
                    #                                             result_msgs.append(msg)

                    #                                         cur13 = conn.cursor()
                    #                                         update_query13 = "UPDATE \"stockBalance_stock_balance\" A SET trading_plan = 'h' where acct_no = %s and proc_yn = 'Y' and code = %s"
                    #                                         record_to_update404 = ([str(acct_no), i[0]])
                    #                                         cur13.execute(update_query13, record_to_update404)
                    #                                         cur13.close()
                    #                                         conn.commit()

                    #                                     else:
                    #                                         print("매도주문 실패")
                    #                                         msg = f"[최종목표가 돌파하고 전일 저가 이탈 현재가 자동처리 매도-{i[1]}] 매도가 : {int(sell_price):,}원, 매도량 : {int(n_sell_amount):,}주 매도주문 실패"
                    #                                         result_msgs.append(msg)

                    #                                 except Exception as e:
                    #                                     print('매도주문 오류.', e)
                    #                                     msg = f"[최종목표가 돌파하고 전일 저가 이탈 현재가 자동처리 매도-{i[1]}] 매도가 : {int(sell_price):,}원, 매도량 : {int(n_sell_amount):,}주 [매도주문 오류] - {str(e)}"
                    #                                     result_msgs.append(msg)

                    #                                 final_message = "\n".join(result_msgs) if result_msgs else "대상이 존재하지 않습니다."
                    #                                 main(final_message)

                    cur04 = conn.cursor()
                    cur04.execute("""
                        SELECT TS.trail_signal_code, TS.trail_time FROM trail_signal TS
                        WHERE TS.acct = %s AND TS.code = %s AND TS.trail_day = TO_CHAR(now(), 'YYYYMMDD') AND trail_signal_code = %s
                    """, (str(acct_no), i[0], trail_signal_code))
                    result_four = cur04.fetchall()
                    cur04.close()

                    if len(result_four) > 0:
                        for j in result_four:
                            if trail_signal_code != "":
                                if trail_signal_code != j[0]:
                                    print("종목명 : " + i[1] + "추적정보 대상 : " + trail_signal_name)
                                    if n_sell_amount > 0:
                                        sell_command = f"/HoldingSell_{i[0]}_{i[6]}"
                                        telegram_text = (f"{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매도량 : {sell_plan_amount}주, 매도금액 : {format(int(n_sell_sum), ',d')}원 => {sell_command}")
                                    else:
                                        telegram_text = i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate']
                                    main(telegram_text)

                                    cur20 = conn.cursor()
                                    insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, cdate = %s, sell_plan_qty = %s, sell_plan_amt = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                                    record_to_insert0 = ([cur_time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                                    cur20.execute(insert_query0, record_to_insert0)

                                    cur2 = conn.cursor()
                                    insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                    record_to_insert = ([acct_no, today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                                    cur2.execute(insert_query, record_to_insert)
                                    conn.commit()
                                    cur20.close()
                                    cur2.close()

                                    # if trail_signal_code == '07' or trail_signal_code == '08' or trail_signal_code == '09' or trail_signal_code == '10':

                                    #     base_dtm = datetime.strptime(today + j[1] + '00', '%Y%m%d%H%M%S')
                                    #     candle_list = fetch_candles_with_base(access_token, app_key, app_secret, i[0], base_dtm)

                                    #     minute_list = []
                                    #     for item in candle_list:
                                    #         minute_list.append({
                                    #             '체결시간': item['stck_cntg_hour'],
                                    #             '종가': item['stck_prpr'],
                                    #             '시가': item['stck_oprc'],
                                    #             '고가': item['stck_hgpr'],
                                    #             '저가': item['stck_lwpr'],
                                    #             '거래량': item['cntg_vol']
                                    #         })

                                    #     df = pd.DataFrame(minute_list)
                                    #     df['체결시간'] = pd.to_datetime(df['체결시간'], format='%H%M%S')
                                    #     df = df.sort_values('체결시간').reset_index(drop=True)
                                    #     df.rename(columns={'종가': 'close', '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume', '체결시간': 'timestamp'}, inplace=True)
                                    #     df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
                                    #     df['body'] = (df['close'] - df['open']).abs()

                                    #     df_10m = df.resample('10T', on='timestamp', label='left', closed='left').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).reset_index()
                                    #     df_10m['body'] = (df_10m['close'] - df_10m['open']).abs()
                                    #     기준봉 = df_10m.loc[df_10m['volume'].idxmax()]
                                    #     avg_body = df_10m['body'].rolling(20).mean().iloc[-1] if len(df_10m) >= 20 else df_10m['body'].mean()

                                    #     body_value = 기준봉['body']
                                    #     if body_value > avg_body * 1.5:
                                    #         candle_body = "L"
                                    #     elif body_value < avg_body * 0.5:
                                    #         candle_body = "S"
                                    #     else:
                                    #         candle_body = "M"

                                    #     cur500 = conn.cursor()
                                    #     insert_query = """
                                    #         INSERT INTO trade_auto_proc (
                                    #             acct_no, name, code, base_day, base_dtm, trade_tp, open_price, high_price,
                                    #             low_price, close_price, vol, candle_body, trade_sum, proc_yn,
                                    #             regr_id, reg_date, chgr_id, chg_date
                                    #         )
                                    #         SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    #         WHERE NOT EXISTS (
                                    #             SELECT 1 FROM trade_auto_proc
                                    #             WHERE acct_no=%s AND code=%s AND base_day=%s
                                    #             AND base_dtm=%s AND trade_tp=%s AND proc_yn='Y'
                                    #         );
                                    #         """
                                    #     cur500.execute(insert_query, (
                                    #         str(acct_no), i[1], i[0], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S"), "S", 기준봉['open'], 기준봉['high'], 기준봉['low'], 기준봉['close'], 기준봉['volume'], candle_body, '100', 'Y', 'AUTO_SELL', datetime.now(), 'AUTO_SELL', datetime.now()
                                    #         , str(acct_no), i[0], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S"), "S"
                                    #     ))
                                    #     was_inserted = cur500.rowcount == 1
                                    #     conn.commit()
                                    #     cur500.close()

                                    #     if was_inserted:
                                    #         cur501 = conn.cursor()
                                    #         update_query = """
                                    #             UPDATE trade_auto_proc
                                    #             SET proc_yn = 'N', chgr_id = 'AUTO_UP_SELL', chg_date = %s
                                    #             WHERE acct_no = %s AND code = %s AND base_day = %s
                                    #             AND base_dtm <> %s AND trade_tp = 'S' AND proc_yn = 'Y'
                                    #         """
                                    #         cur501.execute(update_query, (
                                    #             datetime.now(), str(acct_no), i[0], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S")
                                    #         ))
                                    #         conn.commit()
                                    #         cur501.close()

                    else:
                        if trail_signal_code != "":
                            print("종목명 : " + i[1] + "추적신호 : " + trail_signal_name)
                            if n_sell_amount > 0:
                                sell_command = f"/HoldingSell_{i[0]}_{i[6]}"
                                telegram_text = (f"{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매도량 : {sell_plan_amount}주, 매도금액 : {format(int(n_sell_sum), ',d')}원 => {sell_command}")
                            else:
                                telegram_text = i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate']
                            main(telegram_text)

                            cur20 = conn.cursor()
                            insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, cdate = %s, sell_plan_qty = %s, sell_plan_amt = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                            record_to_insert0 = ([cur_time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                            cur20.execute(insert_query0, record_to_insert0)

                            cur2 = conn.cursor()
                            insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, sell_plan_qty, sell_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            record_to_insert = ([acct_no, today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(n_sell_amount), int(n_sell_sum)])
                            cur2.execute(insert_query, record_to_insert)
                            conn.commit()
                            cur20.close()
                            cur2.close()

                            # if trail_signal_code == '07' or trail_signal_code == '08' or trail_signal_code == '09' or trail_signal_code == '10':

                            #     base_dtm = datetime.strptime(today + cur_time + '00', '%Y%m%d%H%M%S')
                            #     candle_list = fetch_candles_with_base(access_token, app_key, app_secret, i[0], base_dtm)

                            #     minute_list = []
                            #     for item in candle_list:
                            #         minute_list.append({
                            #             '체결시간': item['stck_cntg_hour'],
                            #             '종가': item['stck_prpr'],
                            #             '시가': item['stck_oprc'],
                            #             '고가': item['stck_hgpr'],
                            #             '저가': item['stck_lwpr'],
                            #             '거래량': item['cntg_vol']
                            #         })

                            #     df = pd.DataFrame(minute_list)
                            #     df['체결시간'] = pd.to_datetime(df['체결시간'], format='%H%M%S')
                            #     df = df.sort_values('체결시간').reset_index(drop=True)
                            #     df.rename(columns={'종가': 'close', '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume', '체결시간': 'timestamp'}, inplace=True)
                            #     df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
                            #     df['body'] = (df['close'] - df['open']).abs()

                            #     df_10m = df.resample('10T', on='timestamp', label='left', closed='left').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).reset_index()
                            #     df_10m['body'] = (df_10m['close'] - df_10m['open']).abs()
                            #     기준봉 = df_10m.loc[df_10m['volume'].idxmax()]
                            #     avg_body = df_10m['body'].rolling(20).mean().iloc[-1] if len(df_10m) >= 20 else df_10m['body'].mean()

                            #     body_value = 기준봉['body']
                            #     if body_value > avg_body * 1.5:
                            #         candle_body = "L"
                            #     elif body_value < avg_body * 0.5:
                            #         candle_body = "S"
                            #     else:
                            #         candle_body = "M"

                            #     cur500 = conn.cursor()
                            #     insert_query = """
                            #         INSERT INTO trade_auto_proc (
                            #             acct_no, name, code, base_day, base_dtm, trade_tp, open_price, high_price,
                            #             low_price, close_price, vol, candle_body, trade_sum, proc_yn,
                            #             regr_id, reg_date, chgr_id, chg_date
                            #         )
                            #         SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            #         WHERE NOT EXISTS (
                            #             SELECT 1 FROM trade_auto_proc
                            #             WHERE acct_no=%s AND code=%s AND base_day=%s
                            #             AND base_dtm=%s AND trade_tp=%s AND proc_yn='Y'
                            #         );
                            #         """
                            #     cur500.execute(insert_query, (
                            #         str(acct_no), i[1], i[0], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S"), "S", 기준봉['open'], 기준봉['high'], 기준봉['low'], 기준봉['close'], 기준봉['volume'], candle_body, '100', 'Y', 'AUTO_SELL', datetime.now(), 'AUTO_SELL', datetime.now()
                            #         , str(acct_no), i[0], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S"), "S"
                            #     ))
                            #     was_inserted = cur500.rowcount == 1
                            #     conn.commit()
                            #     cur500.close()

                            #     if was_inserted:
                            #         cur501 = conn.cursor()
                            #         update_query = """
                            #             UPDATE trade_auto_proc
                            #             SET proc_yn = 'N', chgr_id = 'AUTO_UP_SELL', chg_date = %s
                            #             WHERE acct_no = %s AND code = %s AND base_day = %s
                            #             AND base_dtm <> %s AND trade_tp = 'S' AND proc_yn = 'Y'
                            #         """
                            #         cur501.execute(update_query, (
                            #             datetime.now(), str(acct_no), i[0], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S")
                            #         ))
                            #         conn.commit()
                            #         cur501.close()

                    time.sleep(3)

            elif '1530' <= cur_time < '2000':
                # NXT 장후 : 주문체결 처리만 수행
                order_complete_proc(access_token, app_key, app_secret, acct_no)

        except Exception as e:
            print(f"[{nick}] Error holding item : {e}")

    conn.close()

else:
    conn.close()
    print("Today is Holiday")
