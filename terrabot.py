import re
import pandas as pd
from telegram.ext import Updater
from telegram.ext import MessageHandler, Filters
import requests
from io import StringIO
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from datetime import datetime, timedelta
import urllib3
import FinanceDataReader as fdr
from mplfinance.original_flavor import candlestick2_ohlc
import matplotlib.ticker as mticker
import psycopg2 as db
import kis_api_resp as resp
import sys
import math
import json
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import kis_stock_search_api as search
import os

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

arguments = sys.argv

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

urllib3.disable_warnings()
matplotlib.use('SVG')
plt.rcParams["font.family"] = "NanumGothic"
# 해당 링크는 한국거래소에서 상장법인목록을 엑셀로 다운로드하는 링크입니다.
# 다운로드와 동시에 Pandas에 excel 파일이 load가 되는 구조입니다.
stock_code = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', header=0)[0]
# 필요한 것은 "회사명"과 "종목코드" 이므로 필요없는 column들은 제외
stock_code = stock_code[['회사명', '종목코드']]
# 한글 컬럼명을 영어로 변경
stock_code = stock_code.rename(columns={'회사명': 'company', '종목코드': 'code'})
# 종목코드가 6자리이기 때문에 6자리를 맞춰주기 위해 설정해줌
stock_code.code = stock_code.code.map('{:06d}'.format)

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

# 텔레그램봇 updater(토큰, 입력값)
updater = Updater(token=token, use_context=True)
dispatcher = updater.dispatcher

menuNum = "0"
chartReq = "1"
g_buy_code = ""
g_sell_code = ""
g_company = ""
g_buy_price = 0
g_buy_amount = 0
g_sell_price = 0
g_sell_amount = 0

def build_menu(buttons, n_cols, header_buttons=None, footer_buttons=None):
    menu = [buttons[i:i + n_cols] for i in range(0, len(buttons), n_cols)]
    if header_buttons:
        menu.insert(0, header_buttons)
    if footer_buttons:
        menu.append(footer_buttons)
    return menu

def build_button(text_list, callback_header = "") : # make button list
    button_list = []
    text_header = callback_header
    if callback_header != "" :
        text_header += ","

    for text in text_list :
        button_list.append(InlineKeyboardButton(text, callback_data=text_header + text))

    return button_list

def get_command(update, context) :
    button_list = build_button(["보유종목", "매수", "매도", "관심종목", "일별체결", "자산현황", "시장레벨", "종목검색", "초기화", "취소"]) # make button list
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 5)) # make markup
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup

def get_command1(update, context) :
    button_list = build_button(["매수진행", "취소"]) # make button list
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list))) # make markup
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup
    
def get_command2(update, context) :
    button_list = build_button(["매도진행", "취소"]) # make button list
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list))) # make markup
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup

def get_command3(update, context) :
    command_parts = update.message.text.split("_")
    if len(command_parts) < 3:
        update.message.reply_text("잘못된 명령어 형식입니다.")
        return

    stock_code = command_parts[1]
    sell_amount = int(command_parts[2])
    
    button_list = build_button(["전체매도", "절반매도", "취소"]) # make button list
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list))) # make markup
    
    button_list = [
        InlineKeyboardButton(f"전체매도 ({sell_amount}주)", callback_data=f"전체매도_{stock_code}_{sell_amount}"),
        InlineKeyboardButton(f"절반매도 ({int(round(sell_amount/2))}주)", callback_data=f"절반매도_{stock_code}_{sell_amount}"),
        InlineKeyboardButton("취소", callback_data="취소")
    ]
    show_markup = InlineKeyboardMarkup([button_list])

    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup    

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

# 계정정보 조회
def account():

    cur01 = conn.cursor()
    cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date from \"stockAccount_stock_account\" where nick_name = '" + arguments[1] + "'")
    result_two = cur01.fetchone()
    cur01.close()

    acct_no = result_two[0]
    access_token = result_two[1]
    app_key = result_two[2]
    app_secret = result_two[3]

    YmdHMS = datetime.now()
    validTokenDate = datetime.strptime(result_two[4], '%Y%m%d%H%M%S')
    diff = YmdHMS - validTokenDate
    # print("diff : " + str(diff.days))
    if diff.days >= 1:  # 토큰 유효기간(1일) 만료 재발급
        access_token = auth(app_key, app_secret)
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

    account_rtn = {'acct_no':acct_no, 'access_token':access_token, 'app_key':app_key, 'app_secret':app_secret}

    return account_rtn

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

# 주식주문(현금)
def order_cash(buy_flag, access_token, app_key, app_secret, acct_no, stock_code, ord_dvsn, order_qty, order_price):

    if buy_flag:
        tr_id = "TTTC0802U"  #buy : TTTC0802U[실전투자], VTTC0802U[모의투자]
    else:
        tr_id = "TTTC0801U"  #sell : TTTC0801U[실전투자], VTTC0801U[모의투자]

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": tr_id}
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "PDNO": stock_code,
               "ORD_DVSN": ord_dvsn,    # 00 : 지정가, 01 : 시장가
               "ORD_QTY": order_qty,
               "ORD_UNPR": order_price
    }
    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{URL_BASE}/{PATH}"
    #res = requests.get(URL, headers=headers, params=params, verify=False)
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output

# 주식주문(정정취소)
def order_cancel_revice(access_token, app_key, app_secret, acct_no, cncl_dv, order_no, order_qty, order_price):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC0803U"}    # TTTC0803U[실전투자], VTTC0803U[모의투자]
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "KRX_FWDG_ORD_ORGNO": "06010",
               "ORGN_ODNO": order_no,
               "ORD_DVSN": "00",
               "RVSE_CNCL_DVSN_CD": cncl_dv,    # 정정 : 01, 취소 : 02
               "ORD_QTY": str(order_qty),
               "ORD_UNPR": str(order_price),
               "QTY_ALL_ORD_YN": "Y"
    }
    PATH = "uapi/domestic-stock/v1/trading/order-rvsecncl"
    URL = f"{URL_BASE}/{PATH}"
    #res = requests.get(URL, headers=headers, params=params, verify=False)
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output

# 일별주문체결 조회
def daily_order_complete(access_token, app_key, app_secret, acct_no, code, order_no):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8001R"}  # tr_id : TTTC8001R[실전투자], VTTC8001R[모의투자]
    params = {
        "CANO": acct_no,
        "ACNT_PRDT_CD": '01',
        "INQR_STRT_DT": datetime.now().strftime('%Y%m%d'),
        "INQR_END_DT": datetime.now().strftime('%Y%m%d'),
        "SLL_BUY_DVSN_CD": '00',
        "INQR_DVSN": '00',
        "PDNO": code,
        "CCLD_DVSN": "00",
        "ORD_GNO_BRNO": "",
        "ODNO": order_no,
        "INQR_DVSN_3": "00",
        "INQR_DVSN_1": "",
        "INQR_DVSN_2": "",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output1

# 계좌잔고 조회
def get_acct_balance_sell(access_token, app_key, app_secret, acct_no):
    # 잔고조회
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8434R"}  # tr_id : TTTC8434R[실전투자], VTTC8434R[모의투자]
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

    if ar.isOK():
        tdf = pd.DataFrame(ar.getBody().output1)
        tdf.set_index('pdno')
        return tdf[['pdno', 'prdt_name','hldg_qty', 'ord_psbl_qty', 'pchs_avg_pric', 'pchs_amt', 'evlu_amt', 'evlu_pfls_amt', 'evlu_pfls_rt', 'prpr', 'bfdy_cprs_icdc', 'fltt_rt']]
    else:
        ar.printError()
        return pd.DataFrame()

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
        e_avail_amount = int(c['ord_psbl_qty'][i])

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
            update_query301 = "with upsert as (update \"stockBalance_stock_balance\" set purchase_price = %s, purchase_amount = %s, purchase_sum = %s, avail_amount = %s, current_price = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, sell_plan_sum = %s, sell_plan_amount = %s, proc_yn = 'Y', last_chg_date = %s where acct_no = %s and code = %s and asset_num = %s returning * ) insert into \"stockBalance_stock_balance\"(acct_no, code, name, purchase_price, purchase_amount, purchase_sum, avail_amount, current_price, eval_sum, earnings_rate, valuation_sum, asset_num, sell_plan_sum, sell_plan_amount, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, proc_yn, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select A.sign_resist_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_resist_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.sign_support_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_support_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_loss_price from(select row_number() over(order by last_chg_date desc) as num, b.end_loss_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_target_price from(select row_number() over(order by last_chg_date desc) as num, b.end_target_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.trading_plan from(select row_number() over(order by last_chg_date desc) as num, b.trading_plan from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), %s, %s where not exists(select * from upsert)";
            # update 인자값 설정
            record_to_update301 = ([e_purchase_price, e_purchase_amount, e_purchase_sum, e_avail_amount, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, e_sell_plan_sum, e_sell_plan_amount, datetime.now(), acct_no, e_code, asset_num,
                                    acct_no, e_code, e_name, e_purchase_price, e_purchase_amount, e_purchase_sum, e_avail_amount, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, asset_num, e_sell_plan_sum, e_sell_plan_amount, 
                                    acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code,
                                    'Y', datetime.now()])
            # DB 연결된 커서의 쿼리 수행
            cur301.execute(update_query301, record_to_update301)
            conn.commit()
            cur301.close()

        else:
            cur301 = conn.cursor()
            update_query301 = "with upsert as (update \"stockBalance_stock_balance\" set purchase_price = %s, purchase_amount = %s, purchase_sum = %s, avail_amount = %s, current_price = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, proc_yn = 'Y', last_chg_date = %s where acct_no = %s and code = %s and asset_num = %s returning * ) insert into \"stockBalance_stock_balance\"(acct_no, code, name, purchase_price, purchase_amount, purchase_sum, avail_amount, current_price, eval_sum, earnings_rate, valuation_sum, asset_num, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, proc_yn, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select A.sign_resist_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_resist_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.sign_support_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_support_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_loss_price from(select row_number() over(order by last_chg_date desc) as num, b.end_loss_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_target_price from(select row_number() over(order by last_chg_date desc) as num, b.end_target_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.trading_plan from(select row_number() over(order by last_chg_date desc) as num, b.trading_plan from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), %s, %s where not exists(select * from upsert)";
            # update 인자값 설정
            record_to_update301 = ([e_purchase_price, e_purchase_amount, e_purchase_sum, e_avail_amount, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, datetime.now(), acct_no, e_code, asset_num,
                                    acct_no, e_code, e_name, e_purchase_price, e_purchase_amount, e_purchase_sum, e_avail_amount, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, asset_num, 
                                    acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code,
                                    'Y', datetime.now()])
            # DB 연결된 커서의 쿼리 수행
            cur301.execute(update_query301, record_to_update301)
            conn.commit()
            cur301.close()

# 자산정보 처리
def fund_proc(access_token, app_key, app_secret, acct_no):
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
    cur100.execute("select asset_num, cash_rate, tot_evlu_amt, prvs_rcdl_excc_amt from \"stockFundMng_stock_fund_mng\" where acct_no = '" + str(acct_no) + "'")
    result_one00 = cur100.fetchall()
    cur100.close()

    asset_num = 0 

    for i in result_one00:

        asset_num = i[0]
        print("자산번호 : " + str(asset_num))   

    # 자산정보 변경
    cur200 = conn.cursor()
    update_query200 = "update \"stockFundMng_stock_fund_mng\" set tot_evlu_amt = %s, dnca_tot_amt = %s, prvs_rcdl_excc_amt = %s, nass_amt = %s, scts_evlu_amt = %s, asset_icdc_amt = %s, last_chg_date = %s where asset_num = %s and acct_no = %s"
    # update 인자값 설정
    record_to_update200 = ([u_tot_evlu_amt, u_dnca_tot_amt, u_prvs_rcdl_excc_amt, u_nass_amt, u_scts_evlu_amt, u_asst_icdc_amt, datetime.now(), asset_num, acct_no])
    # DB 연결된 커서의 쿼리 수행
    cur200.execute(update_query200, record_to_update200)
    conn.commit()
    cur200.close()

# 시장레벨정보 처리
def marketLevel_proc(access_token, app_key, app_secret, acct_no):
    # 계좌잔고 조회
    b = stock_balance(access_token, app_key, app_secret, acct_no, "all")

    for i, name in enumerate(b.index):
        u_prvs_rcdl_excc_amt = int(b['prvs_rcdl_excc_amt'][i])      # 가수도 정산 금액

    print("가수도 정산 금액 : " + format(int(u_prvs_rcdl_excc_amt), ',d'))

    # 시장레벨정보 조회
    cur100 = conn.cursor()
    cur100.execute("select asset_risk_num, market_level_num from \"stockMarketMng_stock_market_mng\" where acct_no = '" + str(acct_no) + "' and aply_end_dt = '99991231'")
    result_one00 = cur100.fetchall()
    cur100.close()

    asset_risk_num = 0 

    for i in result_one00:

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
    print("전체 자산 : " + format(int(n_asset_sum), ',d'))    
    print("리스크율 : " + str(n_risk_rate))
    print("리스크 금액 : " + format(int(n_risk_sum), ',d'))
    print("종목 갯수 : " + format(int(n_stock_num), ',d'))

    # 시장레벨정보 변경
    cur200 = conn.cursor()
    update_query200 = "update \"stockMarketMng_stock_market_mng\" set total_asset = %s, risk_rate = %s, risk_sum = %s, item_number = %s where asset_risk_num = %s and acct_no = %s and aply_end_dt = '99991231'"
    # update 인자값 설정
    record_to_update200 = ([n_asset_sum, n_risk_rate, n_risk_sum, n_stock_num, asset_risk_num, acct_no])
    # DB 연결된 커서의 쿼리 수행
    cur200.execute(update_query200, record_to_update200)
    conn.commit()
    cur200.close()        

def handle_holding_sell(update, context):
    command_parts = update.message.text.split("_")
    if len(command_parts) < 3:
        update.message.reply_text("잘못된 명령어 형식입니다.")
        return

    stock_code = command_parts[1]
    sell_amount = int(command_parts[2])

    button_list = build_button(["전체매도", "절반매도", "취소"]) # make button list
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list))) # make markup
    
    button_list = [
        InlineKeyboardButton(f"전체매도 ({sell_amount}주)", callback_data=f"전체매도_{stock_code}_{sell_amount}"),
        InlineKeyboardButton(f"절반매도 ({int(round(sell_amount/2))}주)", callback_data=f"절반매도_{stock_code}_{sell_amount}"),
        InlineKeyboardButton("취소", callback_data="취소")
    ]
    show_markup = InlineKeyboardMarkup([button_list])

    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup 

def handle_interest_buy(update, context):
    command_parts = update.message.text.split("_")
    if len(command_parts) < 4:
        update.message.reply_text("잘못된 명령어 형식입니다.")
        return
    
    stock_code = command_parts[1]
    buy_price = int(command_parts[2])
    buy_amount = int(command_parts[3])

    button_list = build_button(["전체매수", "절반매수", "취소"]) # make button list
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list))) # make markup
    
    button_list = [
        InlineKeyboardButton(f"전체매수 ({buy_price}원{buy_amount}주)", callback_data=f"전체매수_{stock_code}_{buy_price}_{buy_amount}"),
        InlineKeyboardButton(f"절반매수 ({buy_price}원{int(round(buy_amount/2))}주)", callback_data=f"절반매수_{stock_code}_{buy_price}_{buy_amount}"),
        InlineKeyboardButton("취소", callback_data="취소")
    ]
    show_markup = InlineKeyboardMarkup([button_list])
    
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup

def callback_get(update, context) :
    data_selected = update.callback_query.data
    global menuNum

    print("callback0 : ", data_selected)
    if data_selected.find("취소") != -1:
        context.bot.edit_message_text(text="취소하였습니다.",
                                      chat_id=update.callback_query.message.chat_id,
                                      message_id=update.callback_query.message.message_id)
        return

    elif data_selected.find("매수진행") != -1:

        if menuNum != "0":
            ac = account()
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']

            # 매수량, 매수금액 입력시 시장가 매수주문
            if menuNum == "23":
                ord_dvsn = "01"
                
                try:
                    # 매수
                    c = order_cash(True, access_token, app_key, app_secret, str(acct_no), g_buy_code, ord_dvsn, str(g_buy_amount), str(0))
                
                    if c['ODNO'] != "":

                        # 일별주문체결 조회
                        output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_buy_code, c['ODNO'])
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                        for i, name in enumerate(d.index):
                            d_order_no = int(d['odno'][i])
                            d_order_type = d['sll_buy_dvsn_cd_name'][i]
                            d_order_dt = d['ord_dt'][i]
                            d_order_tmd = d['ord_tmd'][i]
                            d_name = d['prdt_name'][i]
                            d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                            d_order_amount = d['ord_qty'][i]
                            d_total_complete_qty = d['tot_ccld_qty'][i]
                            d_remain_qty = d['rmn_qty'][i]
                            d_total_complete_amt = d['tot_ccld_amt'][i]

                            print("매수주문 완료")

                            context.bot.edit_message_text(text="[" + d_name + "] 매수가 : " + format(int(d_order_price), ',d') + "원, 매수량 : " + format(int(d_order_amount), ',d') + "주 매수주문 완료, 주문번호 : " + str(d_order_no),
                                                        chat_id=update.callback_query.message.chat_id,
                                                        message_id=update.callback_query.message.message_id)
                    else:
                        print("매수주문 실패")
                        context.bot.edit_message_text(text="[" + g_buy_code + "] 매수가 : " + format(int(g_buy_price), ',d') + "원, 매수량 : " + format(int(g_buy_amount), ',d') + "주 매수주문 실패",
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)
                    menuNum = "0"

                except Exception as e:
                    print('매수주문 오류.', e)
                    menuNum = "0"
                    context.bot.edit_message_text(text="[" + g_buy_code + "] 매수가 : " + format(int(g_buy_price), ',d') + "원, 매수량 : " + format(int(g_buy_amount), ',d') + "주 [매수주문 오류] - "+str(e),
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id)
            else:
                ord_dvsn = "00"    

                try:
                    # 매수
                    c = order_cash(True, access_token, app_key, app_secret, str(acct_no), g_buy_code, ord_dvsn, str(g_buy_amount), str(g_buy_price))
                
                    if c['ODNO'] != "":

                        # 일별주문체결 조회
                        output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_buy_code, c['ODNO'])
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                        for i, name in enumerate(d.index):
                            d_order_no = int(d['odno'][i])
                            d_order_type = d['sll_buy_dvsn_cd_name'][i]
                            d_order_dt = d['ord_dt'][i]
                            d_order_tmd = d['ord_tmd'][i]
                            d_name = d['prdt_name'][i]
                            d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                            d_order_amount = d['ord_qty'][i]
                            d_total_complete_qty = d['tot_ccld_qty'][i]
                            d_remain_qty = d['rmn_qty'][i]
                            d_total_complete_amt = d['tot_ccld_amt'][i]

                            print("매수주문 완료")

                            context.bot.edit_message_text(text="[" + d_name + "] 매수가 : " + format(int(d_order_price), ',d') + "원, 매수량 : " + format(int(d_order_amount), ',d') + "주 매수주문 완료, 주문번호 : " + str(d_order_no),
                                                        chat_id=update.callback_query.message.chat_id,
                                                        message_id=update.callback_query.message.message_id)
                    else:
                        print("매수주문 실패")
                        context.bot.edit_message_text(text="[" + g_buy_code + "] 매수가 : " + format(int(g_buy_price), ',d') + "원, 매수량 : " + format(int(g_buy_amount), ',d') + "주 매수주문 실패",
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)
                    menuNum = "0"

                except Exception as e:
                    print('매수주문 오류.', e)
                    menuNum = "0"
                    context.bot.edit_message_text(text="[" + g_buy_code + "] 매수가 : " + format(int(g_buy_price), ',d') + "원, 매수량 : " + format(int(g_buy_amount), ',d') + "주 [매수주문 오류] - "+str(e),
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id)
            
        else:
            context.bot.edit_message_text(text="처음 메뉴부터 매수 진행하세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id)

    elif data_selected.find("매도진행") != -1:

        if menuNum != "0":
            ac = account()
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']

            # 매도량 입력시 시장가 매도주문
            if menuNum == "33":
                ord_dvsn = "01"
                try:
                    # 매도
                    c = order_cash(False, access_token, app_key, app_secret, str(acct_no), g_sell_code, ord_dvsn, str(g_sell_amount), str(0))
                
                    if c['ODNO'] != "":

                        # 일별주문체결 조회
                        output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_sell_code, c['ODNO'])
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                        for i, name in enumerate(d.index):
                            d_order_no = int(d['odno'][i])
                            d_order_type = d['sll_buy_dvsn_cd_name'][i]
                            d_order_dt = d['ord_dt'][i]
                            d_order_tmd = d['ord_tmd'][i]
                            d_name = d['prdt_name'][i]
                            d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                            d_order_amount = d['ord_qty'][i]
                            d_total_complete_qty = d['tot_ccld_qty'][i]
                            d_remain_qty = d['rmn_qty'][i]
                            d_total_complete_amt = d['tot_ccld_amt'][i]

                            print("매도주문 완료")

                            context.bot.edit_message_text(text="[" + d_name + "] 매도가 : " + format(int(d_order_price), ',d') + "원, 매도량 : " + format(int(d_order_amount), ',d') + "주 매도주문 완료, 주문번호 : " + str(d_order_no),
                                                        chat_id=update.callback_query.message.chat_id,
                                                        message_id=update.callback_query.message.message_id)
                            
                    else:
                        print("매도주문 실패")
                        context.bot.edit_message_text(text="[" + g_sell_code + "] 매도가 : " + format(int(g_sell_price), ',d') + "원, 매도량 : " + format(int(g_sell_amount), ',d') + "주 매도주문 실패",
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)
                    menuNum = "0"

                except Exception as e:
                    print('매도주문 오류.', e)
                    menuNum = "0"
                    context.bot.edit_message_text(text="[" + g_sell_code + "] 매도가 : " + format(int(g_sell_price), ',d') + "원, 매도량 : " + format(int(g_sell_amount), ',d') + "주 [매도주문 오류] - " +str(e),
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id)
            else:
                ord_dvsn = "00"    

                try:
                    # 매도
                    c = order_cash(False, access_token, app_key, app_secret, str(acct_no), g_sell_code, ord_dvsn, str(g_sell_amount), str(g_sell_price))
                
                    if c['ODNO'] != "":

                        # 일별주문체결 조회
                        output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_sell_code, c['ODNO'])
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                        for i, name in enumerate(d.index):
                            d_order_no = int(d['odno'][i])
                            d_order_type = d['sll_buy_dvsn_cd_name'][i]
                            d_order_dt = d['ord_dt'][i]
                            d_order_tmd = d['ord_tmd'][i]
                            d_name = d['prdt_name'][i]
                            d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                            d_order_amount = d['ord_qty'][i]
                            d_total_complete_qty = d['tot_ccld_qty'][i]
                            d_remain_qty = d['rmn_qty'][i]
                            d_total_complete_amt = d['tot_ccld_amt'][i]

                            print("매도주문 완료")

                            context.bot.edit_message_text(text="[" + d_name + "] 매도가 : " + format(int(d_order_price), ',d') + "원, 매도량 : " + format(int(d_order_amount), ',d') + "주 매도주문 완료, 주문번호 : " + str(d_order_no),
                                                        chat_id=update.callback_query.message.chat_id,
                                                        message_id=update.callback_query.message.message_id)

                    else:
                        print("매도주문 실패")
                        context.bot.edit_message_text(text="[" + g_sell_code + "] 매도가 : " + format(int(g_sell_price), ',d') + "원, 매도량 : " + format(int(g_sell_amount), ',d') + "주 매도주문 실패",
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)
                    menuNum = "0"

                except Exception as e:
                    print('매도주문 오류.', e)
                    menuNum = "0"
                    context.bot.edit_message_text(text="[" + g_sell_code + "] 매도가 : " + format(int(g_sell_price), ',d') + "원, 매도량 : " + format(int(g_sell_amount), ',d') + "주 [매도주문 오류] - " +str(e),
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id)
            
        else:
            context.bot.edit_message_text(text="처음 메뉴부터 매도 진행하세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id)                                                

    elif data_selected.find("전체매도") != -1:
        
        parts = data_selected.split("_")
        if len(parts) < 3:
            update.callback_query.message.reply_text("잘못된 매도 명령어 형식입니다.")
            return
        sell_code = parts[1]  # 종목 코드
        sell_amount = int(parts[2])  # 매도 수량

        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            # 계좌잔고 조회
            e = stock_balance(access_token, app_key, app_secret, acct_no, "")

            ord_psbl_qty = 0
            for j, name in enumerate(e.index):
                e_code = e['pdno'][j]
                if e_code == sell_code:
                    ord_psbl_qty = int(e['ord_psbl_qty'][j])
            print("주문가능수량 : " + format(ord_psbl_qty, ',d'))

            if ord_psbl_qty >= sell_amount:  # 주문가능수량이 매도수량보다 큰 경우
                # 입력 종목코드 현재가 시세
                a = inquire_price(access_token, app_key, app_secret, sell_code)
                sell_price = a['stck_prpr']
                print("현재가 : " + format(int(sell_price), ',d'))  # 현재가
                # 전체매도
                c = order_cash(False, access_token, app_key, app_secret, str(acct_no), sell_code, "00", str(sell_amount), str(sell_price))
            
                if c['ODNO'] != "":

                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, sell_code, c['ODNO'])
                    tdf = pd.DataFrame(output1)
                    tdf.set_index('odno')
                    d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                    for i, name in enumerate(d.index):
                        d_order_no = int(d['odno'][i])
                        d_order_type = d['sll_buy_dvsn_cd_name'][i]
                        d_order_dt = d['ord_dt'][i]
                        d_order_tmd = d['ord_tmd'][i]
                        d_name = d['prdt_name'][i]
                        d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                        d_order_amount = d['ord_qty'][i]
                        d_total_complete_qty = d['tot_ccld_qty'][i]
                        d_remain_qty = d['rmn_qty'][i]
                        d_total_complete_amt = d['tot_ccld_amt'][i]

                        print("전체매도주문 완료")

                        context.bot.edit_message_text(text="[" + d_name + "] 매도가 : " + format(int(d_order_price), ',d') + "원, 매도량 : " + format(int(d_order_amount), ',d') + "주 전체매도주문 완료, 주문번호 : " + str(d_order_no),
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)

                else:
                    print("전체매도주문 실패")
                    context.bot.edit_message_text(text="[" + sell_code + "] 매도가 : " + format(int(sell_price), ',d') + "원, 매도량 : " + format(int(sell_amount), ',d') + "주 전체매도주문 실패",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id)

            else:
                print("전체매도 가능수량 부족")
                context.bot.edit_message_text(text="[" + sell_code + "] 매도량 : " + format(int(sell_amount), ',d') + "주, 매도가능수량 : " + format(int(ord_psbl_qty), ',d') + "주 전체매도 가능수량 부족",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id)        

            menuNum = "0"

        except Exception as e:
            print('전체매도주문 오류.', e)
            menuNum = "0"
            context.bot.edit_message_text(text="[" + sell_code + "] 매도가 : " + format(int(sell_price), ',d') + "원, 매도량 : " + format(int(sell_amount), ',d') + "주 [전체매도주문 오류] - " +str(e),
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)

    elif data_selected.find("절반매도") != -1:

        parts = data_selected.split("_")
        if len(parts) < 3:
            update.callback_query.message.reply_text("잘못된 매도 명령어 형식입니다.")
            return
        sell_code = parts[1]  # 종목 코드
        sell_amount = int(parts[2])  # 매도 수량
        # 절반매도
        half_sell_amount = int(round(sell_amount/2))

        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            # 계좌잔고 조회
            e = stock_balance(access_token, app_key, app_secret, acct_no, "")

            ord_psbl_qty = 0
            for j, name in enumerate(e.index):
                e_code = e['pdno'][j]
                if e_code == sell_code:
                    ord_psbl_qty = int(e['ord_psbl_qty'][j])
            print("주문가능수량 : " + format(ord_psbl_qty, ',d'))

            if ord_psbl_qty >= half_sell_amount:  # 주문가능수량이 절반매도수량보다 큰 경우
                # 입력 종목코드 현재가 시세
                a = inquire_price(access_token, app_key, app_secret, sell_code)
                sell_price = a['stck_prpr']
                print("현재가 : " + format(int(sell_price), ',d'))  # 현재가
                
                c = order_cash(False, access_token, app_key, app_secret, str(acct_no), sell_code, "00", str(half_sell_amount), str(sell_price))
            
                if c['ODNO'] != "":

                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, sell_code, c['ODNO'])
                    tdf = pd.DataFrame(output1)
                    tdf.set_index('odno')
                    d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                    for i, name in enumerate(d.index):
                        d_order_no = int(d['odno'][i])
                        d_order_type = d['sll_buy_dvsn_cd_name'][i]
                        d_order_dt = d['ord_dt'][i]
                        d_order_tmd = d['ord_tmd'][i]
                        d_name = d['prdt_name'][i]
                        d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                        d_order_amount = d['ord_qty'][i]
                        d_total_complete_qty = d['tot_ccld_qty'][i]
                        d_remain_qty = d['rmn_qty'][i]
                        d_total_complete_amt = d['tot_ccld_amt'][i]

                        print("절반매도주문 완료")

                        context.bot.edit_message_text(text="[" + d_name + "] 매도가 : " + format(int(d_order_price), ',d') + "원, 매도량 : " + format(int(d_order_amount), ',d') + "주 절반매도주문 완료, 주문번호 : " + str(d_order_no),
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)

                else:
                    print("절반매도주문 실패")
                    context.bot.edit_message_text(text="[" + sell_code + "] 매도가 : " + format(int(sell_price), ',d') + "원, 매도량 : " + format(int(half_sell_amount), ',d') + "주 절반매도주문 실패",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id)
            else:
                print("절반매도 가능수량 부족")
                context.bot.edit_message_text(text="[" + sell_code + "] 매도량 : " + format(int(half_sell_amount), ',d') + "주, 매도가능수량 : " + format(int(ord_psbl_qty), ',d') + "주 절반매도 가능수량 부족",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id)
                        
            menuNum = "0"

        except Exception as e:
            print('절반매도주문 오류.', e)
            menuNum = "0"
            context.bot.edit_message_text(text="[" + sell_code + "] 매도가 : " + format(int(sell_price), ',d') + "원, 매도량 : " + format(int(half_sell_amount), ',d') + "주 [절반매도주문 오류] - " +str(e),
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)

    elif data_selected.find("전체매수") != -1:        
        
        parts = data_selected.split("_")
        if len(parts) < 4:
            update.callback_query.message.reply_text("잘못된 매수 명령어 형식입니다.")
            return
        buy_code = parts[1]  # 종목 코드
        buy_price = int(parts[2])  # 매수가
        buy_amount = int(parts[3])  # 매수량
        n_buy_sum = buy_price * buy_amount

        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        ord_dvsn = "00"    

        try:
            # 매수 가능(현금) 조회
            b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
            print("매수 가능(현금) : " + format(int(b), ',d'));
            if int(b) > n_buy_sum:  # 매수가능(현금)이 매수금액이 더 큰 경우

                # 매수
                c = order_cash(True, access_token, app_key, app_secret, str(acct_no), buy_code, ord_dvsn, str(buy_amount), str(buy_price))
            
                if c['ODNO'] != "":

                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, buy_code, c['ODNO'])
                    tdf = pd.DataFrame(output1)
                    tdf.set_index('odno')
                    d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                    for i, name in enumerate(d.index):
                        d_order_no = int(d['odno'][i])
                        d_order_type = d['sll_buy_dvsn_cd_name'][i]
                        d_order_dt = d['ord_dt'][i]
                        d_order_tmd = d['ord_tmd'][i]
                        d_name = d['prdt_name'][i]
                        d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                        d_order_amount = d['ord_qty'][i]
                        d_total_complete_qty = d['tot_ccld_qty'][i]
                        d_remain_qty = d['rmn_qty'][i]
                        d_total_complete_amt = d['tot_ccld_amt'][i]

                        print("매수주문 완료")

                        context.bot.edit_message_text(text="[" + d_name + "] 매수가 : " + format(int(d_order_price), ',d') + "원, 매수량 : " + format(int(d_order_amount), ',d') + "주 매수주문 완료, 주문번호 : " + str(d_order_no),
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)
                else:
                    print("매수주문 실패")
                    context.bot.edit_message_text(text="[" + buy_code + "] 매수가 : " + format(int(buy_price), ',d') + "원, 매수량 : " + format(int(buy_amount), ',d') + "주 매수주문 실패",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id)

            else:
                print("매수 가능(현금) 부족")
                context.bot.edit_message_text(text="["+company + "] 매수금액 : " + format(int(n_buy_sum), ',d') + "원, 매수 가능(현금) : " + format(int(b) - int(n_buy_sum), ',d') +"원 부족",
                                        chat_id=update.callback_query.message.chat_id, 
                                        message_id=update.callback_query.message.message_id)

            menuNum = "0"    

        except Exception as e:
            print('매수주문 오류.', e)
            menuNum = "0"
            context.bot.edit_message_text(text="[" + buy_code + "] 매수가 : " + format(int(buy_price), ',d') + "원, 매수량 : " + format(int(buy_amount), ',d') + "주 [매수주문 오류] - "+str(e),
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)

    elif data_selected.find("절반매수") != -1:        
        
        parts = data_selected.split("_")
        if len(parts) < 4:
            update.callback_query.message.reply_text("잘못된 매수 명령어 형식입니다.")
            return
        buy_code = parts[1]  # 종목 코드
        buy_price = int(parts[2])  # 매수가
        buy_amount = int(parts[3])  # 매수량
        # 절반매수
        half_buy_amount = int(round(buy_amount/2))
        n_buy_sum = buy_price * half_buy_amount

        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        ord_dvsn = "00"    

        try:
            # 매수 가능(현금) 조회
            b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
            print("매수 가능(현금) : " + format(int(b), ',d'));
            if int(b) > n_buy_sum:  # 매수가능(현금)이 매수금액이 더 큰 경우
                # 매수
                c = order_cash(True, access_token, app_key, app_secret, str(acct_no), buy_code, ord_dvsn, str(half_buy_amount), str(buy_price))
            
                if c['ODNO'] != "":

                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, buy_code, c['ODNO'])
                    tdf = pd.DataFrame(output1)
                    tdf.set_index('odno')
                    d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                    for i, name in enumerate(d.index):
                        d_order_no = int(d['odno'][i])
                        d_order_type = d['sll_buy_dvsn_cd_name'][i]
                        d_order_dt = d['ord_dt'][i]
                        d_order_tmd = d['ord_tmd'][i]
                        d_name = d['prdt_name'][i]
                        d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                        d_order_amount = d['ord_qty'][i]
                        d_total_complete_qty = d['tot_ccld_qty'][i]
                        d_remain_qty = d['rmn_qty'][i]
                        d_total_complete_amt = d['tot_ccld_amt'][i]

                        print("매수주문 완료")

                        context.bot.edit_message_text(text="[" + d_name + "] 매수가 : " + format(int(d_order_price), ',d') + "원, 매수량 : " + format(int(d_order_amount), ',d') + "주 매수주문 완료, 주문번호 : " + str(d_order_no),
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)
                else:
                    print("매수주문 실패")
                    context.bot.edit_message_text(text="[" + buy_code + "] 매수가 : " + format(int(buy_price), ',d') + "원, 매수량 : " + format(int(half_buy_amount), ',d') + "주 매수주문 실패",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id)

            else:
                print("매수 가능(현금) 부족")
                context.bot.edit_message_text(text="["+company + "] 매수금액 : " + format(int(n_buy_sum), ',d') + "원, 매수 가능(현금) : " + format(int(b) - int(n_buy_sum), ',d') +"원 부족",
                                        chat_id=update.callback_query.message.chat_id, 
                                        message_id=update.callback_query.message.message_id)        

            menuNum = "0"

        except Exception as e:
            print('매수주문 오류.', e)
            menuNum = "0"
            context.bot.edit_message_text(text="[" + buy_code + "] 매수가 : " + format(int(buy_price), ',d') + "원, 매수량 : " + format(int(half_buy_amount), ',d') + "주 [매수주문 오류] - "+str(e),
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)

    elif data_selected.find("매수") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["손절금액", "매수금액", "현재가", "수량수가", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

            context.bot.edit_message_text(text="매수 종류를 선택해 주세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id,
                                          reply_markup=show_markup)

        elif len(data_selected.split(",")) == 2:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("손절금액") != -1:
                menuNum = "21"

                context.bot.edit_message_text(text="종목손절금액 기준 매수의 종목코드(종목명), 매수가, 이탈가, 손절금액을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("매수금액") != -1:
                menuNum = "22"

                context.bot.edit_message_text(text="매수금액 기준 매수의 종목코드(종목명), 매수가, 매수금액을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("현재가") != -1:
                menuNum = "23"

                context.bot.edit_message_text(text="현재가 기준 매수의 종목코드(종목명), 매수금액을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("수량수가") != -1:
                menuNum = "24"

                context.bot.edit_message_text(text="종목코드(종목명), 매수량, 매수가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)    

    elif data_selected.find("매도") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["전체", "절반", "현재가", "도량도가", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

            context.bot.edit_message_text(text="매도 종류를 선택해 주세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id,
                                          reply_markup=show_markup)

        elif len(data_selected.split(",")) == 2:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("전체") != -1:
                menuNum = "31"

                context.bot.edit_message_text(text="전체 매도의 종목코드(종목명), 매도가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("절반") != -1:
                menuNum = "32"

                context.bot.edit_message_text(text="절반 매도의 종목코드(종목명), 매도가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("현재가") != -1:
                menuNum = "33"

                context.bot.edit_message_text(text="현재가 기준 매도의 종목코드(종목명), 매도량을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("도량도가") != -1:
                menuNum = "34"

                context.bot.edit_message_text(text="종목코드(종목명), 매도량, 매도가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

    elif data_selected.find("관심종목") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["관심종목조회", "관심종목등록", "관심종목삭제", "관심종목수정", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 2))

            context.bot.edit_message_text(text="관심종목 메뉴를 선택해 주세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id,
                                          reply_markup=show_markup)

        elif len(data_selected.split(",")) == 2:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("관심종목조회") != -1:

                if len(data_selected.split(",")) == 2:
                    button_list = build_button(["전체조회", "개별조회", "취소"], data_selected)
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

                    context.bot.edit_message_text(text="조회할 메뉴를 선택해 주세요.",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id,
                                                reply_markup=show_markup)

            elif data_selected.find("관심종목등록") != -1:
                menuNum = "11"

                context.bot.edit_message_text(text="관심종목 등록할 종목코드(종목명)를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("관심종목삭제") != -1:
                menuNum = "12"

                context.bot.edit_message_text(text="관심종목 삭제할 종목코드(종목명)를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("관심종목수정") != -1:

                if len(data_selected.split(",")) == 2:
                    button_list = build_button(["돌파가수정", "이탈가수정", "저항가수정", "지지가수정", "추세상단가수정", "추세하단가수정", "취소"], data_selected)
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 3))

                    context.bot.edit_message_text(text="수정할 메뉴를 선택해 주세요.",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id,
                                                reply_markup=show_markup)

        elif len(data_selected.split(",")) == 3:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("전체조회") != -1:

                context.bot.edit_message_text(text="[관심종목]",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                ac = account()
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 관심정보 조회
                cur200 = conn.cursor()
                cur200.execute("select code, name, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, buy_expect_sum from \"interestItem_interest_item\" A where acct_no = '" + str(acct_no) + "' and length(code) >= 6")
                result_two00 = cur200.fetchall()
                cur200.close()

                for i in result_two00:
                    print("종목코드 : " + i[0])
                    print("종목명 : " + i[1])
                    print("돌파가 : " + format(int(i[2]), ',d'))
                    print("이탈가 : " + format(int(i[3]), ',d'))
                    print("저항가 : " + format(int(i[4]), ',d'))
                    print("지지가 : " + format(int(i[5]), ',d'))
                    print("추세상단가 : " + format(int(i[6]), ',d'))
                    print("추세하단가 : " + format(int(i[7]), ',d'))
                    print("매수예정금액 : " + format(int(i[8]), ',d'))

                    a = inquire_price(access_token, app_key, app_secret, i[0])
                    print("현재가 : " + format(int(a['stck_prpr']), ',d'))  # 현재가
                    print("최고가 : " + format(int(a['stck_hgpr']), ',d'))  # 최고가
                    print("최저가 : " + format(int(a['stck_lwpr']), ',d'))  # 최저가
                    print("누적거래량 : " + format(int(a['acml_vol']), ',d'))  # 누적거래량
                    print("전일대비거래량비율 : " + a['prdy_vrss_vol_rate'])  # 전일대비거래량비율

                    company = i[1] + "[" + i[0] + "]"

                    context.bot.send_message(chat_id=update.effective_chat.id, text=company + " : 현재가-" + format(int(a['stck_prpr']), ',d') + "원, 고가-" + format(int(a['stck_hgpr']), ',d') + "원, 저가-" + format(int(a['stck_lwpr']), ',d') + "원, 거래량-" + format(int(a['acml_vol']), ',d') + "주, 거래대비-" + a['prdy_vrss_vol_rate'] + "%, 돌파가-" + format(int(i[2]), ',d') + "원, 이탈가-" + format(int(i[3]), ',d') + "원, 저항가-" + format(int(i[4]), ',d') + "원, 지지가-" + format(int(i[5]), ',d') + "원, 추세상단가-" + format(int(i[6]), ',d') + "원, 추세하단가-" + format(int(i[7]), ',d') + "원, 매수예정금액-" + format(int(i[8]), ',d') + "원")

            elif data_selected.find("개별조회") != -1:
                menuNum = "10"

                context.bot.edit_message_text(text="관심종목 조회할 종목코드(종목명)를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("돌파가수정") != -1:
                menuNum = "131"

                context.bot.edit_message_text(text="관심종목 수정할 종목코드(종목명), 돌파가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)            
                
            elif data_selected.find("이탈가수정") != -1:
                menuNum = "132"

                context.bot.edit_message_text(text="관심종목 수정할 종목코드(종목명), 이탈가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)                            

            elif data_selected.find("저항가수정") != -1:
                menuNum = "133"

                context.bot.edit_message_text(text="관심종목 수정할 종목코드(종목명), 저항가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)            

            elif data_selected.find("지지가수정") != -1:
                menuNum = "134"

                context.bot.edit_message_text(text="관심종목 수정할 종목코드(종목명), 지지가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)            

            elif data_selected.find("추세상단가수정") != -1:
                menuNum = "135"

                context.bot.edit_message_text(text="관심종목 수정할 종목코드(종목명), 추세상단가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)            

            elif data_selected.find("추세하단가수정") != -1:
                menuNum = "136"

                context.bot.edit_message_text(text="관심종목 수정할 종목코드(종목명), 추세하단가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)            

    elif data_selected.find("일별체결") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["주문조회", "주문철회", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

            context.bot.edit_message_text(text="일별체결 메뉴를 선택해 주세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id,
                                          reply_markup=show_markup)

        elif len(data_selected.split(",")) == 2:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("주문조회") != -1:

                ac = account()
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                try:
                    context.bot.edit_message_text(text="[일별주문체결 조회]",
                                                  chat_id=update.callback_query.message.chat_id,
                                                  message_id=update.callback_query.message.message_id)

                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, '', '')
                    tdf = pd.DataFrame(output1)
                    tdf.set_index('odno')
                    d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                    for i, name in enumerate(d.index):
                        d_order_no = int(d['odno'][i])
                        d_order_type = d['sll_buy_dvsn_cd_name'][i]
                        d_order_dt = d['ord_dt'][i]
                        d_order_tmd = d['ord_tmd'][i]
                        d_name = d['prdt_name'][i]
                        d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                        d_order_amount = d['ord_qty'][i]
                        d_total_complete_qty = d['tot_ccld_qty'][i]
                        d_remain_qty = d['rmn_qty'][i]
                        d_total_complete_amt = d['tot_ccld_amt'][i]

                        context.bot.send_message(chat_id=update.effective_chat.id, text="[" + d_name + "(" + d['pdno'][i]+ ") - " + d_order_dt + ":" + d_order_tmd + "] 주문번호 : {" + str(d_order_no) + "}, " + d_order_type + "가 : " + format(int(d_order_price), ',d') + "원, " + d_order_type + "량 : " + format(int(d_order_amount), ',d') + "주, 체결수량 : " + format(int(d_total_complete_qty), ',d') + "주, 잔여수량 : " + format(int(d_remain_qty), ',d') + "주, 총체결금액 : " + format(int(d_total_complete_amt), ',d') + "원")

                except Exception as e:
                    print('일별주문체결 조회 오류.', e)
                    context.bot.edit_message_text(text="[일별주문체결 조회] 오류 : "+str(e),
                                                  chat_id=update.callback_query.message.chat_id,
                                                  message_id=update.callback_query.message.message_id)

            elif data_selected.find("주문철회") != -1:
                menuNum = "14"

                context.bot.edit_message_text(text="주문철회할 종목코드(종목명), 주문번호를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

    if data_selected.find("보유종목") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["보유종목조회", "보유종목수정", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

            context.bot.edit_message_text(text="보유종목 메뉴를 선택해 주세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id,
                                          reply_markup=show_markup)

        elif len(data_selected.split(",")) == 2:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("보유종목조회") != -1:

                if len(data_selected.split(",")) == 2:
                    button_list = build_button(["전체조회", "개별조회", "취소"], data_selected)
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

                    context.bot.edit_message_text(text="조회할 메뉴를 선택해 주세요.",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id,
                                                reply_markup=show_markup)
                    
            elif data_selected.find("보유종목수정") != -1:

                if len(data_selected.split(",")) == 2:
                    button_list = build_button(["1차목표가", "1차이탈가", "최종목표가", "최종이탈가", "취소"], data_selected)
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

                    context.bot.edit_message_text(text="수정할 메뉴를 선택해 주세요.",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id,
                                                reply_markup=show_markup)

        elif len(data_selected.split(",")) == 3:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("전체조회") != -1:

                context.bot.edit_message_text(text="[보유종목]",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)
                ac = account()
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 잔고정보 호출
                balance_proc(access_token, app_key, app_secret, acct_no)
                #params = {'acct_no': str(acct_no), 'app_key': app_key, 'app_secret': app_secret, 'access_token': access_token}
                #url = 'http://phills2.gonetis.com:8000/stockBalance/balanceList'   # Django URL 주소
                #url = 'http://localhost:8000/stockBalance/balanceList'  # Django URL 주소
                #response = requests.get(url, params=params)

                #if response.status_code == 200:
                    # 요청이 성공했을 때의 처리
                    #print(response.text)
                #else:
                    # 요청이 실패했을 때의 처리
                    #print('요청이 실패했습니다. 상태 코드:', response.status_code)

                # 보유종목정보 조회
                cur100 = conn.cursor()
                cur100.execute("select purchase_price, purchase_amount, sign_resist_price, sign_support_price, end_target_price, end_loss_price, code, name, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, COALESCE(sell_plan_sum, 0) as sell_plan_sum, COALESCE(sell_plan_amount, 0) as sell_plan_amount, avail_amount from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and proc_yn = 'Y'")
                result_one00 = cur100.fetchall()
                cur100.close()

                for i in result_one00:

                    # 매입가
                    purchase_price = i[0]
                    print("매입가 : " + format(int(purchase_price), ',d'))
                    # 매입수량
                    purchase_amount = i[1]
                    print("매입수량 : " + format(purchase_amount, ',d'))
                    # 매입금액
                    purchase_sum = i[8]
                    print("매입금액 : " + format(purchase_sum, ',d'))
                    # 현재가
                    current_price = i[9]
                    print("현재가 : " + format(current_price, ',d'))
                    # 평가금액
                    eval_sum = i[10]
                    print("평가금액 : " + format(eval_sum, ',d'))
                    # 수익률
                    earning_rate = i[11]
                    print("수익률 : " + str(earning_rate))
                    # 평가손익금액
                    valuation_sum = i[12]
                    print("평가손익금액 : " + format(valuation_sum, ',d'))
                    # 매도예정금액
                    sell_plan_sum = i[13]
                    print("매도예정금액 : " + format(sell_plan_sum, ',d'))
                    # 매도예정수량
                    sell_plan_amount = i[14]
                    print("매도예정수량 : " + format(sell_plan_amount, ',d'))
                    # 매도가능수량
                    avail_amount = i[15]
                    print("매도가능수량 : " + format(avail_amount, ',d'))
                    # 저항가
                    if i[2] != None:
                        sign_resist_price = i[2]
                    else:
                        sign_resist_price = 0
                    print("저항가 : " + format(sign_resist_price, ',d'))
                    # 지지가
                    if i[3] != None:
                        sign_support_price = i[3]
                    else:
                        sign_support_price = 0    
                    print("지지가 : " + format(sign_support_price, ',d'))
                    # 최종목표가
                    if i[4] != None:
                        end_target_price = i[4]
                    else:
                        end_target_price = 0    
                    print("최종목표가 : " + format(end_target_price, ',d'))
                    # 최종이탈가
                    if i[5] != None:
                        end_loss_price = i[5]
                    else:
                        end_loss_price = 0    
                    print("최종이탈가 : " + format(end_loss_price, ',d'))

                    sell_command = f"/BalanceSell_{i[6]}_{avail_amount}"
                    company = i[7] + "[" + i[6] + "]"
            
                    context.bot.send_message(chat_id=update.effective_chat.id, text=(f"{company} : 매입가-{format(int(purchase_price), ',d')}원, 매입수량-{format(purchase_amount, ',d')}주, 매입금액-{format(purchase_sum, ',d')}원, 현재가-{format(current_price, ',d')}원, 평가금액-{format(eval_sum, ',d')}원, 수익률({str(earning_rate)})%, 손수익금액({format(valuation_sum, ',d')})원, 저항가-{format(sign_resist_price, ',d')}원, 지지가-{format(sign_support_price, ',d')}원, 최종목표가-{format(end_target_price, ',d')}원, 최종이탈가-{format(end_loss_price, ',d')}원, 매도예정금액-{format(sell_plan_sum, ',d')}원({format(sell_plan_amount, ',d')}주), 매도가능수량-{format(avail_amount, ',d')}주 => {sell_command}"))
            
                    command_pattern = f"BalanceSell_{i[6]}_{avail_amount}"
                    get_handler = CommandHandler(command_pattern, get_command3)
                    updater.dispatcher.add_handler(get_handler)

            elif data_selected.find("개별조회") != -1:
                menuNum = "15"

                context.bot.edit_message_text(text="보유종목 조회할 종목코드(종목명)를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                
            elif data_selected.find("1차목표가") != -1:
                menuNum = "161"

                context.bot.edit_message_text(text="보유종목 수정할 종목코드(종목명), 1차목표가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id) 

            elif data_selected.find("1차이탈가") != -1:
                menuNum = "162"

                context.bot.edit_message_text(text="보유종목 수정할 종목코드(종목명), 1차이탈가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                
            elif data_selected.find("최종목표가") != -1:
                menuNum = "163"

                context.bot.edit_message_text(text="보유종목 수정할 종목코드(종목명), 최종목표가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("최종이탈가") != -1:
                menuNum = "164"

                context.bot.edit_message_text(text="보유종목 수정할 종목코드(종목명), 최종이탈가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)                                

    if data_selected.find("자산현황") != -1:
        context.bot.edit_message_text(text="[자산현황]",
                                      chat_id=update.callback_query.message.chat_id,
                                      message_id=update.callback_query.message.message_id)
        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        # 자산정보 호출
        fund_proc(access_token, app_key, app_secret, acct_no)
        #params = {'acct_no': str(acct_no), 'app_key': app_key, 'app_secret': app_secret, 'access_token': access_token}
        #url = 'http://phills2.gonetis.com:8000/stockFundMng/list'   # Django URL 주소
        #url = 'http://localhost:8000/stockFundMng/list'  # Django URL 주소
        #response = requests.get(url, params=params)

        #if response.status_code == 200:
            # 요청이 성공했을 때의 처리
            #print(response.text)
        #else:
            # 요청이 실패했을 때의 처리
            #print('요청이 실패했습니다. 상태 코드:', response.status_code)

        # 자산관리정보 조회
        cur300 = conn.cursor()
        cur300.execute("select market_ratio, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt from (select row_number() over (order by id desc) as ROWNUM, COALESCE(market_ratio, 0) as market_ratio, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt from \"stockFundMng_stock_fund_mng\" where acct_no = '" + str(acct_no) + "') A where A.ROWNUM = 1")
        result_three00 = cur300.fetchall()
        cur300.close()
        
        for i in result_three00:
            print("승률 : " + str(i[0]))
            print("현금비중 : " + str(i[1]))
            print("총평가금액 : " + format(int(i[2]), ',d'))
            print("현금액 : " + format(int(i[3]), ',d'))
            print("예수금 : " + format(int(i[4]), ',d'))
            print("가수금 : " + format(int(i[5]), ',d'))
            print("순자산 : " + format(int(i[6]), ',d'))
            print("평가금 : " + format(int(i[7]), ',d'))
            print("증감액 : " + format(int(i[8]), ',d'))
            print("매도예정금 : " + format(int(i[9]), ',d'))
            print("매수예정금 : " + format(int(i[10]), ',d'))

            context.bot.send_message(chat_id=update.effective_chat.id, text="총평가금액-" + format(int(i[2]), ',d') + "원, 현금액-" + format(int(i[3]), ',d') + "원, 현금비중[" + str(i[0]) + "%], 예수금-"+format(int(i[4]), ',d') + "원, 가수금-" + format(int(i[5]), ',d') + "원, 순자산-" + format(int(i[6]), ',d') + "원, 평가금-" + format(int(i[7]), ',d') + "원, 증감액(" + format(int(i[8]), ',d') + ")원, 승률[" + str(i[0]) + "%], 매도예정금-" + format(int(i[9]), ',d') + "원, 매수예정금-" + format(int(i[10]), ',d') + "원")        

    if data_selected.find("시장레벨") != -1:
        context.bot.edit_message_text(text="[시장레벨]",
                                      chat_id=update.callback_query.message.chat_id,
                                      message_id=update.callback_query.message.message_id)
        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        # 시장레벨정보 호출
        marketLevel_proc(access_token, app_key, app_secret, acct_no)
        #params = {'acct_no': str(acct_no), 'app_key': app_key, 'app_secret': app_secret, 'access_token': access_token}
        #url = 'http://phills2.gonetis.com:8000/stockMarketMng/list'   # Django URL 주소
        #url = 'http://localhost:8000/stockMarketMng/list'  # Django URL 주소
        #response = requests.get(url, params=params)

        #if response.status_code == 200:
            # 요청이 성공했을 때의 처리
            #print(response.text)
        #else:
            # 요청이 실패했을 때의 처리
            #print('요청이 실패했습니다. 상태 코드:', response.status_code)  

        # 시장레벨정보 조회
        cur400 = conn.cursor()
        cur400.execute("select market_level_num, total_asset, risk_rate, risk_sum, item_number from \"stockMarketMng_stock_market_mng\" where acct_no = '" + str(acct_no) + "' and aply_end_dt = '99991231'")
        result_four00 = cur400.fetchall()
        cur400.close()
        
        for i in result_four00:
            print("시장레벨번호 : " + str(i[0]))
            print("총자산 : " + format(int(i[1]), ',d'))
            print("리스크 : " + str(i[2]))
            print("리스크금액 : " + format(int(i[3]), ',d'))
            print("종목수 : " + str(i[4]))
            print("종목리스크 : " + format(int(i[3]/i[4]), ',d'))

            context.bot.send_message(chat_id=update.effective_chat.id, text="시장레벨번호[" + str(i[0]) + "], 총자산-" + format(int(i[1]), ',d') + "원, 리스크[" + str(i[2]) + "%], 리스크금액-"+format(int(i[3]), ',d') + "원, 종목수[" + format(int(i[4]), ',d') + "개], 종목리스크-" + format(int(i[3]/i[4]), ',d') + "원")        

    if data_selected.find("종목검색") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["거래폭발", "단기추세", "투자혁명", "파워급등주", "파워종목", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

            context.bot.edit_message_text(text="종목검색 메뉴를 선택해 주세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id,
                                          reply_markup=show_markup)

        elif len(data_selected.split(",")) == 2:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("거래폭발") != -1:

                if len(data_selected.split(",")) == 2:
                    context.bot.edit_message_text(text="[종목검색-거래폭발]",
                                      chat_id=update.callback_query.message.chat_id,
                                      message_id=update.callback_query.message.message_id)
                    ac = account()
                    access_token = ac['access_token']
                    app_key = ac['app_key']
                    app_secret = ac['app_secret']

                    # 종목검색-거래폭발 호출
                    search.search(access_token, app_key, app_secret, arguments[1], '0')
                    
            elif data_selected.find("단기추세") != -1:

                if len(data_selected.split(",")) == 2:
                    context.bot.edit_message_text(text="[종목검색-단기추세]",
                                      chat_id=update.callback_query.message.chat_id,
                                      message_id=update.callback_query.message.message_id)
                    ac = account()
                    access_token = ac['access_token']
                    app_key = ac['app_key']
                    app_secret = ac['app_secret']

                    # 종목검색-단기추세 호출
                    search.search(access_token, app_key, app_secret, arguments[1], '1')           

            elif data_selected.find("투자혁명") != -1:

                if len(data_selected.split(",")) == 2:
                    context.bot.edit_message_text(text="[종목검색-투자혁명]",
                                      chat_id=update.callback_query.message.chat_id,
                                      message_id=update.callback_query.message.message_id)
                    ac = account()
                    access_token = ac['access_token']
                    app_key = ac['app_key']
                    app_secret = ac['app_secret']

                    # 종목검색-투자혁명 호출
                    search.search(access_token, app_key, app_secret, arguments[1], '2')   

            elif data_selected.find("파워급등주") != -1:

                if len(data_selected.split(",")) == 2:
                    context.bot.edit_message_text(text="[종목검색-파워급등주]",
                                      chat_id=update.callback_query.message.chat_id,
                                      message_id=update.callback_query.message.message_id)
                    ac = account()
                    access_token = ac['access_token']
                    app_key = ac['app_key']
                    app_secret = ac['app_secret']

                    # 종목검색-파워급등주 호출
                    search.search(access_token, app_key, app_secret, arguments[1], '3')   

            elif data_selected.find("파워종목") != -1:

                if len(data_selected.split(",")) == 2:
                    context.bot.edit_message_text(text="[종목검색-파워종목]",
                                      chat_id=update.callback_query.message.chat_id,
                                      message_id=update.callback_query.message.message_id)
                    ac = account()
                    access_token = ac['access_token']
                    app_key = ac['app_key']
                    app_secret = ac['app_secret']

                    # 종목검색-파워종목 호출
                    search.search(access_token, app_key, app_secret, arguments[1], '4')                   

    if data_selected.find("초기화") != -1:
        context.bot.edit_message_text(text="초기화 진행합니다.",
                                      chat_id=update.callback_query.message.chat_id,
                                      message_id=update.callback_query.message.message_id)
        os.system ('sudo -su root reboot')
        return

get_handler = CommandHandler('fund', get_command)
updater.dispatcher.add_handler(get_handler)
updater.dispatcher.add_handler(CallbackQueryHandler(callback_get))

# 날짜형식 변환(년월)
def get_date_str(s):
    #print(s)
    date_str = ''
    if s == '2023/12 (IFRS별도)':
        r = re.search("\d{4}/\d{2}", '2022/06(분기)')
    elif s == '2022/03 (IFRS별도)':
        r = re.search("\d{4}/\d{2}", '2022/09(분기)')
    elif s == '2022/12 (IFRS별도)':
        r = re.search("\d{4}/\d{2}", '2022/12(분기)')
    elif s == '2023/03 (IFRS별도)':
        r = re.search("\d{4}/\d{2}", '2023/03(분기)')
    else:
        r = re.search("\d{4}/\d{2}", s)

    if r:
        date_str = r.group()
        date_str = date_str.replace('/', '-')
    #print(date_str)
    return date_str

# 네이버 재무정보 조회
def get_dividiend(code):

    url_tmpl = 'http://companyinfo.stock.naver.com/company/cF1001.aspx?cmp_cd=%s&finGubun=%s'
    url = url_tmpl % (code, 'IFRSS')
    #print(url)

    df = requests.get(url)
    financial_stmt = pd.read_html(df.text)

    dfs = financial_stmt[0]

    dfs.drop('Unnamed: 9_level_0', axis=1, inplace=True, level=0)
    dfs.drop('Unnamed: 10_level_0', axis=1, inplace=True, level=0)
    dfs.drop('Unnamed: 11_level_0', axis=1, inplace=True, level=0)
    dfs.drop('Unnamed: 12_level_0', axis=1, inplace=True, level=0)
    dfs.drop('Unnamed: 13_level_0', axis=1, inplace=True, level=0)
    dfs.drop('Unnamed: 14_level_0', axis=1, inplace=True, level=0)
    dfs.drop('Unnamed: 15_level_0', axis=1, inplace=True, level=0)
    dfs.drop('Unnamed: 16_level_0', axis=1, inplace=True, level=0)
    dfs.drop('Unnamed: 17_level_0', axis=1, inplace=True, level=0)
    dfs.drop('Unnamed: 18_level_0', axis=1, inplace=True, level=0)
    dfs.drop('Unnamed: 19_level_0', axis=1, inplace=True, level=0)
    dfs.drop('Unnamed: 20_level_0', axis=1, inplace=True, level=0)

    dfs.columns = dfs.columns.droplevel(0)
    cols = list(dfs.columns)

    cols = [get_date_str(x) for x in cols]
    dfs.columns = cols
    dft = dfs.T
    dft.index = pd.to_datetime(dft.index)

    # remove if index is NaT
    dft = dft[pd.notnull(dft.index)]
    result = dft.fillna(0)
    print(result)
    return result

def initMenuNum():
    global menuNum
    global chartReq
    menuNum = "0"
    chartReq = "0"

def echo(update, context):
    user_id = update.effective_chat.id
    user_text = update.message.text
    global g_buy_amount
    global g_buy_price
    global g_sell_amount
    global g_sell_price
    global g_buy_code
    global g_sell_code
    global g_company
    global chartReq

    chartReq = "1"

    # 입력메시지가 6자리 이상인 경우,
    if len(user_text) >= 6:
        # 입력메시지가 앞의 6자리가 숫자인 경우,
        if user_text[:6].isdecimal():
            # 입력메시지가 종목코드에 존재하는 경우
            if len(stock_code[stock_code.code == user_text[:6]].values) > 0:
                code = stock_code[stock_code.code == user_text[:6]].code.values[0].strip()  ## strip() : 공백제거
                company = stock_code[stock_code.code == user_text[:6]].company.values[0].strip()  ## strip() : 공백제거
            else:
                code = ""
                ext = user_text[:6] + " : 미존재 종목"
                context.bot.send_message(chat_id=user_id, text=ext)
        else:
            if not ',' in user_text:
                # 입력메시지가 종목명에 존재하는 경우
                if len(stock_code[stock_code.company == user_text].values) > 0:
                    code = stock_code[stock_code.company == user_text].code.values[0].strip()  ## strip() : 공백제거
                    company = stock_code[stock_code.company == user_text].company.values[0].strip()  ## strip() : 공백제거
                else:
                    code = ""
                    ext = user_text + " : 미존재 종목"
                    context.bot.send_message(chat_id=user_id, text=ext)
            else:
                name_text = user_text.split(',')[0].strip()  # ',' 기준으로 첫 번째 값만 사용

                # 입력메시지가 종목명에 존재하는 경우
                if len(stock_code[stock_code.company == name_text].values) > 0:
                    code = stock_code[stock_code.company == name_text].code.values[0].strip()  ## strip() : 공백제거
                    company = stock_code[stock_code.company == name_text].company.values[0].strip()  ## strip() : 공백제거
                else:
                    code = ""
                    ext = name_text + " : 미존재 종목"
                    context.bot.send_message(chat_id=user_id, text=ext)
    else:
        if not ',' in user_text:
            # 입력메시지가 종목명에 존재하는 경우
            if len(stock_code[stock_code.company == user_text].values) > 0:
                code = stock_code[stock_code.company == user_text].code.values[0].strip()  ## strip() : 공백제거
                company = stock_code[stock_code.company == user_text].company.values[0].strip()  ## strip() : 공백제거
            else:
                code = ""
                ext = user_text + " : 미존재 종목"
                context.bot.send_message(chat_id=user_id, text=ext)                        

    if code != "":

        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        # 입력 종목코드 현재가 시세
        a = inquire_price(access_token, app_key, app_secret, code)
        print("현재가 : " + format(int(a['stck_prpr']), ',d'))  # 현재가
        print("최고가 : " + format(int(a['stck_hgpr']), ',d'))  # 최고가
        print("최저가 : " + format(int(a['stck_lwpr']), ',d'))  # 최저가
        print("누적거래량 : " + format(int(a['acml_vol']), ',d'))  # 누적거래량
        print("전일대비거래량비율 : " + a['prdy_vrss_vol_rate'])  # 전일대비거래량비율
        print("시가총액 : " + format(int(a['hts_avls']), ',d')) # 시가총액

        print("menuNum : ", menuNum)

        if menuNum == '10':
            initMenuNum()
            # 관심정보 조회
            cur200 = conn.cursor()
            cur200.execute("select code, name, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, buy_expect_sum from \"interestItem_interest_item\" A where acct_no = '" + str(acct_no) + "' and code = '" + code + "'")
            result_two00 = cur200.fetchall()
            cur200.close()

            if len(result_two00) > 0:
                
                for i in result_two00:
                    print("종목코드 : " + i[0])
                    print("종목명 : " + i[1])
                    print("돌파가 : " + format(int(i[2]), ',d'))
                    print("이탈가 : " + format(int(i[3]), ',d'))
                    print("저항가 : " + format(int(i[4]), ',d'))
                    print("지지가 : " + format(int(i[5]), ',d'))
                    print("추세상단가 : " + format(int(i[6]), ',d'))
                    print("추세하단가 : " + format(int(i[7]), ',d'))
                    print("매수예정금액 : " + format(int(i[8]), ',d'))

                    company = i[1] + "[" + i[0] + "]"

                    context.bot.send_message(chat_id=update.effective_chat.id, text=company + " : 현재가-" + format(int(a['stck_prpr']), ',d') + "원, 고가-" + format(int(a['stck_hgpr']), ',d') + "원, 저가-" + format(int(a['stck_lwpr']), ',d') + "원, 거래량-" + format(int(a['acml_vol']), ',d') + "주, 거래대비-" + a['prdy_vrss_vol_rate'] + "%, 돌파가-" + format(int(i[2]), ',d') + "원, 이탈가-" + format(int(i[3]), ',d') + "원, 저항가-" + format(int(i[4]), ',d') + "원, 지지가-" + format(int(i[5]), ',d') + "원, 추세상단가-" + format(int(i[6]), ',d') + "원, 추세하단가-" + format(int(i[7]), ',d') + "원, 매수예정금액-" + format(int(i[8]), ',d') + "원")

            else:
                print("관심종목 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 미존재")   

        elif menuNum == '11':
            initMenuNum()
            # 관심종목정보 미존재 대상 등록 또는 변경
            # [돌파가] : 금일 고가
            # [이탈가] : 금일 저가,
            # [저항가] : 금일 고가대비 4% 상승가
            # [지지가] : 금일 저가대비 4% 하락가
            # [추세상단가] : 금일 고가대비 12% 상승가
            # [추세하단가] : 금일 저가대비 12% 하락가
            # 매수예정금액 : 시가총액이 5,000억원 초과면 5,000,000원, 5,000억원 이하 2,000억원 초과면 3,000,000원 그외 1,500,000원 설정
            if int(a['hts_avls']) > 5000:
                buy_expect_sum = 5000000
            elif int(a['hts_avls']) < 2000:
                buy_expect_sum = 1500000
            else:
                buy_expect_sum = 3000000

            if int(a['stck_hgpr']) > 0:
                through_price = int(a['stck_hgpr'])
            else:
                through_price = int(a['stck_prpr']) + math.ceil(float(int(a['stck_prpr']) * 0.04))

            if int(a['stck_lwpr']) > 0:
                leave_price = int(a['stck_lwpr'])
            else:
                leave_price = int(a['stck_prpr']) - math.ceil(float(int(a['stck_prpr']) * 0.04))

            cur11 = conn.cursor()
            insert_query0 = "with upsert as (update \"interestItem_interest_item\" set through_price = %s, leave_price = %s, buy_expect_sum = %s, last_chg_date = %s where acct_no = %s and code = %s returning * ) insert into \"interestItem_interest_item\"(acct_no, code, name, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, buy_expect_sum, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
            # insert 인자값 설정
            record_to_insert0 = ([through_price, leave_price, buy_expect_sum, datetime.now(), str(acct_no), code, str(acct_no), code, company, through_price, leave_price, through_price + math.ceil(float(through_price * 0.04)), leave_price - math.ceil(float(leave_price * 0.04)), through_price + math.ceil(float(through_price * 0.12)), leave_price - math.ceil(float(leave_price * 0.12)), buy_expect_sum, datetime.now()])
            # DB 연결된 커서의 쿼리 수행
            cur11.execute(insert_query0, record_to_insert0)
            conn.commit()
            cur11.close()

            # 관심종목 등록
            context.bot.send_message(chat_id=user_id, text=company + " : 현재가[" + format(int(a['stck_prpr']), ',d') + "원], 돌파가[" + format(through_price, ',d') + "원], 이탈가[" + format(leave_price, ',d') + "원], 시가총액[" + format(int(a['hts_avls']), ',d') + "]억원 관심종목 등록")

        elif menuNum == '12':
            initMenuNum()
            # 관심종목정보 조회
            cur12 = conn.cursor()
            cur12.execute("select 1 from \"interestItem_interest_item\" where acct_no = '" + str(acct_no) + "' and code = '" + code + "'")
            result_three = cur12.fetchone()
            cur12.close()

            if result_three != None:

                # 관심종목정보 삭제
                cur121 = conn.cursor()
                delete_query0 = "delete from \"interestItem_interest_item\" where acct_no = %s and code = %s"
                # delete 인자값 설정
                record_to_delete0 = ([str(acct_no), code])
                # DB 연결된 커서의 쿼리 수행
                cur121.execute(delete_query0, record_to_delete0)
                conn.commit()
                cur121.close()

                # 관심종목 삭제
                context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 삭제")

            else:
                print("관심종목 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 미존재")

        elif menuNum == '131':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 돌파가

            # 돌파가 존재시
            if commandBot[1].isdecimal():
                # 관심종목정보 조회
                cur12 = conn.cursor()
                cur12.execute("select 1 from \"interestItem_interest_item\" where acct_no = '" + str(acct_no) + "' and code = '" + code + "'")
                result_three = cur12.fetchone()
                cur12.close()

                if result_three != None:

                    # 관심종목정보 수정
                    cur121 = conn.cursor()
                    delete_query0 = "update \"interestItem_interest_item\" set through_price = %s where acct_no = %s and code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], str(acct_no), code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(delete_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 돌파가 " + format(int(commandBot[1]), ',d') + "원 수정")

                else:
                    print("관심종목 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 미존재")

            else:
                print("돌파가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 돌파가 미존재")        

        elif menuNum == '132':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 이탈가

            # 이탈가 존재시
            if commandBot[1].isdecimal():
                # 관심종목정보 조회
                cur12 = conn.cursor()
                cur12.execute("select 1 from \"interestItem_interest_item\" where acct_no = '" + str(acct_no) + "' and code = '" + code + "'")
                result_three = cur12.fetchone()
                cur12.close()

                if result_three != None:

                    # 관심종목정보 수정
                    cur121 = conn.cursor()
                    delete_query0 = "update \"interestItem_interest_item\" set leave_price = %s where acct_no = %s and code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], str(acct_no), code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(delete_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 이탈가 " + format(int(commandBot[1]), ',d') + "원 수정")

                else:
                    print("관심종목 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 미존재")

            else:
                print("이탈가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 이탈가 미존재")      

        elif menuNum == '133':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 저항가

            # 저항가 존재시
            if commandBot[1].isdecimal():
                # 관심종목정보 조회
                cur12 = conn.cursor()
                cur12.execute("select 1 from \"interestItem_interest_item\" where acct_no = '" + str(acct_no) + "' and code = '" + code + "'")
                result_three = cur12.fetchone()
                cur12.close()

                if result_three != None:

                    # 관심종목정보 수정
                    cur121 = conn.cursor()
                    delete_query0 = "update \"interestItem_interest_item\" set resist_price = %s where acct_no = %s and code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], str(acct_no), code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(delete_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 저항가 " + format(int(commandBot[1]), ',d') + "원 수정")

                else:
                    print("관심종목 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 미존재")

            else:
                print("저항가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 저항가 미존재")

        elif menuNum == '134':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 지지가

            # 지지가 존재시
            if commandBot[1].isdecimal():
                # 관심종목정보 조회
                cur12 = conn.cursor()
                cur12.execute("select 1 from \"interestItem_interest_item\" where acct_no = '" + str(acct_no) + "' and code = '" + code + "'")
                result_three = cur12.fetchone()
                cur12.close()

                if result_three != None:

                    # 관심종목정보 수정
                    cur121 = conn.cursor()
                    delete_query0 = "update \"interestItem_interest_item\" set support_price = %s where acct_no = %s and code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], str(acct_no), code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(delete_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 지지가 " + format(int(commandBot[1]), ',d') + "원 수정")

                else:
                    print("관심종목 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 미존재")

            else:
                print("지지가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 지지가 미존재")

        elif menuNum == '135':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 추세상단가

            # 추세상단가 존재시
            if commandBot[1].isdecimal():
                # 관심종목정보 조회
                cur12 = conn.cursor()
                cur12.execute("select 1 from \"interestItem_interest_item\" where acct_no = '" + str(acct_no) + "' and code = '" + code + "'")
                result_three = cur12.fetchone()
                cur12.close()

                if result_three != None:

                    # 관심종목정보 수정
                    cur121 = conn.cursor()
                    delete_query0 = "update \"interestItem_interest_item\" set trend_high_price = %s where acct_no = %s and code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], str(acct_no), code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(delete_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 추세상단가 " + format(int(commandBot[1]), ',d') + "원 수정")

                else:
                    print("관심종목 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 미존재")

            else:
                print("추세상단가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 추세상단가 미존재")

        elif menuNum == '136':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 저항가

            # 추세하단가 존재시
            if commandBot[1].isdecimal():
                # 관심종목정보 조회
                cur12 = conn.cursor()
                cur12.execute("select 1 from \"interestItem_interest_item\" where acct_no = '" + str(acct_no) + "' and code = '" + code + "'")
                result_three = cur12.fetchone()
                cur12.close()

                if result_three != None:

                    # 관심종목정보 수정
                    cur121 = conn.cursor()
                    delete_query0 = "update \"interestItem_interest_item\" set trend_low_price = %s where acct_no = %s and code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], str(acct_no), code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(delete_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 추세하단가 " + format(int(commandBot[1]), ',d') + "원 수정")

                else:
                    print("관심종목 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 관심종목 미존재")

            else:
                print("추세하단가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 추세하단가 미존재")

        elif menuNum == '14':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 주문번호

            # 주문번호 존재시
            if commandBot[1].isdecimal():
                    
                # 주문번호
                order_no = commandBot[1]
                print("주문번호 : "+order_no)
                try:
                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, code, order_no)
                    tdf = pd.DataFrame(output1)
                    tdf.set_index('odno')
                    d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                    for i, name in enumerate(d.index):
                        d_order_type = d['sll_buy_dvsn_cd_name'][i]
                        d_order_dt = d['ord_dt'][i]
                        d_order_tmd = d['ord_tmd'][i]
                        d_name = d['prdt_name'][i]
                        d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                        d_order_amount = d['ord_qty'][i]
                        d_total_complete_qty = d['tot_ccld_qty'][i]
                        d_remain_qty = d['rmn_qty'][i]
                        d_total_complete_amt = d['tot_ccld_amt'][i]

                        context.bot.send_message(chat_id=user_id, text="일별체결정보 [" + d_name + " - " + d_order_dt + ":" + d_order_tmd + "] " + d_order_type + "가 : " + format(int(d_order_price), ',d') + "원, " + d_order_type + "량 : " + format(int(d_order_amount), ',d') + "주, 체결수량 : " + format(int(d_total_complete_qty), ',d') + "주, 잔여수량 : " + format(int(d_remain_qty), ',d') + "주, 총체결금액 : " + format(int(d_total_complete_amt), ',d')+"원")

                    try:
                        # 주문철회
                        c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "02", order_no, "0", "0")
                        if c['ODNO'] != "":
                            print("주문철회 완료")
                            context.bot.send_message(chat_id=user_id, text= "주문철회 완료 [" + company + "], 주문번호 : " + str(int(c['ODNO'])))
                        else:
                            print("주문철회 실패")
                            context.bot.send_message(chat_id=user_id, text="주문철회 실패 [" + company + "]")

                    except Exception as e:
                        print('주문철회 오류.', e)
                        context.bot.send_message(chat_id=user_id, text="주문철회 오류 [" + company + "] : "+str(e))

                except Exception as e:
                    print('일별주문체결 조회 오류.',e)
                    context.bot.send_message(chat_id=user_id, text="일별주문체결 조회 오류 [" + company + "] : "+str(e))

            else:
                print("주문철회 주문번호 미존재")
                context.bot.send_message(chat_id=user_id, text="주문철회 주문번호 미존재 [" + company + "]")                    

        elif menuNum == '15':
            initMenuNum()
            # 잔고정보 호출
            balance_proc(access_token, app_key, app_secret, acct_no)
            #params = {'acct_no': str(acct_no), 'app_key': app_key, 'app_secret': app_secret, 'access_token': access_token}
            #url = 'http://phills2.gonetis.com:8000/stockBalance/balanceList'   # Django URL 주소
            #url = 'http://localhost:8000/stockBalance/balanceList'  # Django URL 주소
            #response = requests.get(url, params=params)

            #if response.status_code == 200:
                # 요청이 성공했을 때의 처리
                #print(response.text)
            #else:
                # 요청이 실패했을 때의 처리
                #print('요청이 실패했습니다. 상태 코드:', response.status_code)

            # 보유종목정보 조회
            cur100 = conn.cursor()
            cur100.execute("select purchase_price, purchase_amount, sign_resist_price, sign_support_price, end_target_price, end_loss_price, code, name, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, COALESCE(sell_plan_sum, 0) as sell_plan_sum, COALESCE(sell_plan_amount, 0) as sell_plan_amount, avail_amount from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and proc_yn = 'Y' and code = '" + code + "'")
            result_one00 = cur100.fetchall()
            cur100.close()

            if len(result_one00) > 0:
                for i in result_one00:

                    # 매입가
                    purchase_price = i[0]
                    print("매입가 : " + format(int(purchase_price), ',d'))
                    # 매입수량
                    purchase_amount = i[1]
                    print("매입수량 : " + format(purchase_amount, ',d'))
                    # 매입금액
                    purchase_sum = i[8]
                    print("매입금액 : " + format(purchase_sum, ',d'))
                    # 현재가
                    current_price = i[9]
                    print("현재가 : " + format(current_price, ',d'))
                    # 평가금액
                    eval_sum = i[10]
                    print("평가금액 : " + format(eval_sum, ',d'))
                    # 수익률
                    earning_rate = i[11]
                    print("수익률 : " + str(earning_rate))
                    # 평가손익금액
                    valuation_sum = i[12]
                    print("평가손익금액 : " + format(valuation_sum, ',d'))
                    # 매도예정금액
                    sell_plan_sum = i[13]
                    print("매도예정금액 : " + format(sell_plan_sum, ',d'))
                    # 매도예정수량
                    sell_plan_amount = i[14]
                    print("매도예정수량 : " + format(sell_plan_amount, ',d'))
                    # 매도가능수량
                    avail_amount = i[15]
                    print("매도가능수량 : " + format(avail_amount, ',d'))
                    # 저항가
                    if i[2] != None:
                        sign_resist_price = i[2]
                    else:
                        sign_resist_price = 0
                    print("저항가 : " + format(sign_resist_price, ',d'))
                    # 지지가
                    if i[3] != None:
                        sign_support_price = i[3]
                    else:
                        sign_support_price = 0    
                    print("지지가 : " + format(sign_support_price, ',d'))
                    # 최종목표가
                    if i[4] != None:
                        end_target_price = i[4]
                    else:
                        end_target_price = 0    
                    print("최종목표가 : " + format(end_target_price, ',d'))
                    # 최종이탈가
                    if i[5] != None:
                        end_loss_price = i[5]
                    else:
                        end_loss_price = 0    
                    print("최종이탈가 : " + format(end_loss_price, ',d'))

                    sell_command = f"/BalanceSell_{i[6]}_{avail_amount}"
                    company = i[7] + "[" + i[6] + "]"
            
                    context.bot.send_message(chat_id=update.effective_chat.id, text=(f"{company} : 매입가-{format(int(purchase_price), ',d')}원, 매입수량-{format(purchase_amount, ',d')}주, 매입금액-{format(purchase_sum, ',d')}원, 현재가-{format(current_price, ',d')}원, 평가금액-{format(eval_sum, ',d')}원, 수익률({str(earning_rate)})%, 손수익금액({format(valuation_sum, ',d')})원, 저항가-{format(sign_resist_price, ',d')}원, 지지가-{format(sign_support_price, ',d')}원, 최종목표가-{format(end_target_price, ',d')}원, 최종이탈가-{format(end_loss_price, ',d')}원, 매도예정금액-{format(sell_plan_sum, ',d')}원({format(sell_plan_amount, ',d')}주), 매도가능수량-{format(avail_amount, ',d')}주 => {sell_command}"))
            
                    command_pattern = f"BalanceSell_{i[6]}_{avail_amount}"
                    get_handler = CommandHandler(command_pattern, get_command3)
                    updater.dispatcher.add_handler(get_handler)

            else:
                print("보유종목 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 보유종목 미존재")   

        elif menuNum == '161':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 1차목표가

            # 1차목표가 존재시
            if commandBot[1].isdecimal():
                # 보유종목 조회
                cur12 = conn.cursor()
                cur12.execute("select 1 from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and code = '" + code + "' and proc_yn = 'Y'")
                result_three = cur12.fetchone()
                cur12.close()

                if result_three != None:

                    # 보유종목 수정
                    cur121 = conn.cursor()
                    delete_query0 = "update \"stockBalance_stock_balance\" set sign_resist_price = %s where acct_no = %s and code = %s and proc_yn = 'Y'"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], str(acct_no), code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(delete_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 1차목표가 " + format(int(commandBot[1]), ',d') + "원 수정")

                else:
                    print("보유종목 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 보유종목 미존재")

            else:
                print("1차목표가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 1차목표가 미존재") 

        elif menuNum == '162':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 1차이탈가

            # 1차이탈가 존재시
            if commandBot[1].isdecimal():
                # 보유종목 조회
                cur12 = conn.cursor()
                cur12.execute("select 1 from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and code = '" + code + "' and proc_yn = 'Y'")
                result_three = cur12.fetchone()
                cur12.close()

                if result_three != None:

                    # 보유종목 수정
                    cur121 = conn.cursor()
                    delete_query0 = "update \"stockBalance_stock_balance\" set sign_support_price = %s where acct_no = %s and code = %s and proc_yn = 'Y'"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], str(acct_no), code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(delete_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 1차이탈가 " + format(int(commandBot[1]), ',d') + "원 수정")

                else:
                    print("보유종목 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 보유종목 미존재")

            else:
                print("1차이탈가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 1차이탈가 미존재") 

        elif menuNum == '163':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 최종목표가

            # 최종목표가 존재시
            if commandBot[1].isdecimal():
                # 보유종목 조회
                cur12 = conn.cursor()
                cur12.execute("select 1 from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and code = '" + code + "' and proc_yn = 'Y'")
                result_three = cur12.fetchone()
                cur12.close()

                if result_three != None:

                    # 보유종목 수정
                    cur121 = conn.cursor()
                    delete_query0 = "update \"stockBalance_stock_balance\" set end_target_price = %s where acct_no = %s and code = %s and proc_yn = 'Y'"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], str(acct_no), code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(delete_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 최종목표가 " + format(int(commandBot[1]), ',d') + "원 수정")

                else:
                    print("보유종목 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 보유종목 미존재")

            else:
                print("최종목표가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 최종목표가 미존재") 

        elif menuNum == '164':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 최종이탈가

            # 최종이탈가 존재시
            if commandBot[1].isdecimal():
                # 보유종목 조회
                cur12 = conn.cursor()
                cur12.execute("select 1 from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and code = '" + code + "' and proc_yn = 'Y'")
                result_three = cur12.fetchone()
                cur12.close()

                if result_three != None:

                    # 보유종목 수정
                    cur121 = conn.cursor()
                    delete_query0 = "update \"stockBalance_stock_balance\" set end_loss_price = %s where acct_no = %s and code = %s and proc_yn = 'Y'"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], str(acct_no), code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(delete_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 최종이탈가 " + format(int(commandBot[1]), ',d') + "원 수정")

                else:
                    print("보유종목 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 보유종목 미존재")

            else:
                print("최종이탈가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 최종이탈가 미존재") 

        elif menuNum == '21':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=4)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 매수가
                print("commandBot[2] : ", commandBot[2])    # 이탈가
                print("commandBot[3] : ", commandBot[3])    # 손절금액

            # 매수가 존재시
            if commandBot[1].isdecimal():

                # 매수가
                buy_price = commandBot[1]
                print("매수가 : " + format(int(buy_price), ',d'))

                # 이탈가 존재시
                if commandBot[2].isdecimal():
                    # 이탈가
                    loss_price = commandBot[2]
                    print("이탈가 : " + format(int(loss_price), ',d'))

                    # 손절금액 존재시
                    if commandBot[3].isdecimal():
                        # 손절금액
                        item_loss_sum = commandBot[3]
                        print("손절금액 : " + format(int(item_loss_sum), ',d'))
                        # 매수량
                        n_buy_amount = int(item_loss_sum) / (int(buy_price) - int(loss_price))
                        print("매수량 : " + format(int(round(n_buy_amount)), ',d'))
                        # 매수금액
                        n_buy_sum = int(buy_price) * round(n_buy_amount)
                        print("매수금액 : " + format(int(n_buy_sum), ',d'))

                        # 매수 가능(현금) 조회
                        b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                        print("매수 가능(현금) : " + format(int(b), ',d'));
                        if int(b) > n_buy_sum:  # 매수가능(현금)이 매수금액이 더 큰 경우

                            g_buy_amount = round(n_buy_amount)
                            g_buy_price = buy_price
                            g_buy_code = code
                            g_company = company
                            
                            context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가 : " + format(int(buy_price), ',d') + "원, 매수량 : " + format(int(round(n_buy_amount)), ',d') + "주, 손절가 : " + format(int(loss_price), ',d') + "원, 종목손실금액 : " + format(int(item_loss_sum), ',d') + "원, 매수금액 : " + format(int(n_buy_sum), ',d') + "원 => /buy")
                            get_handler = CommandHandler('buy', get_command1)
                            updater.dispatcher.add_handler(get_handler)

                        else:
                            print("매수 가능(현금) 부족")
                            context.bot.send_message(chat_id=user_id, text="["+company + "] 매수금액 : " + format(int(n_buy_sum), ',d') + "원, 매수 가능(현금) : " + format(int(b) - int(n_buy_sum), ',d') +"원 부족")

                    else:
                        print("손절금액 미존재")
                        context.bot.send_message(chat_id=user_id, text=company + " : 손절금액 미존재")    
                else:
                    print("이탈가 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 이탈가 미존재")
                
            else:
                print("매수가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매수가 미존재")

        elif menuNum == '22':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=3)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 매수가    
                print("commandBot[2] : ", commandBot[2])    # 매수예정금액

            # 매수가 존재시
            if commandBot[1].isdecimal():

                # 매수가
                buy_price = commandBot[1]
                print("매수가 : " + format(int(buy_price), ',d'))

                # 매수예정금액 존재시
                if commandBot[2].isdecimal():
                    buy_expect_sum = commandBot[2]
                    print("매수예정금액 : " + format(int(buy_expect_sum), ',d'))
                    # 매수량
                    n_buy_amount = round(int(buy_expect_sum) / int(buy_price))
                    print("매수량 : " + format(int(n_buy_amount), ',d'))
                    # 매수금액
                    n_buy_sum = int(buy_price) * int(n_buy_amount)
                    print("매수금액 : " + format(int(n_buy_sum), ',d'))

                    # 매수 가능(현금) 조회
                    b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                    print("매수 가능(현금) : " + format(int(b), ',d'));
                    if int(b) > n_buy_sum:  # 매수가능(현금)이 매수금액이 더 큰 경우
                                                    
                        g_buy_amount = n_buy_amount
                        g_buy_price = buy_price
                        g_buy_code = code
                        g_company = company
                    
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가 : " + format(int(buy_price), ',d') + "원, 매수량 : " + format(int(n_buy_amount), ',d') + "주, 매수금액 : " + format(int(n_buy_sum), ',d') + "원 => /buy")
                        get_handler = CommandHandler('buy', get_command1)
                        updater.dispatcher.add_handler(get_handler)
                    
                    else:
                        print("매수 가능(현금) 부족")
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수금액 : " + format(n_buy_sum, ',d') + "원, 매수 가능(현금) : " + format(int(b) - n_buy_sum, ',d') +"원 부족")

                else:
                    print("매수예정금액 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 매수예정금액 미존재")   

            else:
                print("매수가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매수가 미존재")


        elif menuNum == '23':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 매수예정금액

            # 매수예정금액 존재시
            if commandBot[1].isdecimal():

                # 매수예정금액
                buy_expect_sum = commandBot[1]
                print("매수예정금액 : " + format(int(buy_expect_sum), ',d'))
                # 매수량
                n_buy_amount = round(int(buy_expect_sum) / int(a['stck_prpr']))
                print("매수량 : " + format(int(n_buy_amount), ',d'))
                # 매수금액
                n_buy_sum = int(a['stck_prpr']) * int(n_buy_amount)
                print("매수금액 : " + format(int(n_buy_sum), ',d'))
                
                # 매수 가능(현금) 조회
                b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                print("매수 가능(현금) : " + format(int(b), ',d'));
                
                if int(b) > n_buy_sum:  # 매수가능(현금)이 매수금액이 더 큰 경우

                    g_buy_amount = n_buy_amount
                    g_buy_price = int(a['stck_prpr'])
                    g_buy_code = code
                    g_company = company
                    
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 매수량 : " + format(int(n_buy_amount), ',d') + "주, 매수금액 : " + format(n_buy_sum, ',d') + "원 => /buy")
                    get_handler = CommandHandler('buy', get_command1)
                    updater.dispatcher.add_handler(get_handler)

                else:
                    print("매수 가능(현금) 부족")
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 현재가 : " + format(int(a['stck_prpr']), ',d') + "원, 매수 가능(현금) : " + format(int(b) - n_buy_sum, ',d') +"원 부족")
                
            else:
                print("매수예정금액 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매수예정금액 미존재")   
                    
        elif menuNum == '24':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=3)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 매수량
                print("commandBot[1] : ", commandBot[2])    # 매수가

            # 매수량, 매수가 존재시
            if commandBot[1].isdecimal() & commandBot[2].isdecimal():

                # 매수량
                buy_amount = commandBot[1]
                print("매수량 : " + format(int(buy_amount), ',d'))
                # 매수금액
                n_buy_sum = int(commandBot[2]) * int(buy_amount)
                print("매수금액 : " + format(int(n_buy_sum), ',d'))

                # 시가총액 기준 매수예상금액 설정
                if int(a['hts_avls']) > 5000:
                    buy_expect_sum = 5000000
                elif int(a['hts_avls']) < 2000:
                    buy_expect_sum = 2000000
                else:
                    buy_expect_sum = 3000000

                print("매수예상금액 : " + format(buy_expect_sum, ',d'))

                if buy_expect_sum >= n_buy_sum: # 매수예상금액이 매수금액보다 큰 경우

                    # 매수 가능(현금) 조회
                    b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                    print("매수 가능(현금) : " + format(int(b), ',d'));
                    if int(b) > n_buy_sum:  # 매수가능(현금)이 매수금액이 더 큰 경우

                        g_buy_amount = buy_amount
                        g_buy_price = int(commandBot[2])
                        g_buy_code = code
                        g_company = company
                        
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가 : " + format(int(commandBot[2]), ',d') + "원, 매수량 : " + format(int(buy_amount), ',d') + "주, 매수금액 : " + format(n_buy_sum, ',d') + "원 => /buy")
                        get_handler = CommandHandler('buy', get_command1)
                        updater.dispatcher.add_handler(get_handler)

                    else:
                        print("매수 가능(현금) 부족")
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가 : " + format(int(commandBot[2]), ',d') + "원, 매수량 : " + format(int(buy_amount), ',d') + "주, 매수 가능(현금) : " + format(int(b) - n_buy_sum, ',d') +"원 부족")
                else:
                    print("매수량 매수예상금액 초과")
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가 : " + format(int(commandBot[2]), ',d') + "원, 매수량 : " + format(int(buy_amount), ',d') + "주, 매수예상금액 : " + format(buy_expect_sum, ',d') + "원, " + format(buy_expect_sum - n_buy_sum, ',d') +"원 매수량 기준 매수예상금액 초과")

            else:
                print("매수량 또는 매수가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매수량 또는 매수가 미존재")        

        elif menuNum == '31':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 매도가

            # 매도가 존재시
            if commandBot[1].isdecimal():

                # 매도가
                sell_price = commandBot[1]
                print("매도가 : " + format(int(sell_price), ',d'))

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
                #e = get_acct_balance_sell(access_token, app_key, app_secret, acct_no)
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))

                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우

                    # 매도금액
                    n_sell_sum = int(sell_price) * ord_psbl_qty
                    print("매도금액 : " + format(n_sell_sum, ',d'))

                    g_sell_amount = ord_psbl_qty
                    g_sell_price = sell_price
                    g_sell_code = code
                    g_company = company

                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(int(sell_price), ',d') + "원, 매도량 : " + format(ord_psbl_qty, ',d') + "주, 매도금액 : " + format(int(n_sell_sum), ',d') + "원 => /sell")
                    get_handler = CommandHandler('sell', get_command2)
                    updater.dispatcher.add_handler(get_handler)

                else:
                    print("주문가능수량 부족")
                    context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 부족")
            else:
                print("매도가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매도가 미존재")

        elif menuNum == '32':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 매도가

            # 매도가 존재시
            if commandBot[1].isdecimal():

                # 매도가
                sell_price = commandBot[1]
                print("매도가 : " + format(int(sell_price), ',d'))

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
                #e = get_acct_balance_sell(access_token, app_key, app_secret, acct_no)
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우

                    sell_amount = int(round(ord_psbl_qty / 2))
                    print("주문가능수량 절반수량 : " + format(sell_amount, ',d'))
                    # 매도금액
                    n_sell_sum = int(sell_price) * sell_amount
                    print("매도금액 : " + format(n_sell_sum, ',d'))

                    g_sell_amount = sell_amount
                    g_sell_price = sell_price
                    g_sell_code = code
                    g_company = company

                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(int(sell_price), ',d') + "원, 매도량 : " + format(sell_amount, ',d') + "주, 매도금액 : " + format(int(n_sell_sum), ',d') + "원 => /sell")
                    get_handler = CommandHandler('sell', get_command2)
                    updater.dispatcher.add_handler(get_handler)

                else:
                    print("주문가능수량 부족")
                    context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 부족")
            else:
                print("매도가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매도가 미존재")

        elif menuNum == '33':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 매도량

            # 매도량 존재시
            if commandBot[1].isdecimal():

                # 매도량
                sell_amount = commandBot[1]
                print("매도량 : " + format(int(sell_amount), ',d'))

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
                #e = get_acct_balance_sell(access_token, app_key, app_secret, acct_no)
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우
                    if ord_psbl_qty >= int(sell_amount):  # 주문가능수량이 매도량보다 큰 경우

                        # 매도금액
                        n_sell_sum = int(a['stck_prpr']) * int(sell_amount)
                        print("매도금액 : " + format(n_sell_sum, ',d'))

                        g_sell_amount = sell_amount
                        g_sell_price = int(a['stck_prpr'])
                        g_sell_code = code
                        g_company = company

                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(int(a['stck_prpr']), ',d') + "원, 매도량 : " + format(int(sell_amount), ',d') + "주, 매도금액 : " + format(int(n_sell_sum), ',d') + "원 => /sell")
                        get_handler = CommandHandler('sell', get_command2)
                        updater.dispatcher.add_handler(get_handler)

                    else:
                        print("주문가능수량 매도량보다 부족")
                        context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 매도량보다 부족")
                else:
                    print("주문가능수량 부족")
                    context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 부족")
            else:
                print("매도량 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매도량 미존재")

        elif menuNum == '34':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=3)

                print("commandBot[0] : ", commandBot[0])    # 종목코드
                print("commandBot[1] : ", commandBot[1])    # 매도량
                print("commandBot[2] : ", commandBot[2])    # 매도가

            # 매도량, 매도가 존재시
            if commandBot[1].isdecimal() & commandBot[2].isdecimal():

                # 매도량
                sell_amount = commandBot[1]
                print("매도량 : " + format(int(sell_amount), ',d'))

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
                #e = get_acct_balance_sell(access_token, app_key, app_secret, acct_no)
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우
                    if ord_psbl_qty >= int(sell_amount):  # 주문가능수량이 매도량보다 큰 경우

                        # 매도금액
                        n_sell_sum = int(commandBot[2]) * int(sell_amount)
                        print("매도금액 : " + format(n_sell_sum, ',d'))

                        g_sell_amount = sell_amount
                        g_sell_price = int(commandBot[2])
                        g_sell_code = code
                        g_company = company

                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(int(commandBot[2]), ',d') + "원, 매도량 : " + format(int(sell_amount), ',d') + "주, 매도금액 : " + format(int(n_sell_sum), ',d') + "원 => /sell")
                        get_handler = CommandHandler('sell', get_command2)
                        updater.dispatcher.add_handler(get_handler)

                    else:
                        print("주문가능수량 매도량보다 부족")
                        context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 매도량보다 부족")
                else:
                    print("주문가능수량 부족")
                    context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 부족")
            else:
                print("매도량 또는 매도가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매도량 또는 매도가 미존재")

    else:
        print("종목코드 미존재")
        ext = user_text + " : 종목코드 미존재"
        context.bot.send_message(chat_id=user_id, text=ext)            

    if not ',' in user_text:
        if len(code) > 0 and chartReq == "1":
            dividend = get_dividiend(code)

    def get_chart(code):
        title = company + '[' + code + ']'
        pre_day = datetime.today() - timedelta(500)
        start = pre_day.strftime("%Y-%m-%d")
        df = fdr.DataReader(code, start)
        df.head()
        df.rename(columns={'Date': '날짜', 'Open': '시가', 'High': '고가', 'Low': '저가', 'Close': '종가', 'Volume': '거래량'},
                  inplace=True)
        df = df[['시가', '고가', '저가', '종가', '거래량']]

        fig = plt.figure(figsize=(10, 7))
        fig.set_facecolor('white')

        num_row = 2
        gs = gridspec.GridSpec(num_row, 1, height_ratios=(3.5, 1.5))

        ax_top = fig.add_subplot(gs[0, :])

        ## 분봉(캔들) 차트
        candlestick2_ohlc(ax_top, df['시가'], df['고가'], df['저가'], df['종가'],
                          width=0.8,  ## 막대 폭 비율 조절
                          colorup='r',  ## 종가가 시가보다 높은 경우에 색상
                          colordown='b'  ## 종가가 시가보다 낮은 경우에 색상
                          )
        xticks = range(len(df))[::5]
        xticklabels = [x.strftime('%m-%d') for x in df.index[::5]]
        ax_top.set_xticks(xticks)
        ax_top.set_xticklabels(xticklabels, fontsize=8)
        ax_top.tick_params(axis='x', rotation=90)
        ax_top.set_title(title, fontsize=15)
        ax_top.grid()

        # 색깔 구분을 위한 함수
        color_fuc = lambda x: 'r' if x >= 0 else 'b'
        color_list = list(df['거래량'].diff().fillna(0).apply(color_fuc))

        ## 거래량 바 차트
        ax_bottom = fig.add_subplot(gs[1, :])

        ax_bottom.bar(range(len(df)), df['거래량'], color=color_list)
        ax_bottom.yaxis.set_major_locator(mticker.FixedLocator(ax_bottom.get_yticks()))
        ax_bottom.set_yticklabels(['{:.0f}'.format(x) for x in ax_bottom.get_yticks()])

        xticks = range(len(df))[::5]
        xticklabels = [x.strftime('%Y-%m-%d') for x in df.index[::5]]
        ax_bottom.set_xticks(xticks)
        ax_bottom.set_xticklabels(xticklabels, fontsize=8)
        ax_bottom.tick_params(axis='x', rotation=90)
        ax_bottom.grid()

        plt.savefig('/home/terra/Public/Batch/save1.png')

    def get_sales_sum(i):

        dict = {}
        count = 0

        for x in dividend.index:

            # 연도 및 분기별 대상 count > 7, 연도별 대상 count > 3
            if count > 7:
                break
            else:
                row = str(x)
                idx = 0

                for val in dividend[i]:
                    dfrow = str(dividend[i].index[idx])

                    if row[0:10] == dfrow[0:10]:
                        if idx > 3:
                            dict["[" + dfrow[0:7] + "]"] = format(int(val), ',d')
                        else:
                            dict[dfrow[0:7]] = format(int(val), ',d')

                    idx += 1

                count += 1

        return dict

    def return_print(*message):
        io = StringIO()
        print(*message, file=io)
        return io.getvalue()

    if not ',' in user_text:
        if len(code) > 0 and chartReq == "1":
            get_chart(code)
            context.bot.send_photo(chat_id=user_id, photo=open('/home/terra/Public/Batch/save1.png', 'rb'))

            text0 = return_print("<" + company + ">")
            text1 = return_print("[매출액]")
            for date in get_sales_sum(0).keys():
                #print("%s : %s" % (date, get_sales_sum(0)[date]))
                text1 = text1+return_print("%s : %s" % (date, get_sales_sum(0)[date]))
            text2 = return_print("[영업이익]")
            for date in get_sales_sum(1).keys():
                #print("%s : %s" % (date, get_sales_sum(1)[date]))
                text2 = text2+return_print("%s : %s" % (date, get_sales_sum(1)[date]))
            text3 = return_print("[당기순이익]")
            for date in get_sales_sum(4).keys():
                #print("%s : %s" % (date, get_sales_sum(4)[date]))
                text3 = text3+return_print("%s : %s" % (date, get_sales_sum(4)[date]))
            text4 = return_print("[BPS(원)]")
            for date in get_sales_sum(27).keys():
                #print("%s : %s" % (date, get_sales_sum(27)[date]))
                text4 = text4+return_print("%s : %s" % (date, get_sales_sum(27)[date]))
            text5 = return_print("[현금DPS(원)]")
            for date in get_sales_sum(29).keys():
                #print("%s : %s" % (date, get_sales_sum(29)[date]))
                text5 = text5+return_print("%s : %s" % (date, get_sales_sum(29)[date]))
            text6 = return_print("[발행주식수(보통주)]")
            for date in get_sales_sum(32).keys():
                #print("%s : %s" % (date, get_sales_sum(32)[date]))
                text6 = text6+return_print("%s : %s" % (date, get_sales_sum(32)[date]))

            context.bot.send_message(chat_id=user_id, text=text0+text1+text2+text3+text4+text5+text6)

# 텔레그램봇 응답 처리
echo_handler = MessageHandler(Filters.text & (~Filters.command), echo)
dispatcher.add_handler(echo_handler)
dispatcher.add_handler(MessageHandler(Filters.regex(r"^/HoldingSell_\w+_\d+"), handle_holding_sell))
dispatcher.add_handler(MessageHandler(Filters.regex(r"^/InterestBuy_\w+_\d+"), handle_interest_buy))

# 텔레그램봇 polling
updater.start_polling()