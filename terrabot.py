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
from bs4 import BeautifulSoup
import time

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
# 숫자로만 이루어진 코드만 필터링
stock_code = stock_code[stock_code['code'].astype(str).str.match(r'^\d+$')]
# 종목코드를 문자열 6자리로 포맷팅
stock_code['code'] = stock_code['code'].astype(str).str.zfill(6)

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
    token = "6008784254:AAGYG-ZqwsJ4EKeidhzxn2EaYNLLFOPRMBI"    

# 텔레그램봇 updater(토큰, 입력값)
updater = Updater(token=token, use_context=True)
dispatcher = updater.dispatcher

menuNum = "0"
chartReq = "1"
g_market_buy_company = ""
g_market_sell_company = ""
g_market_buy_code = ""
g_market_sell_code = ""
g_market_buy_amount = 0
g_market_sell_amount = 0
g_buy_code = ""
g_sell_code = ""
g_code = ""
g_company = ""
g_order_no = ""
g_dvsn_cd = ""
g_buy_price = 0
g_buy_amount = 0
g_sell_price = 0
g_sell_amount = 0
g_revise_price = 0
g_remain_qty = 0

def format_number(value):
    try:
        return f"{float(value):,.2f}" if isinstance(value, float) else f"{int(value):,}"
    except:
        return str(value)

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
    button_list = build_button(["시수", "시도", "매수", "매도", "자동", "역매", "관심", "체결", "보유", "자산", "레벨", "검색"]) # make button list
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 6)) # make markup
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup

def get_command1(update, context) :
    button_list = build_button(["매수진행", "7mjs2c수진", "7m수진", "js수진", "취소"]) # make button list
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list))) # make markup
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup
   
def get_command2(update, context) :
    button_list = build_button(["매도진행", "7mjs2c도진", "7m도진", "js도진", "취소"]) # make button list
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

def get_command4(update, context) :
    button_list = build_button(["정정진행", "7mjs2c정정", "7m정정", "js정정", "취소"]) # make button list
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list))) # make markup
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup

def get_command5(update, context) :
    button_list = build_button(["철회진행", "7mjs2c철회", "7m철회", "js철회", "취소"]) # make button list
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list))) # make markup
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup

# 시장가 재매수
def get_command6(update, context) :
    
    ac = account()
    acct_no = ac['acct_no']
    access_token = ac['access_token']
    app_key = ac['app_key']
    app_secret = ac['app_secret']

    ord_dvsn = "01"

    try:

        # 입력 종목코드 현재가 호가/예상체결
        a1 = inquire_asking_price(access_token, app_key, app_secret, g_market_buy_code)

        # 2-ask trade_qty
        ask_trade_qty = int(a1['askp_rsqn1'])+int(a1['askp_rsqn2'])

        # 매수량
        n_buy_amount = min(g_market_buy_amount, ask_trade_qty)
        # 매수예정금액
        buy_expect_sum = n_buy_amount * int(a1['askp1'])

        # 매수 가능(현금) 조회
        b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
        print("매수 가능(현금) : " + format(int(b), ',d'));
               
        if int(b) > int(buy_expect_sum):  # 매수가능(현금)이 매수예정금액보다 큰 경우

            if ask_trade_qty < g_market_buy_amount:
                message = f"[{g_market_buy_company}] 주문수량 {format_number(g_market_buy_amount)}주가 매도호가 2구간 체결가능수량 {format_number(ask_trade_qty)}주 초과, {format(n_buy_amount)}주만 주문 진행합니다."
                update.message.reply_text(message)

            # 매수
            c = order_cash(True, access_token, app_key, app_secret, str(acct_no), g_market_buy_code, ord_dvsn, str(n_buy_amount), str(0))

            if c['ODNO'] != "":

                # 일별주문체결 조회
                output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_market_buy_code, c['ODNO'])
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
                    message = f"[{g_market_buy_company}] 매수가 : {format_number(int(d_order_price))}원, 체결량 : {format(int(d_total_complete_qty))}주, 체결금액 : {format(int(d_total_complete_amt))}원, 주문번호 : <code>{str(d_order_no)}</code> => /rebuy"
                    update.message.reply_text(message, parse_mode='HTML')

            else:
                print("매수주문 실패")
                message = f"[{g_market_buy_company}] 매수량 : {format_number(int(n_buy_amount))}주 매수주문 실패"
                update.message.reply_text(message)

        else:
            print("매수 가능(현금) 부족")
            message = f"[{g_market_buy_company}] 매수 가능(현금) : {format_number(int(b) - int(buy_expect_sum))}원 부족"
            update.message.reply_text(message)

    except Exception as e:
        print('매수주문 오류.', e)
        message = f"[{g_market_buy_company}] 매수량 : {format_number(int(n_buy_amount))}주 [매수주문 오류] - {str(e)}"
        update.message.reply_text(message)

# 시장가 재매도
def get_command7(update, context) :
    
    ac = account()
    acct_no = ac['acct_no']
    access_token = ac['access_token']
    app_key = ac['app_key']
    app_secret = ac['app_secret']

    # 계좌잔고 조회
    e = stock_balance(access_token, app_key, app_secret, acct_no, "")
    
    ord_psbl_qty = 0
    for j, name in enumerate(e.index):
        e_code = e['pdno'][j]
        if e_code == g_market_sell_code:
            ord_psbl_qty = int(e['ord_psbl_qty'][j])
    print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
    if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우
        # 매도량
        n_sell_sum = min(g_market_sell_amount, ord_psbl_qty)

        ord_dvsn = "01"
        try:

            # 입력 종목코드 현재가 호가/예상체결
            a1 = inquire_asking_price(access_token, app_key, app_secret, g_market_sell_code)

            # 2-bid trade_qty
            bid_trade_qty = int(a1['bidp_rsqn1'])+int(a1['bidp_rsqn2'])

            order_amount = min(n_sell_sum, bid_trade_qty)

            if order_amount < n_sell_sum:
                message = f"[{g_market_sell_company}] 주문수량 {format_number(n_sell_sum)}주가 매수호가 2구간 체결가능수량 {format_number(bid_trade_qty)}주 초과, {format(order_amount)}주만 주문 진행합니다."
                update.message.reply_text(message)

            # 매도
            c = order_cash(False, access_token, app_key, app_secret, str(acct_no), g_market_sell_code, ord_dvsn, str(order_amount), str(0))
    
            if c['ODNO'] != "":

                # 일별주문체결 조회
                output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_market_sell_code, c['ODNO'])
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
                    message = f"[{g_market_sell_company}] 매도가 : {format_number(int(d_order_price))}원, 체결량 : {format(int(d_total_complete_qty))}주, 체결금액 : {format(int(d_total_complete_amt))}원, 주문번호 : <code>{str(d_order_no)}</code> => /resell"
                    update.message.reply_text(message, parse_mode='HTML')
                
            else:
                print("매도주문 실패")
                message = f"[{g_market_sell_company}] 매도량 : {format_number(int(order_amount))}주 매도주문 실패"
                update.message.reply_text(message)

        except Exception as e:
            print('매도주문 오류.', e)
            message = f"[{g_market_sell_company}] 매도량 : {format_number(int(n_sell_sum))}주 [매도주문 오류] - {str(e)}"
            update.message.reply_text(message)

    else:
        print("주문가능수량 부족")
        message = f"[{g_market_sell_company}] 주문가능수량 부족"
        update.message.reply_text(message)

# ngrok URL 가져오기
def get_ngrok_url(retries=5, delay=2):
    url = "http://localhost:4040/api/tunnels"
    for _ in range(retries):
        try:
            response = requests.get(url)
            tunnels = response.json()['tunnels']
            for tunnel in tunnels:
                if tunnel['proto'] == 'https':
                    return tunnel['public_url']
        except Exception as e:
            print(f"Waiting for ngrok... ({e})")
            time.sleep(delay)
    return None

def get_command_info(update, context) :
    ngrok_url = get_ngrok_url()
    if ngrok_url:
        message = f"Streamlit 대시보드가 열렸습니다!\n => [접속하기]({ngrok_url})"
        update.message.reply_text(message)
        return
    else:
        update.message.reply_text("ngrok URL을 가져오지 못했습니다.")
        return

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
def account(nickname=None):
    cur01 = conn.cursor()
    if nickname is None:
        cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day from \"stockAccount_stock_account\" where nick_name = '" + arguments[1] + "'")
    else:
        cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day from \"stockAccount_stock_account\" where nick_name = '" + nickname + "'")        
    result_two = cur01.fetchone()
    cur01.close()

    acct_no = result_two[0]
    access_token = result_two[1]
    app_key = result_two[2]
    app_secret = result_two[3]
    today = datetime.now().strftime("%Y%m%d")

    YmdHMS = datetime.now()
    validTokenDate = datetime.strptime(result_two[4], '%Y%m%d%H%M%S')
    diff = YmdHMS - validTokenDate
    # print("diff : " + str(diff.days))
    if diff.days >= 1 or result_two[5] != today:  # 토큰 유효기간(1일) 만료 재발급
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
                'FID_COND_MRKT_DIV_CODE': "J",  # J:KRX, NX:NXT, UN:통합
                'FID_INPUT_ISCD': code
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)

    return ar.getBody().output

# 주식현재가 호가/예상체결
def inquire_asking_price(access_token, app_key, app_secret, code):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "FHKST01010200",
               "custtype": "P"}
    params = {
                'FID_COND_MRKT_DIV_CODE': "J",  # J:KRX, NX:NXT, UN:통합
                'FID_INPUT_ISCD': code
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)

    return ar.getBody().output1

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

# 매수 가능(현금) 조회
def inquire_psbl_order(access_token, app_key, app_secret, acct_no):
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8908R"             # tr_id : TTTC8908R[실전투자], VTTC8908R[모의투자]
    }            
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "PDNO": "",                      # 종목번호(6자리)
               "ORD_UNPR": "0",                 # 1주당 가격
               "ORD_DVSN": "02",                # 02 : 조건부지정가
               "CMA_EVLU_AMT_ICLD_YN": "Y",     # CMA평가금액포함여부
               "OVRS_ICLD_YN": "N"              # 해외포함여부
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-psbl-order"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)

    return ar.getBody().output['nrcvb_buy_amt']

# 매도 가능 수량 조회
def inquire_psbl_sell(access_token, app_key, app_secret, acct_no, code):
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8408R",
               "custtype": "P"
    }            
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "PDNO": code                      # 종목번호(6자리)
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-psbl-sell"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)

    return ar.getBody().output

# 주식주문(현금)
def order_cash(buy_flag, access_token, app_key, app_secret, acct_no, stock_code, ord_dvsn, order_qty, order_price, cndt_price=None):

    if buy_flag:
        tr_id = "TTTC0012U"                     #buy : TTTC0012U[실전투자], VTTC0012U[모의투자]
    else:
        tr_id = "TTTC0011U"                     #sell : TTTC0011U[실전투자], VTTC0011U[모의투자]

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
               "ORD_DVSN": ord_dvsn,            # 00 : 지정가, 01 : 시장가, 22 : 스톱지정가
               "ORD_QTY": order_qty,
               "ORD_UNPR": order_price          # 시장가 등 주문시, "0"으로 입력
    }
    # 스톱지정가일 때만 조건가격 추가
    if ord_dvsn == "22":
        params["CNDT_PRIC"] = str(cndt_price)

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
               "tr_id": "TTTC0013U",            # TTTC0013U[실전투자], VTTC0013U[모의투자]
               "custtype": "P"
    }
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "KRX_FWDG_ORD_ORGNO": "06010",
               "ORGN_ODNO": order_no,
               "ORD_DVSN": "00" if int(order_price) > 0 else "01",  # 지정가 : 00, 시장가 : 01
               "RVSE_CNCL_DVSN_CD": cncl_dv,    # 정정 : 01, 취소 : 02
               "ORD_QTY": str(order_qty),
               "ORD_UNPR": str(order_price),
               "QTY_ALL_ORD_YN": "Y"            # 전량 : Y, 일부 : N
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
               "tr_id": "TTTC8001R",                                # tr_id : TTTC8001R(실전투자 3개월이내), CTSC9215R(실전투자 3개월이전), VTTC0081R(모의투자 3개월이내), VTSC9215R(모의투자 3개월이전)
               "custtype": "P"
    }  
    params = {
                "CANO": acct_no,
                "ACNT_PRDT_CD": '01',
                "INQR_STRT_DT": datetime.now().strftime('%Y%m%d'),  # 조회시작일자 YYYYMMDD
                "INQR_END_DT": datetime.now().strftime('%Y%m%d'),   # 조회종료일자 YYYYMMDD
                "SLL_BUY_DVSN_CD": '00',                            # 매도매수구분코드 : 00 전체, 01 매도, 02 매수
                "PDNO": code,
                "ORD_GNO_BRNO": "",
                "ODNO": order_no,
                "CCLD_DVSN": "00",                                  # 체결구분 : 00 전체, 01 체결, 02 미체결
                "INQR_DVSN": '00',                                  # 조회구분 : 00 역순, 01 정순
                "INQR_DVSN_1": "",        
                "INQR_DVSN_3": "00",                                # 조회구분3 : 00 전체, 01 현금, 02 신용
                "EXCG_ID_DVSN_CD": "KRX",                           # 거래소ID구분코드 : KRX, NXT
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
            update_query301 = "with upsert as (update \"stockBalance_stock_balance\" set purchase_price = %s, purchase_amount = %s, purchase_sum = %s, current_price = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, sell_plan_sum = %s, sell_plan_amount = %s, limit_price = %s, proc_yn = 'Y', last_chg_date = %s where acct_no = %s and code = %s and asset_num = %s returning * ) insert into \"stockBalance_stock_balance\"(acct_no, code, name, purchase_price, purchase_amount, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, asset_num, sell_plan_sum, sell_plan_amount, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, limit_price, proc_yn, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select A.sign_resist_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_resist_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.sign_support_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_support_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_loss_price from(select row_number() over(order by last_chg_date desc) as num, b.end_loss_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_target_price from(select row_number() over(order by last_chg_date desc) as num, b.end_target_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.trading_plan from(select row_number() over(order by last_chg_date desc) as num, b.trading_plan from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.limit_price from (select row_number() over(order by last_chg_date desc) as num, b.limit_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num = 1), %s, %s where not exists(select * from upsert)";
            # update 인자값 설정
            record_to_update301 = ([e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, e_sell_plan_sum, e_sell_plan_amount, limit_price, datetime.now(), acct_no, e_code, asset_num,
                                    acct_no, e_code, e_name, e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, asset_num, e_sell_plan_sum, e_sell_plan_amount, 
                                    acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code,
                                    'Y', datetime.now()])
            # DB 연결된 커서의 쿼리 수행
            cur301.execute(update_query301, record_to_update301)
            conn.commit()
            cur301.close()

        else:
            cur301 = conn.cursor()
            update_query301 = "with upsert as (update \"stockBalance_stock_balance\" set purchase_price = %s, purchase_amount = %s, purchase_sum = %s, current_price = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, limit_price = %s, proc_yn = 'Y', last_chg_date = %s where acct_no = %s and code = %s and asset_num = %s returning * ) insert into \"stockBalance_stock_balance\"(acct_no, code, name, purchase_price, purchase_amount, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, asset_num, sign_resist_price, sign_support_price, end_loss_price, end_target_price, trading_plan, limit_price, proc_yn, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select A.sign_resist_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_resist_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.sign_support_price from(select row_number() over(order by last_chg_date desc) as num, b.sign_support_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_loss_price from(select row_number() over(order by last_chg_date desc) as num, b.end_loss_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.end_target_price from(select row_number() over(order by last_chg_date desc) as num, b.end_target_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.trading_plan from(select row_number() over(order by last_chg_date desc) as num, b.trading_plan from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where A.num=1), (select A.limit_price from (select row_number() over(order by last_chg_date desc) as num, b.limit_price from \"stockBalance_stock_balance\" b where acct_no = %s and code = %s) A where	A.num = 1), %s, %s where not exists(select * from upsert)";
            # update 인자값 설정
            record_to_update301 = ([e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, limit_price, datetime.now(), acct_no, e_code, asset_num,
                                    acct_no, e_code, e_name, e_purchase_price, e_purchase_amount, e_purchase_sum, e_current_price, e_eval_sum, e_earnings_rate, e_valuation_sum, asset_num, 
                                    acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code, acct_no, e_code,
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
    if len(command_parts) < 3:
        update.message.reply_text("잘못된 명령어 형식입니다.")
        return
   
    stock_code = command_parts[1]
    current_price = int(command_parts[2])
   
    button_list = build_button(["신호매수", "취소"]) # make button list
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list))) # make markup
   
    button_list = [
        InlineKeyboardButton(f"신호매수 ({stock_code}])", callback_data=f"신호매수_{stock_code}_{current_price}"),
        InlineKeyboardButton("취소", callback_data="취소")
    ]
    show_markup = InlineKeyboardMarkup([button_list])
   
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup

def callback_get(update, context) :
    data_selected = update.callback_query.data
    global menuNum
    global g_order_no
    global g_remain_qty

    print("callback0 : ", data_selected)
    if data_selected.find("취소") != -1:
        context.bot.edit_message_text(text="취소하였습니다.",
                                      chat_id=update.callback_query.message.chat_id,
                                      message_id=update.callback_query.message.message_id)
        return

    elif data_selected.find("철회진행") != -1:

        if g_order_no != "":
            ac = account()
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']

            try:
                # 주문취소
                c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "02", g_order_no, "0", "0")
                if c['ODNO'] != "":
                    print("주문취소 완료")
                    context.bot.edit_message_text(text="주문취소 완료 [" + g_company + "], 주문번호 : <code>" + str(int(c['ODNO'])) + "</code>", parse_mode='HTML',
                                                        chat_id=update.callback_query.message.chat_id,
                                                        message_id=update.callback_query.message.message_id)
                else:
                    print("주문취소 실패")
                    context.bot.edit_message_text(text="주문취소 실패 [" + g_company + "]",
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)

            except Exception as e:
                print('주문취소 오류.', e)
                context.bot.edit_message_text(text="주문취소 오류 [" + g_company + "] : "+str(e),
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)
            menuNum = "0"  
            g_order_no = "" 
        else:
            print("주문번호 미존재")
            context.bot.edit_message_text(text="[" + g_company + "] 주문번호 미존재",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)                
    
    elif data_selected.find("7mjs2c철회") != -1:

        if g_order_no != "":
            result_msgs = []
            nickname_list = ['phills75', 'yh480825', 'phills13', 'phills15', 'phills2','chichipa']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']
            
                try:
                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_code, '')
                    
                    if len(output1) > 0:
                    
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['pdno', 'odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]
                        order_type = ""
                        order_no = 0
                        rmn_qty = 0

                        for i, name in enumerate(d.index):

                            # 매수매도구분코드 일치하고 잔여수량 존재시
                            if g_dvsn_cd == d['sll_buy_dvsn_cd'][i]: 
                                order_type = "매도" if g_dvsn_cd == "01" else "매수"
                                
                                if int(d['rmn_qty'][i]) > 0: 
                                    order_no = int(d['odno'][i])
                                    rmn_qty = int(d['rmn_qty'][i])

                                    # 주문취소
                                    c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "02", str(order_no), "0", "0")
                                    if c['ODNO'] != "":
                                        print("주문취소 완료")
                                        msg = f"[{nick}:{g_company}] 주문취소 완료, 주문번호 : <code>{str(int(c['ODNO']))}</code>"
                                        result_msgs.append(msg)

                                    else:
                                        print("주문취소 실패")
                                        msg = f"[{nick}:{g_company}] 주문취소 실패"
                                        result_msgs.append(msg)

                        if rmn_qty == 0:
                            print("주문취소내역 미존재")
                            msg = f"[{nick}:{g_company}] {order_type} 주문취소내역 미존재"
                            result_msgs.append(msg)        
                        
                    else:
                        print("주문내역 미존재")
                        msg = f"[{nick}:{g_company}] 주문내역 미존재"
                        result_msgs.append(msg)
                    
                except Exception as e:
                    print('주문취소 오류.', e)
                    msg = f"[{nick}:{g_company}] 주문취소 오류 - {str(e)}"
                    result_msgs.append(msg)

                final_message = "\n".join(result_msgs) if result_msgs else "주문취소 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )

            menuNum = "0"  
            g_order_no = "" 

        else:
            print("주문번호 미존재")
            context.bot.edit_message_text(text="[" + g_company + "] 주문번호 미존재",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)   
    
    elif data_selected.find("7m철회") != -1:

        if g_order_no != "":
            result_msgs = []
            nickname_list = ['phills75', 'yh480825']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']
            
                try:
                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_code, '')
                    
                    if len(output1) > 0:
                    
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['pdno', 'odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]
                        order_type = ""
                        order_no = 0
                        rmn_qty = 0

                        for i, name in enumerate(d.index):

                            # 매수매도구분코드 일치하고 잔여수량 존재시
                            if g_dvsn_cd == d['sll_buy_dvsn_cd'][i]: 
                                order_type = "매도" if g_dvsn_cd == "01" else "매수"
                                
                                if int(d['rmn_qty'][i]) > 0: 
                                    order_no = int(d['odno'][i])
                                    rmn_qty = int(d['rmn_qty'][i])

                                    # 주문취소
                                    c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "02", str(order_no), "0", "0")
                                    if c['ODNO'] != "":
                                        print("주문취소 완료")
                                        msg = f"[{nick}:{g_company}] 주문취소 완료, 주문번호 : <code>{str(int(c['ODNO']))}</code>"
                                        result_msgs.append(msg)

                                    else:
                                        print("주문취소 실패")
                                        msg = f"[{nick}:{g_company}] 주문취소 실패"
                                        result_msgs.append(msg)

                        if rmn_qty == 0:
                            print("주문취소내역 미존재")
                            msg = f"[{nick}:{g_company}] {order_type} 주문취소내역 미존재"
                            result_msgs.append(msg)        
                        
                    else:
                        print("주문내역 미존재")
                        msg = f"[{nick}:{g_company}] 주문내역 미존재"
                        result_msgs.append(msg)
                    
                except Exception as e:
                    print('주문취소 오류.', e)
                    msg = f"[{nick}:{g_company}] 주문취소 오류 - {str(e)}"
                    result_msgs.append(msg)

                final_message = "\n".join(result_msgs) if result_msgs else "주문취소 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )

            menuNum = "0"  
            g_order_no = "" 

        else:
            print("주문번호 미존재")
            context.bot.edit_message_text(text="[" + g_company + "] 주문번호 미존재",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)  
            
    elif data_selected.find("js철회") != -1:

        if g_order_no != "":
            result_msgs = []
            nickname_list = ['phills13', 'phills15']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']
            
                try:
                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_code, '')
                    
                    if len(output1) > 0:
                    
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['pdno', 'odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]
                        order_type = ""
                        order_no = 0
                        rmn_qty = 0

                        for i, name in enumerate(d.index):

                            # 매수매도구분코드 일치하고 잔여수량 존재시
                            if g_dvsn_cd == d['sll_buy_dvsn_cd'][i]: 
                                order_type = "매도" if g_dvsn_cd == "01" else "매수"
                                
                                if int(d['rmn_qty'][i]) > 0: 
                                    order_no = int(d['odno'][i])
                                    rmn_qty = int(d['rmn_qty'][i])

                                    # 주문취소
                                    c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "02", str(order_no), "0", "0")
                                    if c['ODNO'] != "":
                                        print("주문취소 완료")
                                        msg = f"[{nick}:{g_company}] 주문취소 완료, 주문번호 : <code>{str(int(c['ODNO']))}</code>"
                                        result_msgs.append(msg)

                                    else:
                                        print("주문취소 실패")
                                        msg = f"[{nick}:{g_company}] 주문취소 실패"
                                        result_msgs.append(msg)

                        if rmn_qty == 0:
                            print("주문취소내역 미존재")
                            msg = f"[{nick}:{g_company}] {order_type} 주문취소내역 미존재"
                            result_msgs.append(msg)        
                        
                    else:
                        print("주문내역 미존재")
                        msg = f"[{nick}:{g_company}] 주문내역 미존재"
                        result_msgs.append(msg)
                    
                except Exception as e:
                    print('주문취소 오류.', e)
                    msg = f"[{nick}:{g_company}] 주문취소 오류 - {str(e)}"
                    result_msgs.append(msg)

                final_message = "\n".join(result_msgs) if result_msgs else "주문취소 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )

            menuNum = "0"  
            g_order_no = "" 

        else:
            print("주문번호 미존재")
            context.bot.edit_message_text(text="[" + g_company + "] 주문번호 미존재",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)              

    elif data_selected.find("정정진행") != -1:

        if g_order_no != "" and int(g_remain_qty) > 0:
            ac = account()
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']
        
            # 주문정정
            try:
                c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "01", g_order_no, g_remain_qty, g_revise_price)
                if c['ODNO'] != "":
                    print("주문정정 완료")
                    context.bot.edit_message_text(text="주문정정 완료 [" + g_company + "], 주문번호 : <code>" + str(int(c['ODNO'])) + "</code>", parse_mode='HTML',
                                                        chat_id=update.callback_query.message.chat_id,
                                                        message_id=update.callback_query.message.message_id)
                else:
                    print("주문정정 실패")
                    context.bot.edit_message_text(text="주문정정 실패 [" + g_company + "]",
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)
            except Exception as e:
                print('주문정정 오류.', e)
                context.bot.edit_message_text(text="주문정정 오류 [" + g_company + "] : "+str(e),
                                                    chat_id=update.callback_query.message.chat_id,
                                                    message_id=update.callback_query.message.message_id)
            menuNum = "0"  
            g_order_no = "" 
            g_remain_qty = 0
        else:
            print("주문번호 또는 정정수량 미존재")
            context.bot.edit_message_text(text="[" + g_company + "] 주문번호 또는 정정수량 미존재",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)
    
    elif data_selected.find("7mjs2c정정") != -1:

        if g_order_no != "" and int(g_remain_qty) > 0:
            result_msgs = []
            nickname_list = ['phills75', 'yh480825', 'phills13', 'phills15', 'phills2','chichipa']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']
            
                try:
                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_code, '')
                    
                    if len(output1) > 0:
                    
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['pdno', 'odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]
                        order_type = ""
                        order_no = 0
                        rmn_qty = 0

                        for i, name in enumerate(d.index):

                            # 매수매도구분코드 일치하고 잔여수량 존재시
                            if g_dvsn_cd == d['sll_buy_dvsn_cd'][i]: 
                                order_type = "매도" if g_dvsn_cd == "01" else "매수"
                                
                                if int(d['rmn_qty'][i]) > 0: 
                                    order_no = int(d['odno'][i])
                                    rmn_qty = int(d['rmn_qty'][i])

                                    # 주문정정
                                    c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "01", str(order_no), rmn_qty, g_revise_price)
                                    if c['ODNO'] != "":
                                        print("주문정정 완료")
                                        msg = f"[{nick}:{g_company}] 주문정정 완료, 주문번호 : <code>{str(int(c['ODNO']))}</code>"
                                        result_msgs.append(msg)
                                        
                                    else:
                                        print("주문정정 실패")
                                        msg = f"[{nick}:{g_company}] 주문정정 실패"
                                        result_msgs.append(msg)

                        if rmn_qty == 0:
                            print("주문정정내역 미존재")
                            msg = f"[{nick}:{g_company}] {order_type} 주문정정내역 미존재"
                            result_msgs.append(msg)        
                        
                    else:
                        print("주문내역 미존재")
                        msg = f"[{nick}:{g_company}] 주문내역 미존재"
                        result_msgs.append(msg)
                    
                except Exception as e:
                    print('주문정정 오류.', e)
                    msg = f"[{nick}:{g_company}] 주문정정 오류 - {str(e)}"
                    result_msgs.append(msg)

                final_message = "\n".join(result_msgs) if result_msgs else "주문정정 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )

            menuNum = "0"  
            g_order_no = "" 
            g_remain_qty = 0    

        else:
            print("주문번호 또는 정정수량 미존재")
            context.bot.edit_message_text(text="[" + g_company + "] 주문번호 또는 정정수량 미존재",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)                  
    
    elif data_selected.find("7m정정") != -1:

        if g_order_no != "" and int(g_remain_qty) > 0:
            result_msgs = []
            nickname_list = ['phills75', 'yh480825']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']
            
                try:
                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_code, '')
                    
                    if len(output1) > 0:
                    
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['pdno', 'odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]
                        order_type = ""
                        order_no = 0
                        rmn_qty = 0

                        for i, name in enumerate(d.index):

                            # 매수매도구분코드 일치하고 잔여수량 존재시
                            if g_dvsn_cd == d['sll_buy_dvsn_cd'][i]: 
                                order_type = "매도" if g_dvsn_cd == "01" else "매수"
                                
                                if int(d['rmn_qty'][i]) > 0: 
                                    order_no = int(d['odno'][i])
                                    rmn_qty = int(d['rmn_qty'][i])

                                    # 주문정정
                                    c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "01", str(order_no), rmn_qty, g_revise_price)
                                    if c['ODNO'] != "":
                                        print("주문정정 완료")
                                        msg = f"[{nick}:{g_company}] 주문정정 완료, 주문번호 : <code>{str(int(c['ODNO']))}</code>"
                                        result_msgs.append(msg)
                                        
                                    else:
                                        print("주문정정 실패")
                                        msg = f"[{nick}:{g_company}] 주문정정 실패"
                                        result_msgs.append(msg)

                        if rmn_qty == 0:
                            print("주문정정내역 미존재")
                            msg = f"[{nick}:{g_company}] {order_type} 주문정정내역 미존재"
                            result_msgs.append(msg)        
                        
                    else:
                        print("주문내역 미존재")
                        msg = f"[{nick}:{g_company}] 주문내역 미존재"
                        result_msgs.append(msg)
                    
                except Exception as e:
                    print('주문정정 오류.', e)
                    msg = f"[{nick}:{g_company}] 주문정정 오류 - {str(e)}"
                    result_msgs.append(msg)

                final_message = "\n".join(result_msgs) if result_msgs else "주문정정 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )

            menuNum = "0"  
            g_order_no = "" 
            g_remain_qty = 0    

        else:
            print("주문번호 또는 정정수량 미존재")
            context.bot.edit_message_text(text="[" + g_company + "] 주문번호 또는 정정수량 미존재",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)
    
    elif data_selected.find("js정정") != -1:

        if g_order_no != "" and int(g_remain_qty) > 0:
            result_msgs = []
            nickname_list = ['phills13', 'phills15']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']
            
                try:
                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, g_code, '')
                    
                    if len(output1) > 0:
                    
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['pdno', 'odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]
                        order_type = ""
                        order_no = 0
                        rmn_qty = 0

                        for i, name in enumerate(d.index):

                            # 매수매도구분코드 일치하고 잔여수량 존재시
                            if g_dvsn_cd == d['sll_buy_dvsn_cd'][i]: 
                                order_type = "매도" if g_dvsn_cd == "01" else "매수"
                                
                                if int(d['rmn_qty'][i]) > 0: 
                                    order_no = int(d['odno'][i])
                                    rmn_qty = int(d['rmn_qty'][i])

                                    # 주문정정
                                    c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "01", str(order_no), rmn_qty, g_revise_price)
                                    if c['ODNO'] != "":
                                        print("주문정정 완료")
                                        msg = f"[{nick}:{g_company}] 주문정정 완료, 주문번호 : <code>{str(int(c['ODNO']))}</code>"
                                        result_msgs.append(msg)
                                        
                                    else:
                                        print("주문정정 실패")
                                        msg = f"[{nick}:{g_company}] 주문정정 실패"
                                        result_msgs.append(msg)

                        if rmn_qty == 0:
                            print("주문정정내역 미존재")
                            msg = f"[{nick}:{g_company}] {order_type} 주문정정내역 미존재"
                            result_msgs.append(msg)        
                        
                    else:
                        print("주문내역 미존재")
                        msg = f"[{nick}:{g_company}] 주문내역 미존재"
                        result_msgs.append(msg)
                    
                except Exception as e:
                    print('주문정정 오류.', e)
                    msg = f"[{nick}:{g_company}] 주문정정 오류 - {str(e)}"
                    result_msgs.append(msg)

                final_message = "\n".join(result_msgs) if result_msgs else "주문정정 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )

            menuNum = "0"  
            g_order_no = "" 
            g_remain_qty = 0    

        else:
            print("주문번호 또는 정정수량 미존재")
            context.bot.edit_message_text(text="[" + g_company + "] 주문번호 또는 정정수량 미존재",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)
    
    elif data_selected.find("매수진행") != -1:

        if menuNum != "0":
            ac = account()
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']

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

                        context.bot.edit_message_text(text="[" + d_name + "] 매수가 : " + format(int(d_order_price), ',d') + "원, 매수량 : " + format(int(d_order_amount), ',d') + "주 매수주문 완료, 주문번호 : <code>" + str(d_order_no) +"</code>", parse_mode='HTML',
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

    elif data_selected.find("7mjs2c수진") != -1:

        if menuNum != "0":
            result_msgs = []
            nickname_list = ['phills75', 'yh480825', 'phills13', 'phills15', 'phills2','chichipa']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']
            
                # 매수 가능(현금) 조회
                b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                print("매수 가능(현금) : " + format(int(b), ',d'));
                if int(b) > int(g_buy_amount):  # 매수가능(현금)이 매수금액이 더 큰 경우

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
                                msg = f"[{nick}:{d_name}] 매수가 : {int(d_order_price):,}원, 매수량 : {int(d_order_amount):,}주 매수주문 완료, 주문번호 : <code>{d_order_no}</code>"
                                result_msgs.append(msg)
                                    
                        else:
                            print("매수주문 실패")
                            msg = f"[{nick}:{g_buy_code}] 매수가 : {int(g_buy_price):,}원, 매수량 : {int(g_buy_amount):,}주 매수주문 실패"
                            result_msgs.append(msg)

                        menuNum = "0"

                    except Exception as e:
                        msg = f"[{nick}:{g_buy_code}] 매수가 : {int(g_buy_price):,}원, 매수량 : {int(g_buy_amount):,}주 [매수주문 오류] - {str(e)}"
                        result_msgs.append(msg)
                        menuNum = "0"
                            
                else:
                    print(nick+" : 매수 가능(현금) 부족")
                    msg = f"[{nick}:{g_buy_code}] 매수가 : {int(g_buy_price):,}원, 매수량 : {int(g_buy_amount):,}주 매수 가능(현금) 부족"
                    result_msgs.append(msg)
                    menuNum = "0"
           
            menuNum = "0"
            final_message = "\n".join(result_msgs) if result_msgs else "주문 조건을 충족하지 못했습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=update.callback_query.message.chat_id,
                message_id=update.callback_query.message.message_id
            )

        else:
            context.bot.edit_message_text(text="처음 메뉴부터 매수 진행하세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id)
            
    elif data_selected.find("7m수진") != -1:

        if menuNum != "0":
            result_msgs = []
            nickname_list = ['phills75', 'yh480825']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']
            
                # 매수 가능(현금) 조회
                b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                print("매수 가능(현금) : " + format(int(b), ',d'));
                if int(b) > int(g_buy_amount):  # 매수가능(현금)이 매수금액이 더 큰 경우

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
                                msg = f"[{nick}:{d_name}] 매수가 : {int(d_order_price):,}원, 매수량 : {int(d_order_amount):,}주 매수주문 완료, 주문번호 : <code>{d_order_no}</code>"
                                result_msgs.append(msg)
                                    
                        else:
                            print("매수주문 실패")
                            msg = f"[{nick}:{g_buy_code}] 매수가 : {int(g_buy_price):,}원, 매수량 : {int(g_buy_amount):,}주 매수주문 실패"
                            result_msgs.append(msg)

                        menuNum = "0"

                    except Exception as e:
                        msg = f"[{nick}:{g_buy_code}] 매수가 : {int(g_buy_price):,}원, 매수량 : {int(g_buy_amount):,}주 [매수주문 오류] - {str(e)}"
                        result_msgs.append(msg)
                        menuNum = "0"
                            
                else:
                    print(nick+" : 매수 가능(현금) 부족")
                    msg = f"[{nick}:{g_buy_code}] 매수가 : {int(g_buy_price):,}원, 매수량 : {int(g_buy_amount):,}주 매수 가능(현금) 부족"
                    result_msgs.append(msg)
                    menuNum = "0"
                    
            menuNum = "0"
            final_message = "\n".join(result_msgs) if result_msgs else "주문 조건을 충족하지 못했습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=update.callback_query.message.chat_id,
                message_id=update.callback_query.message.message_id
            )        
           
        else:
            context.bot.edit_message_text(text="처음 메뉴부터 매수 진행하세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id)        
    
    elif data_selected.find("js수진") != -1:

        if menuNum != "0":
            result_msgs = []
            nickname_list = ['phills13', 'phills15']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']
            
                # 매수 가능(현금) 조회
                b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                print("매수 가능(현금) : " + format(int(b), ',d'));
                if int(b) > int(g_buy_amount):  # 매수가능(현금)이 매수금액이 더 큰 경우

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
                                msg = f"[{nick}:{d_name}] 매수가 : {int(d_order_price):,}원, 매수량 : {int(d_order_amount):,}주 매수주문 완료, 주문번호 : <code>{d_order_no}</code>"
                                result_msgs.append(msg)
                                    
                        else:
                            print("매수주문 실패")
                            msg = f"[{nick}:{g_buy_code}] 매수가 : {int(g_buy_price):,}원, 매수량 : {int(g_buy_amount):,}주 매수주문 실패"
                            result_msgs.append(msg)

                        menuNum = "0"

                    except Exception as e:
                        msg = f"[{nick}:{g_buy_code}] 매수가 : {int(g_buy_price):,}원, 매수량 : {int(g_buy_amount):,}주 [매수주문 오류] - {str(e)}"
                        result_msgs.append(msg)
                        menuNum = "0"
                            
                else:
                    print(nick+" : 매수 가능(현금) 부족")
                    msg = f"[{nick}:{g_buy_code}] 매수가 : {int(g_buy_price):,}원, 매수량 : {int(g_buy_amount):,}주 매수 가능(현금) 부족"
                    result_msgs.append(msg)
                    menuNum = "0"
           
            menuNum = "0"
            final_message = "\n".join(result_msgs) if result_msgs else "주문 조건을 충족하지 못했습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=update.callback_query.message.chat_id,
                message_id=update.callback_query.message.message_id
            )
        
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

                        context.bot.edit_message_text(text="[" + d_name + "] 매도가 : " + format(int(d_order_price), ',d') + "원, 매도량 : " + format(int(d_order_amount), ',d') + "주 매도주문 완료, 주문번호 : <code>" + str(d_order_no) +"</code>", parse_mode='HTML',
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

    elif data_selected.find("7mjs2c도진") != -1:

        if menuNum != "0":
            result_msgs = []
            nickname_list = ['phills75', 'yh480825', 'phills13', 'phills15', 'phills2','chichipa']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 계좌종목 조회
                sb = stock_balance(access_token, app_key, app_secret, acct_no, "")

                code_chk = ""
                if isinstance(sb, pd.DataFrame) and not sb.empty:
                    for j, name in enumerate(sb.index):
                        j_code = ""
                        if 'pdno' in sb.columns and pd.notna(sb.loc[name, 'pdno']):
                            j_code = sb.loc[name, 'pdno']
                        
                        # 잔고정보의 매도대상 종목이 존재할 경우
                        if j_code == g_sell_code:
                            code_chk = "hold"
                            j_ord_psbl_qty = int(sb['ord_psbl_qty'][j])
                            # 주문가능수량이 매도수량보다 더 많은 경우
                            if j_ord_psbl_qty >= int(g_sell_amount):

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
                                            msg = f"[{nick}:{d_name}] 매도가 : {int(d_order_price):,}원, 매도량 : {int(d_order_amount):,}주 매도주문 완료, 주문번호 : <code>{d_order_no}</code>"
                                            result_msgs.append(msg)

                                    else:
                                        print("매도주문 실패")
                                        msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 매도주문 실패"
                                        result_msgs.append(msg)

                                    menuNum = "0"

                                except Exception as e:
                                    print('매도주문 오류.', e)
                                    msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 [매도주문 오류] - {str(e)}"
                                    result_msgs.append(msg)
                                    menuNum = "0"
                            
                            else:
                                print(nick+" : "+j_code+" 주문가능수량 부족")
                                msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 주문가능수량 부족"
                                result_msgs.append(msg)
                                menuNum = "0"
                
                if code_chk == "":
                    print(nick+" : 보유종목 미존재")
                    msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 보유종목 미존재"
                    result_msgs.append(msg)
                    menuNum = "0"

            menuNum = "0"
            final_message = "\n".join(result_msgs) if result_msgs else "매도대상 종목이 존재하지 않거나 주문 조건을 충족하지 못했습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=update.callback_query.message.chat_id,
                message_id=update.callback_query.message.message_id
            )

        else:
            context.bot.edit_message_text(text="처음 메뉴부터 매도 진행하세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id)   
    
    elif data_selected.find("7m도진") != -1:

        if menuNum != "0":
            result_msgs = []
            nickname_list = ['phills75', 'yh480825']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 계좌종목 조회
                sb = stock_balance(access_token, app_key, app_secret, acct_no, "")

                code_chk = ""
                if isinstance(sb, pd.DataFrame) and not sb.empty:
                    for j, name in enumerate(sb.index):
                        j_code = ""
                        if 'pdno' in sb.columns and pd.notna(sb.loc[name, 'pdno']):
                            j_code = sb.loc[name, 'pdno']
                        
                        # 잔고정보의 매도대상 종목이 존재할 경우
                        if j_code == g_sell_code:
                            code_chk = "hold"
                            j_ord_psbl_qty = int(sb['ord_psbl_qty'][j])
                            # 주문가능수량이 매도수량보다 더 많은 경우
                            if j_ord_psbl_qty >= int(g_sell_amount):

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
                                            msg = f"[{nick}:{d_name}] 매도가 : {int(d_order_price):,}원, 매도량 : {int(d_order_amount):,}주 매도주문 완료, 주문번호 : <code>{d_order_no}</code>"
                                            result_msgs.append(msg)
                                        
                                    else:
                                        print("매도주문 실패")
                                        msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 매도주문 실패"
                                        result_msgs.append(msg)

                                    menuNum = "0"

                                except Exception as e:
                                    print('매도주문 오류.', e)
                                    msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 [매도주문 오류] - {str(e)}"
                                    result_msgs.append(msg)
                                    menuNum = "0"
                            
                            else:
                                print(nick+" : "+j_code+" 주문가능수량 부족")
                                msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 주문가능수량 부족"
                                result_msgs.append(msg)
                                menuNum = "0"
                
                if code_chk == "":
                    print(nick+" : 보유종목 미존재")
                    msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 보유종목 미존재"
                    result_msgs.append(msg)
                    menuNum = "0"
            
            menuNum = "0"
            final_message = "\n".join(result_msgs) if result_msgs else "매도대상 종목이 존재하지 않거나 주문 조건을 충족하지 못했습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=update.callback_query.message.chat_id,
                message_id=update.callback_query.message.message_id
            )

        else:
            context.bot.edit_message_text(text="처음 메뉴부터 매도 진행하세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id)   
    
    elif data_selected.find("js도진") != -1:

        if menuNum != "0":
            result_msgs = []
            nickname_list = ['phills13', 'phills15']
            for nick in nickname_list:
                ac = account(nick)
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 계좌종목 조회
                sb = stock_balance(access_token, app_key, app_secret, acct_no, "")

                code_chk = ""
                if isinstance(sb, pd.DataFrame) and not sb.empty:
                    for j, name in enumerate(sb.index):
                        j_code = ""
                        if 'pdno' in sb.columns and pd.notna(sb.loc[name, 'pdno']):
                            j_code = sb.loc[name, 'pdno']
                        
                        # 잔고정보의 매도대상 종목이 존재할 경우
                        if j_code == g_sell_code:
                            code_chk = "hold"
                            j_ord_psbl_qty = int(sb['ord_psbl_qty'][j])
                            # 주문가능수량이 매도수량보다 더 많은 경우
                            if j_ord_psbl_qty >= int(g_sell_amount):

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
                                            msg = f"[{nick}:{d_name}] 매도가 : {int(d_order_price):,}원, 매도량 : {int(d_order_amount):,}주 매도주문 완료, 주문번호 : <code>{d_order_no}</code>"
                                            result_msgs.append(msg)
                                        
                                    else:
                                        print("매도주문 실패")
                                        msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 매도주문 실패"
                                        result_msgs.append(msg)

                                    menuNum = "0"

                                except Exception as e:
                                    print('매도주문 오류.', e)
                                    msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 [매도주문 오류] - {str(e)}"
                                    result_msgs.append(msg)
                                    menuNum = "0"
                            
                            else:
                                print(nick+" : "+j_code+" 주문가능수량 부족")
                                msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 주문가능수량 부족"
                                result_msgs.append(msg)
                                menuNum = "0"
                    
                if code_chk == "":
                    print(nick+" : 보유종목 미존재")
                    msg = f"[{nick}:{g_sell_code}] 매도가 : {int(g_sell_price):,}원, 매도량 : {int(g_sell_amount):,}주 보유종목 미존재"
                    result_msgs.append(msg)
                    menuNum = "0"

            menuNum = "0"
            final_message = "\n".join(result_msgs) if result_msgs else "매도대상 종목이 존재하지 않거나 주문 조건을 충족하지 못했습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=update.callback_query.message.chat_id,
                message_id=update.callback_query.message.message_id
            )

        else:
            context.bot.edit_message_text(text="처음 메뉴부터 매도 진행하세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id)   
    
    elif data_selected.find("전체매도") != -1:
       
        parts = data_selected.split("_")
        if len(parts) < 3:
            update.callback_query.message.reply_text("잘못된 매도 명령어 형식입니다.")
            return
        sell_code = parts[1]  # 종목코드
        sell_amount = format(int(parts[2]), ',d')  # 매도 수량

        menuNum = "34"

        context.bot.edit_message_text(text='[<code>'+ sell_code +'</code>] 전체('+ sell_amount+ ') 매도의 종목코드(종목명), 매도가, 매도량을 입력하세요.', parse_mode='HTML',
                                    chat_id=update.callback_query.message.chat_id,
                                    message_id=update.callback_query.message.message_id)    

    elif data_selected.find("절반매도") != -1:

        parts = data_selected.split("_")
        if len(parts) < 3:
            update.callback_query.message.reply_text("잘못된 매도 명령어 형식입니다.")
            return
        sell_code = parts[1]  # 종목코드
        sell_amount = format(int(round(int(parts[2])/2)), ',d')  # 매도 수량

        menuNum = "34"

        context.bot.edit_message_text(text='[<code>'+ sell_code + '</code>] 절반('+ sell_amount+ ') 매도의 종목코드(종목명), 매도가, 매도량을 입력하세요.', parse_mode='HTML',
                                    chat_id=update.callback_query.message.chat_id,
                                    message_id=update.callback_query.message.message_id)                            

    elif data_selected.find("신호매수") != -1:        
       
        parts = data_selected.split("_")
        if len(parts) < 3:
            update.callback_query.message.reply_text("잘못된 매수 명령어 형식입니다.")
            return
        buy_code = parts[1]  # 종목코드
        current_price = format(int(parts[2]), ',d')  # 현재가

        menuNum = "22"

        context.bot.edit_message_text(text='[<code>'+ buy_code + '</code>] 현재가('+ current_price +')원 매수의 종목코드(종목명), 매수가, 매수금액을 입력하세요.', parse_mode='HTML',
                                    chat_id=update.callback_query.message.chat_id,
                                    message_id=update.callback_query.message.message_id)

    elif data_selected.find("시수") != -1:
        menuNum = "23"

        context.bot.edit_message_text(text="현재가 기준 매수의 종목코드(종목명), 매수금액을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
    
    elif data_selected.find("시도") != -1:
        menuNum = "33"

        context.bot.edit_message_text(text="현재가 기준 매도의 종목코드(종목명), 매도비율(%)을 입력하세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)

    elif data_selected.find("매수") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["손절금액", "매수금액", "시총수가수량", "취소"], data_selected)
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

            elif data_selected.find("시총수가수량") != -1:
                menuNum = "24"

                context.bot.edit_message_text(text="종목코드(종목명), 매수가, 매수량을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)    

    elif data_selected.find("매도") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["전체", "절반", "1/4", "1/3", "2/3", "3/4", "도가도량", "손수익율", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 3))

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

            elif data_selected.find("1/4") != -1:
                menuNum = "35"

                context.bot.edit_message_text(text="1/4 매도의 종목코드(종목명), 매도가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("1/3") != -1:
                menuNum = "36"

                context.bot.edit_message_text(text="1/3 매도의 종목코드(종목명), 매도가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("2/3") != -1:
                menuNum = "37"

                context.bot.edit_message_text(text="2/3 매도의 종목코드(종목명), 매도가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("3/4") != -1:
                menuNum = "38"

                context.bot.edit_message_text(text="3/4 매도의 종목코드(종목명), 매도가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)                                                                                                      
            
            elif data_selected.find("손수익율") != -1:
                menuNum = "39"

                context.bot.edit_message_text(text="종목코드(종목명), 매입가(보유단가:0), 매도비율(%), 손실수익율(%)을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("도가도량") != -1:
                menuNum = "34"

                context.bot.edit_message_text(text="종목코드(종목명), 매도가, 매도량을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

    elif data_selected.find("자동") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["자수", "자도", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

            context.bot.edit_message_text(text="매매기준 종류를 선택해 주세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id,
                                          reply_markup=show_markup)

        elif len(data_selected.split(",")) == 2:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("자수") != -1:
                menuNum = "41"

                context.bot.edit_message_text(text="매수기준의 종목코드(종목명), 매수금액을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("자도") != -1:
                menuNum = "42"

                context.bot.edit_message_text(text="매도기준의 종목코드(종목명), 매도비율(%)을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)    
   
    elif data_selected.find("역매") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["역매382", "역매50", "역매618", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

            context.bot.edit_message_text(text="역피보나치 매도 범위를 선택해 주세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id,
                                          reply_markup=show_markup)

        elif len(data_selected.split(",")) == 2:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("역매382") != -1:
                menuNum = "51"

                context.bot.edit_message_text(text="역피보나치 38.2% 매도의 종목코드(종목명), 저가, 고가, 매도비율(%)을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
               
            elif data_selected.find("역매50") != -1:
                menuNum = "52"

                context.bot.edit_message_text(text="역피보나치 50% 매도의 종목코드(종목명), 저가, 고가, 매도비율(%)을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("역매618") != -1:
                menuNum = "53"

                context.bot.edit_message_text(text="역피보나치 61.8% 매도의 종목코드(종목명), 저가, 고가, 매도비율(%)을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)        
   
    elif data_selected.find("관심") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["관종조회", "관종등록", "관종삭제", "관종수정", "피보나치", "역피조회", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 3))

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

            elif data_selected.find("관종조회") != -1:

                if len(data_selected.split(",")) == 2:
                    button_list = build_button(["전체조회", "개별조회", "취소"], data_selected)
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

                    context.bot.edit_message_text(text="조회할 메뉴를 선택해 주세요.",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id,
                                                reply_markup=show_markup)

            elif data_selected.find("관종등록") != -1:
                menuNum = "11"

                context.bot.edit_message_text(text="관심종목 등록할 종목코드(종목명)를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("관종삭제") != -1:
                menuNum = "12"

                context.bot.edit_message_text(text="관심종목 삭제할 종목코드(종목명)를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("관종수정") != -1:

                if len(data_selected.split(",")) == 2:
                    button_list = build_button(["돌파가수정", "이탈가수정", "저항가수정", "지지가수정", "추세상단가수정", "추세하단가수정", "취소"], data_selected)
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 3))

                    context.bot.edit_message_text(text="수정할 메뉴를 선택해 주세요.",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id,
                                                reply_markup=show_markup)
                   
            elif data_selected.find("피보나치") != -1:
                menuNum = "171"

                context.bot.edit_message_text(text="피보나치 구간 가격정보 받을 종목코드(종목명), 저가, 고가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)        
               
            elif data_selected.find("역피조회") != -1:
                menuNum = "172"

                context.bot.edit_message_text(text="역피보나치 구간 가격정보 받을 종목코드(종목명), 저가, 고가를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)    

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
                    a = ""
                    a = inquire_price(access_token, app_key, app_secret, i[0])

                    company = i[1] + "[<code>" + i[0] + "</code>]"

                    context.bot.send_message(chat_id=update.effective_chat.id, text=company + " : 현재가-" + format(int(a['stck_prpr']), ',d') + "원, 고가-" + format(int(a['stck_hgpr']), ',d') + "원, 저가-" + format(int(a['stck_lwpr']), ',d') + "원, 거래량-" + format(int(a['acml_vol']), ',d') + "주, 거래대비-" + a['prdy_vrss_vol_rate'] + "%, 돌파가-" + format(int(i[2]), ',d') + "원, 이탈가-" + format(int(i[3]), ',d') + "원, 저항가-" + format(int(i[4]), ',d') + "원, 지지가-" + format(int(i[5]), ',d') + "원, 추세상단가-" + format(int(i[6]), ',d') + "원, 추세하단가-" + format(int(i[7]), ',d') + "원, 매수예정금액-" + format(int(i[8]), ',d') + "원", parse_mode="HTML")

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

    elif data_selected.find("체결") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["전체주문", "개별주문", "주문정정", "주문철회", "취소"], data_selected)
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

            elif data_selected.find("전체주문") != -1:

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

                    if len(output1) > 0:
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]
                        result_msgs = []

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

                            msg = f"[{d_name} - {d_order_tmd[:2]}:{d_order_tmd[2:4]}:{d_order_tmd[4:]}] 주문번호 : <code>{str(d_order_no)}</code>, {d_order_type}가 : {format(int(d_order_price), ',d')}원, {d_order_type}량 : {format(int(d_order_amount), ',d')}주, 체결량 : {format(int(d_total_complete_qty), ',d')}주, 잔량 : {format(int(d_remain_qty), ',d')}주, 체결금 : {format(int(d_total_complete_amt), ',d')}원"
                            result_msgs.append(msg)

                        final_message = "\n".join(result_msgs) if result_msgs else "일별주문체결 조회 대상이 존재하지 않습니다."

                        context.bot.edit_message_text(
                            text=final_message,
                            parse_mode='HTML',
                            chat_id=update.callback_query.message.chat_id,
                            message_id=update.callback_query.message.message_id
                        )

                    else:
                        context.bot.send_message(text="일별주문체결 조회 미존재 : " + g_company,
                                                 chat_id=update.callback_query.message.chat_id,
                                                 message_id=update.callback_query.message.message_id)

                except Exception as e:
                    print('일별주문체결 조회 오류.', e)
                    context.bot.edit_message_text(text="[일별주문체결 조회] 오류 : "+str(e),
                                                  chat_id=update.callback_query.message.chat_id,
                                                  message_id=update.callback_query.message.message_id)

            elif data_selected.find("개별주문") != -1:
                menuNum = "141"

                context.bot.edit_message_text(text="주문조회할 종목코드(종목명), 매매구분(전체:0 매수:1 매도:2)을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
            
            elif data_selected.find("주문정정") != -1:
                menuNum = "142"

                context.bot.edit_message_text(text="주문정정할 종목코드(종목명), 주문번호, 정정가(시장가:0)를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

            elif data_selected.find("주문철회") != -1:
                menuNum = "143"

                context.bot.edit_message_text(text="주문취소할 종목코드(종목명), 주문번호를 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)

    if data_selected.find("보유") != -1:
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
                    button_list = build_button(["1차목표가", "1차이탈가", "최종목표가", "최종이탈가", "매매계획", "손실금액", "취소"], data_selected)
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

                # 보유종목정보 조회
                cur100 = conn.cursor()
                cur100.execute("select purchase_price, purchase_amount, sign_resist_price, sign_support_price, end_target_price, end_loss_price, code, name, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, COALESCE(sell_plan_sum, 0) as sell_plan_sum, COALESCE(sell_plan_amount, 0) as sell_plan_amount, avail_amount, trading_plan, COALESCE(limit_price, 0) as limit_price, COALESCE(limit_amt, 0) as limit_amt from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and proc_yn = 'Y'")
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
                    # 매메계획
                    trading_plan = i[16]
                    print("매매계획 : " + trading_plan)
                    # 손절가
                    limit_price = i[17]
                    print("손절가 : " + format(limit_price, ',d'))
                    # 손절금액
                    limit_amt = i[18]
                    print("손절금액 : " + format(limit_amt, ',d'))
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
                    company = i[7] + "[<code>" + i[6] + "</code>]"
           
                    context.bot.send_message(chat_id=update.effective_chat.id, text=(f"{company} : 매입가-{format(int(purchase_price), ',d')}원, 매입수량-{format(purchase_amount, ',d')}주, 매입금액-{format(purchase_sum, ',d')}원, 현재가-{format(current_price, ',d')}원, 평가금액-{format(eval_sum, ',d')}원, 수익률({str(earning_rate)})%, 손수익금액({format(valuation_sum, ',d')})원, 저항가-{format(sign_resist_price, ',d')}원, 지지가-{format(sign_support_price, ',d')}원, 최종목표가-{format(end_target_price, ',d')}원, 최종이탈가-{format(end_loss_price, ',d')}원, 손절가-{format(limit_price, ',d')}원, 손절금액({format(limit_amt, ',d')})원, 매매계획-{trading_plan} => {sell_command}"), parse_mode="HTML")
           
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
                
            elif data_selected.find("매매계획") != -1:
                menuNum = "165"

                context.bot.edit_message_text(text="보유종목 수정할 종목코드(종목명), 매매계획을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)                                    

            elif data_selected.find("손실금액") != -1:
                menuNum = "166"

                context.bot.edit_message_text(text="보유종목 수정할 종목코드(종목명), 손실금액을 입력하세요.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)                                        

    if data_selected.find("자산") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["자산조회", "자산정리", "초기화", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

            context.bot.edit_message_text(text="자산관리 메뉴를 선택해 주세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id,
                                          reply_markup=show_markup)

        elif len(data_selected.split(",")) == 2:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("자산조회") != -1:

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

                # 자산관리정보 조회
                cur300 = conn.cursor()
                cur300.execute("select market_ratio, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt from (select row_number() over (order by id desc) as ROWNUM, COALESCE(market_ratio, 0) as market_ratio, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt from \"stockFundMng_stock_fund_mng\" where acct_no = '" + str(acct_no) + "') A where A.ROWNUM = 1")
                result_three00 = cur300.fetchall()
                cur300.close()
            
                for i in result_three00:
                    context.bot.send_message(chat_id=update.effective_chat.id, text="총평가금액 : " + format(int(i[2]), ',d') + "원, 잔고금액 : "+ format(int(i[7]), ',d') +"원, 총예수금 : "+format(int(i[4]), ',d') + "원, 가정산금 : " + format(int(i[5]), ',d') + "원, 전일비증감 : " + format(int(i[8]), ',d') + "원")

            elif data_selected.find("자산정리") != -1:

                if len(data_selected.split(",")) == 2:
                    button_list = build_button(["100", "66", "50", "33", "25", "20", "취소"], data_selected)
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))
                    
                    context.bot.edit_message_text(text="자산정리 비율 메뉴를 선택해 주세요.",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id,
                                                reply_markup=show_markup)
                    
            elif data_selected.find("초기화") != -1:
                context.bot.edit_message_text(text="초기화 진행합니다.",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)
                os.system ('sudo -su root reboot')
                return        
    
        elif len(data_selected.split(",")) == 3:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("100") != -1:
                    
                ac = account()
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 잔고정보 호출
                balance_proc(access_token, app_key, app_secret, acct_no)

                # 보유종목정보 조회
                cur100 = conn.cursor()
                cur100.execute("select trading_plan, purchase_price, purchase_amount, sign_resist_price, sign_support_price, end_target_price, end_loss_price, code, name from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and proc_yn = 'Y'")
                result_one00 = cur100.fetchall()
                cur100.close()
                result_msgs = []
                
                for i in result_one00:
                    trading_plan = i[0]
                    purchase_price = i[1]
                    purchase_amount = i[2]
                    sign_resist_price = i[3]
                    sign_support_price = i[4]
                    end_target_price = i[5]
                    end_loss_price = i[6]
                    code = i[7]
                    company_name = i[8]

                    # 100% 매도 계획 대상
                    if trading_plan == "as":
                        # 매도 가능 수량 조회
                        b = inquire_psbl_sell(access_token, app_key, app_secret, acct_no, code)
                        print("매도 가능 수량 : " + format(int(b['ord_psbl_qty']), ',d'))
                        print("현재가 : " + format(int(b['now_pric']), ',d'))

                        if int(b['ord_psbl_qty']) > 0:

                            sell_qty = b['ord_psbl_qty']
                            sell_price = b['now_pric']
                            sell_rate = 100

                            # base_dtm datetime 변환
                            base_dtm = datetime.now().strftime("%Y%m%d%H%M%S")
                            
                            # 주식당일분봉조회
                            candle_list = fetch_candles_with_base(access_token, app_key, app_secret, code, base_dtm)

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

                final_message = "\n".join(result_msgs) if result_msgs else "매도대상 종목이 존재하지 않거나 주문 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )

            elif data_selected.find("66") != -1:
                    
                ac = account()
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 잔고정보 호출
                balance_proc(access_token, app_key, app_secret, acct_no)

                # 보유종목정보 조회
                cur100 = conn.cursor()
                cur100.execute("select trading_plan, purchase_price, purchase_amount, sign_resist_price, sign_support_price, end_target_price, end_loss_price, code, name from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and proc_yn = 'Y'")
                result_one00 = cur100.fetchall()
                cur100.close()
                result_msgs = []
                
                for i in result_one00:
                    trading_plan = i[0]
                    purchase_price = i[1]
                    purchase_amount = i[2]
                    sign_resist_price = i[3]
                    sign_support_price = i[4]
                    end_target_price = i[5]
                    end_loss_price = i[6]
                    code = i[7]
                    company_name = i[8]

                    # 66% 매도 계획 대상
                    if trading_plan == "66s":
                        # 매도 가능 수량 조회
                        b = inquire_psbl_sell(access_token, app_key, app_secret, acct_no, code)
                        print("매도 가능 수량 : " + format(int(b['ord_psbl_qty']), ',d'))
                        print("현재가 : " + format(int(b['now_pric']), ',d'))

                        if int(b['ord_psbl_qty']) > 0:

                            sell_qty = round(int(b['ord_psbl_qty']) * 0.6667)
                            sell_price = b['now_pric']
                            sell_rate = 66

                            # base_dtm datetime 변환
                            base_dtm = datetime.now().strftime("%Y%m%d%H%M%S")
                            
                            # 주식당일분봉조회
                            candle_list = fetch_candles_with_base(access_token, app_key, app_secret, code, base_dtm)

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

                final_message = "\n".join(result_msgs) if result_msgs else "매도대상 종목이 존재하지 않거나 주문 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )

            elif data_selected.find("50") != -1:

                ac = account()
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 잔고정보 호출
                balance_proc(access_token, app_key, app_secret, acct_no)

                # 보유종목정보 조회
                cur100 = conn.cursor()
                cur100.execute("select trading_plan, purchase_price, purchase_amount, sign_resist_price, sign_support_price, end_target_price, end_loss_price, code, name from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and proc_yn = 'Y'")
                result_one00 = cur100.fetchall()
                cur100.close()
                result_msgs = []
                
                for i in result_one00:
                    trading_plan = i[0]
                    purchase_price = i[1]
                    purchase_amount = i[2]
                    sign_resist_price = i[3]
                    sign_support_price = i[4]
                    end_target_price = i[5]
                    end_loss_price = i[6]
                    code = i[7]
                    company_name = i[8]

                    # 50% 매도 계획 대상
                    if trading_plan == "50s":
                        # 매도 가능 수량 조회
                        b = inquire_psbl_sell(access_token, app_key, app_secret, acct_no, code)
                        print("매도 가능 수량 : " + format(int(b['ord_psbl_qty']), ',d'))
                        print("현재가 : " + format(int(b['now_pric']), ',d'))

                        if int(b['ord_psbl_qty']) > 0:

                            sell_qty = round(int(b['ord_psbl_qty']) / 2)
                            sell_price = b['now_pric']
                            sell_rate = 50

                            # base_dtm datetime 변환
                            base_dtm = datetime.now().strftime("%Y%m%d%H%M%S")
                            
                            # 주식당일분봉조회
                            candle_list = fetch_candles_with_base(access_token, app_key, app_secret, code, base_dtm)

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

                final_message = "\n".join(result_msgs) if result_msgs else "매도대상 종목이 존재하지 않거나 주문 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )
                
            elif data_selected.find("33") != -1:

                ac = account()
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 잔고정보 호출
                balance_proc(access_token, app_key, app_secret, acct_no)

                # 보유종목정보 조회
                cur100 = conn.cursor()
                cur100.execute("select trading_plan, purchase_price, purchase_amount, sign_resist_price, sign_support_price, end_target_price, end_loss_price, code, name from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and proc_yn = 'Y'")
                result_one00 = cur100.fetchall()
                cur100.close()
                result_msgs = []
                
                for i in result_one00:
                    trading_plan = i[0]
                    purchase_price = i[1]
                    purchase_amount = i[2]
                    sign_resist_price = i[3]
                    sign_support_price = i[4]
                    end_target_price = i[5]
                    end_loss_price = i[6]
                    code = i[7]
                    company_name = i[8]

                    # 33% 매도 계획 대상
                    if trading_plan == "33s":
                        # 매도 가능 수량 조회
                        b = inquire_psbl_sell(access_token, app_key, app_secret, acct_no, code)
                        print("매도 가능 수량 : " + format(int(b['ord_psbl_qty']), ',d'))
                        print("현재가 : " + format(int(b['now_pric']), ',d'))

                        if int(b['ord_psbl_qty']) > 0:

                            sell_qty = round(int(b['ord_psbl_qty']) * 0.3333)
                            sell_price = b['now_pric']
                            sell_rate = 33

                            # base_dtm datetime 변환
                            base_dtm = datetime.now().strftime("%Y%m%d%H%M%S")
                            
                            # 주식당일분봉조회
                            candle_list = fetch_candles_with_base(access_token, app_key, app_secret, code, base_dtm)

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

                final_message = "\n".join(result_msgs) if result_msgs else "매도대상 종목이 존재하지 않거나 주문 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )

            elif data_selected.find("25") != -1:
                    
                ac = account()
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 잔고정보 호출
                balance_proc(access_token, app_key, app_secret, acct_no)

                # 보유종목정보 조회
                cur100 = conn.cursor()
                cur100.execute("select trading_plan, purchase_price, purchase_amount, sign_resist_price, sign_support_price, end_target_price, end_loss_price, code, name from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and proc_yn = 'Y'")
                result_one00 = cur100.fetchall()
                cur100.close()
                result_msgs = []
                
                for i in result_one00:
                    trading_plan = i[0]
                    purchase_price = i[1]
                    purchase_amount = i[2]
                    sign_resist_price = i[3]
                    sign_support_price = i[4]
                    end_target_price = i[5]
                    end_loss_price = i[6]
                    code = i[7]
                    company_name = i[8]

                    # 25% 매도 계획 대상
                    if trading_plan == "25s":
                        # 매도 가능 수량 조회
                        b = inquire_psbl_sell(access_token, app_key, app_secret, acct_no, code)
                        print("매도 가능 수량 : " + format(int(b['ord_psbl_qty']), ',d'))
                        print("현재가 : " + format(int(b['now_pric']), ',d'))

                        if int(b['ord_psbl_qty']) > 0:

                            sell_qty = round(int(b['ord_psbl_qty']) * 0.25)
                            sell_price = b['now_pric']
                            sell_rate = 25

                            # base_dtm datetime 변환
                            base_dtm = datetime.now().strftime("%Y%m%d%H%M%S")
                            
                            # 주식당일분봉조회
                            candle_list = fetch_candles_with_base(access_token, app_key, app_secret, code, base_dtm)

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

                final_message = "\n".join(result_msgs) if result_msgs else "매도대상 종목이 존재하지 않거나 주문 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )
                
            elif data_selected.find("20") != -1:

                ac = account()
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 잔고정보 호출
                balance_proc(access_token, app_key, app_secret, acct_no)

                # 보유종목정보 조회
                cur100 = conn.cursor()
                cur100.execute("select trading_plan, purchase_price, purchase_amount, sign_resist_price, sign_support_price, end_target_price, end_loss_price, code, name from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and proc_yn = 'Y'")
                result_one00 = cur100.fetchall()
                cur100.close()
                result_msgs = []
                
                for i in result_one00:
                    trading_plan = i[0]
                    purchase_price = i[1]
                    purchase_amount = i[2]
                    sign_resist_price = i[3]
                    sign_support_price = i[4]
                    end_target_price = i[5]
                    end_loss_price = i[6]
                    code = i[7]
                    company_name = i[8]

                    # 20% 매도 계획 대상
                    if trading_plan == "20s":
                        # 매도 가능 수량 조회
                        b = inquire_psbl_sell(access_token, app_key, app_secret, acct_no, code)
                        print("매도 가능 수량 : " + format(int(b['ord_psbl_qty']), ',d'))
                        print("현재가 : " + format(int(b['now_pric']), ',d'))

                        if int(b['ord_psbl_qty']) > 0:

                            sell_qty = round(int(b['ord_psbl_qty']) * 0.2)
                            sell_price = b['now_pric']
                            sell_rate = 20

                            # base_dtm datetime 변환
                            base_dtm = datetime.now().strftime("%Y%m%d%H%M%S")
                            
                            # 주식당일분봉조회
                            candle_list = fetch_candles_with_base(access_token, app_key, app_secret, code, base_dtm)

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

                final_message = "\n".join(result_msgs) if result_msgs else "매도대상 종목이 존재하지 않거나 주문 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )

    if data_selected.find("레벨") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["시장레벨조회", "시장레벨변경", "취소"], data_selected)
            show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))

            context.bot.edit_message_text(text="시장레벨관리 메뉴를 선택해 주세요.",
                                          chat_id=update.callback_query.message.chat_id,
                                          message_id=update.callback_query.message.message_id,
                                          reply_markup=show_markup)

        elif len(data_selected.split(",")) == 2:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("시장레벨조회") != -1:

                context.bot.edit_message_text(text="[시장레벨]",
                                      chat_id=update.callback_query.message.chat_id,
                                      message_id=update.callback_query.message.message_id)
                ac = account()
                acct_no = ac['acct_no']
                access_token = ac['access_token']
                app_key = ac['app_key']
                app_secret = ac['app_secret']

                # 시장레벨정보 호출
                # marketLevel_proc(access_token, app_key, app_secret, acct_no)

                # 시장레벨정보 조회
                cur400 = conn.cursor()
                cur400.execute("select asset_risk_num, total_asset, risk_rate, risk_sum, item_number from \"stockMarketMng_stock_market_mng\" where acct_no = '" + str(acct_no) + "' and aply_end_dt = '99991231'")
                result_four00 = cur400.fetchall()
                cur400.close()
            
                for i in result_four00:
                    context.bot.send_message(chat_id=update.effective_chat.id, text="자산리스크번호 : " + str(i[0]) + ", 총자산 : " + format(int(i[1]), ',d') + "원, 리스크 : " + str(i[2]) + "%, 리스크금액 : "+format(int(i[3]), ',d') + "원, 종목수 : " + format(int(i[4]), ',d') + "개], 종목리스크 : " + format(int(i[3]/i[4]), ',d') + "원")

            elif data_selected.find("시장레벨변경") != -1:

                if len(data_selected.split(",")) == 2:
                    button_list = build_button(["하락지속", "단기상승", "패턴", "눌림", "상승지속", "취소"], data_selected)
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 1))
                    
                    context.bot.edit_message_text(text="시장레벨변경 메뉴를 선택해 주세요.",
                                                chat_id=update.callback_query.message.chat_id,
                                                message_id=update.callback_query.message.message_id,
                                                reply_markup=show_markup)

        elif len(data_selected.split(",")) == 3:

            if data_selected.find("취소") != -1:
                context.bot.edit_message_text(text="취소하였습니다.",
                                              chat_id=update.callback_query.message.chat_id,
                                              message_id=update.callback_query.message.message_id)
                return

            elif data_selected.find("하락지속") != -1:

                result_msgs = []
                nickname_list = ['phills75', 'yh480825', 'phills13', 'phills15', 'phills2','chichipa']
                for nick in nickname_list:
                    ac = account(nick)
                    acct_no = ac['acct_no']
                    market_level_num = "1"
                    today = datetime.now().strftime("%Y%m%d")
                    risk_rate = 1
                    item_number = 1
                    asset_sum = 10000000
                    risk_sum = asset_sum * risk_rate * 0.01

                    # 시장레벨정보 조회
                    cur300 = conn.cursor()
                    cur300.execute("select asset_risk_num, market_level_num from \"stockMarketMng_stock_market_mng\" where acct_no = '" + str(acct_no) + "' and asset_risk_num = '"+market_level_num + today+"'")
                    result_one01 = cur300.fetchall()
                    cur300.close()

                    if len(result_one01) < 1:

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
                        [int(market_level_num + today), acct_no, market_level_num, asset_sum, risk_rate, risk_sum, item_number, today, '99991231'])
                        # DB 연결된 커서의 쿼리 수행
                        cur201.execute(insert_query201, record_to_insert201)

                        conn.commit()
                        cur200.close()
                        cur201.close()

                        msg = f"[{nick}:하락지속 후, 기술적반등] 자산리스크번호 : {market_level_num + today}, 총자산 : {int(asset_sum):,}원, 리스크 : {str(risk_rate)}%, 리스크금액 : {int(risk_sum):,}원, 종목수 : {str(item_number)}개, 종목리스크 : {int(risk_sum/item_number):,}원"
                        result_msgs.append(msg)

                    else:

                        msg = f"[{nick}:하락지속 후, 기술적반등] 자산리스크번호 기존재"
                        result_msgs.append(msg)
                    
                final_message = "\n".join(result_msgs) if result_msgs else "시장레벨변경 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )    

            elif data_selected.find("단기상승") != -1:

                result_msgs = []
                nickname_list = ['phills75', 'yh480825', 'phills13', 'phills15', 'phills2','chichipa']
                for nick in nickname_list:
                    ac = account(nick)
                    acct_no = ac['acct_no']
                    market_level_num = "2"
                    today = datetime.now().strftime("%Y%m%d")
                    risk_rate = 2
                    item_number = 2
                    asset_sum = 10000000
                    risk_sum = asset_sum * risk_rate * 0.01

                    # 시장레벨정보 조회
                    cur300 = conn.cursor()
                    cur300.execute("select asset_risk_num, market_level_num from \"stockMarketMng_stock_market_mng\" where acct_no = '" + str(acct_no) + "' and asset_risk_num = '"+market_level_num + today+"'")
                    result_one01 = cur300.fetchall()
                    cur300.close()

                    if len(result_one01) < 1:

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
                        [int(market_level_num + today), acct_no, market_level_num, asset_sum, risk_rate, risk_sum, item_number, today, '99991231'])
                        # DB 연결된 커서의 쿼리 수행
                        cur201.execute(insert_query201, record_to_insert201)

                        conn.commit()
                        cur200.close()
                        cur201.close()

                        msg = f"[{nick}:단기상승 후, 기술적반등] 자산리스크번호 : {market_level_num + today}, 총자산 : {int(asset_sum):,}원, 리스크 : {str(risk_rate)}%, 리스크금액 : {int(risk_sum):,}원, 종목수 : {str(item_number)}개, 종목리스크 : {int(risk_sum/item_number):,}원"
                        result_msgs.append(msg)

                    else:

                        msg = f"[{nick}:단기상승 후, 기술적반등] 자산리스크번호 기존재"
                        result_msgs.append(msg)
                    
                final_message = "\n".join(result_msgs) if result_msgs else "시장레벨변경 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )    

            elif data_selected.find("패턴") != -1:

                result_msgs = []
                nickname_list = ['phills75', 'yh480825', 'phills13', 'phills15', 'phills2','chichipa']
                for nick in nickname_list:
                    ac = account(nick)
                    acct_no = ac['acct_no']
                    market_level_num = "3"
                    today = datetime.now().strftime("%Y%m%d")
                    risk_rate = 3
                    item_number = 3
                    asset_sum = 10000000
                    risk_sum = asset_sum * risk_rate * 0.01

                    # 시장레벨정보 조회
                    cur300 = conn.cursor()
                    cur300.execute("select asset_risk_num, market_level_num from \"stockMarketMng_stock_market_mng\" where acct_no = '" + str(acct_no) + "' and asset_risk_num = '"+market_level_num + today+"'")
                    result_one01 = cur300.fetchall()
                    cur300.close()

                    if len(result_one01) < 1:

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
                        [int(market_level_num + today), acct_no, market_level_num, asset_sum, risk_rate, risk_sum, item_number, today, '99991231'])
                        # DB 연결된 커서의 쿼리 수행
                        cur201.execute(insert_query201, record_to_insert201)

                        conn.commit()
                        cur200.close()
                        cur201.close()

                        msg = f"[{nick}:패턴내 기술적반등] 자산리스크번호 : {market_level_num + today}, 총자산 : {int(asset_sum):,}원, 리스크 : {str(risk_rate)}%, 리스크금액 : {int(risk_sum):,}원, 종목수 : {str(item_number)}개, 종목리스크 : {int(risk_sum/item_number):,}원"
                        result_msgs.append(msg)

                    else:

                        msg = f"[{nick}:패턴내 기술적반등] 자산리스크번호 기존재"
                        result_msgs.append(msg)
                    
                final_message = "\n".join(result_msgs) if result_msgs else "시장레벨변경 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )        

            elif data_selected.find("눌림") != -1:

                result_msgs = []
                nickname_list = ['phills75', 'yh480825', 'phills13', 'phills15', 'phills2','chichipa']
                for nick in nickname_list:
                    ac = account(nick)
                    acct_no = ac['acct_no']
                    market_level_num = "4"
                    today = datetime.now().strftime("%Y%m%d")
                    risk_rate = 4
                    item_number = 2
                    asset_sum = 10000000
                    risk_sum = asset_sum * risk_rate * 0.01

                    # 시장레벨정보 조회
                    cur300 = conn.cursor()
                    cur300.execute("select asset_risk_num, market_level_num from \"stockMarketMng_stock_market_mng\" where acct_no = '" + str(acct_no) + "' and asset_risk_num = '"+market_level_num + today+"'")
                    result_one01 = cur300.fetchall()
                    cur300.close()

                    if len(result_one01) < 1:

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
                        [int(market_level_num + today), acct_no, market_level_num, asset_sum, risk_rate, risk_sum, item_number, today, '99991231'])
                        # DB 연결된 커서의 쿼리 수행
                        cur201.execute(insert_query201, record_to_insert201)

                        conn.commit()
                        cur200.close()
                        cur201.close()

                        msg = f"[{nick}:상승전환 후, 눌림구간에서 반등] 자산리스크번호 : {market_level_num + today}, 총자산 : {int(asset_sum):,}원, 리스크 : {str(risk_rate)}%, 리스크금액 : {int(risk_sum):,}원, 종목수 : {str(item_number)}개, 종목리스크 : {int(risk_sum/item_number):,}원"
                        result_msgs.append(msg)

                    else:

                        msg = f"[{nick}:상승전환 후, 눌림구간에서 반등] 자산리스크번호 기존재"
                        result_msgs.append(msg)
                    
                final_message = "\n".join(result_msgs) if result_msgs else "시장레벨변경 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )        

            elif data_selected.find("상승지속") != -1:

                result_msgs = []
                nickname_list = ['phills75', 'yh480825', 'phills13', 'phills15', 'phills2','chichipa']
                for nick in nickname_list:
                    ac = account(nick)
                    acct_no = ac['acct_no']
                    market_level_num = "5"
                    today = datetime.now().strftime("%Y%m%d")
                    risk_rate = 4
                    item_number = 4
                    asset_sum = 10000000
                    risk_sum = asset_sum * risk_rate * 0.01

                    # 시장레벨정보 조회
                    cur300 = conn.cursor()
                    cur300.execute("select asset_risk_num, market_level_num from \"stockMarketMng_stock_market_mng\" where acct_no = '" + str(acct_no) + "' and asset_risk_num = '"+market_level_num + today+"'")
                    result_one01 = cur300.fetchall()
                    cur300.close()

                    if len(result_one01) < 1:

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
                        [int(market_level_num + today), acct_no, market_level_num, asset_sum, risk_rate, risk_sum, item_number, today, '99991231'])
                        # DB 연결된 커서의 쿼리 수행
                        cur201.execute(insert_query201, record_to_insert201)

                        conn.commit()
                        cur200.close()
                        cur201.close()

                        msg = f"[{nick}:상승지속 후, 패턴내에서 기술적반등] 자산리스크번호 : {market_level_num + today}, 총자산 : {int(asset_sum):,}원, 리스크 : {str(risk_rate)}%, 리스크금액 : {int(risk_sum):,}원, 종목수 : {str(item_number)}개, 종목리스크 : {int(risk_sum/item_number):,}원"
                        result_msgs.append(msg)

                    else:

                        msg = f"[{nick}:상승지속 후, 패턴내에서 기술적반등] 자산리스크번호 기존재"
                        result_msgs.append(msg)
                    
                final_message = "\n".join(result_msgs) if result_msgs else "시장레벨변경 조건을 충족하지 못했습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )        

    if data_selected.find("검색") != -1:
        if len(data_selected.split(",")) == 1:
            button_list = build_button(["거래폭발", "단기추세", "투자혁명", "파워급등", "파워종목", "취소"], data_selected)
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

            elif data_selected.find("파워급등") != -1:

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

get_handler = CommandHandler('fund', get_command)
updater.dispatcher.add_handler(get_handler)

get_handler_info = CommandHandler('info', get_command_info)
updater.dispatcher.add_handler(get_handler_info)

updater.dispatcher.add_handler(CallbackQueryHandler(callback_get))

def is_positive_int(val: str) -> bool:
    """양수 정수만 허용 (1~100 범위)"""
    if val.isdigit():
        num = int(val)
        return 0 < num <= 100
    return False    

def is_signed_float_2dec(val: str) -> bool:
    """양수/음수 실수 허용, 소숫점 2자리까지"""
    pattern = r"^-?\d+(\.\d{1,2})?$"
    return re.match(pattern, val) is not None

def get_tick_size(price):
    """가격에 따른 국내 주식 호가단위 계산 (2025년 기준)"""
    if price < 2000:
        return 1
    elif price < 5000:
        return 5
    elif price < 20000:
        return 10
    elif price < 50000:
        return 50
    elif price < 200000:
        return 100
    elif price < 500000:
        return 500
    else:
        return 1000

def round_to_valid_price(price, tick, direction='nearest'):
    """입력 가격을 호가단위에 맞게 조정"""
    if direction == 'up':
        return math.ceil(price / tick) * tick
    elif direction == 'down':
        return math.floor(price / tick) * tick
    else:
        return round(price / tick) * tick

# 날짜형식 변환(년월)
def get_date_str(s):
    r = re.search(r"\d{4}/\d{2}", s)
    return r.group().replace('/', '-') if r else ''

# 네이버 재무정보 조회
def get_dividiend(code):

    url = f'http://companyinfo.stock.naver.com/company/cF1001.aspx?cmp_cd={code}&finGubun=IFRSS'
    df = requests.get(url)
    financial_stmt = pd.read_html(df.text)
    dfs = financial_stmt[0]

    # 불필요한 컬럼 제거
    for i in range(9, 21):
        col_name = f'Unnamed: {i}_level_0'
        if col_name in dfs.columns.get_level_values(0):
            dfs.drop(col_name, axis=1, inplace=True, level=0)

    dfs.columns = dfs.columns.droplevel(0)
    dfs.columns = [get_date_str(x) for x in dfs.columns]
    dft = dfs.T
    dft.index = pd.to_datetime(dft.index, errors='coerce')
    dft = dft[pd.notnull(dft.index)]
    dft = dft.fillna(0)
    return dft

def get_dividend_yield(code):
    url = f'https://finance.naver.com/item/main.naver?code={code}'
    headers = {'User-Agent': 'Mozilla/5.0'}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')

    table = soup.select_one('table.per_table')
    if not table:
        return None

    rows = table.select('tr')
    for row in rows:
        th = row.select_one('th')
        td = row.select_one('td')
        if th and '배당수익률' in th.text:
            return td.text.strip()

    return None

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
    global g_code
    global g_company
    global g_market_buy_company
    global g_market_sell_company
    global g_market_buy_code
    global g_market_sell_code
    global g_market_buy_amount
    global g_market_sell_amount
    global chartReq
    global g_order_no
    global g_revise_price
    global g_dvsn_cd
    global g_remain_qty

    code = ""
    company = ""
    # 주식 현재가
    stck_prpr = ''
    # 전일 대비율
    prdy_ctrt = ''
    # 누적 거래량
    acml_vol = ''
    # 전일 대비 거래량 비율
    prdy_vrss_vol_rate = ''
    # HTS 시가총액
    hts_avls = ''
    # PBR
    pbr = ''
    # BPS
    bps = ''

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
        a = ""
        # 입력 종목코드 현재가 시세
        a = inquire_price(access_token, app_key, app_secret, code)
        stck_prpr = a['stck_prpr']                      # 현재가
        stck_hgpr = a['stck_hgpr']                      # 고가
        stck_lwpr = a['stck_lwpr']                      # 저가
        upper_limit = float(a["stck_mxpr"])             # 상한가
        lower_limit = float(a["stck_llam"])             # 하한가
        prdy_ctrt = a['prdy_ctrt']                      # 전일 대비율
        acml_vol = a['acml_vol']                        # 누적거래량
        prdy_vrss_vol_rate = a['prdy_vrss_vol_rate']    # 전일 대비 거래량 비율
        hts_avls = a['hts_avls']                        # 시가총액
        pbr = a['pbr']
        bps = a['bps']

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
                    delete_query0 = "update \"interestItem_interest_item\" set through_price = %s where code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], code])
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
                    delete_query0 = "update \"interestItem_interest_item\" set leave_price = %s where code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], code])
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
                    delete_query0 = "update \"interestItem_interest_item\" set resist_price = %s where code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], code])
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
                    delete_query0 = "update \"interestItem_interest_item\" set support_price = %s where code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], code])
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
                    delete_query0 = "update \"interestItem_interest_item\" set trend_high_price = %s where code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], code])
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
                    delete_query0 = "update \"interestItem_interest_item\" set trend_low_price = %s where code = %s"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], code])
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

        elif menuNum == '141':
            initMenuNum()
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 매매구분(전체:0 매수:1 매도:2)

            # 매매구분(전체:0 매수:1 매도:2) 존재시
            if commandBot[1].isdecimal():
                   
                # 매매구분(전체:0 매수:1 매도:2)
                if commandBot[1] == '1':
                    trade_dvsn = '02'
                    trade_dvsn_nm = '매수'
                elif commandBot[1] == '2':
                    trade_dvsn = '01'
                    trade_dvsn_nm = '매도'
                else:
                    trade_dvsn = '00'
                    trade_dvsn_nm = '전체'
                print("매매구분(전체:0 매수:1 매도:2) : "+trade_dvsn_nm)

                try:
                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, code, '')

                    if len(output1) > 0:
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['pdno', 'odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]
                        order_type = ""
                        order_no = 0
                        rmn_qty = 0
                        trade_list_chk = ""

                        if trade_dvsn == '00':
                            for i, name in enumerate(d.index):
                                trade_list_chk = "exists"
                                d_order_no = int(d['odno'][i])
                                d_dvsn_cd = d['sll_buy_dvsn_cd'][i]
                                d_order_type = d['sll_buy_dvsn_cd_name'][i]
                                d_order_dt = d['ord_dt'][i]
                                d_order_tmd = d['ord_tmd'][i]
                                d_name = d['prdt_name'][i]
                                d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                                d_order_amount = d['ord_qty'][i]
                                d_total_complete_qty = d['tot_ccld_qty'][i]
                                d_remain_qty = d['rmn_qty'][i]
                                d_total_complete_amt = d['tot_ccld_amt'][i]

                                context.bot.send_message(chat_id=user_id, text="[" + d_name + " - " + d_order_tmd[:2] + ":" + d_order_tmd[2:4] + ":" + d_order_tmd[4:] + "] 주문번호 : <code>" + str(d_order_no) + "</code>, " + d_order_type + "가 : " + format(int(d_order_price), ',d') + "원, " + d_order_type + "량 : " + format(int(d_order_amount), ',d') + "주, 체결량 : " + format(int(d_total_complete_qty), ',d') + "주, 잔량 : " + format(int(d_remain_qty), ',d') + "주, 체결금 : " + format(int(d_total_complete_amt), ',d')+"원", parse_mode='HTML')
                            
                        else:
                            for i, name in enumerate(d.index):
                                # 매수매도구분코드 일치
                                if trade_dvsn == d['sll_buy_dvsn_cd'][i]: 
                                    trade_list_chk = "exists"
                                    d_order_no = int(d['odno'][i])
                                    d_dvsn_cd = d['sll_buy_dvsn_cd'][i]
                                    d_order_type = d['sll_buy_dvsn_cd_name'][i]
                                    d_order_dt = d['ord_dt'][i]
                                    d_order_tmd = d['ord_tmd'][i]
                                    d_name = d['prdt_name'][i]
                                    d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                                    d_order_amount = d['ord_qty'][i]
                                    d_total_complete_qty = d['tot_ccld_qty'][i]
                                    d_remain_qty = d['rmn_qty'][i]
                                    d_total_complete_amt = d['tot_ccld_amt'][i]

                                    context.bot.send_message(chat_id=user_id, text="[" + d_name + " - " + d_order_tmd[:2] + ":" + d_order_tmd[2:4] + ":" + d_order_tmd[4:] + "] 주문번호 : <code>" + str(d_order_no) + "</code>, " + d_order_type + "가 : " + format(int(d_order_price), ',d') + "원, " + d_order_type + "량 : " + format(int(d_order_amount), ',d') + "주, 체결량 : " + format(int(d_total_complete_qty), ',d') + "주, 잔량 : " + format(int(d_remain_qty), ',d') + "주, 체결금 : " + format(int(d_total_complete_amt), ',d')+"원", parse_mode='HTML')

                        if trade_list_chk == "":
                            context.bot.send_message(chat_id=user_id, text="일별주문체결 " + trade_dvsn_nm + " 대상 미존재 : " + company)                

                    else:
                        context.bot.send_message(chat_id=user_id, text="일별주문체결 조회 미존재 : " + company)

                except Exception as e:
                    print('일별주문체결 조회 오류.',e)
                    context.bot.send_message(chat_id=user_id, text="일별주문체결 조회 오류 [" + company + "] : "+str(e))

            else:
                print("매매구분(전체:0 매수:1 매도:2) 미존재")
                context.bot.send_message(chat_id=user_id, text="매매구분(전체:0 매수:1 매도:2) 미존재 [" + company + "]")    
        
        elif menuNum == '142':
            initMenuNum()
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=3)
                print("commandBot[1] : ", commandBot[1])    # 주문번호
                print("commandBot[2] : ", commandBot[2])    # 정정가

            # 주문번호 존재시
            if commandBot[1].isdecimal():
                   
                # 주문번호
                order_no = commandBot[1]
                print("주문번호 : "+order_no)

                 # 정정가 존재시
                if commandBot[2].isdecimal():

                    revise_price = commandBot[2]
                    print("정정가 : "+revise_price)

                    try:
                        # 일별주문체결 조회
                        output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, code, order_no)
                        
                        if len(output1) > 0:
                            tdf = pd.DataFrame(output1)
                            tdf.set_index('odno')
                            d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                            udt_qty = 0
                            for i, name in enumerate(d.index):
                                d_order_no = int(d['odno'][i])
                                d_dvsn_cd = d['sll_buy_dvsn_cd'][i]
                                d_order_type = d['sll_buy_dvsn_cd_name'][i]
                                d_order_dt = d['ord_dt'][i]
                                d_order_tmd = d['ord_tmd'][i]
                                d_name = d['prdt_name'][i]
                                d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                                d_order_amount = d['ord_qty'][i]
                                d_total_complete_qty = d['tot_ccld_qty'][i]
                                d_remain_qty = d['rmn_qty'][i]
                                d_total_complete_amt = d['tot_ccld_amt'][i]

                                context.bot.send_message(chat_id=user_id, text="[" + d_name + " - " + d_order_tmd[:2] + ":" + d_order_tmd[2:4] + ":" + d_order_tmd[4:] + "] 주문번호 : <code>" + str(d_order_no) + "</code>, " + d_order_type + "가 : " + format(int(d_order_price), ',d') + "원, " + d_order_type + "량 : " + format(int(d_order_amount), ',d') + "주, 체결량 : " + format(int(d_total_complete_qty), ',d') + "주, 잔량 : " + format(int(d_remain_qty), ',d') + "주, 체결금 : " + format(int(d_total_complete_amt), ',d')+"원", parse_mode='HTML')

                                if int(d_remain_qty) > 0:
                                    udt_qty = int(d_remain_qty)

                            g_order_no = order_no
                            g_revise_price = revise_price
                            g_dvsn_cd = d_dvsn_cd
                            g_remain_qty = udt_qty
                            g_code = code
                            g_company = company
                            
                            context.bot.send_message(chat_id=user_id, text="[" + company + "] 정정가 : " + format(int(revise_price), ',d') + "원, 정정수량 : " + format(udt_qty, ',d') + "주 => /revise")
                            get_handler = CommandHandler('revise', get_command4)
                            updater.dispatcher.add_handler(get_handler)

                        else:
                            context.bot.send_message(chat_id=user_id, text="일별주문체결 조회 미존재 : " + company)

                    except Exception as e:
                        print('일별주문체결 조회 오류.',e)
                        context.bot.send_message(chat_id=user_id, text="일별주문체결 조회 오류 [" + company + "] : "+str(e))

                else:
                    print("주문정정 정정가 미존재")
                    context.bot.send_message(chat_id=user_id, text="주문정정 정정가 미존재 [" + company + "]")                    

            else:
                print("주문정정 주문번호 미존재")
                context.bot.send_message(chat_id=user_id, text="주문정정 주문번호 미존재 [" + company + "]")                    

        elif menuNum == '143':
            initMenuNum()
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 주문번호

            # 주문번호 존재시
            if commandBot[1].isdecimal():
                   
                # 주문번호
                order_no = commandBot[1]
                print("주문번호 : "+order_no)
                try:
                    # 일별주문체결 조회
                    output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, code, order_no)
                    
                    if len(output1) > 0:
                        tdf = pd.DataFrame(output1)
                        tdf.set_index('odno')
                        d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                        for i, name in enumerate(d.index):
                            d_order_no = int(d['odno'][i])
                            d_dvsn_cd = d['sll_buy_dvsn_cd'][i]
                            d_order_type = d['sll_buy_dvsn_cd_name'][i]
                            d_order_dt = d['ord_dt'][i]
                            d_order_tmd = d['ord_tmd'][i]
                            d_name = d['prdt_name'][i]
                            d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                            d_order_amount = d['ord_qty'][i]
                            d_total_complete_qty = d['tot_ccld_qty'][i]
                            d_remain_qty = d['rmn_qty'][i]
                            d_total_complete_amt = d['tot_ccld_amt'][i]

                            context.bot.send_message(chat_id=user_id, text="[" + d_name + " - " + d_order_tmd[:2] + ":" + d_order_tmd[2:4] + ":" + d_order_tmd[4:] + "] 주문번호 : <code>" + str(d_order_no) + "</code>, " + d_order_type + "가 : " + format(int(d_order_price), ',d') + "원, " + d_order_type + "량 : " + format(int(d_order_amount), ',d') + "주, 체결량 : " + format(int(d_total_complete_qty), ',d') + "주, 잔량 : " + format(int(d_remain_qty), ',d') + "주, 체결금 : " + format(int(d_total_complete_amt), ',d')+"원", parse_mode='HTML')

                        g_order_no = order_no
                        g_dvsn_cd = d_dvsn_cd
                        g_code = code
                        g_company = company
                        
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 주문가 : " + format(int(d_order_price), ',d') + "원, 취소수량 : " + format(int(d_remain_qty), ',d') + "주 => /cancel")
                        get_handler = CommandHandler('cancel', get_command5)
                        updater.dispatcher.add_handler(get_handler)

                    else:
                        context.bot.send_message(chat_id=user_id, text="일별주문체결 조회 미존재 : " + company)

                except Exception as e:
                    print('일별주문체결 조회 오류.',e)
                    context.bot.send_message(chat_id=user_id, text="일별주문체결 조회 오류 [" + company + "] : "+str(e))

            else:
                print("주문취소 주문번호 미존재")
                context.bot.send_message(chat_id=user_id, text="주문취소 주문번호 미존재 [" + company + "]")      

        elif menuNum == '15':
            initMenuNum()
            # 잔고정보 호출
            balance_proc(access_token, app_key, app_secret, acct_no)

            # 보유종목정보 조회
            cur100 = conn.cursor()
            cur100.execute("select purchase_price, purchase_amount, sign_resist_price, sign_support_price, end_target_price, end_loss_price, code, name, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum, COALESCE(sell_plan_sum, 0) as sell_plan_sum, COALESCE(sell_plan_amount, 0) as sell_plan_amount, avail_amount, trading_plan, COALESCE(limit_price, 0) as limit_price, COALESCE(limit_amt, 0) as limit_amt from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and proc_yn = 'Y' and code = '" + code + "'")
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
                    # 매메계획
                    trading_plan = i[16]
                    print("매매계획 : " + trading_plan)
                    # 손절가
                    limit_price = i[17]
                    print("손절가 : " + format(limit_price, ',d'))
                    # 손절금액
                    limit_amt = i[18]
                    print("손절금액 : " + format(limit_amt, ',d'))
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
           
                    context.bot.send_message(chat_id=update.effective_chat.id, text=(f"{company} : 매입가-{format(int(purchase_price), ',d')}원, 매입수량-{format(purchase_amount, ',d')}주, 매입금액-{format(purchase_sum, ',d')}원, 현재가-{format(current_price, ',d')}원, 평가금액-{format(eval_sum, ',d')}원, 수익률({str(earning_rate)})%, 손수익금액({format(valuation_sum, ',d')})원, 저항가-{format(sign_resist_price, ',d')}원, 지지가-{format(sign_support_price, ',d')}원, 최종목표가-{format(end_target_price, ',d')}원, 최종이탈가-{format(end_loss_price, ',d')}원, 손절가-{format(limit_price, ',d')}원, 손절금액({format(limit_amt, ',d')})원, 매매계획-{trading_plan} => {sell_command}"), parse_mode="HTML")
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
                    delete_query0 = "update \"stockBalance_stock_balance\" set sign_resist_price = %s where code = %s and proc_yn = 'Y'"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], code])
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
                    delete_query0 = "update \"stockBalance_stock_balance\" set sign_support_price = %s where code = %s and proc_yn = 'Y'"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], code])
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
                    delete_query0 = "update \"stockBalance_stock_balance\" set end_target_price = %s where code = %s and proc_yn = 'Y'"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], code])
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
                    delete_query0 = "update \"stockBalance_stock_balance\" set end_loss_price = %s where code = %s and proc_yn = 'Y'"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], code])
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

        elif menuNum == '165':
            initMenuNum()
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 매매계획

            # 20% 물량 매도, 25% 물량 매도, 33% 물량 매도, 50% 물량 매도, 66% 물량 매도, 전체 물량 매도, 1차 매수, 2차 매수, 3차 매수, 4차 매수, 5차 매수, 홀딩, 투자
            trading_plan_list = ['20s', '25s', '33s', '50s','66s', 'as', '1b', '2b', '3b', '4b', '5b', 'h', 'i']

            # 매매계획 존재시
            if len(commandBot[1]) > 0:
                
                if commandBot[1] in trading_plan_list:
                    # 보유종목 수정
                    cur121 = conn.cursor()
                    update_query0 = "update \"stockBalance_stock_balance\" set trading_plan = %s where code = %s and proc_yn = 'Y'"
                    # update 인자값 설정
                    record_to_update0 = ([commandBot[1], code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(update_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 매매계획 [" + commandBot[1] + "] 수정")

                else:
                    print("매매계획리스트 미존재")
                    context.bot.send_message(chat_id=user_id, text=company + " : 매매계획리스트 미존재")    
            
            else:
                print("매매계획 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매매계획 미존재")

        elif menuNum == '166':
            initMenuNum()
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                value = commandBot[1].strip()
                print("commandBot[1] : ", value)    # 손실금액

            # 손실금액(음수 및 양수) 존재시
            if value.lstrip('-').isdigit():
                    # 보유종목 수정
                    cur121 = conn.cursor()
                    update_query0 = "update \"stockBalance_stock_balance\" set limit_amt = %s where acct_no = %s and code = %s and proc_yn = 'Y'"
                    # update 인자값 설정
                    record_to_update0 = ([value, str(acct_no), code])
                    # DB 연결된 커서의 쿼리 수행
                    cur121.execute(update_query0, record_to_update0)
                    conn.commit()
                    cur121.close()

                    context.bot.send_message(chat_id=user_id, text=company + " : 매매계획 [" + commandBot[1] + "] 수정")

            else:
                print("손실금액 음수 또는 양수만 입력 가능")
                context.bot.send_message(chat_id=user_id, text=company + " : 손실금액 음수 또는 양수만 입력 가능")

        elif menuNum == '171':
            initMenuNum()
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 저가
                print("commandBot[2] : ", commandBot[2])    # 고가

            # 저가, 고가 존재시
            if commandBot[1].isdecimal() & commandBot[2].isdecimal():
                low_price = commandBot[1]
                high_price = commandBot[2]
                           
                price_382 = int(high_price) - ((int(high_price) - int(low_price)) * 382 / 1000)
                # 가격에 따른 국내 주식 호가단위 계산
                tick = get_tick_size(price_382)
                # 입력 가격을 호가단위에 맞게 조정
                corrected_price_382 = round_to_valid_price(price_382, tick)

                price_50 = int(high_price) - ((int(high_price) - int(low_price)) / 2)
                # 가격에 따른 국내 주식 호가단위 계산
                tick = get_tick_size(price_50)
                # 입력 가격을 호가단위에 맞게 조정
                corrected_price_50 = round_to_valid_price(price_50, tick)

                price_618 = int(high_price) - ((int(high_price) - int(low_price)) * 618 / 1000)
                # 가격에 따른 국내 주식 호가단위 계산
                tick = get_tick_size(price_618)
                # 입력 가격을 호가단위에 맞게 조정
                corrected_price_618 = round_to_valid_price(price_618, tick)

                context.bot.send_message(chat_id=user_id, text=company + " : 저가 " + format(int(commandBot[1]), ',d') + "원, 고가 " + format(int(commandBot[2]), ',d') + "원, 피보나치 38.2% " + format(corrected_price_382, ',d') + "원, 50% " + format(corrected_price_50, ',d') + "원, 61.8% " + format(corrected_price_618, ',d') + "원")

            else:
                print("저가 또는 고가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 저가 또는 고가 미존재")
       
        elif menuNum == '172':
            initMenuNum()
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 저가
                print("commandBot[2] : ", commandBot[2])    # 고가

            # 저가, 고가 존재시
            if commandBot[1].isdecimal() & commandBot[2].isdecimal():
                low_price = commandBot[1]
                high_price = commandBot[2]
                           
                price_382 = ((int(high_price) - int(low_price)) * 382 / 1000) + int(low_price)
                # 가격에 따른 국내 주식 호가단위 계산
                tick = get_tick_size(price_382)
                # 입력 가격을 호가단위에 맞게 조정
                corrected_price_382 = round_to_valid_price(price_382, tick)

                price_50 = ((int(high_price) - int(low_price)) / 2) + int(low_price)
                # 가격에 따른 국내 주식 호가단위 계산
                tick = get_tick_size(price_50)
                # 입력 가격을 호가단위에 맞게 조정
                corrected_price_50 = round_to_valid_price(price_50, tick)

                price_618 = ((int(high_price) - int(low_price)) * 618 / 1000) + int(low_price)
                # 가격에 따른 국내 주식 호가단위 계산
                tick = get_tick_size(price_618)
                # 입력 가격을 호가단위에 맞게 조정
                corrected_price_618 = round_to_valid_price(price_618, tick)

                context.bot.send_message(chat_id=user_id, text=company + " : 저가 " + format(int(commandBot[1]), ',d') + "원, 고가 " + format(int(commandBot[2]), ',d') + "원, 역피보나치 38.2% " + format(corrected_price_382, ',d') + "원, 50% " + format(corrected_price_50, ',d') + "원, 61.8% " + format(corrected_price_618, ',d') + "원")

            else:
                print("저가 또는 고가 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 저가 또는 고가 미존재")

        elif menuNum == '21':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=4)
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
                print("commandBot[1] : ", commandBot[1])    # 매수예정금액

            # 매수예정금액 존재시
            if commandBot[1].isdecimal():

                # 매수예정금액
                buy_expect_sum = commandBot[1]
                print("매수예정금액 : " + format(int(buy_expect_sum), ',d'))
                # 매수 가능(현금) 조회
                b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                print("매수 가능(현금) : " + format(int(b), ',d'));
               
                if int(b) > int(buy_expect_sum):  # 매수가능(현금)이 매수예정금액보다 큰 경우

                    ord_dvsn = "01"
                
                    try:

                        # 입력 종목코드 현재가 호가/예상체결
                        a1 = inquire_asking_price(access_token, app_key, app_secret, code)
           
                        # 2-ask trade_amt
                        ask_trade_sum = int(a1['askp1'])*int(a1['askp_rsqn1'])+int(a1['askp2'])*int(a1['askp_rsqn2'])

                        order_amount = min(int(buy_expect_sum), ask_trade_sum)

                        if order_amount < int(buy_expect_sum):
                            context.bot.send_message(chat_id=user_id, text="[" + company + "] 입력금액 " + format(int(buy_expect_sum), ',d') + "원이 매도호가 2구간 체결가능금액 " + format(ask_trade_sum, ',d') + "원 초과, " + format(order_amount, ',d') + "원만 주문 진행합니다.")

                        # 매수량
                        n_buy_amount = round(int(order_amount) / int(a1['askp1']))
                        print("매수량 : " + format(int(n_buy_amount), ',d'))

                        # 매수
                        c = order_cash(True, access_token, app_key, app_secret, str(acct_no), code, ord_dvsn, str(n_buy_amount), str(0))
                
                        if c['ODNO'] != "":

                            # 일별주문체결 조회
                            output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, code, c['ODNO'])
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
                                g_market_buy_amount = n_buy_amount
                                g_market_buy_code = code
                                g_market_buy_company = company
                                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가 : " + format(int(d_order_price), ',d') + "원, 체결량 : " + format(int(d_total_complete_qty), ',d') + "주, 체결금액 : " + format(int(d_total_complete_amt), ',d') + "원, 주문번호 : <code>" + str(d_order_no) +"</code> => /rebuy", parse_mode='HTML')
                                get_handler = CommandHandler('rebuy', get_command6)
                                updater.dispatcher.add_handler(get_handler)

                        else:
                            print("매수주문 실패")
                            context.bot.send_message(chat_id=user_id, text="[" + code + "] 매수주문 실패")

                    except Exception as e:
                        print('매수주문 오류.', e)
                        context.bot.send_message(chat_id=user_id, text="[" + code + "] [매수주문 오류] - "+str(e))
                    
                else:
                    print("매수 가능(현금) 부족")
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수 가능(현금) : " + format(int(b) - int(buy_expect_sum), ',d') +"원 부족")
               
            else:
                print("매수예정금액 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매수예정금액 미존재")  
                   
        elif menuNum == '24':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=3)
                print("commandBot[1] : ", commandBot[1])    # 매수가
                print("commandBot[1] : ", commandBot[2])    # 매수량

            # 매수가, 매수량 존재시
            if commandBot[1].isdecimal() & commandBot[2].isdecimal():

                # 매수량
                buy_amount = commandBot[2]
                print("매수량 : " + format(int(buy_amount), ',d'))
                # 매수금액
                n_buy_sum = int(commandBot[1]) * int(buy_amount)
                print("매수금액 : " + format(int(n_buy_sum), ',d'))

                # 시가총액 기준 매수한도금액 설정
                if int(a['hts_avls']) > 5000:
                    buy_limit_sum = 5000000
                elif int(a['hts_avls']) < 2000:
                    buy_limit_sum = 2000000
                else:
                    buy_limit_sum = 3000000

                print("매수한도금액 : " + format(buy_limit_sum, ',d'))

                if buy_limit_sum >= n_buy_sum: # 매수한도금액이 매수금액보다 큰 경우

                    # 매수 가능(현금) 조회
                    b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                    print("매수 가능(현금) : " + format(int(b), ',d'));
                    if int(b) > n_buy_sum:  # 매수가능(현금)이 매수금액이 더 큰 경우

                        g_buy_amount = buy_amount
                        g_buy_price = int(commandBot[1])
                        g_buy_code = code
                        g_company = company
                       
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 시가총액 : " + format(int(a['hts_avls']), ',d') + "억원, 매수가 : " + format(int(commandBot[1]), ',d') + "원, 매수량 : " + format(int(buy_amount), ',d') + "주, 매수금액 : " + format(n_buy_sum, ',d') + "원 => /buy")
                        get_handler = CommandHandler('buy', get_command1)
                        updater.dispatcher.add_handler(get_handler)

                    else:
                        print("매수 가능(현금) 부족")
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 시가총액 : " + format(int(a['hts_avls']), ',d') + "억원, 매수가 : " + format(int(commandBot[1]), ',d') + "원, 매수량 : " + format(int(buy_amount), ',d') + "주, 매수 가능(현금) : " + format(int(b) - n_buy_sum, ',d') +"원 부족")
                else:
                    print("매수량 매수한도금액 초과")
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 시가총액 : " + format(int(a['hts_avls']), ',d') + "억원, 매수가 : " + format(int(commandBot[1]), ',d') + "원, 매수량 : " + format(int(buy_amount), ',d') + "주, 매수한도금액 : " + format(buy_limit_sum, ',d') + "원, " + format(n_buy_sum - buy_limit_sum, ',d') +"원 매수한도금액 초과")

            else:
                print("매수가 또는 매수량 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매수가 또는 매수량 미존재")        

        elif menuNum == '31':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 매도가

            # 매도가 존재시
            if commandBot[1].isdecimal():

                # 매도가
                sell_price = commandBot[1]
                print("매도가 : " + format(int(sell_price), ',d'))

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
               
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
                print("commandBot[1] : ", commandBot[1])    # 매도가

            # 매도가 존재시
            if commandBot[1].isdecimal():

                # 매도가
                sell_price = commandBot[1]
                print("매도가 : " + format(int(sell_price), ',d'))

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
               
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
                print("commandBot[1] : ", commandBot[1])    # 매도비율

            # 매도비율 존재시
            if is_positive_int(commandBot[1]):
                # 매도비율
                sell_rate = commandBot[1]

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
               
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        # 매입가 = 보유단가
                        hold_price = float(e['pchs_avg_pric'][j])
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우
                    sell_amount = int(round(ord_psbl_qty * int(sell_rate) / 100))
                    print(f"주문가능수량 {sell_rate}% 비율 수량 : {format(sell_amount, ',d')}")
                    # 매도금액
                    n_sell_sum = int(hold_price * sell_amount)
                    print("매도금액 : " + format(n_sell_sum, ',d'))
                    if ord_psbl_qty >= int(sell_amount):  # 주문가능수량이 매도량보다 큰 경우

                        ord_dvsn = "01"
                        try:

                            # 입력 종목코드 현재가 호가/예상체결
                            a1 = inquire_asking_price(access_token, app_key, app_secret, code)
            
                            # 2-bid trade_amt
                            bid_trade_sum = int(a1['bidp1'])*int(a1['bidp_rsqn1'])+int(a1['bidp2'])*int(a1['bidp_rsqn2'])

                            order_amount = min(n_sell_sum, bid_trade_sum)

                            if order_amount < n_sell_sum:
                                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도금액 " + format(n_sell_sum, ',d') + "원이 매수호가 2구간 체결가능금액 " + format(bid_trade_sum, ',d') + "원 초과, " + format(order_amount, ',d') + "원만 주문 진행합니다.")

                            # 매도량
                            n_sell_amount = round(int(order_amount) / int(hold_price))
                            print("매도량 : " + format(int(n_sell_amount), ',d'))

                            # 매도
                            c = order_cash(False, access_token, app_key, app_secret, str(acct_no), code, ord_dvsn, str(n_sell_amount), str(0))
                    
                            if c['ODNO'] != "":

                                # 일별주문체결 조회
                                output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, code, c['ODNO'])
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
                                    g_market_sell_amount = n_sell_amount
                                    g_market_sell_code = code
                                    g_market_sell_company = company
                                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(int(d_order_price), ',d') + "원, 체결량 : " + format(int(d_total_complete_qty), ',d') + "주, 체결금액 : " + format(int(d_total_complete_amt), ',d') + "원, 주문번호 : <code>" + str(d_order_no) +"</code> => /resell", parse_mode='HTML')
                                    get_handler = CommandHandler('resell', get_command7)
                                    updater.dispatcher.add_handler(get_handler)
                                
                            else:
                                print("매도주문 실패")
                                context.bot.send_message(chat_id=user_id, text="[" + code + "] 매도주문 실패")

                        except Exception as e:
                            print('매도주문 오류.', e)
                            context.bot.send_message(chat_id=user_id, text="[" + code + "] [매도주문 오류] - "+str(e))

                    else:
                        print("주문가능수량 매도량보다 부족")
                        context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 매도량보다 부족")
                else:
                    print("주문가능수량 부족")
                    context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 부족")
            else:
                print("매도비율 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매도비율(%) 미존재")

        elif menuNum == '34':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=3)
                print("commandBot[1] : ", commandBot[1])    # 매도가
                print("commandBot[2] : ", commandBot[2])    # 매도량

            # 매도가, 매도량 존재시
            if commandBot[1].isdecimal() & commandBot[2].isdecimal():

                # 매도량
                sell_amount = commandBot[2]
                print("매도량 : " + format(int(sell_amount), ',d'))

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
               
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우
                    if ord_psbl_qty >= int(sell_amount):  # 주문가능수량이 매도량보다 큰 경우

                        # 매도금액
                        n_sell_sum = int(commandBot[1]) * int(sell_amount)
                        print("매도금액 : " + format(n_sell_sum, ',d'))

                        g_sell_amount = sell_amount
                        g_sell_price = int(commandBot[1])
                        g_sell_code = code
                        g_company = company

                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(int(commandBot[1]), ',d') + "원, 매도량 : " + format(int(sell_amount), ',d') + "주, 매도금액 : " + format(int(n_sell_sum), ',d') + "원 => /sell")
                        get_handler = CommandHandler('sell', get_command2)
                        updater.dispatcher.add_handler(get_handler)

                    else:
                        print("주문가능수량 매도량보다 부족")
                        context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 매도량보다 부족")
                else:
                    print("주문가능수량 부족")
                    context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 부족")
            else:
                print("매도가 또는 매도량 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매도가 또는 매도량 미존재")

        elif menuNum == '35':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 매도가

            # 매도가 존재시
            if commandBot[1].isdecimal():

                # 매도가
                sell_price = commandBot[1]
                print("매도가 : " + format(int(sell_price), ',d'))

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
               
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우

                    sell_amount = int(round(ord_psbl_qty / 4))
                    print("주문가능수량 1/4수량 : " + format(sell_amount, ',d'))
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

        elif menuNum == '36':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 매도가

            # 매도가 존재시
            if commandBot[1].isdecimal():

                # 매도가
                sell_price = commandBot[1]
                print("매도가 : " + format(int(sell_price), ',d'))

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
               
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우

                    sell_amount = int(round(ord_psbl_qty / 3))
                    print("주문가능수량 1/3수량 : " + format(sell_amount, ',d'))
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

        elif menuNum == '37':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 매도가

            # 매도가 존재시
            if commandBot[1].isdecimal():

                # 매도가
                sell_price = commandBot[1]
                print("매도가 : " + format(int(sell_price), ',d'))

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
               
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우

                    sell_amount = int(round(ord_psbl_qty * 2 / 3))
                    print("주문가능수량 2/3수량 : " + format(sell_amount, ',d'))
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

        elif menuNum == '38':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 매도가

            # 매도가 존재시
            if commandBot[1].isdecimal():

                # 매도가
                sell_price = commandBot[1]
                print("매도가 : " + format(int(sell_price), ',d'))

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
               
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우

                    sell_amount = int(round(ord_psbl_qty * 3 / 4))
                    print("주문가능수량 3/4수량 : " + format(sell_amount, ',d'))
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

        elif menuNum == '39':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=4)
                print("commandBot[1] : ", commandBot[1])    # 매입가
                print("commandBot[2] : ", commandBot[2])    # 매도비율(%)
                print("commandBot[3] : ", commandBot[3])    # 손실수익율(%)

            # 매입가-양수/음수 소숫점 2자리까지 실수, 매도비율(%)-양수, 손실수익율(%)-양수/음수 소숫점 2자리까지 실수
            if (is_signed_float_2dec(commandBot[1]) and is_positive_int(commandBot[2]) and is_signed_float_2dec(commandBot[3])):

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
               
                hold_price = 0
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        if float(commandBot[1]) == 0:
                            # 매입가 = 보유단가
                            hold_price = e['pchs_avg_pric'][j]
                        else:
                            # 매입가
                            hold_price = commandBot[1]
                        print("매입가 : " + format(float(hold_price), ',.2f'))    
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])

                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우
                    # 매도비율
                    sell_rate = commandBot[2]
                    sell_amount = int(round(ord_psbl_qty * int(sell_rate) / 100))
                    print(f"매도수량 {format(sell_amount, ',d')}")
                    if ord_psbl_qty >= int(sell_amount):  # 주문가능수량이 매도량보다 큰 경우
                        # 손실수익율
                        loss_profie_rate = commandBot[3]
                        sell_price = int(round(float(hold_price))) + int(round(float(hold_price) * float(loss_profie_rate) / 100))
                        print(f"매도가 : {format(sell_price, ',d')}")

                        # 가격에 따른 국내 주식 호가단위 계산
                        tick = get_tick_size(sell_price)
                        # 입력 가격을 호가단위에 맞게 조정
                        corrected_price = round_to_valid_price(sell_price, tick)

                        # 상하한가 범위 안으로 보정
                        if corrected_price < lower_limit:
                            corrected_price = round_to_valid_price(lower_limit, tick, direction="up")
                        elif corrected_price > upper_limit:
                            corrected_price = round_to_valid_price(upper_limit, tick, direction="down")

                        # 매도금액
                        n_sell_sum = int(corrected_price) * sell_amount
                        print("매도금액 : " + format(n_sell_sum, ',d'))

                        g_sell_amount = sell_amount
                        g_sell_price = corrected_price
                        g_sell_code = code
                        g_company = company

                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(int(corrected_price), ',d') + "원, 매도량 : " + format(sell_amount, ',d') + "주, 매도금액 : " + format(int(n_sell_sum), ',d') + "원 => /sell")
                        get_handler = CommandHandler('sell', get_command2)
                        updater.dispatcher.add_handler(get_handler)

                    else:
                        print("주문가능수량 매도량보다 부족")
                        context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 매도량보다 부족")
                else:
                    print("주문가능수량 부족")
                    context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 부족")
            else:
                print("매입가 또는 매도비율(%) 또는 손실수익율(%) 입력값이 유효하지 않습니다.")
                context.bot.send_message(chat_id=user_id, text=company + " : 매입가 또는 매도비율(%) 또는 손실수익율(%) 입력값이 유효하지 않습니다.")
        
        elif menuNum == '41':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 매수금액

            # 매수금액 존재시
            if commandBot[1].isdecimal():

                # 매수금액
                buy_amount = commandBot[1]
                print("매수금액 : " + format(int(buy_amount), ',d'))

                # 매수 가능(현금) 조회
                b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                print("매수 가능(현금) : " + format(int(b), ',d'));
                if int(b) > int(buy_amount):  # 매수가능(현금)이 매수금액이 더 큰 경우
                    # base_dtm datetime 변환
                    base_dtm = datetime.now().strftime("%Y%m%d%H%M%S")
                    
                    # 주식당일분봉조회
                    candle_list = fetch_candles_with_base(access_token, app_key, app_secret, code, base_dtm)

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
                        acct_no, company, code, datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S"), "B", 기준봉['open'], 기준봉['high'], 기준봉['low'], 기준봉['close'], 기준봉['volume'], candle_body, buy_amount, 'Y', 'AUTO_BUY', datetime.now(), 'AUTO_BUY', datetime.now()
                    ))

                    was_inserted = cur500.rowcount == 1
                
                    conn.commit()
                    cur500.close()

                    if was_inserted:
                        context.bot.send_message(chat_id=user_id, text="[" + company + "{<code>"+code+"</code>}] 매수기준 : " + 기준봉['timestamp'].strftime("%H:%M:%S") + ", 고가 : " + format(int(기준봉['high']), ',d') + "원, 저가 : " + format(int(기준봉['low']), ',d') + "원, 거래량 : " + format(int(기준봉['volume']), ',d') + "주 정보 등록", parse_mode='HTML')
                        
                        # 매매자동처리 update
                        cur501 = conn.cursor()
                        update_query = """
                            UPDATE trade_auto_proc
                            SET
                                proc_yn = 'N'
                                , chgr_id = 'AUTO_UP_BUY'
                                , chg_date = %s
                            WHERE acct_no = %s
                            AND code = %s
                            AND base_day = %s
                            AND base_dtm <> %s
                            AND trade_tp = 'B'
                            AND proc_yn = 'Y'
                        """

                        # update 인자값 설정
                        cur501.execute(update_query, (
                            datetime.now(), str(acct_no), code, datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S")
                        ))

                        conn.commit()
                        cur501.close()

                    else:
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수기준 : " + 기준봉['timestamp'].strftime("%H:%M:%S") + " 정보 존재 미처리")

                else:
                    print("매수 가능(현금) 부족")
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수 가능(현금) : " + format(int(b) - int(buy_amount), ',d') +"원 부족")

            else:
                print("매수금액 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매수금액 미존재")        
       
        elif menuNum == '42':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 매도비율(%)

            # 매도비율(%) 존재시
            if is_positive_int(commandBot[1]):
               
               # 매도비율(%)
                sell_rate = commandBot[1]
                print("매도비율(%) : " + format(int(sell_rate), ',d'))

                if int(sell_rate) <= 100 and int(sell_rate) > 0:

                    # base_dtm datetime 변환
                    base_dtm = datetime.now().strftime("%Y%m%d%H%M%S")
                    
                    # 주식당일분봉조회
                    candle_list = fetch_candles_with_base(access_token, app_key, app_secret, code, base_dtm)

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
                        acct_no, company, code, datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S"), "S", 기준봉['open'], 기준봉['high'], 기준봉['low'], 기준봉['close'], 기준봉['volume'], candle_body, sell_rate, 'Y', 'AUTO_SELL', datetime.now(), 'AUTO_SELL', datetime.now()
                    ))

                    was_inserted = cur500.rowcount == 1

                    conn.commit()
                    cur500.close()

                    if was_inserted:
                        context.bot.send_message(chat_id=user_id, text="[" + company + "{<code>"+code+"</code>}] 매도기준 : " + 기준봉['timestamp'].strftime("%H:%M:%S") + ", 고가 : " + format(int(기준봉['high']), ',d') + "원, 저가 : " + format(int(기준봉['low']), ',d') + "원, 거래량 : " + format(int(기준봉['volume']), ',d') + "주 정보 등록", parse_mode='HTML')

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
                            datetime.now(), str(acct_no), code, datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S")
                        ))

                        conn.commit()
                        cur501.close()

                    else:
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도기준 : " + 기준봉['timestamp'].strftime("%H:%M:%S") + " 정보 존재 미처리")

                else:
                    print("매도비율(%) 범위 미충족")
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도비율(%) : " + sell_rate +" 범위 미충족")        
                                             
            else:
                print("매도비율(%) 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 매도비율(%) 미존재")
       
        elif menuNum == '51':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=4)
                print("commandBot[1] : ", commandBot[1])    # 저가
                print("commandBot[2] : ", commandBot[2])    # 고가
                print("commandBot[3] : ", commandBot[3])    # 매도비율

            # 저가, 고가, 매도비율 존재시
            if commandBot[1].isdecimal() & commandBot[2].isdecimal() & is_positive_int(commandBot[3]):

                # 저가
                low_price = commandBot[1]
                print("저가 : " + format(int(low_price), ',d'))
                # 고가
                high_price = commandBot[2]
                print("고가 : " + format(int(high_price), ',d'))
                # 매도비율
                sell_rate = commandBot[3]
                print("매도비율 : " + format(int(sell_rate), ',d'))

                sell_price = ((int(high_price) - int(low_price)) * 382 / 1000) + int(low_price)

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")
               
                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우

                    sell_amount = int(round(ord_psbl_qty * int(sell_rate) / 100))
                    print(f"주문가능수량 {sell_rate}% 비율 수량 : {format(sell_amount, ',d')}")
                   
                    # 가격에 따른 국내 주식 호가단위 계산
                    tick = get_tick_size(sell_price)
                    # 입력 가격을 호가단위에 맞게 조정
                    corrected_price = round_to_valid_price(sell_price, tick)

                    # 상하한가 범위 안으로 보정
                    if corrected_price < lower_limit:
                        corrected_price = round_to_valid_price(lower_limit, tick, direction="up")
                    elif corrected_price > upper_limit:
                        corrected_price = round_to_valid_price(upper_limit, tick, direction="down")

                    # 매도금액
                    n_sell_sum = int(corrected_price) * sell_amount
                    print("매도금액 : " + format(n_sell_sum, ',d'))

                    g_sell_amount = sell_amount
                    g_sell_price = corrected_price
                    g_sell_code = code
                    g_company = company

                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(int(corrected_price), ',d') + "원, 매도량 : " + format(sell_amount, ',d') + "주, 매도금액 : " + format(int(n_sell_sum), ',d') + "원 => /sell")
                    get_handler = CommandHandler('sell', get_command2)
                    updater.dispatcher.add_handler(get_handler)

                else:
                    print("주문가능수량 부족")
                    context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 부족")
            else:
                print("저가 또는 고가 또는 매도비율(%) 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 저가 또는 고가 또는 매도비율(%) 미존재")        

        elif menuNum == '52':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=4)
                print("commandBot[1] : ", commandBot[1])    # 저가
                print("commandBot[2] : ", commandBot[2])    # 고가
                print("commandBot[3] : ", commandBot[3])    # 매도비율

            # 저가, 고가, 매도비율 존재시
            if commandBot[1].isdecimal() & commandBot[2].isdecimal() & is_positive_int(commandBot[3]):

                # 저가
                low_price = commandBot[1]
                print("저가 : " + format(int(low_price), ',d'))
                # 고가
                high_price = commandBot[2]
                print("고가 : " + format(int(high_price), ',d'))
                # 매도비율
                sell_rate = commandBot[3]
                print("매도비율 : " + format(int(sell_rate), ',d'))

                sell_price = ((int(high_price) - int(low_price)) / 2) + int(low_price)

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")

                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우

                    sell_amount = int(round(ord_psbl_qty * int(sell_rate) / 100))
                    print(f"주문가능수량 {sell_rate}% 비율 수량 : {format(sell_amount, ',d')}")
                   
                    # 가격에 따른 국내 주식 호가단위 계산
                    tick = get_tick_size(sell_price)
                    # 입력 가격을 호가단위에 맞게 조정
                    corrected_price = round_to_valid_price(sell_price, tick)

                    # 상하한가 범위 안으로 보정
                    if corrected_price < lower_limit:
                        corrected_price = round_to_valid_price(lower_limit, tick, direction="up")
                    elif corrected_price > upper_limit:
                        corrected_price = round_to_valid_price(upper_limit, tick, direction="down")

                    # 매도금액
                    n_sell_sum = int(corrected_price) * sell_amount
                    print("매도금액 : " + format(n_sell_sum, ',d'))

                    g_sell_amount = sell_amount
                    g_sell_price = corrected_price
                    g_sell_code = code
                    g_company = company

                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(int(corrected_price), ',d') + "원, 매도량 : " + format(sell_amount, ',d') + "주, 매도금액 : " + format(int(n_sell_sum), ',d') + "원 => /sell")
                    get_handler = CommandHandler('sell', get_command2)
                    updater.dispatcher.add_handler(get_handler)

                else:
                    print("주문가능수량 부족")
                    context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 부족")
            else:
                print("저가 또는 고가 또는 매도비율(%) 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 저가 또는 고가 또는 매도비율(%) 미존재")  

        elif menuNum == '53':
            chartReq = "0"
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=4)
                print("commandBot[1] : ", commandBot[1])    # 저가
                print("commandBot[2] : ", commandBot[2])    # 고가
                print("commandBot[3] : ", commandBot[3])    # 매도비율

            # 저가, 고가, 매도비율 존재시
            if commandBot[1].isdecimal() & commandBot[2].isdecimal() & is_positive_int(commandBot[3]):

                # 저가
                low_price = commandBot[1]
                print("저가 : " + format(int(low_price), ',d'))
                # 고가
                high_price = commandBot[2]
                print("고가 : " + format(int(high_price), ',d'))
                # 매도비율
                sell_rate = commandBot[3]
                print("매도비율 : " + format(int(sell_rate), ',d'))

                sell_price = ((int(high_price) - int(low_price)) * 618 / 1000) + int(low_price)

                # 계좌잔고 조회
                e = stock_balance(access_token, app_key, app_secret, acct_no, "")

                ord_psbl_qty = 0
                for j, name in enumerate(e.index):
                    e_code = e['pdno'][j]
                    if e_code == code:
                        ord_psbl_qty = int(e['ord_psbl_qty'][j])
                print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우

                    sell_amount = int(round(ord_psbl_qty * int(sell_rate) / 100))
                    print(f"주문가능수량 {sell_rate}% 비율 수량 : {format(sell_amount, ',d')}")
                   
                    # 가격에 따른 국내 주식 호가단위 계산
                    tick = get_tick_size(sell_price)
                    # 입력 가격을 호가단위에 맞게 조정
                    corrected_price = round_to_valid_price(sell_price, tick)

                    # 상하한가 범위 안으로 보정
                    if corrected_price < lower_limit:
                        corrected_price = round_to_valid_price(lower_limit, tick, direction="up")
                    elif corrected_price > upper_limit:
                        corrected_price = round_to_valid_price(upper_limit, tick, direction="down")

                    # 매도금액
                    n_sell_sum = int(corrected_price) * sell_amount
                    print("매도금액 : " + format(n_sell_sum, ',d'))

                    g_sell_amount = sell_amount
                    g_sell_price = corrected_price
                    g_sell_code = code
                    g_company = company

                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(int(corrected_price), ',d') + "원, 매도량 : " + format(sell_amount, ',d') + "주, 매도금액 : " + format(int(n_sell_sum), ',d') + "원 => /sell")
                    get_handler = CommandHandler('sell', get_command2)
                    updater.dispatcher.add_handler(get_handler)

                else:
                    print("주문가능수량 부족")
                    context.bot.send_message(chat_id=user_id, text=company + " : 주문가능수량 부족")
            else:
                print("저가 또는 고가 또는 매도비율(%) 미존재")
                context.bot.send_message(chat_id=user_id, text=company + " : 저가 또는 고가 또는 매도비율(%) 미존재")  

    else:
        print("종목코드 미존재")
        ext = user_text + " : 종목코드 미존재"
        context.bot.send_message(chat_id=user_id, text=ext)            

    if not ',' in user_text:
        if len(code) > 0 and chartReq == "1":
            data = get_dividiend(code)

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

    # 바 차트 생성 및 저장
    def plot_financials_bar_chart(data, company_name):

        plt.figure(figsize=(12, 8))
        col_names = list(data.columns)

        label_map = {
            "매출액": 0,
            "영업이익": 1,
            "당기순이익": 4,
        }

        for i, (label, idx) in enumerate(label_map.items(), 1):
            if idx >= len(col_names):
                continue

            colname = col_names[idx]
            df_plot = data[[colname]].copy()
            df_plot.columns = [label]

            values = df_plot[label].values
            x_pos = range(len(values))  # 고유한 x 인덱스
            x_labels = df_plot.index.strftime('%Y-%m')  # 중복 없이 표시
            colors = ['red' if v >= 0 else 'blue' for v in values]

            plt.subplot(3, 1, i)
            plt.bar(x_pos, values, color=colors)
            plt.title(f"{company_name} - {label}")
            plt.xticks(x_pos, x_labels, rotation=45)

        plt.tight_layout()
        filename = f"/home/terra/Public/Batch/financials/{company_name}.png"
        plt.savefig(filename)
        plt.close()
        return filename
   
    if not ',' in user_text:
        if len(code) > 0 and chartReq == "1":
           
            # 입력 종목코드 현재가 호가/예상체결
            a1 = inquire_asking_price(access_token, app_key, app_secret, code)
           
            # 3-ask trade_amt
            ask_trade_sum = int(a1['askp1'])*int(a1['askp_rsqn1'])+int(a1['askp2'])*int(a1['askp_rsqn2'])+int(a1['askp1'])*int(a1['askp_rsqn3'])
            # 3-bid trade_amt
            bid_trade_sum = int(a1['bidp1'])*int(a1['bidp_rsqn1'])+int(a1['bidp2'])*int(a1['bidp_rsqn2'])+int(a1['bidp3'])*int(a1['bidp_rsqn3'])

            get_chart(code)
            context.bot.send_photo(chat_id=user_id, photo=open('/home/terra/Public/Batch/save1.png', 'rb'))

            summary_text = f"[{company} - 주요 지표]\n"
            summary_text += f"• 현재가:{format(int(stck_prpr), ',d')}원, 고가:{format(int(stck_hgpr), ',d')}원, 저가:{format(int(stck_lwpr), ',d')}원, 등락률:{prdy_ctrt}%, 전일비:{prdy_vrss_vol_rate}%, 거래량:{format(int(acml_vol), ',d')}주\n"
            summary_text += f"• 3호가 매도 총액 : {format(ask_trade_sum, ',d')}원\n"
            summary_text += f"• 3호가 매수 총액 : {format(bid_trade_sum, ',d')}원\n"
            summary_text += f"• 시가총액 : {format(int(hts_avls), ',d')}억원\n"
            summary_text += f"• PBR : {pbr}\n"
            summary_text += f"• BPS : {format(int(float(bps)), ',d')}원\n"

            dividend_rate = get_dividend_yield(code)
            summary_text += f"• 배당수익률 : {dividend_rate}\n"

            context.bot.send_message(chat_id=user_id, text=summary_text)

            filename = plot_financials_bar_chart(data, company)
            context.bot.send_photo(chat_id=user_id, photo=open(filename, 'rb'))

# 텔레그램봇 응답 처리
echo_handler = MessageHandler(Filters.text & (~Filters.command), echo)
dispatcher.add_handler(echo_handler)
dispatcher.add_handler(MessageHandler(Filters.regex(r"^/HoldingSell_\w+_\d+"), handle_holding_sell))
dispatcher.add_handler(MessageHandler(Filters.regex(r"^/InterestBuy_\w+_\d+"), handle_interest_buy))

# 텔레그램봇 polling
updater.start_polling()