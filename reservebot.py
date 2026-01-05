import re
import pandas as pd
from telegram.ext import Updater
from telegram.ext import MessageHandler, Filters
import requests
from datetime import datetime, timedelta
import psycopg2 as db
import kis_api_resp as resp
import sys
import math
import json
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from dateutil.relativedelta import relativedelta

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

arguments = sys.argv

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='asdf1234'"
# DB 연결
conn = db.connect(conn_string)

cur001 = conn.cursor()
cur001.execute("select bot_token2 from \"stockAccount_stock_account\" where nick_name = '" + arguments[1] + "'")
result_001 = cur001.fetchone()
cur001.close()
token = result_001[0]    

# 해당 링크는 한국거래소에서 상장법인목록을 엑셀로 다운로드하는 링크입니다.
# 다운로드와 동시에 Pandas에 excel 파일이 load가 되는 구조입니다.
krx_url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download'
# requests로 먼저 가져오기, 인코딩 지정
krx_res = requests.get(krx_url)
krx_res.encoding = 'EUC-KR'  # KRX는 EUC-KR로 인코딩됨
# pandas로 읽기
stock_code = pd.read_html(krx_res.text, header=0)[0]
# 필요한 것은 "회사명"과 "종목코드" 이므로 필요없는 column들은 제외
stock_code = stock_code[['회사명', '종목코드']]
# 한글 컬럼명을 영어로 변경
stock_code = stock_code.rename(columns={'회사명': 'company', '종목코드': 'code'})

# 맨 앞 문자만 제거 후 필터링 함수
def filter_code(code):
    code = str(code).strip()
    # 맨 앞이 문자이면 제거
    if code and code[0].isalpha():
        code = code[1:]
    # 제거 후 길이가 1 이상이면 통과
    return len(code) > 0

stock_code = stock_code[stock_code['code'].apply(filter_code)]

# 종목코드 6자리로 포맷
def normalize_code(code):
    code = str(code).strip()
    if code and code[0].isalpha():
        code = code[1:]
    # 길이 맞춤
    if len(code) < 6:
        code = code.zfill(6)
    elif len(code) > 6:
        code = code[-6:]
    return code

stock_code['code'] = stock_code['code'].apply(normalize_code)

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
g_loss_price = 0
g_risk_sum = 0
g_low_price = 0

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
    button_list = build_button(["보유종목", "전체주문", "전체예약", "예약주문", "예약정정", "예약철회", "매수등록", "매도등록", "매도추적", "추적삭제", "취소"])
    show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list) - 6))
    
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

# 일별주문체결 조회
def daily_order_complete(access_token, app_key, app_secret, acct_no, code, order_no):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC0081R",                                # (3개월이내) TTTC0081R, (3개월이전) CTSC9215R
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
                "EXCG_ID_DVSN_CD": "ALL",                           # 거래소ID구분코드 KRX : KRX, NXT : NXT, SOR (Smart Order Routing) : SOR, ALL : 전체
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": ""
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output1

# 주식예약주문 : 15시 40분 ~ 다음 영업일 07시 30분까지 가능(23시 40분 ~ 0시 10분까지 서버초기화 작업시간 불가)
def order_reserve(access_token, app_key, app_secret, acct_no, code, ord_qty, ord_price, trade_cd, ord_dvsn_cd, reserve_end_dt):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "CTSC0008U",                                # tr_id : CTSC0008U(국내예약매수입력/주문예약매도입력)
    }  
    params = {
                "CANO": acct_no,
                "ACNT_PRDT_CD": '01',
                "PDNO": code,
                "ORD_QTY": ord_qty,                                 # 주문주식수
                "ORD_UNPR": ord_price,                              # 주문단가 : 시장가인 경우 0
                "SLL_BUY_DVSN_CD": trade_cd,                        # 매도매수구분코드 : 01 매도, 02 매수
                "ORD_DVSN_CD": ord_dvsn_cd,                         # 주문구분코드 : 00 지정가, 01 시장가, 02 조건부지정가, 05 장전 시간외
                "ORD_OBJT_CBLC_DVSN_CD":"10",                       # 주문대상잔고구분코드 : 10 현금
                "RSVN_ORD_END_DT": reserve_end_dt,                  # 예약주문종료일자 : 현재일자 이후 8자리(YYYYMMDD), 미입력시 다음날 주문 처리되고 예약주문 종료, 익영업일부터 최대 30일까지 입력 가능
    }
    PATH = "uapi/domestic-stock/v1/trading/order-resv"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output

# 주식예약주문정정취소 : 15시 40분 ~ 다음 영업일 07시 30분까지 가능(23시 40분 ~ 0시 10분까지 서버초기화 작업시간 불가)
def order_reserve_cancel_revice(access_token, app_key, app_secret, acct_no, reserve_cd, code, ord_qty, ord_price, trade_cd, ord_dvsn_cd, reserve_end_dt, rsvn_ord_seq):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "CTSC0013U" if reserve_cd == "01" else "CTSC0009U", # tr_id : CTSC0013U 예약정정, CTSC0009U 예약최소
    }  
    params = {
                "CANO": acct_no,
                "ACNT_PRDT_CD": '01',
                "PDNO": code,
                "ORD_QTY": ord_qty,                                 # 주문주식수
                "ORD_UNPR": ord_price,                              # 주문단가 : 시장가인 경우 0
                "SLL_BUY_DVSN_CD": trade_cd,                        # 매도매수구분코드 : 01 매도, 02 매수
                "ORD_DVSN_CD": ord_dvsn_cd,                         # 주문구분코드 : 00 지정가, 01 시장가, 02 조건부지정가, 05 장전 시간외
                "ORD_OBJT_CBLC_DVSN_CD":"10",                       # 주문대상잔고구분코드 : 10 현금
                "RSVN_ORD_END_DT": reserve_end_dt,                  # 예약주문종료일자 : 현재일자 이후 8자리(YYYYMMDD), 미입력시 다음날 주문 처리되고 예약주문 종료, 익영업일부터 최대 30일까지 입력 가능
                "RSVN_ORD_SEQ": rsvn_ord_seq                        # 예약주문순번
    }
    PATH = "uapi/domestic-stock/v1/trading/order-resv-rvsecncl"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output

# 주식예약주문조회 : 15시 40분 ~ 다음 영업일 07시 30분까지 가능(23시 40분 ~ 0시 10분까지 서버초기화 작업시간 불가)
def order_reserve_complete(access_token, app_key, app_secret, reserve_strt_dt, reserve_end_dt, acct_no, code):

    # 현재 시간이 15:40 이후인지 체크
    now = datetime.now()
    cutoff = now.replace(hour=15, minute=40, second=0, microsecond=0)

    # reserve_strt_dt 문자열 → datetime 변환
    try:
        start_dt = datetime.strptime(reserve_strt_dt, "%Y%m%d")
    except ValueError:
        raise ValueError("reserve_strt_dt 는 YYYYMMDD 형식이어야 합니다.")

    # 현재 날짜와 reserve_strt_dt 날짜가 같고, 현재 시간이 15:40 이후라면 다음날로 변경
    if now.date() == start_dt.date() and now > cutoff:
        start_dt = start_dt + timedelta(days=1)
        reserve_strt_dt = start_dt.strftime("%Y%m%d")

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "CTSC0004R",                                # tr_id : CTSC0004R
    }  
    params = {
                "RSVN_ORD_ORD_DT": reserve_strt_dt,                 # 예약주문시작일자
                "RSVN_ORD_END_DT": reserve_end_dt,                  # 예약주문종료일자
                "RSVN_ORD_SEQ": "",                                 # 예약주문순번
                "TMNL_MDIA_KIND_CD": "00",
                "CANO": acct_no,
                "ACNT_PRDT_CD": '01',
                "PRCS_DVSN_CD": "0",                                # 처리구분코드 : 전체 0, 처리내역 1, 미처리내역 2
                "CNCL_YN": "Y",                                     # 취소여부 : 'Y'
                "PDNO": code if code != "" else "",                 # 종목코드 : 공백 입력 시 전체 조회
                "SLL_BUY_DVSN_CD": "",                              # 매도매수구분코드 : 01 매도, 02 매수        
                "CTX_AREA_FK200": "",                               
                "CTX_AREA_NK200": "",                               
    }
    PATH = "uapi/domestic-stock/v1/trading/order-resv-ccnl"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output

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

def is_positive_int(val: str) -> bool:
    """양수 정수만 허용 (1~100 범위)"""
    if val.isdigit():
        num = int(val)
        return 0 < num <= 100
    return False    

def post_business_day_char(business_day:str):
    cur100 = conn.cursor()
    cur100.execute("select post_business_day_char('"+business_day+"'::date)")
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

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

    elif data_selected.find("보유종목") != -1:

        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[보유종목 조회]",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)
            # 계좌잔고 조회
            c = stock_balance(access_token, app_key, app_secret, acct_no, "")
        
            result_msgs = []
            ord_psbl_qty = 0
            for i, name in enumerate(c.index):
                code = c['pdno'][i]
                name = c['prdt_name'][i]
                purchase_price = c['pchs_avg_pric'][i]
                purchase_amount = int(c['hldg_qty'][i])
                purchase_sum = int(c['pchs_amt'][i])
                current_price = int(c['prpr'][i])
                eval_sum = int(c['evlu_amt'][i])
                earnings_rate = c['evlu_pfls_rt'][i]
                valuation_sum = int(c['evlu_pfls_amt'][i])
                ord_psbl_qty = int(c['ord_psbl_qty'][i])

                msg = f"* {name}[<code>{code}</code>] 단가:{format(float(purchase_price), ',.2f')}원, 보유량:{format(purchase_amount, ',d')}주, 보유금액:{format(purchase_sum, ',d')}원, 현재가:{format(current_price, ',d')}원, 평가금액:{format(eval_sum, ',d')}원, 수익률:{str(earnings_rate)}%, 손수익금액:{format(valuation_sum, ',d')}원"
                result_msgs.append(msg)

            final_message = "\n".join(result_msgs) if result_msgs else "보유종목 조회 대상이 존재하지 않습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=update.callback_query.message.chat_id,
                message_id=update.callback_query.message.message_id
            )


        except Exception as e:
            print('보유종목 조회 오류.', e)
            context.bot.edit_message_text(text="[보유종목 조회] 오류 : "+str(e),
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)
                
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

                    msg = f"* [{d_name} - {d_order_tmd[:2]}:{d_order_tmd[2:4]}:{d_order_tmd[4:]}] 주문번호:<code>{str(d_order_no)}</code>, {d_order_type}가:{format(int(d_order_price), ',d')}원, {d_order_type}량:{format(int(d_order_amount), ',d')}주, 체결량:{format(int(d_total_complete_qty), ',d')}주, 잔량:{format(int(d_remain_qty), ',d')}주, 체결금:{format(int(d_total_complete_amt), ',d')}원"
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

    elif data_selected.find("전체예약") != -1:
    
        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[전체예약 조회]",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)

            
            reserve_strt_dt = datetime.now().strftime("%Y%m%d")
            reserve_end_dt = (datetime.now() + relativedelta(months=1)).strftime("%Y%m%d")
            # 전체예약 조회
            output = order_reserve_complete(access_token, app_key, app_secret, reserve_strt_dt, reserve_end_dt, str(acct_no), "")

            if len(output) > 0:
                tdf = pd.DataFrame(output)
                tdf.set_index('rsvn_ord_seq')
                d = tdf[['rsvn_ord_seq', 'rsvn_ord_ord_dt', 'rsvn_ord_rcit_dt', 'pdno', 'ord_dvsn_cd', 'ord_rsvn_qty', 'tot_ccld_qty', 'cncl_ord_dt', 'ord_tmd', 'odno', 'rsvn_ord_rcit_tmd', 'kor_item_shtn_name', 'sll_buy_dvsn_cd', 'ord_rsvn_unpr', 'tot_ccld_amt', 'cncl_rcit_tmd', 'prcs_rslt', 'ord_dvsn_name', 'rsvn_end_dt']]
                result_msgs = []

                for i, name in enumerate(d.index):
                    d_rsvn_ord_seq = int(d['rsvn_ord_seq'][i])          # 예약주문 순번
                    d_rsvn_ord_ord_dt = d['rsvn_ord_ord_dt'][i]         # 예약주문주문일자
                    d_rsvn_ord_rcit_dt = d['rsvn_ord_rcit_dt'][i]       # 예약주문접수일자
                    d_code = d['pdno'][i]
                    d_ord_dvsn_cd = d['ord_dvsn_cd'][i]                 # 주문구분코드
                    d_ord_rsvn_qty = int(d['ord_rsvn_qty'][i])          # 주문예약수량
                    d_tot_ccld_qty = int(d['tot_ccld_qty'][i])          # 총체결수량
                    d_cncl_ord_dt = d['cncl_ord_dt'][i]                 # 취소주문일자
                    d_ord_tmd = d['ord_tmd'][i]                         # 주문시각
                    d_order_no = d['odno'][i]                           # 주문번호
                    d_rsvn_ord_rcit_tmd = d['rsvn_ord_rcit_tmd'][i]     # 예약주문접수시각
                    d_name = d['kor_item_shtn_name'][i]                 # 종목명
                    d_sll_buy_dvsn_cd = d['sll_buy_dvsn_cd'][i]         # 매도매수구분코드
                    d_ord_rsvn_unpr = int(d['ord_rsvn_unpr'][i])        # 주문예약단가
                    d_tot_ccld_amt = int(d['tot_ccld_amt'][i])          # 총체결금액
                    d_cncl_rcit_tmd = d['cncl_rcit_tmd'][i]             # 취소접수시각
                    d_prcs_rslt = d['prcs_rslt'][i]                     # 처리결과
                    d_ord_dvsn_name = d['ord_dvsn_name'][i]             # 주문구분명
                    d_rsvn_end_dt = d['rsvn_end_dt'][i]                 # 예약종료일자

                    msg1 = f"* {d_name}[<code>{d_code}</code>] {d_rsvn_ord_ord_dt[:4]}/{d_rsvn_ord_ord_dt[4:6]}/{d_rsvn_ord_ord_dt[6:]}~{d_rsvn_end_dt[:4]}/{d_rsvn_end_dt[4:6]}/{d_rsvn_end_dt[6:]} 예약번호:<code>{str(d_rsvn_ord_seq)}</code>, {d_ord_dvsn_name}:{format(d_ord_rsvn_unpr, ',d')}원, 예약수량:{format(d_ord_rsvn_qty, ',d')}주 {d_prcs_rslt}"
                    result_msgs.append(msg1)
                    
                    if d_cncl_ord_dt != "":
                        msg2 = f", 취소주문일자:{d_cncl_ord_dt[:4]}/{d_cncl_ord_dt[4:6]}/{d_cncl_ord_dt[6:]}"
                        result_msgs.append(msg2)
                    if str(d_order_no) != "":
                        msg3 = f", 주문번호:<code>{str(d_order_no)}</code>, 체결수량:{format(d_tot_ccld_qty, ',d')}주, 체결금액:{format(d_tot_ccld_amt, ',d')}원"
                        result_msgs.append(msg3)

                final_message = "\n".join(result_msgs) if result_msgs else "전체예약 조회 대상이 존재하지 않습니다."

                context.bot.edit_message_text(
                    text=final_message,
                    parse_mode='HTML',
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )

            else:
                context.bot.send_message(text="전체예약 조회 미존재 : " + g_company,
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)

        except Exception as e:
            print('전체예약 조회 오류.', e)
            context.bot.edit_message_text(text="[전체예약 조회] 오류 : "+str(e),
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)

    elif data_selected.find("예약주문") != -1:
        menuNum = "61"

        context.bot.edit_message_text(text="예약주문할 종목코드(종목명), 매매구분(매수:1 매도:2), 단가(시장가:0), 수량, 예약종료일-8자리(YYYYMMDD)를 입력하세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)
    
    elif data_selected.find("예약정정") != -1:
        menuNum = "62"

        context.bot.edit_message_text(text="예약정정할 종목코드(종목명), 예약주문번호, 정정가(시장가:0), 예약종료일-8자리(YYYYMMDD)를 입력하세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)
    
    elif data_selected.find("예약철회") != -1:
        menuNum = "63"

        context.bot.edit_message_text(text="예약취소할 종목코드(종목명), 예약주문번호를 입력하세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)
        
    elif data_selected.find("매수등록") != -1:
        menuNum = "71"

        context.bot.edit_message_text(text="매수등록할 종목코드(종목명), 날짜(8자리), 시간(6자리), 매수가, 이탈가를 입력하세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)

    elif data_selected.find("매도등록") != -1:
        menuNum = "81"

        context.bot.edit_message_text(text="매도등록할 종목코드(종목명), 날짜(8자리), 시간(6자리), 매도가, 비중(%)을 입력하세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)        
    
    elif data_selected.find("매도추적") != -1:
        ac = account()
        acct_no = ac['acct_no']

        try:
            context.bot.edit_message_text(text="[매도추적 등록]",
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)
            
            business_day = datetime.now().strftime("%Y%m%d")
            trail_day = post_business_day_char(business_day)
            result_msgs = []
            # 매도추적 insert
            cur200 = conn.cursor()
            insert_query = """
                WITH AA AS (
                    SELECT
                        B.acct_no,
                        B.code,
                        B.name,
                        B.trade_day,
                        B.trade_dtm,
                        B.buy_price,
                        B.loss_price,
                        B.profit_price
                    FROM (
                        SELECT
                            acct_no,
                            code,
                            MAX(trade_day || trade_dtm) AS trdtm
                        FROM public.tradng_simulation
                        WHERE trade_tp = '1'
                        AND proc_yn = 'N'
                        GROUP BY acct_no, code
                    ) A
                    JOIN public.tradng_simulation B
                    ON A.acct_no = B.acct_no
                    AND A.code    = B.code
                    AND substr(A.trdtm, 1, 8) = B.trade_day
                    AND substr(A.trdtm, 9, 6) = B.trade_dtm
                    AND B.trade_tp = '1'
                    AND B.proc_yn  = 'N'
                )
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
                )
                SELECT
                    AA.acct_no,
                    AA.name,
                    AA.code,
                    %s,
                    %s,
                    %s,
                    AA.buy_price,
                    CASE
                        WHEN BB.acct_no IS NULL THEN AA.loss_price
                        ELSE BB.stop_price
                    END AS stop_price,
                    CASE
                        WHEN BB.acct_no IS NULL THEN AA.profit_price
                        ELSE BB.target_price
                    END AS target_price,
                    now(),
                    now()
                FROM AA
                LEFT JOIN trading_trail BB
                ON AA.acct_no = BB.acct_no
                AND AA.code = BB.code
                AND BB.trail_day = get_previous_business_day(now()::date)::char
                AND BB.trail_tp = '1'
                WHERE AA.acct_no = %s
                AND NOT EXISTS (
                    SELECT 1
                    FROM trading_trail T
                    WHERE T.acct_no = AA.acct_no
                    AND T.code = AA.code
                    AND T.trail_day = %s
                    AND T.trail_dtm = %s
                    AND T.trail_tp = %s
                );
                """
            # insert 인자값 설정
            cur200.execute(insert_query, (trail_day, '090000', '1', acct_no, trail_day, '090000', '1'))

            countProc = cur200.rowcount

            conn.commit()
            cur200.close()

            if countProc >= 1:
                msg = f"* 매도추적 등록 {countProc}건 처리"
                result_msgs.append(msg)
            else:
                msg = f"* 매도추적 등록 미처리"
                result_msgs.append(msg)

            final_message = "\n".join(result_msgs) if result_msgs else "매도추적 등록 대상이 존재하지 않습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=update.callback_query.message.chat_id,
                message_id=update.callback_query.message.message_id
            )

        except Exception as e:
            print('매도추적 등록 오류.', e)
            context.bot.edit_message_text(text="[매도추적 등록] 오류 : "+str(e),
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)   

    elif data_selected.find("추적삭제") != -1:
        ac = account()
        acct_no = ac['acct_no']

        try:
            context.bot.edit_message_text(text="[추적삭제]",
                                chat_id=update.callback_query.message.chat_id,
                                message_id=update.callback_query.message.message_id)
            
            business_day = datetime.now().strftime("%Y%m%d")
            trail_day = post_business_day_char(business_day)
            result_msgs = []
        
            # 추적 delete
            cur200 = conn.cursor()
            delete_query = """
                DELETE FROM trading_trail WHERE acct_no = %s AND trail_day = %s
                """
            # delete 인자값 설정
            cur200.execute(delete_query, (acct_no, trail_day))

            countProc = cur200.rowcount

            conn.commit()
            cur200.close()

            if countProc >= 1:
                msg = f"* 추적 삭제 {countProc}건 처리"
                result_msgs.append(msg)
            else:
                msg = f"* 추적 삭제 미처리"
                result_msgs.append(msg)

            final_message = "\n".join(result_msgs) if result_msgs else "추적 삭제 대상이 존재하지 않습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=update.callback_query.message.chat_id,
                message_id=update.callback_query.message.message_id
            )                

        except Exception as e:
            print('추적 삭제 오류.', e)
            context.bot.edit_message_text(text="[추적 삭제] 오류 : "+str(e),
                                            chat_id=update.callback_query.message.chat_id,
                                            message_id=update.callback_query.message.message_id)   
            
get_handler = CommandHandler('reserve', get_command)
updater.dispatcher.add_handler(get_handler)

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
    global g_loss_price
    global g_risk_sum
    global g_low_price

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
        g_low_price = stck_lwpr

        print("menuNum : ", menuNum)

        if menuNum == '61':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=5)
                print("commandBot[1] : ", commandBot[1])    # 매매구분(매수:1 매도:2)
                print("commandBot[2] : ", commandBot[2])    # 단가(시장가:0)
                print("commandBot[3] : ", commandBot[3])    # 수량
                print("commandBot[4] : ", commandBot[4])    # 예약종료일-8자리(YYYYMMDD)

            # 매매구분(매수:1 매도:2)
            if commandBot[1] not in ["1", "2"]:
                print("매매구분 값은 1(매수), 2(매도)만 허용됩니다.")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매매구분 값은 1(매수), 2(매도)만 허용됩니다.")
            else:    
                # 단가(시장가:0), 수량, 예약종료일-8자리(YYYYMMDD) 존재시
                if commandBot[2].isdecimal() and commandBot[3].isdecimal() and len(commandBot[4]) == 8 and commandBot[4].isdigit():

                    ord_rsv_price = int(commandBot[2])      # 예약단가
                    ord_rsv_qty = int(commandBot[3])        # 예약수량
                    ord_rsv_end_dt = commandBot[4]          # 예약죵료일

                    # 매매구분(전체:0 매수:1 매도:2)
                    if commandBot[1] == '1':

                        # 매수예정금액
                        buy_expect_sum = ord_rsv_price * ord_rsv_qty
                        print("매수예정금액 : " + format(int(buy_expect_sum), ',d'))
                        # 매수 가능(현금) 조회
                        b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                        print("매수 가능(현금) : " + format(int(b), ',d'));
                    
                        if int(b) > int(buy_expect_sum):  # 매수가능(현금)이 매수예정금액보다 큰 경우

                            trade_cd = "02"
                            try:
                                # 주식예약주문
                                rsv_ord_result = order_reserve(access_token, app_key, app_secret, str(acct_no), code, str(ord_rsv_qty), str(ord_rsv_price), trade_cd, "01" if ord_rsv_price == 0 else "00", ord_rsv_end_dt)
                        
                                if rsv_ord_result['RSVN_ORD_SEQ'] != "":
                                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 예약주문번호 : <code>" + rsv_ord_result['RSVN_ORD_SEQ'] + "</code> 예약매수주문", parse_mode='HTML')

                                else:
                                    print("예약매수주문 실패")
                                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 예약매수주문 실패")

                            except Exception as e:
                                print('예약매수주문 오류.', e)
                                context.bot.send_message(chat_id=user_id, text="[" + code + "] [예약매수주문 오류] - "+str(e))
                            
                        else:
                            print("매수 가능(현금) 부족")
                            context.bot.send_message(chat_id=user_id, text="[" + company + "] 예약매수 가능(현금) : " + format(int(b) - int(buy_expect_sum), ',d') +"원 부족")

                    elif commandBot[1] == '2':

                        # 계좌잔고 조회
                        e = stock_balance(access_token, app_key, app_secret, acct_no, "")
                    
                        ord_psbl_qty = 0
                        for j, name in enumerate(e.index):
                            e_code = e['pdno'][j]
                            if e_code == code:
                                ord_psbl_qty = int(e['ord_psbl_qty'][j])
                        print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                        if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우
                            if ord_psbl_qty >= ord_rsv_qty:  # 주문가능수량이 예약수량보다 큰 경우

                                trade_cd = "01"
                                try:
                                    # 주식예약주문
                                    rsv_ord_result = order_reserve(access_token, app_key, app_secret, str(acct_no), code, str(ord_rsv_qty), str(ord_rsv_price), trade_cd, "01" if ord_rsv_price == 0 else "00", ord_rsv_end_dt)
                            
                                    if rsv_ord_result['RSVN_ORD_SEQ'] != "":
                                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 예약주문번호 : <code>" + rsv_ord_result['RSVN_ORD_SEQ'] + "</code> 예약매도주문", parse_mode='HTML')

                                    else:
                                        print("예약주문 실패")
                                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 예약매도주문 실패")

                                except Exception as e:
                                    print('예약주문 오류.', e)
                                    context.bot.send_message(chat_id=user_id, text="[" + code + "] [예약매도주문 오류] - "+str(e))

                            else:
                                context.bot.send_message(chat_id=user_id, text="[" + company + "] 예약수량("+format(ord_rsv_qty, ',d')+"주)이 주문가능수량("+format(ord_psbl_qty, ',d')+"주)보다 커서 예약매도주문 불가")     
                        else:
                            context.bot.send_message(chat_id=user_id, text="[" + company + "] 주문가능수량이 없어 예약매도주문 불가")      

                else:
                    print("단가, 수량, 예약종료일-8자리(YYYYMMDD) 미존재 또는 부적합")
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 단가, 수량, 예약종료일-8자리(YYYYMMDD) 미존재 또는 부적합")     

        elif menuNum == '62':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=4)
                print("commandBot[1] : ", commandBot[1])    # 예약주문번호
                print("commandBot[2] : ", commandBot[2])    # 정정가(시장가:0)
                print("commandBot[3] : ", commandBot[3])    # 예약종료일-8자리(YYYYMMDD)

            # 예약주문번호, 정정가(시장가:0), 예약종료일-8자리(YYYYMMDD) 존재시
            if commandBot[1].isdecimal() and commandBot[2].isdecimal() and len(commandBot[3]) == 8 and commandBot[3].isdigit():

                ord_rsv_no = commandBot[1]              # 예약주문번호
                ord_rsv_price = int(commandBot[2])      # 정정가(시장가:0)
                ord_rsv_end_dt = commandBot[3]          # 예약죵료일

                # 현재 날짜 계산 (15:40 이후이면 다음날)
                now = datetime.now()
                cutoff = now.replace(hour=15, minute=40, second=0, microsecond=0)

                if now > cutoff:
                    start_date = (now + timedelta(days=1)).strftime("%Y%m%d")
                else:
                    start_date = now.strftime("%Y%m%d")

                # 전체예약 조회
                output = order_reserve_complete(access_token, app_key, app_secret, start_date, ord_rsv_end_dt, str(acct_no), "")

                if len(output) > 0:
                    tdf = pd.DataFrame(output)
                    tdf.set_index('rsvn_ord_seq')
                    d = tdf[['rsvn_ord_seq', 'rsvn_ord_ord_dt', 'rsvn_ord_rcit_dt', 'pdno', 'ord_dvsn_cd', 'ord_rsvn_qty', 'tot_ccld_qty', 'cncl_ord_dt', 'ord_tmd', 'odno', 'rsvn_ord_rcit_tmd', 'kor_item_shtn_name', 'sll_buy_dvsn_cd', 'ord_rsvn_unpr', 'tot_ccld_amt', 'cncl_rcit_tmd', 'prcs_rslt', 'ord_dvsn_name', 'rsvn_end_dt']]

                    d_ord_rsvn_qty = 0
                    d_sll_buy_dvsn_cd = ""
                    d_ord_dvsn_name = ""
                    for i, name in enumerate(d.index):
                        d_rsvn_ord_seq = int(d['rsvn_ord_seq'][i])          # 예약주문 순번
                        d_code = d['pdno'][i]

                        if d_code == code and str(d_rsvn_ord_seq) == ord_rsv_no:
                            d_ord_rsvn_qty = int(d['ord_rsvn_qty'][i])          # 주문예약수량
                            d_sll_buy_dvsn_cd = d['sll_buy_dvsn_cd'][i]         # 매도매수구분코드
                            d_ord_dvsn_name = d['ord_dvsn_name'][i]             # 주문구분명

                    if d_ord_rsvn_qty >= 0:  # 주문예약수량이 존재하는 경우

                        try:
                            # 주식예약주문정정
                            rsv_ord_result = order_reserve_cancel_revice(access_token, app_key, app_secret, str(acct_no), "01", code, str(d_ord_rsvn_qty), ord_rsv_price, d_sll_buy_dvsn_cd, "01" if ord_rsv_price == 0 else "00", ord_rsv_end_dt, ord_rsv_no)
                    
                            if rsv_ord_result['NRML_PRCS_YN'] == "Y":
                                context.bot.send_message(chat_id=user_id, text="[" + company + "-" + d_ord_dvsn_name + "] 정정가 : "+format(ord_rsv_price, ',d')+"원 예약주문정정")

                            else:
                                context.bot.send_message(chat_id=user_id, text="[" + company + "-" + d_ord_dvsn_name + "] 정정가 : "+format(ord_rsv_price, ',d')+"원 예약주문정정 실패")

                        except Exception as e:
                            context.bot.send_message(chat_id=user_id, text="[" + code + "] [예약주문정정 오류] - "+str(e))

                    else:
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 주문예약수량("+format(d_ord_rsvn_qty, ',d')+"주)이 없어 예약주문정정 불가")     
                else:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 예약정보 미존재")     

            else:
                print("예약주문번호, 정장가, 예약종료일-8자리(YYYYMMDD) 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 예약주문번호, 정장가, 예약종료일-8자리(YYYYMMDD) 미존재 또는 부적합")         

        elif menuNum == '63':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 예약주문번호

            # 예약주문번호 존재시
            if commandBot[1].isdecimal():

                ord_rsv_no = commandBot[1]              # 예약주문번호

                # 현재 날짜 계산 (15:40 이후이면 다음날)
                now = datetime.now()
                cutoff = now.replace(hour=15, minute=40, second=0, microsecond=0)

                if now > cutoff:
                    reserve_strt_dt = (now + timedelta(days=1)).strftime("%Y%m%d")
                else:
                    reserve_strt_dt = now.strftime("%Y%m%d")

                reserve_end_dt = (datetime.now() + relativedelta(months=1)).strftime("%Y%m%d")                    

                # 전체예약 조회
                output = order_reserve_complete(access_token, app_key, app_secret, reserve_strt_dt, reserve_end_dt, str(acct_no), "")

                if len(output) > 0:
                    tdf = pd.DataFrame(output)
                    tdf.set_index('rsvn_ord_seq')
                    d = tdf[['rsvn_ord_seq', 'rsvn_ord_ord_dt', 'rsvn_ord_rcit_dt', 'pdno', 'ord_dvsn_cd', 'ord_rsvn_qty', 'tot_ccld_qty', 'cncl_ord_dt', 'ord_tmd', 'odno', 'rsvn_ord_rcit_tmd', 'kor_item_shtn_name', 'sll_buy_dvsn_cd', 'ord_rsvn_unpr', 'tot_ccld_amt', 'cncl_rcit_tmd', 'prcs_rslt', 'ord_dvsn_name', 'rsvn_end_dt']]

                    d_ord_rsvn_qty = 0
                    d_ord_rsvn_unpr = 0
                    d_sll_buy_dvsn_cd = ""
                    d_rsvn_end_dt = ""
                    d_ord_dvsn_name = ""
                    for i, name in enumerate(d.index):
                        d_rsvn_ord_seq = int(d['rsvn_ord_seq'][i])          # 예약주문 순번
                        d_code = d['pdno'][i]

                        if d_code == code and str(d_rsvn_ord_seq) == ord_rsv_no:               
                            d_ord_rsvn_qty = int(d['ord_rsvn_qty'][i])          # 주문예약수량
                            d_ord_rsvn_unpr = int(d['ord_rsvn_unpr'][i])        # 주문예약단가
                            d_sll_buy_dvsn_cd = d['sll_buy_dvsn_cd'][i]         # 매도매수구분코드
                            d_rsvn_end_dt = d['rsvn_end_dt'][i]                 # 예약종료일자
                            d_ord_dvsn_name = d['ord_dvsn_name'][i]             # 주문구분명

                    if d_ord_rsvn_qty >= 0:  # 주문예약수량이 존재하는 경우

                        try:
                            # 주식예약주문취소
                            rsv_ord_result = order_reserve_cancel_revice(access_token, app_key, app_secret, str(acct_no), "02", code, str(d_ord_rsvn_qty), d_ord_rsvn_unpr, d_sll_buy_dvsn_cd, "01" if d_ord_rsvn_unpr == 0 else "00", d_rsvn_end_dt, ord_rsv_no)
                    
                            if rsv_ord_result['NRML_PRCS_YN'] == "Y":
                                context.bot.send_message(chat_id=user_id, text="[" + company + "-" + d_ord_dvsn_name + "] 예약주문취소")

                            else:
                                context.bot.send_message(chat_id=user_id, text="[" + company + "-" + d_ord_dvsn_name + "] 예약주문취소 실패")

                        except Exception as e:
                            context.bot.send_message(chat_id=user_id, text="[" + code + "] [예약주문취소 오류] - "+str(e))

                    else:
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 주문예약수량("+format(d_ord_rsvn_qty, ',d')+"주)이 없어 예약주문취소 불가")     
                else:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 예약정보 미존재")      

            else:
                print("예약주문번호 미존재")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 예약주문번호 미존재")               

        elif menuNum == '71':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=5)
                print("commandBot[1] : ", commandBot[1])    # 날짜-8자리(YYYYMMDD)
                print("commandBot[2] : ", commandBot[2])    # 시간-6자리(HHMMSS)
                print("commandBot[3] : ", commandBot[3])    # 매수가
                print("commandBot[4] : ", commandBot[4])    # 이탈가

            # 날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매수가, 이탈가 존재시
            if len(commandBot[1]) == 8 and commandBot[1].isdigit() and len(commandBot[2]) == 6 and commandBot[2].isdigit() and commandBot[3].isdecimal() and commandBot[4].isdecimal():
                year_day = commandBot[1]                                # 날짜-8자리(YYYYMMDD)
                hour_minute = commandBot[2]                             # 시간-6자리(HHMMSS)
                buy_price = int(commandBot[3])                          # 매수가
                loss_price = int(commandBot[4])                         # 이탈가
                safe_margin_price = int(buy_price + buy_price * 0.04)   # 안전마진가

                # 매매시뮬레이션 insert
                cur500 = conn.cursor()
                insert_query = """
                    INSERT INTO tradng_simulation (
                        acct_no, name, code, trade_day, trade_dtm, trade_tp, buy_price, loss_price, profit_price, proc_yn, crt_dt, mod_dt
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM tradng_simulation
                        WHERE acct_no=%s AND code=%s AND trade_day=%s AND trade_dtm=%s AND trade_tp=%s 
                    );
                    """
                # insert 인자값 설정
                cur500.execute(insert_query, (
                    acct_no, company, code, year_day, hour_minute, "1", buy_price,  loss_price, safe_margin_price, 'N', datetime.now(), datetime.now()
                    , acct_no, code, year_day, hour_minute, "1"
                ))

                was_inserted = cur500.rowcount == 1

                conn.commit()
                cur500.close()

                if was_inserted:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "{<code>"+code+"</code>}] 매수가 : " + format(buy_price, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 안전마진가 : " + format(safe_margin_price, ',d') + "원 매수등록", parse_mode='HTML')
                else:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가 : " + format(buy_price, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 안전마진가 : " + format(safe_margin_price, ',d') + "원 매수등록 미처리")                        

            else:
                print("날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매수가, 이탈가 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매수가, 이탈가 미존재 또는 부적합")         

        elif menuNum == '81':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=5)
                print("commandBot[1] : ", commandBot[1])    # 날짜-8자리(YYYYMMDD)
                print("commandBot[2] : ", commandBot[2])    # 시간-6자리(HHMMSS)
                print("commandBot[3] : ", commandBot[3])    # 매도가
                print("commandBot[4] : ", commandBot[4])    # 비중(%)

            # 날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매도가, 비중(%) 존재시
            if len(commandBot[1]) == 8 and commandBot[1].isdigit() and len(commandBot[2]) == 6 and commandBot[2].isdigit() and commandBot[3].isdecimal() and is_positive_int(commandBot[4]):
                year_day = commandBot[1]                                # 날짜-8자리(YYYYMMDD)
                hour_minute = commandBot[2]                             # 시간-6자리(HHMMSS)
                sell_price = int(commandBot[3])                         # 매도가
                sell_rate = int(commandBot[4])                          # 비중(%)

                # 계좌잔고 조회
                c = stock_balance(access_token, app_key, app_secret, acct_no, "")
            
                hldg_qty = 0

                for i, name in enumerate(c.index):
                    if code == c['pdno'][i]:
                        hldg_qty = int(c['hldg_qty'][i])

                sell_qty = int(hldg_qty * sell_rate * 0.01)   # 매도량

                # 매매시뮬레이션 insert
                cur500 = conn.cursor()
                insert_query = """
                    INSERT INTO tradng_simulation (
                        acct_no, name, code, trade_day, trade_dtm, trade_tp, sell_price, sell_qty, trading_plan, proc_yn, crt_dt, mod_dt 
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM tradng_simulation
                        WHERE acct_no=%s AND code=%s AND trade_day=%s AND trade_dtm=%s AND trade_tp=%s 
                    );
                    """
                # insert 인자값 설정
                cur500.execute(insert_query, (
                    acct_no, company, code, year_day, hour_minute, "2", sell_price,  sell_qty, str(sell_rate), 'N', datetime.now(), datetime.now()
                    , acct_no, code, year_day, hour_minute, "2"
                ))

                was_inserted = cur500.rowcount == 1

                conn.commit()
                cur500.close()

                if was_inserted:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "{<code>"+code+"</code>}] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 매도등록", parse_mode='HTML')
                else:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 매도등록 미처리")                        

            else:
                print("날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매도가, 비중(%) 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매도가, 비중(%) 미존재 또는 부적합")                         

# 텔레그램봇 응답 처리
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, echo))

# 텔레그램봇 polling
updater.start_polling()
