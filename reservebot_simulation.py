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
from psycopg2.extras import execute_values
import time

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

arguments = sys.argv

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
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
    
def build_date_buttons1(days=7):
    today = datetime.now().date()

    cur00 = conn.cursor()
    cur00.execute("SELECT holiday FROM stock_holiday")
    holidays = {row[0] for row in cur00.fetchall()}
    cur00.close()

    buttons = []
    cnt = 0
    offset = 0

    while cnt < days:
        d = today - timedelta(days=offset)
        date_str = d.strftime("%Y%m%d")
        offset += 1

        # 주말 제외
        if d.weekday() >= 5:
            continue

        # 휴장일 제외
        if date_str in holidays:
            continue

        buttons.append(
            InlineKeyboardButton(
                text=d.strftime("%Y-%m-%d"),
                callback_data=f"sell_trace_date:{d.strftime('%Y-%m-%d')}"
            )
        )
        cnt += 1

    return InlineKeyboardMarkup(build_menu(buttons, 2))

def build_date_buttons2(days=7):
    today = datetime.now().date()

    cur00 = conn.cursor()
    cur00.execute("SELECT holiday FROM stock_holiday")
    holidays = {row[0] for row in cur00.fetchall()}
    cur00.close()

    buttons = []
    cnt = 0
    offset = 0

    while cnt < days:
        d = today - timedelta(days=offset)
        date_str = d.strftime("%Y%m%d")
        offset += 1

        # 주말 제외
        if d.weekday() >= 5:
            continue

        # 휴장일 제외
        if date_str in holidays:
            continue

        buttons.append(
            InlineKeyboardButton(
                text=d.strftime("%Y-%m-%d"),
                callback_data=f"trace_delete_date:{d.strftime('%Y-%m-%d')}"
            )
        )
        cnt += 1

    return InlineKeyboardMarkup(build_menu(buttons, 2))

def build_date_buttons3(days=7):
    today = datetime.now().date()

    cur00 = conn.cursor()
    cur00.execute("SELECT holiday FROM stock_holiday")
    holidays = {row[0] for row in cur00.fetchall()}
    cur00.close()

    buttons = []
    cnt = 0
    offset = 0

    while cnt < days:
        d = today - timedelta(days=offset)
        date_str = d.strftime("%Y%m%d")
        offset += 1

        # 주말 제외
        if d.weekday() >= 5:
            continue

        # 휴장일 제외
        if date_str in holidays:
            continue

        buttons.append(
            InlineKeyboardButton(
                text=d.strftime("%Y-%m-%d"),
                callback_data=f"trading_signal_date:{d.strftime('%Y-%m-%d')}"
            )
        )
        cnt += 1

    return InlineKeyboardMarkup(build_menu(buttons, 2))

def build_date_buttons4(days=7):
    today = datetime.now().date()

    cur00 = conn.cursor()
    cur00.execute("SELECT holiday FROM stock_holiday")
    holidays = {row[0] for row in cur00.fetchall()}
    cur00.close()

    buttons = []
    cnt = 0
    offset = 0

    while cnt < days:
        d = today - timedelta(days=offset)
        date_str = d.strftime("%Y%m%d")
        offset += 1

        # 주말 제외
        if d.weekday() >= 5:
            continue

        # 휴장일 제외
        if date_str in holidays:
            continue

        buttons.append(
            InlineKeyboardButton(
                text=d.strftime("%Y-%m-%d"),
                callback_data=f"trading_trail_date:{d.strftime('%Y-%m-%d')}"
            )
        )
        cnt += 1

    return InlineKeyboardMarkup(build_menu(buttons, 2))

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
    main_buttons = build_button(["보유종목", "전체주문", "전체예약", "예약주문",
                                  "예약정정", "예약철회", "매수등록", "매도등록", 
                                  "매도추적", "추적삭제", "매매신호", "매매추적"],
                                  callback_header="menu")
    cancel_button = build_button(["취소"], callback_header="menu")
    show_markup = InlineKeyboardMarkup(build_menu(main_buttons, n_cols=4, footer_buttons=cancel_button))
    
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup

def start(update, context) :
    chat_id = update.effective_chat.id
    cur = conn.cursor()
    cur.execute("""
        UPDATE \"stockAccount_stock_account\"
        SET chat_id = %s
        WHERE nick_name = %s
    """, (chat_id, arguments[1]))
    conn.commit()
    cur.close()

    context.bot.send_message(
        chat_id=chat_id,
        text="텔레그램 chat_id 등록이 완료되었습니다."
    )

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
        cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day, bot_token1, bot_token2, chat_id from \"stockAccount_stock_account\" where nick_name = '" + arguments[1] + "'")
    else:
        cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day, bot_token1, bot_token2, chat_id from \"stockAccount_stock_account\" where nick_name = '" + nickname + "'")        
    result_two = cur01.fetchone()
    cur01.close()

    acct_no = result_two[0]
    access_token = result_two[1]
    app_key = result_two[2]
    app_secret = result_two[3]
    bot_token1 = result_two[6]
    bot_token2 = result_two[7]
    chat_id = result_two[8] 
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

    account_rtn = {'acct_no':acct_no, 'access_token':access_token, 'app_key':app_key, 'app_secret':app_secret, 'bot_token1':bot_token1, 'bot_token2':bot_token2, 'chat_id':chat_id}

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

# 주식현재가 일자별
def get_kis_daily_chart(
        stock_code: str,
        trade_date: str,
        access_token: str,
        app_key: str,
        app_secret: str,
        market_code: str = "J",           # J:KRX, NX:NXT, UN:통합
        period: str = "D",                # D:최근30거래일, W:최근30주, M:최근30개월
        adjust_price: str = "1",          # 0:수정주가미반영, 1:수정주가반영
        verbose: bool = True              # 출력 제어 옵션
    ):
    url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-price"

    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKST01010400",
        "custtype": "P"
    }

    params = {
        "FID_COND_MRKT_DIV_CODE": market_code,
        "FID_INPUT_ISCD": stock_code,
        "FID_PERIOD_DIV_CODE": period,
        "FID_ORG_ADJ_PRC": adjust_price,
    }

    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    if "output" not in data or not data["output"]:
        if verbose:
            print(f"⛔ 일봉 데이터 없음")
        return None

    df = pd.DataFrame(data["output"])
    if df.empty:
        return None

    # 날짜 필터 (YYYYMMDD)
    day_df = df[df["stck_bsop_date"] == trade_date]

    if day_df.empty:
        if verbose:
            print(f"⛔ {trade_date} 일봉 없음")
        return None

    # trade_date 종가
    return int(day_df.iloc[0]["stck_clpr"])

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

def get_previous_business_day(day):
    cur100 = conn.cursor()
    cur100.execute("select prev_business_day_char(%s)", (day,))
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

def callback_get(update, context) :
    data_selected = update.callback_query.data
    query = update.callback_query

    command = data_selected.split(",")[-1] if "," in data_selected else data_selected

    global menuNum
    global g_order_no
    global g_remain_qty

    print("command : ", command)
    if command == "취소":
        context.bot.edit_message_text(text="취소하였습니다.",
                                      chat_id=query.message.chat_id,
                                      message_id=query.message.message_id)
        return

    elif command == "보유종목":

        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[보유종목 조회]",
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)
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

            if result_msgs:
                # 메시지를 10개씩 묶어서 보냅니다 (원하는 개수로 조정 가능)
                chunk_size = 10
                chunks = [result_msgs[i:i + chunk_size] for i in range(0, len(result_msgs), chunk_size)]

                for idx, chunk in enumerate(chunks):
                    final_message = "\n\n".join(chunk) # 가독성을 위해 두 줄 바꿈 사용
                    
                    if idx == 0:
                        # 첫 번째 묶음은 기존 메뉴 메시지를 수정해서 출력
                        context.bot.edit_message_text(
                            text=final_message,
                            parse_mode='HTML',
                            chat_id=query.message.chat_id,
                            message_id=query.message.message_id
                        )
                    else:
                        # 두 번째 묶음부터는 새로운 메시지로 전송
                        context.bot.send_message(
                            text=final_message,
                            parse_mode='HTML',
                            chat_id=query.message.chat_id
                        )
            else:
                context.bot.edit_message_text(
                    text="보유종목 조회 대상이 존재하지 않습니다.",
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )  

        except Exception as e:
            print('보유종목 조회 오류.', e)
            context.bot.edit_message_text(text="[보유종목 조회] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)
                
    elif command == "전체주문":

        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[일별주문체결 조회]",
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

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

                if result_msgs:
                    # 메시지를 10개씩 묶어서 보냅니다 (원하는 개수로 조정 가능)
                    chunk_size = 10
                    chunks = [result_msgs[i:i + chunk_size] for i in range(0, len(result_msgs), chunk_size)]

                    for idx, chunk in enumerate(chunks):
                        final_message = "\n\n".join(chunk) # 가독성을 위해 두 줄 바꿈 사용
                        
                        if idx == 0:
                            # 첫 번째 묶음은 기존 메뉴 메시지를 수정해서 출력
                            context.bot.edit_message_text(
                                text=final_message,
                                parse_mode='HTML',
                                chat_id=query.message.chat_id,
                                message_id=query.message.message_id
                            )
                        else:
                            # 두 번째 묶음부터는 새로운 메시지로 전송
                            context.bot.send_message(
                                text=final_message,
                                parse_mode='HTML',
                                chat_id=query.message.chat_id
                            )
                else:
                    context.bot.edit_message_text(
                        text="일별주문체결 조회 대상이 존재하지 않습니다.",
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id
                    )                                

            else:
                context.bot.send_message(text="일별주문체결 조회 미존재 : " + g_company,
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

        except Exception as e:
            print('일별주문체결 조회 오류.', e)
            context.bot.edit_message_text(text="[일별주문체결 조회] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

    elif command == "전체예약":
    
        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[전체예약 조회]",
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

            
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

                if result_msgs:
                    # 메시지를 10개씩 묶어서 보냅니다 (원하는 개수로 조정 가능)
                    chunk_size = 10
                    chunks = [result_msgs[i:i + chunk_size] for i in range(0, len(result_msgs), chunk_size)]

                    for idx, chunk in enumerate(chunks):
                        final_message = "\n\n".join(chunk) # 가독성을 위해 두 줄 바꿈 사용
                        
                        if idx == 0:
                            # 첫 번째 묶음은 기존 메뉴 메시지를 수정해서 출력
                            context.bot.edit_message_text(
                                text=final_message,
                                parse_mode='HTML',
                                chat_id=query.message.chat_id,
                                message_id=query.message.message_id
                            )
                        else:
                            # 두 번째 묶음부터는 새로운 메시지로 전송
                            context.bot.send_message(
                                text=final_message,
                                parse_mode='HTML',
                                chat_id=query.message.chat_id
                            )
                else:
                    context.bot.edit_message_text(
                        text="전체예약 조회 대상이 존재하지 않습니다.",
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id
                    )                

            else:
                context.bot.send_message(text="전체예약 조회 미존재 : " + g_company,
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

        except Exception as e:
            print('전체예약 조회 오류.', e)
            context.bot.edit_message_text(text="[전체예약 조회] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

    elif command == "예약주문":
        menuNum = "61"

        context.bot.edit_message_text(text="예약주문할 종목코드(종목명), 매매구분(매수:1 매도:2), 단가(시장가:0), 수량, 예약종료일-8자리(YYYYMMDD)를 입력하세요.",
                                        chat_id=query.message.chat_id,
                                        message_id=query.message.message_id)
    
    elif command == "예약정정":
        menuNum = "62"

        context.bot.edit_message_text(text="예약정정할 종목코드(종목명), 예약주문번호, 정정가(시장가:0), 예약종료일-8자리(YYYYMMDD)를 입력하세요.",
                                        chat_id=query.message.chat_id,
                                        message_id=query.message.message_id)
    
    elif command == "예약철회":
        menuNum = "63"

        context.bot.edit_message_text(text="예약취소할 종목코드(종목명), 예약주문번호를 입력하세요.",
                                        chat_id=query.message.chat_id,
                                        message_id=query.message.message_id)
        
    elif command == "매수등록":
        menuNum = "71"

        context.bot.edit_message_text(text="매수등록할 종목코드(종목명), 날짜(8자리-현재일자:0), 시간(6자리-현재시간:0), 매수가(시장가:0), 이탈가, 매수금액, 대상(단독:1)을 입력하세요.",
                                        chat_id=query.message.chat_id,
                                        message_id=query.message.message_id)

    elif command == "매도등록":
        menuNum = "81"

        context.bot.edit_message_text(text="매도등록할 종목코드(종목명), 날짜(8자리-현재일자:0), 시간(6자리-현재시간:0), 매도가(시장가:0), 비중(%), 대상(단독:1)을 입력하세요.",
                                        chat_id=query.message.chat_id,
                                        message_id=query.message.message_id)        
    
    elif command == "매도추적":
        query.edit_message_text(
            text="📅 매도 추적 시작일을 선택하세요",
            reply_markup=build_date_buttons1(38)  # 최근 38일
        )

    elif command.startswith("sell_trace_date:"):
        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[매도추적 등록]",
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)
            
            business_day = command.split(":")[1]
            trail_day = post_business_day_char(business_day)
            prev_date = get_previous_business_day((datetime.strptime(business_day, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"))
            result_msgs = []

            # 계좌잔고 조회
            c = stock_balance(access_token, app_key, app_secret, acct_no, "")
            
            cur199 = conn.cursor()
            balance_rows = []
            
            #  일별 매매 잔고 현행화
            for i in range(len(c)):
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
                        buy_qty,
                        sell_qty,
                        mod_dt
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (acct_no, code, balance_day)
                    DO UPDATE SET
                        balance_price = EXCLUDED.balance_price,
                        balance_qty   = EXCLUDED.balance_qty,
                        balance_amt   = EXCLUDED.balance_amt,
                        value_rate    = EXCLUDED.value_rate,
                        value_amt     = EXCLUDED.value_amt,
                        buy_qty       = EXCLUDED.buy_qty,
                        sell_qty      = EXCLUDED.sell_qty,
                        mod_dt        = EXCLUDED.mod_dt;
                """
                record_to_insert199 = (
                    acct_no,
                    c['pdno'][i],
                    c['prdt_name'][i],
                    business_day.replace('-', ''),
                    float(c['pchs_avg_pric'][i]),
                    int(c['hldg_qty'][i]),
                    int(c['pchs_amt'][i]) if int(c['hldg_qty'][i]) > 0 else 0,
                    float(c['evlu_pfls_rt'][i]) if int(c['hldg_qty'][i]) > 0 else 0,
                    int(c['evlu_pfls_amt'][i]) if int(c['hldg_qty'][i]) > 0 else 0,
                    int(c['thdt_buyqty'][i]) if int(c['thdt_buyqty'][i]) > 0 else 0,
                    int(c['thdt_sll_qty'][i]) if int(c['thdt_sll_qty'][i]) > 0 else 0,
                    datetime.now()
                )
                cur199.execute(insert_query199, record_to_insert199)
                conn.commit()

                if int(c['hldg_qty'][i]) > 0:
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
                            COALESCE(B.basic_price, A.buy_price) AS buy_price,
                            COALESCE(B.basic_qty, A.buy_qty) AS buy_qty,
                            COALESCE(B.stop_price, A.loss_price) AS loss_price,
                            COALESCE(B.target_price, A.profit_price) AS profit_price,
                            A.proc_yn,
                            ROW_NUMBER() OVER (
                                PARTITION BY A.acct_no, A.code
                                ORDER BY A.trade_day DESC, A.trade_dtm DESC, A.crt_dt DESC
                            ) AS rn
                        FROM trading_simulation A
                        LEFT JOIN trading_trail B
                            ON B.acct_no = A.acct_no
                            AND B.code = A.code
                            AND B.trail_day = '{prev_date}'
                            AND B.trail_dtm = CASE WHEN A.trade_day = '{prev_date}' THEN A.trade_dtm ELSE '090000' END
                            AND B.trail_tp IN ('1','2','3','L')
                        WHERE A.trade_tp = '1'
                        AND A.acct_no = {acct_no}
                        AND A.proc_yn IN ('N','C','L')
                        AND SUBSTR(COALESCE(A.proc_dtm,'{prev_date}'), 1, 8) < '{trail_day}'
                        AND A.trade_day <= replace('{business_day}', '-', '')
                    ) t
                    WHERE rn = 1
                )
                """

                insert_query = f"""
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
                )
                SELECT
                    COALESCE(BAL.acct_no, S.acct_no) AS acct_no,
                    COALESCE(BAL.name, S.name) AS name,
                    COALESCE(BAL.code, S.code) AS code,
                    '{trail_day}' AS trail_day,
                    CASE WHEN S.trade_day = '{trail_day}' THEN S.trade_dtm ELSE '090000' END AS trail_dtm,
                    CASE WHEN BAL.acct_no IS NOT NULL AND S.acct_no IS NULL THEN 'L' WHEN S.proc_yn = 'L' THEN 'L' ELSE '1' END AS trail_tp,
                    CASE WHEN COALESCE(BAL.purchase_qty, 0) > 0 THEN BAL.purchase_price ELSE S.buy_price END AS basic_price,
                    CASE WHEN COALESCE(BAL.purchase_qty, 0) > 0 THEN BAL.purchase_qty ELSE S.buy_qty END AS basic_qty,
                    CASE WHEN COALESCE(BAL.purchase_qty, 0) > 0 THEN BAL.purchase_price*BAL.purchase_qty ELSE S.buy_price*S.buy_qty END AS basic_amt,
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
                    AND T.trail_dtm >= CASE WHEN S.trade_day = '{trail_day}' THEN S.trade_dtm ELSE '090000' END
                );
                """

                cur200 = conn.cursor()
                full_query = balance_sql + insert_query

                execute_values(
                    cur200,
                    full_query,
                    balance_rows,
                    template="(%s, %s, %s, %s, %s)"
                )

                countProc = cur200.rowcount

                conn.commit()
                cur200.close()

                if countProc >= 1:
                    msg = f"* [{trail_day}]-매도추적 등록 {countProc}건 처리"
                    result_msgs.append(msg)
                else:
                    msg = f"* [{trail_day}]-매도추적 등록 미처리"
                    result_msgs.append(msg)
            
            else:

                balance_sql = f"""
                WITH sim AS (
                    SELECT *
                    FROM (
                        SELECT
                            A.acct_no,
                            A.name,
                            A.code,
                            A.trade_day,
                            A.trade_dtm,
                            COALESCE(B.basic_price, A.buy_price) AS buy_price,
                            COALESCE(B.basic_qty, A.buy_qty) AS buy_qty,
                            COALESCE(B.stop_price, A.loss_price) AS loss_price,
                            COALESCE(B.target_price, A.profit_price) AS profit_price,
                            A.proc_yn,
                            ROW_NUMBER() OVER (
                                PARTITION BY A.acct_no, A.code
                                ORDER BY A.trade_day DESC, A.trade_dtm DESC, A.crt_dt DESC
                            ) AS rn
                        FROM trading_simulation A
                        LEFT JOIN trading_trail B
                            ON B.acct_no = A.acct_no
                            AND B.code = A.code
                            AND B.trail_day = '{prev_date}'
                            AND B.trail_dtm = CASE WHEN A.trade_day = '{prev_date}' THEN A.trade_dtm ELSE '090000' END
                            AND B.trail_tp IN ('1','2','3','L')
                        WHERE A.trade_tp = '1'
                        AND A.acct_no = {acct_no}
                        AND A.proc_yn IN ('N','C','L')
                        AND SUBSTR(COALESCE(A.proc_dtm,'{prev_date}'), 1, 8) < '{trail_day}'
                        AND A.trade_day <= replace('{business_day}', '-', '')
                    ) t
                    WHERE rn = 1
                )
                """

                insert_query = f"""
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
                )
                SELECT
                    S.acct_no,
                    S.name,
                    S.code,
                    '{trail_day}' AS trail_day,
                    CASE WHEN S.trade_day = '{trail_day}' THEN S.trade_dtm ELSE '090000' END AS trail_dtm,
                    CASE WHEN S.proc_yn = 'L' THEN 'L' ELSE '1' END AS trail_tp,
                    S.buy_price,
                    S.buy_qty,
                    S.buy_price*S.buy_qty AS basic_amt,
                    COALESCE(S.loss_price, 0) AS stop_price,
                    COALESCE(S.profit_price, 0) AS target_price,
                    CASE WHEN S.trade_day = '{trail_day}' THEN S.trade_dtm ELSE '090000' END AS proc_min,
                    now(),
                    now()
                FROM sim S
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM trading_trail T
                    WHERE T.acct_no = S.acct_no
                    AND T.code = S.code
                    AND T.trail_day = '{trail_day}'
                    AND T.trail_dtm = CASE WHEN S.trade_day = '{trail_day}' THEN S.trade_dtm ELSE '090000' END
                );
                """

                cur200 = conn.cursor()
                full_query = balance_sql + insert_query
                cur200.execute(full_query)

                countProc = cur200.rowcount

                conn.commit()
                cur200.close()

                if countProc >= 1:
                    msg = f"* [{trail_day}]-매도추적 등록 {countProc}건 처리"
                    result_msgs.append(msg)
                else:
                    msg = f"* [{trail_day}]-매도추적 등록 미처리"
                    result_msgs.append(msg)

            final_message = "\n".join(result_msgs) if result_msgs else "매도추적 등록 대상이 존재하지 않습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=query.message.chat_id,
                message_id=query.message.message_id
            )

        except Exception as e:
            print('매도추적 등록 오류.', e)
            context.bot.edit_message_text(text="[매도추적 등록] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)   

    elif command == "추적삭제":
        query.edit_message_text(
            text="📅 추적 삭제 시작일을 선택하세요",
            reply_markup=build_date_buttons2(38)  # 최근 38일
        )
            
    elif command.startswith("trace_delete_date:"):            
        ac = account()
        acct_no = ac['acct_no']

        try:
            context.bot.edit_message_text(text="[추적삭제]",
                                chat_id=query.message.chat_id,
                                message_id=query.message.message_id)
            
            business_day = command.split(":")[1]
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
                msg = f"* [{trail_day}]-추적 삭제 {countProc}건 처리"
                result_msgs.append(msg)
            else:
                msg = f"* [{trail_day}]-추적 삭제 미처리"
                result_msgs.append(msg)

            final_message = "\n".join(result_msgs) if result_msgs else "추적 삭제 대상이 존재하지 않습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=query.message.chat_id,
                message_id=query.message.message_id
            )                

        except Exception as e:
            print('추적 삭제 오류.', e)
            context.bot.edit_message_text(text="[추적 삭제] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)          

    elif command == "매매신호":
        query.edit_message_text(
            text="📅 매매 신호 시작일을 선택하세요",
            reply_markup=build_date_buttons3(38)  # 최근 38일
        )
            
    elif command.startswith("trading_signal_date:"):            
        ac = account()
        acct_no = ac['acct_no']

        try:
            context.bot.edit_message_text(text="[매매신호]",
                                chat_id=query.message.chat_id,
                                message_id=query.message.message_id)
            
            business_day = command.split(":")[1]
            trade_day = post_business_day_char(business_day)
            result_msgs = []
        
            # 매매신호 select
            cur200 = conn.cursor()
            select_query = """
                SELECT code, name, trade_day, trade_dtm, case when trade_tp = '1' then '매수' else '매도' end as trade_tp, buy_price, buy_qty, buy_amt, sell_price, sell_qty, sell_amt, loss_price, profit_price, proc_yn, proc_dtm FROM trading_simulation WHERE acct_no = %s AND trade_day = %s ORDER BY proc_yn, trade_dtm DESC
                """
            # select 인자값 설정
            cur200.execute(select_query, (acct_no, trade_day))
            result_two00 = cur200.fetchall()
            cur200.close()

            if len(result_two00) > 0:
            
                for row in result_two00:
                    # 각 값이 None이면 0으로, 아니면 원래 값 유지
                    r = [val if val is not None else 0 for val in row]
                    # 각 컬럼을 변수에 할당 (언패킹)
                    (code, name, t_day, t_dtm, t_tp, buy_p, buy_q, buy_a, sell_p, sell_q, sell_a, loss_p, profit_p, p_yn, p_dtm) = r

                    if p_dtm == 0:
                        last_val = "상태: "+p_yn
                    else:
                        last_val = "상태: "+p_yn+" ["+p_dtm[:8]+"-"+p_dtm[8:10]+":"+p_dtm[10:12]+"]"

                    # t_tp(매수/매도 구분) 값에 따라 메시지 구성
                    if t_tp == '매도':
                        msg = (f"[{t_day}-{t_dtm[:2]}:{t_dtm[2:4]}]{name}[<code>{code}</code>] "
                            f"매도가:{sell_p:,}원({sell_q:,}주), 매도금액:{sell_p*sell_q:,}원")
                    else: # '매수'인 경우
                        msg = (f"[{t_day}-{t_dtm[:2]}:{t_dtm[2:4]}]{name}[<code>{code}</code>] "
                            f"매수가:{buy_p:,}원({buy_q:,}주), 매수금액:{buy_p*buy_q:,}원, "
                            f"손절가:{loss_p:,}원, 목표가:{profit_p:,}원, {last_val}")
                        
                    result_msgs.append(msg)

            if result_msgs:
                # 메시지를 10개씩 묶어서 보냅니다 (원하는 개수로 조정 가능)
                chunk_size = 10
                chunks = [result_msgs[i:i + chunk_size] for i in range(0, len(result_msgs), chunk_size)]

                for idx, chunk in enumerate(chunks):
                    final_message = "\n\n".join(chunk) # 가독성을 위해 두 줄 바꿈 사용
                    
                    if idx == 0:
                        # 첫 번째 묶음은 기존 메뉴 메시지를 수정해서 출력
                        context.bot.edit_message_text(
                            text=final_message,
                            parse_mode='HTML',
                            chat_id=query.message.chat_id,
                            message_id=query.message.message_id
                        )
                    else:
                        # 두 번째 묶음부터는 새로운 메시지로 전송
                        context.bot.send_message(
                            text=final_message,
                            parse_mode='HTML',
                            chat_id=query.message.chat_id
                        )
            else:
                context.bot.edit_message_text(
                    text="매매 신호 대상이 존재하지 않습니다.",
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )

        except Exception as e:
            print('매매 신호 오류.', e)
            context.bot.edit_message_text(text="[매매 신호] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)     

    elif command == "매매추적":
        query.edit_message_text(
            text="📅 매매 추적 시작일을 선택하세요",
            reply_markup=build_date_buttons4(38)  # 최근 38일
        )
            
    elif command.startswith("trading_trail_date:"):            
        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[매매추적]",
                                chat_id=query.message.chat_id,
                                message_id=query.message.message_id)
            
            business_day = command.split(":")[1]
            trail_day = post_business_day_char(business_day)
            result_msgs = []
        
            # 매매추적 select
            cur200 = conn.cursor()
            select_query = """
                SELECT code, name, trail_day, trail_dtm, trail_tp, trail_price, trail_qty, trail_amt, trail_rate, basic_price, basic_qty, basic_amt, stop_price, target_price, proc_min FROM trading_trail WHERE acct_no = %s AND trail_day = %s ORDER BY trail_tp, proc_min DESC 
                """
            # select 인자값 설정
            cur200.execute(select_query, (acct_no, trail_day))
            result_two00 = cur200.fetchall()
            cur200.close()

            if len(result_two00) > 0:
            
                for row in result_two00:
                    # 각 값이 None이면 0으로, 아니면 원래 값 유지
                    r = [val if val is not None else 0 for val in row]
                    # 각 컬럼을 변수에 할당 (언패킹)
                    (code, name, trail_day, trail_dtm, trail_tp, 
                    trail_price, trail_qty, trail_amt, trail_rate, basic_price, basic_qty, basic_amt, 
                    stop_price, target_price, proc_min) = r
                    
                    # 일자별 종가
                    stck_prpr = get_kis_daily_chart(
                        stock_code=code,
                        trade_date=trail_day,
                        access_token=access_token,
                        app_key=app_key,
                        app_secret=app_secret
                    )
                    stck_rate = round((100-(stck_prpr/basic_price)*100)*-1,2)   # 수익률
                    trail = ""
                    if trail_price > 0:
                        trail_qty = int(round(trail_amt/trail_price)) if trail_qty == 0 else trail_qty  # 추적수량
                        basic_qty = trail_qty if basic_qty == 0 else basic_qty
                        trail = (f", 추적가:{trail_price:,}원({trail_qty:,}주), 추적율:{trail_rate}%, 추적금액:{trail_amt:,}원")

                    msg = (f"[{trail_day}-{proc_min[:2]}:{proc_min[2:4]}]{name}[<code>{code}</code>] -{trail_tp}- "
                        f"보유가:{basic_price:,}원({basic_qty:,}주), 보유금액:{basic_price*basic_qty:,}원, 현재가:{stck_prpr:,}원, "
                        f"수익율:{str(stck_rate)}%, 손절가:{stop_price:,}원, 목표가:{target_price:,}원{trail}")
                    
                    result_msgs.append(msg)

            if result_msgs:
                # 메시지를 10개씩 묶어서 보냅니다 (원하는 개수로 조정 가능)
                chunk_size = 10
                chunks = [result_msgs[i:i + chunk_size] for i in range(0, len(result_msgs), chunk_size)]

                for idx, chunk in enumerate(chunks):
                    final_message = "\n\n".join(chunk) # 가독성을 위해 두 줄 바꿈 사용
                    
                    if idx == 0:
                        # 첫 번째 묶음은 기존 메뉴 메시지를 수정해서 출력
                        context.bot.edit_message_text(
                            text=final_message,
                            parse_mode='HTML',
                            chat_id=query.message.chat_id,
                            message_id=query.message.message_id
                        )
                    else:
                        # 두 번째 묶음부터는 새로운 메시지로 전송
                        context.bot.send_message(
                            text=final_message,
                            parse_mode='HTML',
                            chat_id=query.message.chat_id
                        )
            else:
                context.bot.edit_message_text(
                    text="매매 추적 대상이 존재하지 않습니다.",
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )
            
        except Exception as e:
            print('매매 추적 오류.', e)
            context.bot.edit_message_text(text="[매매 추적] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)                                                             
            
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
        # 입력메시지가 앞의 1자리가 숫자인 경우,
        if user_text[:1].isdecimal():
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
                
                commandBot = user_text.split(sep=',', maxsplit=7)
                print("commandBot[1] : ", commandBot[1])    # 날짜-8자리(YYYYMMDD, 현재일자:0)
                print("commandBot[2] : ", commandBot[2])    # 시간-6자리(HHMMSS, 현재일시:0)
                print("commandBot[3] : ", commandBot[3])    # 매수가(시장가:0)
                print("commandBot[4] : ", commandBot[4])    # 이탈가
                print("commandBot[5] : ", commandBot[5])    # 매수금액
                print("commandBot[6] : ", commandBot[6])    # 대상(단독:1)

            # 날짜-8자리(YYYYMMDD, 현재일자:0), 시간-6자리(HHMMSS, 현재일시:0), 매수가(시장가:0), 이탈가, 매수금액, 대상(단독:1) 존재시
            if (commandBot[1] == '0' or len(commandBot[1]) == 8) and commandBot[1].isdigit() and (commandBot[2] == '0' or len(commandBot[2]) == 6) and commandBot[2].isdigit() and commandBot[3].isdecimal() and commandBot[4].isdecimal() and commandBot[5].isdecimal() and commandBot[6].isdecimal():
                year_day = datetime.now().strftime("%Y%m%d") if commandBot[1] == '0' else commandBot[1]     # 날짜-8자리(YYYYMMDD, 현재일자:0)
                hour_minute = datetime.now().strftime('%H%M%S') if commandBot[2] == '0' else commandBot[2]  # 시간-6자리(HHMMSS, 현재일시:0)
                buy_price = int(stck_prpr) if commandBot[3] == '0' else int(commandBot[3])                  # 매수가(시장가:0)
                loss_price = int(commandBot[4])                         # 이탈가
                buy_amt = int(commandBot[5])                            # 매수금액
                buy_qty = int(round(buy_amt/buy_price))                 # 매수량
                
                safe_margin_price = int(buy_price + buy_price * 0.05)   # 안전마진가(매수가 대비 5%)

                nickname_list = ['yh480825', 'mamalong']
                target_nicks = nickname_list if int(commandBot[6]) > 1 else [None]
                for nick in target_nicks:
                    # 다수 계좌일 경우에만 정보를 새로 가져옴 (nick이 None이 아닐 때)
                    if nick is not None:
                        ac = account(nick)
                        acct_no = ac['acct_no']
                        access_token = ac['access_token']
                        app_key = ac['app_key']
                        app_secret = ac['app_secret']
                
                    # 계좌잔고 조회
                    c = stock_balance(access_token, app_key, app_secret, acct_no, "")
                
                    ord_psbl_qty = 0
                    hold_price = 0
                    hldg_qty = 0
                    hold_amt = 0

                    for i, name in enumerate(c.index):
                        if code == c['pdno'][i]:
                            ord_psbl_qty = int(c['ord_psbl_qty'][i])
                            hold_price = float(c['pchs_avg_pric'][i])
                            hldg_qty = int(c['hldg_qty'][i])
                            hold_amt = int(c['pchs_amt'][i])
                            
                    # 매매추적 보유가, 보유수량, 보유금액 조회
                    cur300 = conn.cursor()
                    select_query = """
                        SELECT 
                            basic_price,
                            basic_qty,
                            basic_amt
                        FROM (
                            SELECT
                                COALESCE(basic_price, 0) AS basic_price,
                                COALESCE(basic_qty, 0) AS basic_qty,
                                COALESCE(basic_amt, 0) AS basic_amt,
                                row_number() OVER (
                                    PARTITION BY acct_no, code
                                    ORDER BY trail_dtm DESC
                                ) AS rn
                            FROM public.trading_trail
                            WHERE acct_no = %s
                            AND trail_tp IN ('1', '2', '3', 'L')
                            AND trail_day = %s
                            AND code = %s
                        ) T
                        WHERE rn = 1
                        """
                    cur300.execute(select_query, (acct_no, year_day, code))
                    row = cur300.fetchone()
                    cur300.close()

                    basic_price = int(row[0]) if row else 0
                    basic_qty = int(row[1]) if row else 0
                    basic_amt = int(row[2]) if row else 0
                    # 보유가
                    base_price = hold_price if hold_price > 0 else basic_price
                    # 보유량
                    base_qty = hldg_qty if hldg_qty > 0 else basic_qty
                    # 보유금액
                    base_amt = hold_amt if hold_amt > 0 else basic_amt
                    # 총보유량
                    sum_base_qty = base_qty + buy_qty
                    # 평균보유가
                    avg_base_price = int(round((base_amt + buy_amt) / sum_base_qty))

                    # 매매추적 update 및 insert
                    cur400 = conn.cursor()
                    merge_query = """
                        WITH upd AS (
                            UPDATE trading_trail tt
                            SET
                                trail_dtm = %s,
                                trail_tp = %s,
                                stop_price = %s,
                                target_price = %s,
                                basic_price = %s,
                                basic_qty = %s,
                                basic_amt = %s,
                                proc_min = %s,
                                mod_dt = %s
                            FROM (
                                SELECT
                                    acct_no,
                                    code,
                                    trail_day,
                                    row_number() OVER (
                                        PARTITION BY acct_no, code
                                        ORDER BY trail_dtm DESC
                                    ) AS rn
                                FROM public.trading_trail
                                WHERE acct_no = %s
                                AND code = %s
                                AND trail_day = %s
                                AND trail_tp IN ('1', '2', '3', 'L')
                            ) sub
                            WHERE tt.acct_no  = sub.acct_no
                            AND tt.code     = sub.code
                            AND tt.trail_day = sub.trail_day
                            AND sub.rn = 1
                            RETURNING 1 AS flag
                        ),
                        ins AS (
                            INSERT INTO trading_trail (
                                acct_no,
                                code,
                                name,
                                trail_day,
                                trail_dtm,
                                trail_tp,
                                stop_price,
                                target_price,
                                basic_price,
                                basic_qty,
                                basic_amt,
                                proc_min,
                                crt_dt,
                                mod_dt
                            )
                            SELECT
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            WHERE NOT EXISTS (SELECT 1 FROM upd)
                            RETURNING 1 AS flag
                        )
                        SELECT flag FROM upd
                        UNION ALL 
                        SELECT flag FROM ins;
                        """
                    # merge 인자값 설정
                    cur400.execute(merge_query, (
                            hour_minute, "1", loss_price, safe_margin_price, avg_base_price, sum_base_qty, avg_base_price * sum_base_qty, hour_minute, datetime.now(), acct_no, code, year_day,
                            acct_no, code, company, year_day, hour_minute, "1", loss_price, safe_margin_price, avg_base_price if base_price > 0 else buy_price, sum_base_qty if base_qty > 0 else buy_qty, avg_base_price*sum_base_qty if base_qty > 0 else buy_price*buy_qty, hour_minute, datetime.now(), datetime.now()
                    ))

                    was_updated = cur400.fetchone() is not None

                    conn.commit()
                    cur400.close()

                    if was_updated:
                        context.bot.send_message(chat_id=user_id, text="[" + company + "{<code>"+code+"</code>}] 평균보유가 : " + format(avg_base_price if base_price > 0 else buy_price, ',d') + "원, 총보유량 : " + format(sum_base_qty if base_qty > 0 else buy_qty, ',d') + "주, 총보유금액 : " + format(avg_base_price*sum_base_qty if base_qty > 0 else buy_price*buy_qty, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 안전마진가 : " + format(safe_margin_price, ',d') + "원 매매추적 처리", parse_mode='HTML')
                    else:
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가 : " + format(buy_price, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 안전마진가 : " + format(safe_margin_price, ',d') + "원, 매수량 : " + format(buy_qty, ',d') + "주, 매수금액 : " + format(buy_price*buy_qty, ',d') + "원 매매추적 미처리")       

                    # 매매시뮬레이션 insert
                    cur500 = conn.cursor()
                    insert_query = """
                        INSERT INTO trading_simulation (
                            acct_no, name, code, trade_day, trade_dtm, trade_tp, buy_price, buy_qty, buy_amt, loss_price, profit_price, proc_yn, crt_dt, mod_dt
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (acct_no, code, trade_day, trade_dtm, trade_tp)
                        DO NOTHING
                        RETURNING 1;
                        """
                    # insert 인자값 설정
                    cur500.execute(insert_query, (
                        acct_no, company, code, year_day, hour_minute, "1", buy_price, buy_qty, buy_price*buy_qty, loss_price, safe_margin_price, 'N', datetime.now(), datetime.now()
                    ))

                    was_inserted = cur500.fetchone() is not None

                    conn.commit()
                    cur500.close()

                    if was_inserted:
                        context.bot.send_message(chat_id=user_id, text="[" + company + "{<code>"+code+"</code>}] 매수가 : " + format(buy_price, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 안전마진가 : " + format(safe_margin_price, ',d') + "원, 매수량 : " + format(buy_qty, ',d') + "주, 매수금액 : " + format(buy_price*buy_qty, ',d') + "원 매수등록", parse_mode='HTML')
                    else:
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가 : " + format(buy_price, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 안전마진가 : " + format(safe_margin_price, ',d') + "원, 매수량 : " + format(buy_qty, ',d') + "주, 매수금액 : " + format(buy_price*buy_qty, ',d') + "원 매수등록 미처리")

            else:
                print("날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매수가(시장가:0), 이탈가, 매수금액, 대상(단독:1) 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매수가(시장가:0), 이탈가, 매수금액, 대상(단독:1) 미존재 또는 부적합")         

        elif menuNum == '81':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=6)
                print("commandBot[1] : ", commandBot[1])    # 날짜-8자리(YYYYMMDD, 현재일자:0)
                print("commandBot[2] : ", commandBot[2])    # 시간-6자리(HHMMSS, 현재일시:0)
                print("commandBot[3] : ", commandBot[3])    # 매도가(시장가:0)
                print("commandBot[4] : ", commandBot[4])    # 비중(%)
                print("commandBot[5] : ", commandBot[5])    # 대상(단독:1)

            # 날짜-8자리(YYYYMMDD, 현재일자:0), 시간-6자리(HHMMSS, 현재일시:0), 매도가(시장가:0), 비중(%), 대상(단독:1) 존재시
            if (commandBot[1] == '0' or len(commandBot[1]) == 8) and commandBot[1].isdigit() and (commandBot[2] == '0' or len(commandBot[2]) == 6) and commandBot[2].isdigit() and commandBot[3].isdecimal() and is_positive_int(commandBot[4]) and commandBot[5].isdecimal():
                year_day = datetime.now().strftime("%Y%m%d") if commandBot[1] == '0' else commandBot[1]     # 날짜-8자리(YYYYMMDD, 현재일자:0)
                hour_minute = datetime.now().strftime('%H%M%S') if commandBot[2] == '0' else commandBot[2]  # 시간-6자리(HHMMSS, 현재일시:0)
                sell_price = int(stck_prpr) if commandBot[3] == '0' else int(commandBot[3])                 # 매도가(시장가:0)
                sell_rate = int(commandBot[4])                          # 비중(%)

                nickname_list = ['yh480825', 'mamalong']
                target_nicks = nickname_list if int(commandBot[5]) > 1 else [None]
                for nick in target_nicks:
                    # 다수 계좌일 경우에만 정보를 새로 가져옴 (nick이 None이 아닐 때)
                    if nick is not None:
                        ac = account(nick)
                        acct_no = ac['acct_no']
                        access_token = ac['access_token']
                        app_key = ac['app_key']
                        app_secret = ac['app_secret']

                    # 계좌잔고 조회
                    c = stock_balance(access_token, app_key, app_secret, acct_no, "")
                
                    hldg_qty = 0

                    for i, name in enumerate(c.index):
                        if code == c['pdno'][i]:
                            hldg_qty = int(c['hldg_qty'][i])

                    # 매매추적 보유가, 보유수량, 추적유형 조회
                    cur300 = conn.cursor()
                    select_query = """
                        SELECT 
                            basic_price,
                            basic_qty,
                            trail_tp
                        FROM (
                            SELECT
                                COALESCE(basic_price, 0) AS basic_price,
                                COALESCE(basic_qty, 0) AS basic_qty,
                                trail_tp, 
                                row_number() OVER (
                                    PARTITION BY acct_no, code
                                    ORDER BY trail_dtm DESC
                                ) AS rn
                            FROM public.trading_trail
                            WHERE acct_no = %s
                            AND trail_tp IN ('1', '2', '3', 'L')
                            AND trail_day = %s
                            AND code = %s
                        ) T
                        WHERE rn = 1
                        """
                    cur300.execute(select_query, (acct_no, year_day, code))
                    row = cur300.fetchone()
                    cur300.close()

                    basic_price = int(row[0]) if row else 0
                    basic_qty = int(row[1]) if row else 0
                    prev_trail_tp = row[2] if row else "1"
                    # 보유량
                    base_qty = hldg_qty if hldg_qty > 0 else basic_qty
                    # 매도량
                    sell_qty = int(base_qty * sell_rate * 0.01)

                    if sell_qty > 0:

                        try:
                            with conn.cursor() as cur:
                                # 매매추적 update
                                update_query1 = """
                                    UPDATE trading_trail tt SET
                                        trail_dtm = %s, trail_tp = %s, trail_plan = %s, stop_price = %s, target_price = %s, proc_min = %s, mod_dt = %s
                                    FROM (
                                        SELECT
                                            acct_no,
                                            code,
                                            trail_day,
                                            row_number() OVER (
                                                PARTITION BY acct_no, code
                                                ORDER BY trail_dtm DESC
                                            ) AS rn
                                        FROM public.trading_trail
                                        WHERE acct_no = %s
                                        AND code = %s
                                        AND trail_day = %s
                                        AND trail_tp IN ('1', '2', '3', 'L')
                                    ) sub
                                    WHERE tt.acct_no = sub.acct_no
                                    AND tt.code = sub.code
                                    AND tt.trail_day = sub.trail_day
                                    AND sub.rn = 1
                                    RETURNING 1;
                                    """
                                # update 인자값 설정
                                cur.execute(update_query1, (hour_minute, "2", str(sell_rate), int(stck_lwpr), sell_price, hour_minute, datetime.now(), acct_no, code, year_day))

                                was_updated1 = cur.fetchone() is not None

                                # 매매시뮬레이션 update
                                update_query2 = """
                                    UPDATE public.trading_simulation SET 
                                        loss_price = %s
                                        , profit_price = %s
                                        , proc_dtm = %s 
                                        , proc_yn = %s      
                                        , mod_dt = %s
                                    WHERE acct_no = %s
                                    AND code = %s
                                    AND trade_tp = %s
                                    AND trade_day <= %s
                                    AND proc_yn != 'Y'
                                    RETURNING 1;
                                    """
                                # update 인자값 설정
                                cur.execute(update_query2, (int(stck_lwpr), sell_price, datetime.now().strftime("%Y%m%d%H%M"), "C", datetime.now(), acct_no, code, "1", year_day))
                                
                                was_updated2 = cur.fetchone() is not None

                                if was_updated1 and was_updated2:
                                    conn.commit()
                                    context.bot.send_message(chat_id=user_id, text="[" + company + "{<code>"+code+"</code>}] 저가 : " + format(int(stck_lwpr), ',d') + "원, 보유가 : " + format(basic_price, ',d') + "원, 보유량 : " + format(base_qty, ',d') + "주, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 매매추적 처리", parse_mode='HTML')

                                    # 매매시뮬레이션 insert
                                    insert_query = """
                                        INSERT INTO trading_simulation (
                                            acct_no, name, code, trade_day, trade_dtm, trade_tp, sell_price, sell_qty, trading_plan, proc_yn, crt_dt, mod_dt 
                                        )
                                        VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                        )
                                        ON CONFLICT (acct_no, code, trade_day, trade_dtm, trade_tp)
                                        DO NOTHING
                                        RETURNING 1;
                                        """
                                    # insert 인자값 설정
                                    cur.execute(insert_query, (
                                        acct_no, company, code, year_day, hour_minute, "2", sell_price,  sell_qty, str(sell_rate), 'N', datetime.now(), datetime.now()
                                    ))

                                    was_inserted = cur.fetchone() is not None
                                    conn.commit()

                                    if was_inserted:
                                        context.bot.send_message(chat_id=user_id, text="[" + company + "{<code>"+code+"</code>}] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 매도등록", parse_mode='HTML')
                                    else:
                                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 매도등록 미처리")

                                else:
                                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 매매추적 미처리")   

                        except Exception as e:
                            conn.rollback()
                            print(f"Error 발생: {e}")
                            context.bot.send_message(chat_id=user_id, text=f"처리 중 오류가 발생했습니다: {e}")                            

                    else:
                        context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도량 부족 미처리")                                

            else:
                print("날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매도가(시장가:0), 비중(%), 대상(단독:1) 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매도가(시장가:0), 비중(%), 대상(단독:1) 미존재 또는 부적합")                         

# CLI 모드 실행 함수
def cli_run(menu, user_input):
    bot = updater.bot
    chat_id = account()['chat_id']

    # 종목명 추출 및 종목코드 조회
    name_text = user_input.split(',')[0].strip()
    if len(stock_code[stock_code.company == name_text].values) > 0:
        code = stock_code[stock_code.company == name_text].code.values[0].strip()
        company = stock_code[stock_code.company == name_text].company.values[0].strip()
    elif len(name_text) >= 6 and name_text[:6].isdecimal() and len(stock_code[stock_code.code == name_text[:6]].values) > 0:
        code = stock_code[stock_code.code == name_text[:6]].code.values[0].strip()
        company = stock_code[stock_code.code == name_text[:6]].company.values[0].strip()
    else:
        print(f"{name_text} : 미존재 종목")
        return

    # 계정정보 및 현재가 조회
    ac = account()
    acct_no = ac['acct_no']
    access_token = ac['access_token']
    app_key = ac['app_key']
    app_secret = ac['app_secret']

    a = inquire_price(access_token, app_key, app_secret, code)
    stck_prpr = a['stck_prpr']
    stck_lwpr = a['stck_lwpr']

    if menu == '매수등록':
        commandBot = user_input.split(sep=',', maxsplit=7)
        print("commandBot[1] : ", commandBot[1])    # 날짜-8자리(YYYYMMDD, 현재일자:0)
        print("commandBot[2] : ", commandBot[2])    # 시간-6자리(HHMMSS, 현재일시:0)
        print("commandBot[3] : ", commandBot[3])    # 매수가(시장가:0)
        print("commandBot[4] : ", commandBot[4])    # 이탈가
        print("commandBot[5] : ", commandBot[5])    # 매수금액
        print("commandBot[6] : ", commandBot[6])    # 대상(단독:1)

        if (commandBot[1] == '0' or len(commandBot[1]) == 8) and commandBot[1].isdigit() and (commandBot[2] == '0' or len(commandBot[2]) == 6) and commandBot[2].isdigit() and commandBot[3].isdecimal() and commandBot[4].isdecimal() and commandBot[5].isdecimal() and commandBot[6].isdecimal():
            year_day = datetime.now().strftime("%Y%m%d") if commandBot[1] == '0' else commandBot[1]
            hour_minute = datetime.now().strftime('%H%M%S') if commandBot[2] == '0' else commandBot[2]
            buy_price = int(stck_prpr) if commandBot[3] == '0' else int(commandBot[3])
            loss_price = int(commandBot[4])
            buy_amt = int(commandBot[5])
            buy_qty = int(round(buy_amt/buy_price))

            safe_margin_price = int(buy_price + buy_price * 0.05)

            nickname_list = ['yh480825', 'mamalong']
            target_nicks = nickname_list if int(commandBot[6]) > 1 else [None]
            for nick in target_nicks:
                if nick is not None:
                    ac = account(nick)
                    acct_no = ac['acct_no']
                    access_token = ac['access_token']
                    app_key = ac['app_key']
                    app_secret = ac['app_secret']

                # 계좌잔고 조회
                c = stock_balance(access_token, app_key, app_secret, acct_no, "")

                ord_psbl_qty = 0
                hold_price = 0
                hldg_qty = 0
                hold_amt = 0

                for i, name in enumerate(c.index):
                    if code == c['pdno'][i]:
                        ord_psbl_qty = int(c['ord_psbl_qty'][i])
                        hold_price = float(c['pchs_avg_pric'][i])
                        hldg_qty = int(c['hldg_qty'][i])
                        hold_amt = int(c['pchs_amt'][i])

                # 매매추적 보유가, 보유수량, 보유금액 조회
                cur300 = conn.cursor()
                select_query = """
                    SELECT
                        basic_price,
                        basic_qty,
                        basic_amt
                    FROM (
                        SELECT
                            COALESCE(basic_price, 0) AS basic_price,
                            COALESCE(basic_qty, 0) AS basic_qty,
                            COALESCE(basic_amt, 0) AS basic_amt,
                            row_number() OVER (
                                PARTITION BY acct_no, code
                                ORDER BY trail_dtm DESC
                            ) AS rn
                        FROM public.trading_trail
                        WHERE acct_no = %s
                        AND trail_tp IN ('1', '2', '3', 'L')
                        AND trail_day = %s
                        AND code = %s
                    ) T
                    WHERE rn = 1
                    """
                cur300.execute(select_query, (acct_no, year_day, code))
                row = cur300.fetchone()
                cur300.close()

                basic_price = int(row[0]) if row else 0
                basic_qty = int(row[1]) if row else 0
                basic_amt = int(row[2]) if row else 0
                base_price = hold_price if hold_price > 0 else basic_price
                base_qty = hldg_qty if hldg_qty > 0 else basic_qty
                base_amt = hold_amt if hold_amt > 0 else basic_amt
                sum_base_qty = base_qty + buy_qty
                avg_base_price = int(round((base_amt + buy_amt) / sum_base_qty))

                # 매매추적 update 및 insert
                cur400 = conn.cursor()
                merge_query = """
                    WITH upd AS (
                        UPDATE trading_trail tt
                        SET
                            trail_dtm = %s,
                            trail_tp = %s,
                            stop_price = %s,
                            target_price = %s,
                            basic_price = %s,
                            basic_qty = %s,
                            basic_amt = %s,
                            proc_min = %s,
                            mod_dt = %s
                        FROM (
                            SELECT
                                acct_no,
                                code,
                                trail_day,
                                row_number() OVER (
                                    PARTITION BY acct_no, code
                                    ORDER BY trail_dtm DESC
                                ) AS rn
                            FROM public.trading_trail
                            WHERE acct_no = %s
                            AND code = %s
                            AND trail_day = %s
                            AND trail_tp IN ('1', '2', '3', 'L')
                        ) sub
                        WHERE tt.acct_no  = sub.acct_no
                        AND tt.code     = sub.code
                        AND tt.trail_day = sub.trail_day
                        AND sub.rn = 1
                        RETURNING 1 AS flag
                    ),
                    ins AS (
                        INSERT INTO trading_trail (
                            acct_no,
                            code,
                            name,
                            trail_day,
                            trail_dtm,
                            trail_tp,
                            stop_price,
                            target_price,
                            basic_price,
                            basic_qty,
                            basic_amt,
                            proc_min,
                            crt_dt,
                            mod_dt
                        )
                        SELECT
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        WHERE NOT EXISTS (SELECT 1 FROM upd)
                        RETURNING 1 AS flag
                    )
                    SELECT flag FROM upd
                    UNION ALL
                    SELECT flag FROM ins;
                    """
                cur400.execute(merge_query, (
                        hour_minute, "1", loss_price, safe_margin_price, avg_base_price, sum_base_qty, avg_base_price * sum_base_qty, hour_minute, datetime.now(), acct_no, code, year_day,
                        acct_no, code, company, year_day, hour_minute, "1", loss_price, safe_margin_price, avg_base_price if base_price > 0 else buy_price, sum_base_qty if base_qty > 0 else buy_qty, avg_base_price*sum_base_qty if base_qty > 0 else buy_price*buy_qty, hour_minute, datetime.now(), datetime.now()
                ))

                was_updated = cur400.fetchone() is not None
                conn.commit()
                cur400.close()

                nick_label = nick if nick else arguments[1]
                if was_updated:
                    msg = "[" + company + "{" + code + "}] 평균보유가 : " + format(avg_base_price if base_price > 0 else buy_price, ',d') + "원, 총보유량 : " + format(sum_base_qty if base_qty > 0 else buy_qty, ',d') + "주, 총보유금액 : " + format(avg_base_price*sum_base_qty if base_qty > 0 else buy_price*buy_qty, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 안전마진가 : " + format(safe_margin_price, ',d') + "원 매매추적 처리"
                    print(f"[{nick_label}] {msg}")
                    bot.send_message(chat_id=chat_id, text=msg)
                else:
                    msg = "[" + company + "] 매수가 : " + format(buy_price, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 안전마진가 : " + format(safe_margin_price, ',d') + "원, 매수량 : " + format(buy_qty, ',d') + "주, 매수금액 : " + format(buy_price*buy_qty, ',d') + "원 매매추적 미처리"
                    print(f"[{nick_label}] {msg}")
                    bot.send_message(chat_id=chat_id, text=msg)

                # 매매시뮬레이션 insert
                cur500 = conn.cursor()
                insert_query = """
                    INSERT INTO trading_simulation (
                        acct_no, name, code, trade_day, trade_dtm, trade_tp, buy_price, buy_qty, buy_amt, loss_price, profit_price, proc_yn, crt_dt, mod_dt
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (acct_no, code, trade_day, trade_dtm, trade_tp)
                    DO NOTHING
                    RETURNING 1;
                    """
                cur500.execute(insert_query, (
                    acct_no, company, code, year_day, hour_minute, "1", buy_price, buy_qty, buy_price*buy_qty, loss_price, safe_margin_price, 'N', datetime.now(), datetime.now()
                ))

                was_inserted = cur500.fetchone() is not None
                conn.commit()
                cur500.close()

                if was_inserted:
                    msg = "[" + company + "{" + code + "}] 매수가 : " + format(buy_price, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 안전마진가 : " + format(safe_margin_price, ',d') + "원, 매수량 : " + format(buy_qty, ',d') + "주, 매수금액 : " + format(buy_price*buy_qty, ',d') + "원 매수등록"
                    print(f"[{nick_label}] {msg}")
                    bot.send_message(chat_id=chat_id, text=msg)
                else:
                    msg = "[" + company + "] 매수가 : " + format(buy_price, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 안전마진가 : " + format(safe_margin_price, ',d') + "원, 매수량 : " + format(buy_qty, ',d') + "주, 매수금액 : " + format(buy_price*buy_qty, ',d') + "원 매수등록 미처리"
                    print(f"[{nick_label}] {msg}")
                    bot.send_message(chat_id=chat_id, text=msg)

        else:
            print("날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매수가(시장가:0), 이탈가, 매수금액, 대상(단독:1) 미존재 또는 부적합")
    elif menu == '매도등록':
        commandBot = user_input.split(sep=',', maxsplit=6)
        print("commandBot[1] : ", commandBot[1])    # 날짜-8자리(YYYYMMDD, 현재일자:0)
        print("commandBot[2] : ", commandBot[2])    # 시간-6자리(HHMMSS, 현재일시:0)
        print("commandBot[3] : ", commandBot[3])    # 매도가(시장가:0)
        print("commandBot[4] : ", commandBot[4])    # 비중(%)
        print("commandBot[5] : ", commandBot[5])    # 대상(단독:1)

        if (commandBot[1] == '0' or len(commandBot[1]) == 8) and commandBot[1].isdigit() and (commandBot[2] == '0' or len(commandBot[2]) == 6) and commandBot[2].isdigit() and commandBot[3].isdecimal() and is_positive_int(commandBot[4]) and commandBot[5].isdecimal():
            year_day = datetime.now().strftime("%Y%m%d") if commandBot[1] == '0' else commandBot[1]
            hour_minute = datetime.now().strftime('%H%M%S') if commandBot[2] == '0' else commandBot[2]
            sell_price = int(stck_prpr) if commandBot[3] == '0' else int(commandBot[3])
            sell_rate = int(commandBot[4])

            nickname_list = ['yh480825', 'mamalong']
            target_nicks = nickname_list if int(commandBot[5]) > 1 else [None]
            for nick in target_nicks:
                if nick is not None:
                    ac = account(nick)
                    acct_no = ac['acct_no']
                    access_token = ac['access_token']
                    app_key = ac['app_key']
                    app_secret = ac['app_secret']

                # 계좌잔고 조회
                c = stock_balance(access_token, app_key, app_secret, acct_no, "")

                hldg_qty = 0

                for i, name in enumerate(c.index):
                    if code == c['pdno'][i]:
                        hldg_qty = int(c['hldg_qty'][i])

                # 매매추적 보유가, 보유수량, 추적유형 조회
                cur300 = conn.cursor()
                select_query = """
                    SELECT
                        basic_price,
                        basic_qty,
                        trail_tp
                    FROM (
                        SELECT
                            COALESCE(basic_price, 0) AS basic_price,
                            COALESCE(basic_qty, 0) AS basic_qty,
                            trail_tp,
                            row_number() OVER (
                                PARTITION BY acct_no, code
                                ORDER BY trail_dtm DESC
                            ) AS rn
                        FROM public.trading_trail
                        WHERE acct_no = %s
                        AND trail_tp IN ('1', '2', '3', 'L')
                        AND trail_day = %s
                        AND code = %s
                    ) T
                    WHERE rn = 1
                    """
                cur300.execute(select_query, (acct_no, year_day, code))
                row = cur300.fetchone()
                cur300.close()

                basic_price = int(row[0]) if row else 0
                basic_qty = int(row[1]) if row else 0
                prev_trail_tp = row[2] if row else "1"
                base_qty = hldg_qty if hldg_qty > 0 else basic_qty
                sell_qty = int(base_qty * sell_rate * 0.01)

                nick_label = nick if nick else arguments[1]

                if sell_qty > 0:
                    try:
                        with conn.cursor() as cur:
                            # 매매추적 update
                            update_query1 = """
                                UPDATE trading_trail tt SET
                                    trail_dtm = %s, trail_tp = %s, trail_plan = %s, stop_price = %s, target_price = %s, proc_min = %s, mod_dt = %s
                                FROM (
                                    SELECT
                                        acct_no,
                                        code,
                                        trail_day,
                                        row_number() OVER (
                                            PARTITION BY acct_no, code
                                            ORDER BY trail_dtm DESC
                                        ) AS rn
                                    FROM public.trading_trail
                                    WHERE acct_no = %s
                                    AND code = %s
                                    AND trail_day = %s
                                    AND trail_tp IN ('1', '2', '3', 'L')
                                ) sub
                                WHERE tt.acct_no = sub.acct_no
                                AND tt.code = sub.code
                                AND tt.trail_day = sub.trail_day
                                AND sub.rn = 1
                                RETURNING 1;
                                """
                            cur.execute(update_query1, (hour_minute, "2", str(sell_rate), int(stck_lwpr), sell_price, hour_minute, datetime.now(), acct_no, code, year_day))

                            was_updated1 = cur.fetchone() is not None

                            # 매매시뮬레이션 update
                            update_query2 = """
                                UPDATE public.trading_simulation SET
                                    loss_price = %s
                                    , profit_price = %s
                                    , proc_dtm = %s
                                    , proc_yn = %s
                                    , mod_dt = %s
                                WHERE acct_no = %s
                                AND code = %s
                                AND trade_tp = %s
                                AND trade_day <= %s
                                AND proc_yn != 'Y'
                                RETURNING 1;
                                """
                            cur.execute(update_query2, (int(stck_lwpr), sell_price, datetime.now().strftime("%Y%m%d%H%M"), "C", datetime.now(), acct_no, code, "1", year_day))

                            was_updated2 = cur.fetchone() is not None

                            if was_updated1 and was_updated2:
                                conn.commit()
                                msg = "[" + company + "{" + code + "}] 저가 : " + format(int(stck_lwpr), ',d') + "원, 보유가 : " + format(basic_price, ',d') + "원, 보유량 : " + format(base_qty, ',d') + "주, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 매매추적 처리"
                                print(f"[{nick_label}] {msg}")
                                bot.send_message(chat_id=chat_id, text=msg)

                                # 매매시뮬레이션 insert
                                insert_query = """
                                    INSERT INTO trading_simulation (
                                        acct_no, name, code, trade_day, trade_dtm, trade_tp, sell_price, sell_qty, trading_plan, proc_yn, crt_dt, mod_dt
                                    )
                                    VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                    ON CONFLICT (acct_no, code, trade_day, trade_dtm, trade_tp)
                                    DO NOTHING
                                    RETURNING 1;
                                    """
                                cur.execute(insert_query, (
                                    acct_no, company, code, year_day, hour_minute, "2", sell_price, sell_qty, str(sell_rate), 'N', datetime.now(), datetime.now()
                                ))

                                was_inserted = cur.fetchone() is not None
                                conn.commit()

                                if was_inserted:
                                    msg = "[" + company + "{" + code + "}] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 매도등록"
                                    print(f"[{nick_label}] {msg}")
                                    bot.send_message(chat_id=chat_id, text=msg)
                                else:
                                    msg = "[" + company + "] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 매도등록 미처리"
                                    print(f"[{nick_label}] {msg}")
                                    bot.send_message(chat_id=chat_id, text=msg)

                            else:
                                msg = "[" + company + "] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 매매추적 미처리"
                                print(f"[{nick_label}] {msg}")
                                bot.send_message(chat_id=chat_id, text=msg)

                    except Exception as e:
                        conn.rollback()
                        print(f"[{nick_label}] Error 발생: {e}")

                else:
                    msg = "[" + company + "] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도량 부족 미처리"
                    print(f"[{nick_label}] {msg}")

        else:
            print("날짜-8자리(YYYYMMDD), 시간-6자리(HHMMSS), 매도가(시장가:0), 비중(%), 대상(단독:1) 미존재 또는 부적합")
    else:
        print(f"지원하지 않는 메뉴: {menu}")

# CLI 모드 / 텔레그램 봇 모드 분기
if len(arguments) >= 4:
    # CLI 모드: python reservebot_simulation.py yh480825 매수등록 "파라다이스,20251121,100000,18520,16000,2000000,2"
    cli_run(arguments[2], arguments[3])
    conn.close()
else:
    # 텔레그램봇 응답 처리
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, echo))
    dispatcher.add_handler(CommandHandler("start", start))
    # 텔레그램봇 polling
    updater.start_polling()
