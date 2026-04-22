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
import threading

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

arguments = sys.argv

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='asdf1234'"
# DB 연결
conn = db.connect(conn_string)

def get_conn():
    global conn
    try:
        conn.isolation_level
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        conn = db.connect(conn_string)
    return conn                

cur001 = conn.cursor()
cur001.execute("select bot_token2 from \"stockAccount_stock_account\" where nick_name = '" + arguments[1] + "'")
result_001 = cur001.fetchone()
cur001.close()
token = result_001[0]    

# 해당 링크는 한국거래소에서 상장법인목록을 엑셀로 다운로드하는 링크입니다.
# 다운로드와 동시에 Pandas에 excel 파일이 load가 되는 구조입니다.
krx_url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download'
# requests로 먼저 가져오기, 인코딩 지정
krx_res = requests.get(krx_url, timeout=10)
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
g_selected_accounts = []  # 계좌 다중 선택 목록

# 매수주문 미리보기 → 진행 콜백 공유 상태
g_buy21_code = ""
g_buy21_company = ""
g_buy21_buy_price = 0
g_buy21_loss_price = 0
g_buy21_buy_qty = 0
g_buy21_buy_amt = 0
g_buy21_loss_buy_qty = 0     # 손절금액 기준 매수량
g_buy21_loss_buy_amt = 0     # 손절금액 기준 매수금액
g_buy21_amt_buy_qty = 0      # 매수금액 기준 매수량
g_buy21_amt_buy_amt = 0      # 매수금액 기준 매수금액

# 보유종목 매도 수량
g_holding_sell_qty = 0

# 추적등록(손절금액) 미리보기 → 진행 콜백 공유 상태
g_trail71_code = ""
g_trail71_company = ""
g_trail71_buy_price = 0
g_trail71_loss_price = 0
g_trail71_item_loss_sum = ""
g_trail71_buy_qty = 0
g_trail71_buy_amt = 0
g_trail71_year_day = ""
g_trail71_hour_minute = ""
g_trail71_loss_buy_qty = 0   # 손절금액 기준 매수량
g_trail71_loss_buy_amt = 0   # 손절금액 기준 매수금액
g_trail71_amt_buy_qty = 0    # 매수금액 기준 매수량
g_trail71_amt_buy_amt = 0    # 매수금액 기준 매수금액

SELECTABLE_ACCOUNTS = ['phills2', 'mamalong', 'worry106', 'phills75', 'yh480825', 'phills13', 'phills15', 'chichipa', 'honeylong']  # 선택 가능 계좌 목록

def format_number(value):
    try:
        return f"{float(value):,.2f}" if isinstance(value, float) else f"{int(value):,}"
    except Exception:
        return str(value)
    
def build_date_buttons1(days=7):
    today = datetime.now().date()
    conn = get_conn()
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
    conn = get_conn()
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
    conn = get_conn()
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
    conn = get_conn()
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
    main_buttons = build_button(["매수주문", "매도주문", "주문정정", "주문취소",
                                 "전체예약", "예약주문", "예약정정", "예약철회", 
                                 "전체주문", "보유종목", "추적준비", "추적삭제", 
                                 "추적등록", "추적변경", "추적상태", "매매추적",
                                 "손실금액계산"], callback_header="menu")
    cancel_button = build_button(["취소"], callback_header="menu")
    show_markup = InlineKeyboardMarkup(build_menu(main_buttons, n_cols=4, footer_buttons=cancel_button))
    
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup

def start(update, context) :
    chat_id = update.effective_chat.id
    conn = get_conn()
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
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False, timeout=10)
    ACCESS_TOKEN = res.json()["access_token"]

    return ACCESS_TOKEN

# 계정정보 조회
def account(nickname=None):
    conn = get_conn()
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
                'FID_COND_MRKT_DIV_CODE': "UN",  # J:KRX, NX:NXT, UN:통합
                'FID_INPUT_ISCD': code
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
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

    res = requests.get(url, headers=headers, params=params, timeout=10)
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
                'FID_COND_MRKT_DIV_CODE': "UN",  # J:KRX, NX:NXT, UN:통합
                'FID_INPUT_ISCD': code
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
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
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)

    return ar.getBody().output['nrcvb_buy_amt']

# 매도 주문정보 존재시 취소 처리
def sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, code):
    
    result_msgs = []

    try:
        # 일별주문체결 조회
        output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, code, '')

        if len(output1) > 0:
        
            tdf = pd.DataFrame(output1)
            tdf.set_index('odno')
            d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]
            order_no = 0

            for i, name in enumerate(d.index):

                # 매도주문 잔여수량 존재시
                if d['sll_buy_dvsn_cd'][i] == "01": 
                    
                    if int(d['rmn_qty'][i]) > 0: 
                        order_no = int(d['odno'][i])

                        # 주문취소
                        c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "02", str(order_no), "0", "0")
                        if c is not None and c['ODNO'] != "":
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
def get_excg_id():
    """정규시장(09:00~15:30)이면 KRX, 그 외 시간이면 NXT 반환"""
    t = datetime.now().strftime('%H%M')
    return "KRX" if '0900' <= t < '1530' else "NXT"

def order_cash(buy_flag, access_token, app_key, app_secret, acct_no, stock_code, ord_dvsn, order_qty, order_price, cndt_price=None, excg_id=None):

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
               "ORD_UNPR": order_price,         # 시장가 등 주문시, "0"으로 입력
               "EXCG_ID_DVSN_CD": excg_id if excg_id is not None else get_excg_id()   # 한국거래소 : KRX, 대체거래소 (넥스트레이드) : NXT, SOR (Smart Order Routing) : SOR
    }
    # 스톱지정가일 때만 조건가격 추가
    if ord_dvsn == "22":
        params["CNDT_PRIC"] = str(cndt_price)

    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
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
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output1

# 주식주문(정정취소)
def order_cancel_revice(access_token, app_key, app_secret, acct_no, cncl_dv, order_no, order_qty, order_price, excg_id=None):

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
               "QTY_ALL_ORD_YN": "Y",           # 전량 : Y, 일부 : N
               "EXCG_ID_DVSN_CD": excg_id if excg_id is not None else get_excg_id()   # 한국거래소 : KRX, 대체거래소 (넥스트레이드) : NXT, SOR (Smart Order Routing) : SOR
    }
    PATH = "uapi/domestic-stock/v1/trading/order-rvsecncl"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
    ar = resp.APIResp(res)
    #ar.printAll()
    if ar.isOK():
        return ar.getBody().output
    else:
        print(f"주문정정취소 API 오류: {ar.getErrorCode()} {ar.getErrorMessage()}")
        return None

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
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
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
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
    ar = resp.APIResp(res)
    #ar.printAll()
    if not ar.isOK():
        raise Exception(f"{ar.getErrorCode()} {ar.getErrorMessage()}")
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
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
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
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
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
    conn = get_conn()
    cur100 = conn.cursor()
    cur100.execute("select post_business_day_char('"+business_day+"'::date)")
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

def get_previous_business_day(day):
    conn = get_conn()
    cur100 = conn.cursor()
    cur100.execute("select prev_business_day_char(%s)", (day,))
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

def show_account_selection_keyboard(query, menu_num):
    """계좌 다중 선택 인라인 키보드를 표시한다. ✅/⬜ 토글 방식."""
    current_acc = arguments[1] if len(arguments) > 1 else ""
    extra = [current_acc] if current_acc and current_acc not in SELECTABLE_ACCOUNTS else []
    all_accounts = extra + SELECTABLE_ACCOUNTS
    buttons = []
    row = []
    for acc in all_accounts:
        check = "✅" if acc in g_selected_accounts else "⬜"
        row.append(InlineKeyboardButton(f"{check} {acc}", callback_data=f"acc_{menu_num}_t_{acc}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("✔ 확인", callback_data=f"acc_{menu_num}_confirm")])
    query.edit_message_text(text="처리할 계좌를 선택하세요 (복수 선택 가능):", reply_markup=InlineKeyboardMarkup(buttons))

def callback_get(update, context) :
    data_selected = update.callback_query.data
    query = update.callback_query

    command = data_selected.split(",")[-1] if "," in data_selected else data_selected

    global menuNum
    global g_order_no
    global g_remain_qty
    global g_selected_accounts

    print("command : ", command)
    if command == "취소":
        context.bot.edit_message_text(text="취소하였습니다.",
                                      chat_id=query.message.chat_id,
                                      message_id=query.message.message_id)
        return

    elif command.startswith("holding_"):
        # 보유종목 종목 선택 → 기준계좌 잔고로 종목명 확인 후 계좌 선택 키보드 표시
        h_code = command[len("holding_"):]
        ac_h = account()
        try:
            c_h = stock_balance(ac_h['access_token'], ac_h['app_key'], ac_h['app_secret'], ac_h['acct_no'], "")
            matched = [(c_h['prdt_name'][i], int(c_h['hldg_qty'][i]), int(c_h['prpr'][i]))
                       for i, _ in enumerate(c_h.index) if c_h['pdno'][i] == h_code]
            h_name = matched[0][0] if matched else h_code
            h_price = matched[0][2] if matched else 0
            global g_holding_sell_code, g_holding_sell_name, g_holding_sell_price
            g_holding_sell_code = h_code
            g_holding_sell_name = h_name
            g_holding_sell_price = h_price
            g_selected_accounts.clear()
            show_account_selection_keyboard(query, "92")
        except Exception as e:
            query.edit_message_text(text=f"[보유종목 매도] 조회 오류: {str(e)}")

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
            
            result_msgs = []
            # 계좌잔고 조회
            b = stock_balance(access_token, app_key, app_secret, acct_no, "all")

            for i, name in enumerate(b.index):
                u_tot_evlu_amt = int(b['tot_evlu_amt'][i])                  # 총평가금액
                u_dnca_tot_amt = int(b['dnca_tot_amt'][i])                  # 예수금총금액
                u_nass_amt = int(b['nass_amt'][i])                          # 순자산금액(세금비용 제외)
                u_prvs_rcdl_excc_amt = int(b['prvs_rcdl_excc_amt'][i])      # 가수도 정산 금액
                u_scts_evlu_amt = int(b['scts_evlu_amt'][i])                # 유저 평가 금액
                u_asst_icdc_amt = int(b['asst_icdc_amt'][i])                # 자산 증감액

                msg = f"* 총 평가금액:{format(u_tot_evlu_amt, ',d')}원, 잔고금액:{format(u_scts_evlu_amt, ',d')}원, 가정산금:{format(u_prvs_rcdl_excc_amt, ',d')}원, 전일증감:{format(u_asst_icdc_amt, ',d')}원"
                result_msgs.append(msg)
                
            # 계좌잔고 조회
            c = stock_balance(access_token, app_key, app_secret, acct_no, "")
        
            
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

                # 보유종목 매도 선택 버튼 (종목별 1개씩)
                if len(c.index) > 0:
                    hold_buttons = []
                    for i, name in enumerate(c.index):
                        h_code = c['pdno'][i]
                        h_name = c['prdt_name'][i]
                        h_qty = int(c['hldg_qty'][i])
                        if h_qty > 0:
                            hold_buttons.append(
                                InlineKeyboardButton(
                                    f"{h_name}({h_code})",
                                    callback_data=f"menu,holding_{h_code}"
                                )
                            )
                    if hold_buttons:
                        hold_buttons.append(InlineKeyboardButton("취소", callback_data="취소"))
                        # 한 행에 2개씩
                        rows = [hold_buttons[i:i+2] for i in range(0, len(hold_buttons), 2)]
                        context.bot.send_message(
                            text="매도할 종목을 선택하세요:",
                            chat_id=query.message.chat_id,
                            reply_markup=InlineKeyboardMarkup(rows)
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
                d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]
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
                    d_excg_dvsn = d['excg_id_dvsn_cd'][i]

                    msg = f"* [{d_excg_dvsn} {d_name} - {d_order_tmd[:2]}:{d_order_tmd[2:4]}:{d_order_tmd[4:]}] 주문번호:<code>{str(d_order_no)}</code>, {d_order_type}가:{format(int(d_order_price), ',d')}원, {d_order_type}량:{format(int(d_order_amount), ',d')}주, 체결량:{format(int(d_total_complete_qty), ',d')}주, 잔량:{format(int(d_remain_qty), ',d')}주, 체결금:{format(int(d_total_complete_amt), ',d')}원"
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
                                            chat_id=query.message.chat_id)

        except Exception as e:
            print('일별주문체결 조회 오류.', e)
            context.bot.edit_message_text(text="[일별주문체결 조회] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)
    
    elif command == "매수주문":
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "21")

    elif command in ("손절금액", "매수금액") and "buy21" in data_selected:
        # 매수주문 미리보기 → 실제 주문 처리
        if command == "손절금액":
            g_buy21_buy_qty = g_buy21_loss_buy_qty
        else:
            g_buy21_buy_qty = g_buy21_amt_buy_qty

        cb_user_id = query.message.chat_id
        cb_bot = context.bot
        cb_code = g_buy21_code
        cb_company = g_buy21_company
        cb_buy_price = g_buy21_buy_price
        cb_buy_qty = g_buy21_buy_qty
        target_nicks = g_selected_accounts[:] if g_selected_accounts else [None]
        ac_default = account()

        def process_buy21(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            try:
                t_buy_price = cb_buy_price
                t_buy_qty = cb_buy_qty

                buy_expect_sum = t_buy_price * t_buy_qty
                b = inquire_psbl_order(t_access_token, t_app_key, t_app_secret, t_acct_no)

                if int(b) > int(buy_expect_sum):
                    try:
                        c_ord = order_cash(True, t_access_token, t_app_key, t_app_secret, str(t_acct_no), cb_code, "00", str(t_buy_qty), str(t_buy_price))
                        if c_ord is not None and c_ord['ODNO'] != "":
                            time.sleep(0.5)
                            output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret, t_acct_no, cb_code, c_ord['ODNO'])
                            tdf = pd.DataFrame(output1)
                            tdf.set_index('odno')
                            d_ord = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]
                            for i, _ in enumerate(d_ord.index):
                                d_order_price = d_ord['avg_prvs'][i] if int(d_ord['avg_prvs'][i]) > 0 else d_ord['ord_unpr'][i]
                                d_order_amount = d_ord['ord_qty'][i]
                                d_order_no = int(d_ord['odno'][i])
                                cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] 매수가 : " + format(int(d_order_price), ',d') + "원, 매수량 : " + format(int(d_order_amount), ',d') + "주 매수주문 완료, 주문번호 : <code>" + str(d_order_no) + "</code>", parse_mode='HTML')
                        else:
                            cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] 매수가 : " + format(int(t_buy_price), ',d') + "원, 매수량 : " + format(int(t_buy_qty), ',d') + "주 매수주문 실패", parse_mode='HTML')
                    except Exception as e:
                        cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] [매수주문 오류] - " + str(e), parse_mode='HTML')
                else:
                    cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "] 매수 가능(현금) : " + format(int(b) - int(buy_expect_sum), ',d') + "원 부족")

            except Exception as top_e:
                print(f"process_buy21 오류 [{nick}]: {top_e}")
                cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "] [매수주문 오류] " + str(top_e))

        query.edit_message_text(text="[" + cb_company + "] 주문 처리 중...")
        threads_b21 = []
        for nick in target_nicks:
            if nick is not None:
                ac_t = account(nick)
                t_acct_no = ac_t['acct_no']
                t_access_token = ac_t['access_token']
                t_app_key = ac_t['app_key']
                t_app_secret = ac_t['app_secret']
                t_nick = nick
            else:
                t_acct_no = ac_default['acct_no']
                t_access_token = ac_default['access_token']
                t_app_key = ac_default['app_key']
                t_app_secret = ac_default['app_secret']
                t_nick = arguments[1]
            t = threading.Thread(target=process_buy21, args=(t_nick, t_acct_no, t_access_token, t_app_key, t_app_secret))
            threads_b21.append(t)
            t.start()
        for t in threads_b21:
            t.join()

    elif command == "다시계산" and "buy21" in data_selected:
        menuNum = "21"
        selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "선택 없음(현재계좌)"
        query.edit_message_text(text="[선택계좌: " + selected_str + "]\n종목코드(종목명), 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액을 입력하세요.")

    elif command == "매도주문":
        button_list = build_button(["도가도량", "전체", "절반"], data_selected)
        show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list)))

        context.bot.edit_message_text(text="매도방식을 선택해 주세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id,
                                        reply_markup=show_markup)

    elif command == "도가도량" and "매도주문" in data_selected:
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "31")

    elif command == "전체" and "매도주문" in data_selected:
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "32")

    elif command == "절반" and "매도주문" in data_selected:
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "33")
    
    elif command == "주문정정":
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "51")

    elif command == "주문취소":
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "52")
    
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
                                            chat_id=query.message.chat_id)

        except Exception as e:
            print('전체예약 조회 오류.', e)
            context.bot.edit_message_text(text="[전체예약 조회] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

    elif command == "예약주문":
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "61")
    
    elif command == "예약정정":
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "62")

    elif command == "예약철회":
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "63")
        
    elif command == "추적등록":
        button_list = build_button(["매수주문등록", "주문제외등록"], data_selected)
        show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list)))

        context.bot.edit_message_text(text="추적등록 방식을 선택해 주세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id,
                                        reply_markup=show_markup)

    elif command == "매수주문등록" and "추적등록" in data_selected:
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "71")

    elif command == "주문제외등록" and "추적등록" in data_selected:
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "72")

    elif command in ("손절금액", "매수금액") and "trail71" in data_selected:
        # 손절금액/매수금액 선택에 따라 매수량·매수금액 설정
        if command == "손절금액":
            g_trail71_buy_qty = g_trail71_loss_buy_qty
        else:
            g_trail71_buy_qty = g_trail71_amt_buy_qty

        # 추적등록 미리보기 → 실제 주문 + 매매추적 처리
        cb_user_id = query.message.chat_id
        cb_bot = context.bot          # 스레드에서 안전하게 사용하기 위해 bot 참조 분리
        cb_code = g_trail71_code
        cb_company = g_trail71_company
        cb_buy_price = g_trail71_buy_price
        cb_loss_price = g_trail71_loss_price
        cb_buy_qty = g_trail71_buy_qty
        cb_year_day = g_trail71_year_day
        cb_hour_minute = g_trail71_hour_minute
        target_nicks = g_selected_accounts[:] if g_selected_accounts else [None]
        ac_default = account()

        def process_trail71(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            try:
                t_buy_price = cb_buy_price
                t_buy_qty = cb_buy_qty

                c_bal = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")
                hold_price = 0
                hldg_qty = 0
                hold_amt = 0
                for i, _ in enumerate(c_bal.index):
                    if cb_code == c_bal['pdno'][i]:
                        hold_price = float(c_bal['pchs_avg_pric'][i])
                        hldg_qty = int(c_bal['hldg_qty'][i])
                        hold_amt = int(c_bal['pchs_amt'][i])

                buy_expect_sum = t_buy_price * t_buy_qty
                b = inquire_psbl_order(t_access_token, t_app_key, t_app_secret, t_acct_no)
                d_order_no = None
                d_order_type = None
                d_order_dt = None
                d_order_tmd = None
                d_order_price = 0
                d_order_amount = 0
                d_order_complete_qty = 0
                d_order_remain_qty = 0

                if int(b) > int(buy_expect_sum):
                    try:
                        c_ord = order_cash(True, t_access_token, t_app_key, t_app_secret, str(t_acct_no), cb_code, "00", str(t_buy_qty), str(t_buy_price))
                        if c_ord is not None and c_ord['ODNO'] != "":
                            time.sleep(0.5)
                            output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret, t_acct_no, cb_code, c_ord['ODNO'])
                            tdf = pd.DataFrame(output1)
                            tdf.set_index('odno')
                            d_ord = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]
                            for i, _ in enumerate(d_ord.index):
                                d_order_no = int(d_ord['odno'][i])
                                d_order_type = d_ord['sll_buy_dvsn_cd_name'][i]
                                d_order_dt = d_ord['ord_dt'][i]
                                d_order_tmd = d_ord['ord_tmd'][i]
                                d_order_price = d_ord['avg_prvs'][i] if int(d_ord['avg_prvs'][i]) > 0 else d_ord['ord_unpr'][i]
                                d_order_amount = d_ord['ord_qty'][i]
                                d_order_complete_qty = d_ord['tot_ccld_qty'][i]
                                d_order_remain_qty = d_ord['rmn_qty'][i]
                                cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] 매수가 : " + format(int(d_order_price), ',d') + "원, 매수량 : " + format(int(d_order_amount), ',d') + "주 매수주문 완료, 주문번호 : <code>" + str(d_order_no) + "</code>", parse_mode='HTML')
                        else:
                            cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] 매수가 : " + format(int(t_buy_price), ',d') + "원, 매수량 : " + format(int(t_buy_qty), ',d') + "주 매수주문 실패", parse_mode='HTML')
                    except Exception as e:
                        cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] [매수주문 오류] - " + str(e), parse_mode='HTML')
                else:
                    cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "] 매수 가능(현금) : " + format(int(b) - int(buy_expect_sum), ',d') + "원 부족")

                if int(d_order_price) > 0 and int(d_order_amount) > 0:
                    t_buy_price = int(d_order_price)
                    t_buy_qty = int(d_order_amount)
                    t_buy_amt = t_buy_price * t_buy_qty
                    safe_margin_price = int(t_buy_price + t_buy_price * 0.05)
                    base_qty = hldg_qty
                    base_amt = hold_amt
                    sum_base_qty = base_qty + t_buy_qty
                    avg_base_price = int(round((base_amt + t_buy_amt) / sum_base_qty))
                    avg_safe_margin_price = int(avg_base_price + avg_base_price * 0.05)
                    loss_amt = int((avg_base_price - cb_loss_price) * sum_base_qty) if base_qty > 0 else int((t_buy_price - cb_loss_price) * t_buy_qty)

                    thread_conn = db.connect(conn_string)
                    try:
                        cur400 = thread_conn.cursor()
                        merge_query = """
                            WITH ins AS (
                                INSERT INTO trading_trail (
                                    order_no, order_type, order_dt, order_tmd,
                                    order_price, order_amount, complete_qty, remain_qty,
                                    acct_no, code, name, trail_day, trail_dtm, trail_tp,
                                    stop_price, target_price, basic_price, basic_qty, basic_amt,
                                    proc_min, trade_tp, exit_price, loss_amt, crt_dt, mod_dt
                                )
                                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                RETURNING 1 AS flag
                            )
                            SELECT flag FROM ins;
                        """
                        cur400.execute(merge_query, (
                            str(d_order_no), d_order_type, d_order_dt, d_order_tmd,
                            int(d_order_price), int(d_order_amount), int(d_order_complete_qty), int(d_order_remain_qty),
                            t_acct_no, cb_code, cb_company, cb_year_day, cb_hour_minute, "1",
                            cb_loss_price, avg_safe_margin_price if hold_price > 0 else safe_margin_price,
                            avg_base_price if hold_price > 0 else t_buy_price,
                            sum_base_qty if base_qty > 0 else t_buy_qty,
                            avg_base_price * sum_base_qty if base_qty > 0 else t_buy_amt,
                            cb_hour_minute, 'S' if command == "손절금액" else 'M', cb_loss_price, loss_amt, datetime.now(), datetime.now()
                        ))
                        was_updated = cur400.fetchone() is not None
                        thread_conn.commit()
                        cur400.close()
                        if was_updated:
                            cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] 평균보유가 : " + format(avg_base_price if hold_price > 0 else t_buy_price, ',d') + "원, 총보유량 : " + format(sum_base_qty if base_qty > 0 else t_buy_qty, ',d') + "주, 이탈가 : " + format(cb_loss_price, ',d') + "원, 안전마진가 : " + format(avg_safe_margin_price if hold_price > 0 else safe_margin_price, ',d') + "원 매매추적 처리", parse_mode='HTML')
                        else:
                            cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "] 매매추적 미처리")
                    finally:
                        thread_conn.close()

            except Exception as top_e:
                print(f"process_trail71 오류 [{nick}]: {top_e}")
                cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "] [추적등록 오류] " + str(top_e))

        query.edit_message_text(text="[" + cb_company + "] 주문 처리 중...")
        threads_t71 = []
        for nick in target_nicks:
            if nick is not None:
                ac_t = account(nick)
                t_acct_no = ac_t['acct_no']
                t_access_token = ac_t['access_token']
                t_app_key = ac_t['app_key']
                t_app_secret = ac_t['app_secret']
                t_nick = nick
            else:
                t_acct_no = ac_default['acct_no']
                t_access_token = ac_default['access_token']
                t_app_key = ac_default['app_key']
                t_app_secret = ac_default['app_secret']
                t_nick = arguments[1]
            t = threading.Thread(target=process_trail71, args=(t_nick, t_acct_no, t_access_token, t_app_key, t_app_secret))
            threads_t71.append(t)
            t.start()
        for t in threads_t71:
            t.join()

    elif command == "다시계산" and "trail71" in data_selected:
        menuNum = "71"
        selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "선택 없음(현재계좌)"
        query.edit_message_text(text="[선택계좌: " + selected_str + "]\n종목코드(종목명), 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액을 입력하세요.")

    elif command == "추적변경":
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "81")

    elif command.startswith("acc_") and "_t_" in command:
        # 계좌 토글: callback_data = "acc_{menu_num}_t_{account_name}"
        parts = command.split("_t_", 1)          
        account_name = parts[1]
        menu_num = parts[0].split("_", 1)[1]    
        if account_name in g_selected_accounts:
            g_selected_accounts.remove(account_name)
        else:
            g_selected_accounts.append(account_name)
        show_account_selection_keyboard(query, menu_num)

    elif command.startswith("acc_") and command.endswith("_confirm"):
        # 계좌 선택 확인: callback_data = "acc_{menu_num}_confirm"
        menu_num = command.split("_")[1]         
        menuNum = menu_num
        prompt_texts = {
            "21": "종목코드(종목명), 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액을 입력하세요.",
            "31": "종목코드(종목명), 매도가(현재가:0), 매도량을 입력하세요.",
            "32": "종목코드(종목명), 매도가(현재가:0)를 입력하세요.",
            "33": "종목코드(종목명), 매도가(현재가:0)를 입력하세요.",
            "51": "주문정정할 종목코드(종목명), 정정가(시장가:0)를 입력하세요.",
            "52": "주문취소할 종목코드(종목명)를 입력하세요.",
            "61": "예약주문할 종목코드(종목명), 매매구분(매수:1 매도:2), 단가(시장가:0), 수량, 예약종료일-8자리(YYYYMMDD)를 입력하세요.",
            "62": "예약정정할 종목코드(종목명), 정정가(시장가:0), 예약종료일-8자리(YYYYMMDD)를 입력하세요.",
            "63": "예약철회할 종목코드(종목명)를 입력하세요.",
            "71": "종목코드(종목명), 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액을 입력하세요.",
            "72": "종목코드(종목명), 매도가(현재가:0), 이탈가(저가:0), 비중(%)을 입력하세요.",
            "81": "종목코드(종목명), 매도가(현재가:0), 이탈가(저가:0), 비중(%)을 입력하세요.",
            "92": "종목코드(종목명), 매도가(현재가:0), 매도율(%)을 입력하세요.",
        }
        selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "선택 없음(현재계좌)"
        prompt = prompt_texts.get(menu_num, "입력하세요.")
        query.edit_message_text(text=f"[선택계좌: {selected_str}]\n{prompt}")

    elif command == "추적준비":
        query.edit_message_text(
            text="📅 추적준비 시작일을 선택하세요",
            reply_markup=build_date_buttons1(38)  # 최근 38일
        )

    elif command.startswith("sell_trace_date:"):
        ac = account()
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[추적 준비]",
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
                            acct_no,
                            name,
                            code,
                            trail_day,
                            trail_dtm,
                            basic_price,
                            basic_qty,
                            stop_price,
                            target_price,
                            volumn,
                            trail_tp,
                            trade_tp,
                            exit_price,
                            loss_amt
                        FROM trading_trail
                        WHERE acct_no = {acct_no}                            
                        AND trail_day = '{prev_date}'
                        AND trail_tp IN ('1','2','3','L','P','C','U')
                    ) t
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
                    volumn,
                    stop_price,
                    target_price,
                    proc_min,
                    trade_tp,
                    exit_price,
                    loss_amt,
                    crt_dt,
                    mod_dt
                )
                SELECT
                    BAL.acct_no,
                    BAL.name,
                    BAL.code,
                    '{trail_day}' AS trail_day,
                    '090000' AS trail_dtm,
                    CASE WHEN COALESCE(S.trail_tp, '1') IN ('3', 'L') THEN 'L' ELSE  CASE WHEN COALESCE(S.trail_tp, '1') IN ('P','C','U') THEN 'P' ELSE '1' END END AS trail_tp,
                    CASE WHEN COALESCE(BAL.purchase_qty, 0) > 0 THEN BAL.purchase_price ELSE S.basic_price END AS basic_price,
                    CASE WHEN COALESCE(BAL.purchase_qty, 0) > 0 THEN BAL.purchase_qty ELSE S.basic_qty END AS basic_qty,
                    CASE WHEN COALESCE(BAL.purchase_qty, 0) > 0 THEN BAL.purchase_price*BAL.purchase_qty ELSE S.basic_price*S.basic_qty END AS basic_amt,
                    COALESCE(S.volumn, 0) AS volumn,
                    COALESCE(S.stop_price, 0) AS stop_price,
                    COALESCE(S.target_price, 0) AS target_price,
                    '090000' AS proc_min,
                    COALESCE(S.trade_tp, 'M') AS trade_tp,
                    COALESCE(S.exit_price, 0) AS exit_price,
                    COALESCE(S.loss_amt, 0) AS loss_amt,
                    now(),
                    now()
                FROM balance BAL
                LEFT JOIN sim S ON S.acct_no = BAL.acct_no AND S.code = BAL.code
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM trading_trail T
                    WHERE T.acct_no = BAL.acct_no
                    AND T.code = BAL.code
                    AND T.trail_day = '{trail_day}'
                    AND T.trail_dtm >= CASE WHEN S.trail_day = '{trail_day}' THEN S.trail_dtm ELSE '090000' END
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM public.dly_stock_balance DSB
                    WHERE DSB.acct::int = BAL.acct_no
                    AND DSB.code = BAL.code
                    AND DSB.dt = '{prev_date}'
                    AND DSB.trading_plan IN ('i', 'h')
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
                    msg = f"* [{trail_day}]-추적준비 등록 {countProc}건 처리"
                    result_msgs.append(msg)
                else:
                    msg = f"* [{trail_day}]-추적준비 등록 미처리"
                    result_msgs.append(msg)
            
            final_message = "\n".join(result_msgs) if result_msgs else "추적준비 등록 대상이 존재하지 않습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=query.message.chat_id,
                message_id=query.message.message_id
            )

        except Exception as e:
            print('추적준비 등록 오류.', e)
            context.bot.edit_message_text(text="[추적준비 등록] 오류 : "+str(e),
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

    elif command == "추적상태":
        button_list = build_button(["재개", "멈춤"], data_selected)
        show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list)))

        context.bot.edit_message_text(text="상태를 선택해 주세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id,
                                        reply_markup=show_markup)

    elif command == "재개" and "추적상태" in data_selected:
        menuNum = "41"

        context.bot.edit_message_text(text="재개 종목코드(종목명), 이탈가(저가:0), 목표가(고가:0), 추적상태(L,1,2)를 입력하세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)
        
    elif command == "멈춤" and "추적상태" in data_selected:
        menuNum = "42"

        context.bot.edit_message_text(text="멈춤 종목코드(종목명)를 입력하세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id)

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
                SELECT code, name, trail_day, trail_dtm, trail_tp, trail_price, trail_qty, trail_amt, trail_rate, basic_price, basic_qty, basic_amt, stop_price, target_price, proc_min, exit_price, trade_tp, trade_result FROM trading_trail WHERE acct_no = %s AND trail_day = %s ORDER BY trail_tp, proc_min DESC 
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
                    stop_price, target_price, proc_min, exit_price, trade_tp, trade_result) = r
                    
                    # 일자별 종가
                    stck_prpr = get_kis_daily_chart(
                        stock_code=code,
                        trade_date=trail_day,
                        access_token=access_token,
                        app_key=app_key,
                        app_secret=app_secret
                    )
                    if stck_prpr is None:
                        stck_prpr = 0
                    stck_rate = round((100-(stck_prpr/basic_price)*100)*-1,2) if basic_price > 0 else 0  # 수익률
                    trail = ""
                    if trail_price > 0:
                        trail_qty = int(round(trail_amt/trail_price)) if trail_qty == 0 else trail_qty  # 추적수량
                        basic_qty = trail_qty if basic_qty == 0 else basic_qty
                        trail = (f", 추적가:{trail_price:,}원({trail_qty:,}주), 추적율:{trail_rate}%, 추적금액:{trail_amt:,}원, {'손절' if trade_tp == 'S' else '정액'} 매매결과:{trade_result}")

                    msg = (f"[{trail_day}-{proc_min[:2]}:{proc_min[2:4]}]{name}[<code>{code}</code>] -{trail_tp}- "
                        f"보유가:{basic_price:,}원({basic_qty:,}주), 보유금액:{basic_price*basic_qty:,}원, 현재가:{stck_prpr:,}원, "
                        f"수익율:{str(stck_rate)}%, 손절가:{stop_price:,}원, 목표가:{target_price:,}원, 최종이탈가:{exit_price:,}원{trail}")
                    
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

    elif command == "손실금액계산":
        menuNum = "91"

        context.bot.edit_message_text(text="종목코드(종목명), 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액을 입력하세요.",
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
    global g_selected_accounts

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

        if menuNum == '21':
            initMenuNum()
            parts21 = user_text.split(',', 4)
            if len(parts21) < 5 or not parts21[1].strip().isdecimal() or not parts21[2].strip().isdecimal() or not parts21[3].strip().isdecimal() or not parts21[4].strip().isdecimal():
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액 미존재 또는 부적합")
            else:
                buy_price_21 = int(stck_prpr) if parts21[1].strip() == '0' else int(parts21[1].strip())
                loss_price_21 = int(stck_lwpr) if parts21[2].strip() == '0' else int(parts21[2].strip())
                input_buy_amt_21 = int(parts21[3].strip())    # 입력 매수금액
                item_loss_sum_21 = int(parts21[4].strip())    # 입력 손절금액

                if buy_price_21 <= loss_price_21:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(" + format(buy_price_21, ',d') + ")가 이탈가(" + format(loss_price_21, ',d') + ") 이하입니다.")
                else:
                    # 공통 손절율
                    loss_rate_21 = round((100 - (loss_price_21 / buy_price_21) * 100) * -1, 2)

                    # ① 손절금액 기준
                    loss_buy_qty_21 = int(round(item_loss_sum_21 / (buy_price_21 - loss_price_21)))
                    loss_buy_amt_21 = buy_price_21 * loss_buy_qty_21

                    # ② 매수금액 기준
                    amt_buy_qty_21 = int(round(input_buy_amt_21 / buy_price_21))
                    amt_buy_amt_21 = buy_price_21 * amt_buy_qty_21
                    amt_item_loss_21 = (buy_price_21 - loss_price_21) * amt_buy_qty_21

                    # 콜백에서 사용할 전역 상태 저장
                    global g_buy21_code, g_buy21_company, g_buy21_buy_price, g_buy21_loss_price
                    global g_buy21_buy_qty, g_buy21_buy_amt
                    global g_buy21_loss_buy_qty, g_buy21_loss_buy_amt
                    global g_buy21_amt_buy_qty, g_buy21_amt_buy_amt
                    g_buy21_code = code
                    g_buy21_company = company
                    g_buy21_buy_price = buy_price_21
                    g_buy21_loss_price = loss_price_21
                    g_buy21_buy_qty = 0
                    g_buy21_buy_amt = 0
                    g_buy21_loss_buy_qty = loss_buy_qty_21
                    g_buy21_loss_buy_amt = loss_buy_amt_21
                    g_buy21_amt_buy_qty = amt_buy_qty_21
                    g_buy21_amt_buy_amt = amt_buy_amt_21

                    selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "현재계좌"
                    preview_text = (
                        "[선택계좌: " + selected_str + "]\n"
                        "[" + company + "(<code>" + code + "</code>)]\n"
                        "매수가: " + format(buy_price_21, ',d') + "원 | 이탈가: " + format(loss_price_21, ',d') + "원 | 손절율: " + str(loss_rate_21) + "%\n"
                        "─────────────────\n"
                        "  손절금액 기준\n"
                        "  매수금액: " + format(loss_buy_amt_21, ',d') + "원 | 매수량: " + format(loss_buy_qty_21, ',d') + "주 | 손실금액: " + format(item_loss_sum_21, ',d') + "원\n"
                        "─────────────────\n"
                        "  매수금액 기준\n"
                        "  매수금액: " + format(amt_buy_amt_21, ',d') + "원 | 매수량: " + format(amt_buy_qty_21, ',d') + "주 | 손실금액: " + format(amt_item_loss_21, ',d') + "원"
                    )
                    button_list = build_button(["손절금액", "매수금액", "다시계산", "취소"], "buy21")
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, 2))
                    context.bot.send_message(chat_id=user_id, text=preview_text, reply_markup=show_markup, parse_mode='HTML')         

        elif menuNum == '31':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=3)
                print("commandBot[1] : ", commandBot[1])    # 매도가(현재가:0)
                print("commandBot[2] : ", commandBot[2])    # 매도량

            # 매도가(현재가:0), 매도량 존재시
            if commandBot[1].isdecimal() and commandBot[2].isdecimal():
                sell_price = int(stck_prpr) if commandBot[1] == '0' else int(commandBot[1])                  # 매도가(현재가:0)
                # 매도량
                sell_qty = commandBot[2]
                target_nicks = g_selected_accounts if g_selected_accounts else [None]
                
                def process_nick_31(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                    t_sell_price = sell_price
                    t_sell_qty = sell_qty
                    ord_dvsn = "00"
                    try:
                        # 매도 주문정보 존재시 취소 처리
                        if sell_order_cancel_proc(t_access_token, t_app_key, t_app_secret, str(t_acct_no), code) == 'success':
                        
                            # 매도 : 지정가 주문
                            c = order_cash(False, t_access_token, t_app_key, t_app_secret, str(t_acct_no), code, ord_dvsn, str(int(t_sell_qty)), str(int(t_sell_price)))

                            if c is not None and c['ODNO'] != "":
                                # 일별주문체결 조회
                                output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret, t_acct_no, code, c['ODNO'])
                                tdf = pd.DataFrame(output1)
                                tdf.set_index('odno')
                                d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]

                                for k, name in enumerate(d.index):
                                    d_order_no = int(d['odno'][k])
                                    d_order_type = d['sll_buy_dvsn_cd_name'][k]
                                    d_order_dt = d['ord_dt'][k]
                                    d_order_tmd = d['ord_tmd'][k]
                                    d_name = d['prdt_name'][k]
                                    d_order_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                                    d_order_amount = d['ord_qty'][k]
                                    d_total_complete_qty = d['tot_ccld_qty'][k]
                                    d_remain_qty = d['rmn_qty'][k]
                                    d_total_complete_amt = d['tot_ccld_amt'][k]

                                    print("매도주문 완료")
                                    context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 매도가 : "+format(int(d_order_price),',d')+"원, 매도체결량 : "+format(int(d_total_complete_qty), ',d')+"주, 매도체결금액 : "+format(int(d_total_complete_amt), ',d')+"원 주문 완료, 주문번호 : <code>"+str(d_order_no)+"</code>", parse_mode='HTML')

                            else:
                                print("매도주문 실패")
                                context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 매도가 : "+format(int(t_sell_price),',d')+"원, 매도량 : "+format(int(t_sell_qty), ',d')+"주, 매도주문실패", parse_mode='HTML')

                    except Exception as e:
                        print('매도주문 오류.', e)
                        context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 매도가 : " + format(int(t_sell_price), ',d') + "원, 매도량 : " + format(int(t_sell_qty), ',d') + "주 [매도주문 오류] - " + str(e), parse_mode='HTML')
                        
                threads_31 = []
                for nick in target_nicks:
                    if nick is not None:
                        ac = account(nick)
                        t_acct_no = ac['acct_no']
                        t_access_token = ac['access_token']
                        t_app_key = ac['app_key']
                        t_app_secret = ac['app_secret']
                    else:
                        t_acct_no = acct_no
                        t_access_token = access_token
                        t_app_key = app_key
                        t_app_secret = app_secret
                    t = threading.Thread(target=process_nick_31, args=(nick if nick is not None else arguments[1], t_acct_no, t_access_token, t_app_key, t_app_secret))
                    threads_31.append(t)
                    t.start()
                for t in threads_31:
                    t.join()

            else:
                print("매도가(현재가:0), 매도량 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가(현재가:0), 매도량 미존재 또는 부적합")         

        elif menuNum == '32':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 매도가(현재가:0)

            # 매도가(현재가:0) 존재시 : 전체
            if commandBot[1].isdecimal():
                sell_price = int(stck_prpr) if commandBot[1] == '0' else int(commandBot[1])                  # 매도가(현재가:0)
                target_nicks = g_selected_accounts if g_selected_accounts else [None]
                
                def process_nick_32(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                    t_sell_price = sell_price
                    t_sell_qty = 0
                    ord_dvsn = "00"
                    try:
                        # 매도 주문정보 존재시 취소 처리
                        if sell_order_cancel_proc(t_access_token, t_app_key, t_app_secret, str(t_acct_no), code) == 'success':

                            # 계좌잔고 조회
                            e = stock_balance(t_access_token, t_app_key, t_app_secret, str(t_acct_no), "")
                            
                            ord_psbl_qty = 0
                            for j, name in enumerate(e.index):
                                e_code = e['pdno'][j]
                                if e_code == code:
                                    ord_psbl_qty = int(e['ord_psbl_qty'][j])
                            print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                            if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우
                                t_sell_qty = int(round(ord_psbl_qty))

                                # 매도 : 지정가 주문
                                c = order_cash(False, t_access_token, t_app_key, t_app_secret, str(t_acct_no), code, ord_dvsn, str(int(t_sell_qty)), str(int(t_sell_price)))

                                if c is not None and c['ODNO'] != "":
                                    # 일별주문체결 조회
                                    output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret, t_acct_no, code, c['ODNO'])
                                    tdf = pd.DataFrame(output1)
                                    tdf.set_index('odno')
                                    d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]

                                    for k, name in enumerate(d.index):
                                        d_order_no = int(d['odno'][k])
                                        d_order_type = d['sll_buy_dvsn_cd_name'][k]
                                        d_order_dt = d['ord_dt'][k]
                                        d_order_tmd = d['ord_tmd'][k]
                                        d_name = d['prdt_name'][k]
                                        d_order_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                                        d_order_amount = d['ord_qty'][k]
                                        d_total_complete_qty = d['tot_ccld_qty'][k]
                                        d_remain_qty = d['rmn_qty'][k]
                                        d_total_complete_amt = d['tot_ccld_amt'][k]

                                        print("매도주문 완료")
                                        context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 매도가 : "+format(int(d_order_price),',d')+"원, 매도체결량 : "+format(int(d_total_complete_qty), ',d')+"주, 매도체결금액 : "+format(int(d_total_complete_amt), ',d')+"원 주문 완료, 주문번호 : <code>"+str(d_order_no)+"</code>", parse_mode='HTML')

                                else:
                                    print("매도주문 실패")
                                    context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 매도가 : "+format(int(t_sell_price),',d')+"원, 매도량 : "+format(int(t_sell_qty), ',d')+"주, 매도주문실패", parse_mode='HTML')

                    except Exception as e:
                        print('매도주문 오류.', e)
                        context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 매도가 : " + format(int(t_sell_price), ',d') + "원, 매도량 : " + format(int(t_sell_qty), ',d') + "주 [매도주문 오류] - " + str(e), parse_mode='HTML')
                        
                threads_32 = []
                for nick in target_nicks:
                    if nick is not None:
                        ac = account(nick)
                        t_acct_no = ac['acct_no']
                        t_access_token = ac['access_token']
                        t_app_key = ac['app_key']
                        t_app_secret = ac['app_secret']
                    else:
                        t_acct_no = acct_no
                        t_access_token = access_token
                        t_app_key = app_key
                        t_app_secret = app_secret
                    t = threading.Thread(target=process_nick_32, args=(nick if nick is not None else arguments[1], t_acct_no, t_access_token, t_app_key, t_app_secret))
                    threads_32.append(t)
                    t.start()
                for t in threads_32:
                    t.join()

            else:
                print("매도가(현재가:0) 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가(현재가:0) 미존재 또는 부적합")         

        elif menuNum == '33':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=2)
                print("commandBot[1] : ", commandBot[1])    # 매도가(현재가:0)

            # 매도가(현재가:0) 존재시 : 절반
            if commandBot[1].isdecimal():
                sell_price = int(stck_prpr) if commandBot[1] == '0' else int(commandBot[1])                  # 매도가(현재가:0)
                target_nicks = g_selected_accounts if g_selected_accounts else [None]
                
                def process_nick_33(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                    t_sell_price = sell_price
                    t_sell_qty = 0
                    ord_dvsn = "00"
                    try:
                        # 매도 주문정보 존재시 취소 처리
                        if sell_order_cancel_proc(t_access_token, t_app_key, t_app_secret, str(t_acct_no), code) == 'success':

                            # 계좌잔고 조회
                            e = stock_balance(t_access_token, t_app_key, t_app_secret, str(t_acct_no), "")
                            
                            ord_psbl_qty = 0
                            for j, name in enumerate(e.index):
                                e_code = e['pdno'][j]
                                if e_code == code:
                                    ord_psbl_qty = int(e['ord_psbl_qty'][j])
                            print("주문가능수량 : " + format(ord_psbl_qty, ',d'))
                            if ord_psbl_qty > 0:  # 주문가능수량이 존재하는 경우
                                t_sell_qty = int(round(ord_psbl_qty / 2))

                                # 매도 : 지정가 주문
                                c = order_cash(False, t_access_token, t_app_key, t_app_secret, str(t_acct_no), code, ord_dvsn, str(int(t_sell_qty)), str(int(t_sell_price)))

                                if c is not None and c['ODNO'] != "":
                                    # 일별주문체결 조회
                                    output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret, t_acct_no, code, c['ODNO'])
                                    tdf = pd.DataFrame(output1)
                                    tdf.set_index('odno')
                                    d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]

                                    for k, name in enumerate(d.index):
                                        d_order_no = int(d['odno'][k])
                                        d_order_type = d['sll_buy_dvsn_cd_name'][k]
                                        d_order_dt = d['ord_dt'][k]
                                        d_order_tmd = d['ord_tmd'][k]
                                        d_name = d['prdt_name'][k]
                                        d_order_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                                        d_order_amount = d['ord_qty'][k]
                                        d_total_complete_qty = d['tot_ccld_qty'][k]
                                        d_remain_qty = d['rmn_qty'][k]
                                        d_total_complete_amt = d['tot_ccld_amt'][k]

                                        print("매도주문 완료")
                                        context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 매도가 : "+format(int(d_order_price),',d')+"원, 매도체결량 : "+format(int(d_total_complete_qty), ',d')+"주, 매도체결금액 : "+format(int(d_total_complete_amt), ',d')+"원 주문 완료, 주문번호 : <code>"+str(d_order_no)+"</code>", parse_mode='HTML')

                                else:
                                    print("매도주문 실패")
                                    context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 매도가 : "+format(int(t_sell_price),',d')+"원, 매도량 : "+format(int(t_sell_qty), ',d')+"주, 매도주문실패", parse_mode='HTML')

                    except Exception as e:
                        print('매도주문 오류.', e)
                        context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 매도가 : " + format(int(t_sell_price), ',d') + "원, 매도량 : " + format(int(t_sell_qty), ',d') + "주 [매도주문 오류] - " + str(e), parse_mode='HTML')
                        
                threads_33 = []
                for nick in target_nicks:
                    if nick is not None:
                        ac = account(nick)
                        t_acct_no = ac['acct_no']
                        t_access_token = ac['access_token']
                        t_app_key = ac['app_key']
                        t_app_secret = ac['app_secret']
                    else:
                        t_acct_no = acct_no
                        t_access_token = access_token
                        t_app_key = app_key
                        t_app_secret = app_secret
                    t = threading.Thread(target=process_nick_33, args=(nick if nick is not None else arguments[1], t_acct_no, t_access_token, t_app_key, t_app_secret))
                    threads_33.append(t)
                    t.start()
                for t in threads_33:
                    t.join()

            else:
                print("매도가(현재가:0) 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가(현재가:0) 미존재 또는 부적합")         

        elif menuNum == '41':
            initMenuNum()
            if len(user_text.split(",")) > 0:
               
                commandBot = user_text.split(sep=',', maxsplit=4)
                print("commandBot[1] : ", commandBot[1])    # 이탈가(저가:0)
                print("commandBot[2] : ", commandBot[2])    # 목표가(고가:0)
                print("commandBot[3] : ", commandBot[3])    # 추적상태(L,1,2)

            # 이탈가(저가:0), 목표가(고가:0), 추적상태(L,1,2) 존재시
            if commandBot[1].isdecimal() and commandBot[2].isdecimal() and commandBot[3] in ('L', '1', '2'):

                stop_price = int(stck_lwpr) if int(commandBot[1]) == 0 else int(commandBot[1])
                target_price = int(stck_hgpr) if int(commandBot[2]) == 0 else int(commandBot[2])

                # 계좌잔고 조회
                c = stock_balance(access_token, app_key, app_secret, acct_no, "")

                hold_price = 0
                hldg_qty = 0
                hold_amt = 0

                for i, name in enumerate(c.index):
                    if code == c['pdno'][i]:
                        hold_price = float(c['pchs_avg_pric'][i])
                        hldg_qty = int(c['hldg_qty'][i])
                        hold_amt = int(c['pchs_amt'][i])

                try:
                    with conn.cursor() as cur:
                        # 매매추적 update
                        update_query1 = """
                            UPDATE trading_trail tt SET
                                trail_dtm = %s, trail_tp = %s, stop_price = %s, target_price = %s, proc_min = %s, mod_dt = %s, basic_price = %s, basic_qty = %s, basic_amt = %s, trail_plan = NULL, trail_price = NULL, trail_rate = NULL, trail_qty = NULL, trail_amt = NULL, volumn = NULL
                            WHERE code = %s
                            AND trail_day = %s
                            AND trail_tp IN ('C', 'U', 'P')
                            RETURNING 1;
                            """
                        cur.execute(update_query1, (datetime.now().strftime('%H%M%S'), str(commandBot[3]), stop_price, target_price, datetime.now().strftime('%H%M%S'), datetime.now(), int(hold_price), hldg_qty, hold_amt, code, datetime.now().strftime("%Y%m%d")))
                        was_updated1 = cur.fetchone() is not None

                        if was_updated1:
                            conn.commit()
                            context.bot.send_message(chat_id=user_id, text="["+datetime.now().strftime('%Y%m%d')+"]" + company + "[{<code>"+code+"</code>}] 저가 : " + format(int(stck_lwpr), ',d') + "원, 고가 : " + format(int(stck_hgpr), ',d') + "원, 보유가 : " + format(int(hold_price), ',d') + "원, 보유량 : " + format(hldg_qty, ',d') + "주, 이탈가 : " + format(stop_price, ',d') + "원, 목표가 : " + format(target_price, ',d') + "원, 추적상태 : " + str(commandBot[3]) + " 추적재개 처리", parse_mode='HTML')
                        else:
                            context.bot.send_message(chat_id=user_id, text="["+datetime.now().strftime('%Y%m%d')+"]" + company + " 이탈가 : " + format(stop_price, ',d') + "원, 목표가 : " + format(target_price, ',d') + "원, 추적상태 : " + str(commandBot[3]) + " 추적재개 미처리")                        
                except Exception as e:
                    conn.rollback()
                    print(f"Error 발생: {e}")
                    context.bot.send_message(chat_id=user_id, text=f"처리 중 오류가 발생했습니다:[{datetime.now().strftime('%Y%m%d')}] {e}")
            
            else:
                print("이탈가(저가:0), 목표가(고가:0), 추적상태(L,1,2) 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 이탈가(저가:0), 목표가(고가:0), 추적상태(L,1,2) 미존재 또는 부적합")

        elif menuNum == '42':
            initMenuNum()

            try:
                with conn.cursor() as cur:
                    # 매매추적 update : trail_tp = 'P' 추적멈춤
                    update_query1 = """
                        UPDATE trading_trail tt SET
                            trail_tp = %s, mod_dt = %s
                        WHERE code = %s
                        AND trail_day = %s
                        AND trail_tp IN ('1', '2', 'L')
                        RETURNING 1;
                        """
                    cur.execute(update_query1, ("P", datetime.now(), code, datetime.now().strftime("%Y%m%d")))
                    was_updated1 = cur.fetchone() is not None

                    if was_updated1:
                        conn.commit()
                        context.bot.send_message(chat_id=user_id, text="["+datetime.now().strftime('%Y%m%d')+"]" + company + "[{<code>"+code+"</code>}] 추적멈춤 처리", parse_mode='HTML')
                    else:
                        context.bot.send_message(chat_id=user_id, text="["+datetime.now().strftime('%Y%m%d')+"]" + company + " 추적멈춤 미처리")                        
            except Exception as e:
                conn.rollback()
                print(f"Error 발생: {e}")
                context.bot.send_message(chat_id=user_id, text=f"처리 중 오류가 발생했습니다:[{datetime.now().strftime('%Y%m%d')}] {e}") 

        elif menuNum == '51':
            initMenuNum()
            # 입력 형식: 종목코드(종목명), 정정가
            parts51 = user_text.split(',', 1)
            if len(parts51) < 2 or not parts51[1].strip().isdecimal():
                context.bot.send_message(chat_id=user_id, text=f"입력 형식 오류 [{company}] - 종목코드(종목명), 정정가(시장가:0)")
            else:
                revise_price_51 = parts51[1].strip()
                target_nicks = g_selected_accounts if g_selected_accounts else [None]

                def process_nick_51(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                    try:
                        output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret, t_acct_no, code, '')
                        if not output1:
                            context.bot.send_message(chat_id=user_id, text=f"[{nick}] 주문 미존재 [{company}]")
                            return
                        tdf = pd.DataFrame(output1)
                        tdf = tdf[tdf['rmn_qty'].astype(int) > 0]
                        if tdf.empty:
                            context.bot.send_message(chat_id=user_id, text=f"[{nick}] 미체결 주문 없음 [{company}]")
                            return
                        for _, row in tdf.iterrows():
                            order_no = row['odno']
                            remain_qty = int(row['rmn_qty'])
                            ord_type = row['sll_buy_dvsn_cd_name']
                            ord_price = row['ord_unpr']
                            try:
                                c = order_cancel_revice(t_access_token, t_app_key, t_app_secret, t_acct_no, "01", order_no, remain_qty, int(revise_price_51))
                                if c is not None and c['ODNO'] != "":
                                    context.bot.send_message(chat_id=user_id,
                                        text=f"[{nick}] 주문정정 완료 [{company}] {ord_type} {format(int(ord_price),',')}→{format(int(revise_price_51),',')}원, 주문번호: <code>{str(int(c['ODNO']))}</code>",
                                        parse_mode='HTML')
                                    try:
                                        with conn.cursor() as cur:
                                            cur.execute("""
                                                UPDATE trading_trail SET trail_tp = %s, mod_dt = %s
                                                WHERE acct_no = %s AND code = %s AND trail_day = %s AND order_no = %s
                                            """, ("U", datetime.now(), t_acct_no, code, datetime.now().strftime("%Y%m%d"), str(int(order_no))))
                                            conn.commit()
                                    except Exception as e:
                                        conn.rollback()
                                        print(f"매매추적 update 오류: {e}")
                                else:
                                    context.bot.send_message(chat_id=user_id, text=f"[{nick}] 주문정정 실패 [{company}]")
                            except Exception as e:
                                print('주문정정 오류.', e)
                                context.bot.send_message(chat_id=user_id, text=f"[{nick}] 주문정정 오류 [{company}]: {str(e)}")
                    except Exception as e:
                        print('일별주문체결 조회 오류.', e)
                        context.bot.send_message(chat_id=user_id, text=f"[{nick}] 일별주문체결 조회 오류 [{company}]: {str(e)}")

                threads_51 = []
                for nick in target_nicks:
                    if nick is not None:
                        ac = account(nick)
                        t_acct_no = ac['acct_no']
                        t_access_token = ac['access_token']
                        t_app_key = ac['app_key']
                        t_app_secret = ac['app_secret']
                    else:
                        t_acct_no = acct_no
                        t_access_token = access_token
                        t_app_key = app_key
                        t_app_secret = app_secret
                    t = threading.Thread(target=process_nick_51, args=(nick if nick is not None else arguments[1], t_acct_no, t_access_token, t_app_key, t_app_secret))
                    threads_51.append(t)
                    t.start()
                for t in threads_51:
                    t.join()

        elif menuNum == '52':
            initMenuNum()
            target_nicks = g_selected_accounts if g_selected_accounts else [None]

            def process_nick_52(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                try:
                    output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret, t_acct_no, code, '')
                    if not output1:
                        context.bot.send_message(chat_id=user_id, text=f"[{nick}] 주문 미존재 [{company}]")
                        return
                    tdf = pd.DataFrame(output1)
                    tdf = tdf[tdf['rmn_qty'].astype(int) > 0]
                    if tdf.empty:
                        context.bot.send_message(chat_id=user_id, text=f"[{nick}] 미체결 주문 없음 [{company}]")
                        return
                    for _, row in tdf.iterrows():
                        order_no = row['odno']
                        ord_type = row['sll_buy_dvsn_cd_name']
                        ord_price = row['ord_unpr']
                        ord_qty = row['ord_qty']
                        try:
                            c = order_cancel_revice(t_access_token, t_app_key, t_app_secret, t_acct_no, "02", order_no, "0", "0")
                            if c is not None and c['ODNO'] != "":
                                context.bot.send_message(chat_id=user_id,
                                    text=f"[{nick}] 주문취소 완료 [{company}] {ord_type} {format(int(ord_price),',')}원 {format(int(ord_qty),',')}주, 주문번호: <code>{str(int(c['ODNO']))}</code>",
                                    parse_mode='HTML')
                                try:
                                    with conn.cursor() as cur:
                                        cur.execute("""
                                            UPDATE trading_trail SET trail_tp = %s, mod_dt = %s
                                            WHERE acct_no = %s AND code = %s AND trail_day = %s AND order_no = %s
                                        """, ("C", datetime.now(), t_acct_no, code, datetime.now().strftime("%Y%m%d"), str(int(order_no))))
                                        conn.commit()
                                except Exception as e:
                                    conn.rollback()
                                    print(f"매매추적 update 오류: {e}")
                            else:
                                context.bot.send_message(chat_id=user_id, text=f"[{nick}] 주문취소 실패 [{company}]")
                        except Exception as e:
                            print('주문취소 오류.', e)
                            context.bot.send_message(chat_id=user_id, text=f"[{nick}] 주문취소 오류 [{company}]: {str(e)}")
                except Exception as e:
                    print('일별주문체결 조회 오류.', e)
                    context.bot.send_message(chat_id=user_id, text=f"[{nick}] 일별주문체결 조회 오류 [{company}]: {str(e)}")

            threads_52 = []
            for nick in target_nicks:
                if nick is not None:
                    ac = account(nick)
                    t_acct_no = ac['acct_no']
                    t_access_token = ac['access_token']
                    t_app_key = ac['app_key']
                    t_app_secret = ac['app_secret']
                else:
                    t_acct_no = acct_no
                    t_access_token = access_token
                    t_app_key = app_key
                    t_app_secret = app_secret
                t = threading.Thread(target=process_nick_52, args=(nick if nick is not None else arguments[1], t_acct_no, t_access_token, t_app_key, t_app_secret))
                threads_52.append(t)
                t.start()
            for t in threads_52:
                t.join()
        
        elif menuNum == '61':
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

                    ord_rsv_price_61 = int(commandBot[2])
                    ord_rsv_qty_61 = int(commandBot[3])
                    ord_rsv_end_dt_61 = commandBot[4]
                    trade_dvsn_61 = commandBot[1]
                    target_nicks = g_selected_accounts if g_selected_accounts else [None]

                    def process_nick_61(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                        if trade_dvsn_61 == '1':
                            # 매수예약
                            buy_expect_sum = ord_rsv_price_61 * ord_rsv_qty_61
                            print("매수예정금액 : " + format(int(buy_expect_sum), ',d'))
                            b = inquire_psbl_order(t_access_token, t_app_key, t_app_secret, t_acct_no)
                            print("매수 가능(현금) : " + format(int(b), ',d'))

                            if int(b) > int(buy_expect_sum):
                                try:
                                    rsv_ord_result = order_reserve(t_access_token, t_app_key, t_app_secret, str(t_acct_no), code, str(ord_rsv_qty_61), str(ord_rsv_price_61), "02", "01" if ord_rsv_price_61 == 0 else "00", ord_rsv_end_dt_61)
                                    if rsv_ord_result['RSVN_ORD_SEQ'] != "":
                                        context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 예약주문번호 : <code>{rsv_ord_result['RSVN_ORD_SEQ']}</code> 예약매수주문", parse_mode='HTML')
                                    else:
                                        context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 예약매수주문 실패")
                                except Exception as e:
                                    print('예약매수주문 오류.', e)
                                    context.bot.send_message(chat_id=user_id, text=f"[{nick}][{code}] [예약매수주문 오류] - {str(e)}")
                            else:
                                context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 예약매수 가능(현금) : {format(int(b) - int(buy_expect_sum), ',d')}원 부족")

                        elif trade_dvsn_61 == '2':
                            # 매도예약
                            e = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")
                            ord_psbl_qty = 0
                            for j, _ in enumerate(e.index):
                                if e['pdno'][j] == code:
                                    ord_psbl_qty = int(e['ord_psbl_qty'][j])
                            print("주문가능수량 : " + format(ord_psbl_qty, ',d'))

                            if ord_psbl_qty > 0:
                                if ord_psbl_qty >= ord_rsv_qty_61:
                                    try:
                                        rsv_ord_result = order_reserve(t_access_token, t_app_key, t_app_secret, str(t_acct_no), code, str(ord_rsv_qty_61), str(ord_rsv_price_61), "01", "01" if ord_rsv_price_61 == 0 else "00", ord_rsv_end_dt_61)
                                        if rsv_ord_result['RSVN_ORD_SEQ'] != "":
                                            context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 예약주문번호 : <code>{rsv_ord_result['RSVN_ORD_SEQ']}</code> 예약매도주문", parse_mode='HTML')
                                        else:
                                            context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 예약매도주문 실패")
                                    except Exception as e:
                                        print('예약매도주문 오류.', e)
                                        context.bot.send_message(chat_id=user_id, text=f"[{nick}][{code}] [예약매도주문 오류] - {str(e)}")
                                else:
                                    context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 예약수량({format(ord_rsv_qty_61, ',d')}주)이 주문가능수량({format(ord_psbl_qty, ',d')}주)보다 커서 예약매도주문 불가")
                            else:
                                context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 주문가능수량이 없어 예약매도주문 불가")

                    threads_61 = []
                    for nick in target_nicks:
                        if nick is not None:
                            ac = account(nick)
                            t_acct_no = ac['acct_no']
                            t_access_token = ac['access_token']
                            t_app_key = ac['app_key']
                            t_app_secret = ac['app_secret']
                        else:
                            t_acct_no = acct_no
                            t_access_token = access_token
                            t_app_key = app_key
                            t_app_secret = app_secret
                        t = threading.Thread(target=process_nick_61, args=(nick if nick is not None else arguments[1], t_acct_no, t_access_token, t_app_key, t_app_secret))
                        threads_61.append(t)
                        t.start()
                    for t in threads_61:
                        t.join()

                else:
                    print("단가, 수량, 예약종료일-8자리(YYYYMMDD) 미존재 또는 부적합")
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 단가, 수량, 예약종료일-8자리(YYYYMMDD) 미존재 또는 부적합")

        elif menuNum == '62':
            initMenuNum()
            # 입력 형식: 종목코드(종목명), 정정가, 예약종료일
            parts62 = user_text.split(',', 2)
            if len(parts62) < 3 or not parts62[1].strip().isdecimal() or len(parts62[2].strip()) != 8 or not parts62[2].strip().isdigit():
                context.bot.send_message(chat_id=user_id, text=f"입력 형식 오류 [{company}] - 종목코드(종목명), 정정가(시장가:0), 예약종료일-8자리(YYYYMMDD)")
            else:
                ord_rsv_price_62 = int(parts62[1].strip())
                ord_rsv_end_dt_62 = parts62[2].strip()
                target_nicks = g_selected_accounts if g_selected_accounts else [None]

                def process_nick_62(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                    now = datetime.now()
                    start_date = (now + timedelta(days=1)).strftime("%Y%m%d") if now > now.replace(hour=15, minute=40, second=0, microsecond=0) else now.strftime("%Y%m%d")
                    try:
                        output = order_reserve_complete(t_access_token, t_app_key, t_app_secret, start_date, ord_rsv_end_dt_62, str(t_acct_no), "")
                        if not output:
                            context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 예약정보 미존재")
                            return
                        d = pd.DataFrame(output)
                        # 해당 종목 + 미취소 건만 필터
                        matched = d[(d['pdno'] == code) & (d['cncl_ord_dt'] == "")]
                        if matched.empty:
                            context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 미처리 예약주문 없음")
                            return
                        for _, row in matched.iterrows():
                            ord_rsv_no = str(int(row['rsvn_ord_seq']))
                            d_ord_rsvn_qty = int(row['ord_rsvn_qty'])
                            d_sll_buy_dvsn_cd = row['sll_buy_dvsn_cd']
                            d_ord_dvsn_name = row['ord_dvsn_name']
                            try:
                                rsv_ord_result = order_reserve_cancel_revice(t_access_token, t_app_key, t_app_secret, str(t_acct_no), "01", code, str(d_ord_rsvn_qty), ord_rsv_price_62, d_sll_buy_dvsn_cd, "01" if ord_rsv_price_62 == 0 else "00", ord_rsv_end_dt_62, ord_rsv_no)
                                if rsv_ord_result['NRML_PRCS_YN'] == "Y":
                                    context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}-{d_ord_dvsn_name}] 예약번호:{ord_rsv_no} 정정가:{format(ord_rsv_price_62, ',d')}원 예약주문정정")
                                else:
                                    context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}-{d_ord_dvsn_name}] 예약번호:{ord_rsv_no} 예약주문정정 실패")
                            except Exception as e:
                                context.bot.send_message(chat_id=user_id, text=f"[{nick}][{code}] 예약주문정정 오류: {str(e)}")
                    except Exception as e:
                        context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 예약조회 오류: {str(e)}")

                threads_62 = []
                for nick in target_nicks:
                    if nick is not None:
                        ac = account(nick)
                        t_acct_no = ac['acct_no']
                        t_access_token = ac['access_token']
                        t_app_key = ac['app_key']
                        t_app_secret = ac['app_secret']
                    else:
                        t_acct_no = acct_no
                        t_access_token = access_token
                        t_app_key = app_key
                        t_app_secret = app_secret
                    t = threading.Thread(target=process_nick_62, args=(nick if nick is not None else arguments[1], t_acct_no, t_access_token, t_app_key, t_app_secret))
                    threads_62.append(t)
                    t.start()
                for t in threads_62:
                    t.join()

        elif menuNum == '63':
            initMenuNum()
            target_nicks = g_selected_accounts if g_selected_accounts else [None]

            def process_nick_63(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                now = datetime.now()
                reserve_strt_dt = (now + timedelta(days=1)).strftime("%Y%m%d") if now > now.replace(hour=15, minute=40, second=0, microsecond=0) else now.strftime("%Y%m%d")
                reserve_end_dt = (now + relativedelta(months=1)).strftime("%Y%m%d")
                try:
                    output = order_reserve_complete(t_access_token, t_app_key, t_app_secret, reserve_strt_dt, reserve_end_dt, str(t_acct_no), "")
                    if not output:
                        context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 예약정보 미존재")
                        return
                    d = pd.DataFrame(output)
                    # 해당 종목 + 미취소 건만 필터
                    matched = d[(d['pdno'] == code) & (d['cncl_ord_dt'] == "")]
                    if matched.empty:
                        context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 미처리 예약주문 없음")
                        return
                    for _, row in matched.iterrows():
                        ord_rsv_no = str(int(row['rsvn_ord_seq']))
                        d_ord_rsvn_qty = int(row['ord_rsvn_qty'])
                        d_ord_rsvn_unpr = int(row['ord_rsvn_unpr'])
                        d_sll_buy_dvsn_cd = row['sll_buy_dvsn_cd']
                        d_rsvn_end_dt = row['rsvn_end_dt']
                        d_ord_dvsn_name = row['ord_dvsn_name']
                        try:
                            rsv_ord_result = order_reserve_cancel_revice(t_access_token, t_app_key, t_app_secret, str(t_acct_no), "02", code, str(d_ord_rsvn_qty), d_ord_rsvn_unpr, d_sll_buy_dvsn_cd, "01" if d_ord_rsvn_unpr == 0 else "00", d_rsvn_end_dt, ord_rsv_no)
                            if rsv_ord_result['NRML_PRCS_YN'] == "Y":
                                context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}-{d_ord_dvsn_name}] 예약번호:{ord_rsv_no} 예약주문철회")
                            else:
                                context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}-{d_ord_dvsn_name}] 예약번호:{ord_rsv_no} 예약주문철회 실패")
                        except Exception as e:
                            context.bot.send_message(chat_id=user_id, text=f"[{nick}][{code}] 예약주문철회 오류: {str(e)}")
                except Exception as e:
                    context.bot.send_message(chat_id=user_id, text=f"[{nick}][{company}] 예약조회 오류: {str(e)}")

            threads_63 = []
            for nick in target_nicks:
                if nick is not None:
                    ac = account(nick)
                    t_acct_no = ac['acct_no']
                    t_access_token = ac['access_token']
                    t_app_key = ac['app_key']
                    t_app_secret = ac['app_secret']
                else:
                    t_acct_no = acct_no
                    t_access_token = access_token
                    t_app_key = app_key
                    t_app_secret = app_secret
                t = threading.Thread(target=process_nick_63, args=(nick if nick is not None else arguments[1], t_acct_no, t_access_token, t_app_key, t_app_secret))
                threads_63.append(t)
                t.start()
            for t in threads_63:
                t.join()               

        elif menuNum == '71':
            initMenuNum()
            parts71 = user_text.split(',', 4)
            if len(parts71) < 5 or not parts71[1].strip().isdecimal() or not parts71[2].strip().isdecimal() or not parts71[3].strip().isdecimal() or not parts71[4].strip().isdecimal():
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액 미존재 또는 부적합")
            else:
                buy_price_71 = int(stck_prpr) if parts71[1].strip() == '0' else int(parts71[1].strip())
                loss_price_71 = int(stck_lwpr) if parts71[2].strip() == '0' else int(parts71[2].strip())
                input_buy_amt_71 = int(parts71[3].strip())    # 입력 매수금액
                item_loss_sum_71 = int(parts71[4].strip())    # 입력 손절금액

                if buy_price_71 <= loss_price_71:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(" + format(buy_price_71, ',d') + ")가 이탈가(" + format(loss_price_71, ',d') + ") 이하입니다.")
                else:
                    # 공통 손절율
                    loss_rate_71 = round((100 - (loss_price_71 / buy_price_71) * 100) * -1, 2)

                    # ① 손절금액 기준
                    loss_buy_qty_71 = int(round(item_loss_sum_71 / (buy_price_71 - loss_price_71)))
                    loss_buy_amt_71 = buy_price_71 * loss_buy_qty_71

                    # ② 매수금액 기준
                    amt_buy_qty_71 = int(round(input_buy_amt_71 / buy_price_71))
                    amt_buy_amt_71 = buy_price_71 * amt_buy_qty_71
                    amt_item_loss_71 = (buy_price_71 - loss_price_71) * amt_buy_qty_71

                    # 진행 콜백에서 사용할 전역 상태 저장
                    global g_trail71_code, g_trail71_company, g_trail71_buy_price, g_trail71_loss_price
                    global g_trail71_item_loss_sum, g_trail71_buy_qty, g_trail71_buy_amt
                    global g_trail71_year_day, g_trail71_hour_minute
                    global g_trail71_loss_buy_qty, g_trail71_loss_buy_amt
                    global g_trail71_amt_buy_qty, g_trail71_amt_buy_amt
                    g_trail71_code = code
                    g_trail71_company = company
                    g_trail71_buy_price = buy_price_71
                    g_trail71_loss_price = loss_price_71
                    g_trail71_item_loss_sum = item_loss_sum_71
                    g_trail71_buy_qty = 0
                    g_trail71_buy_amt = 0
                    g_trail71_loss_buy_qty = loss_buy_qty_71
                    g_trail71_loss_buy_amt = loss_buy_amt_71
                    g_trail71_amt_buy_qty = amt_buy_qty_71
                    g_trail71_amt_buy_amt = amt_buy_amt_71
                    g_trail71_year_day = datetime.now().strftime("%Y%m%d")
                    g_trail71_hour_minute = datetime.now().strftime('%H%M%S')

                    selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "현재계좌"
                    preview_text = (
                        "[선택계좌: " + selected_str + "]\n"
                        "[" + company + "(<code>" + code + "</code>)]\n"
                        "매수가: " + format(buy_price_71, ',d') + "원 | 이탈가: " + format(loss_price_71, ',d') + "원 | 손절율: " + str(loss_rate_71) + "%\n"
                        "─────────────────\n"
                        "  손절금액 기준\n"
                        "  매수금액: " + format(loss_buy_amt_71, ',d') + "원 | 매수량: " + format(loss_buy_qty_71, ',d') + "주 | 손실금액: " + format(item_loss_sum_71, ',d') + "원\n"
                        "─────────────────\n"
                        "  매수금액 기준\n"
                        "  매수금액: " + format(amt_buy_amt_71, ',d') + "원 | 매수량: " + format(amt_buy_qty_71, ',d') + "주 | 손실금액: " + format(amt_item_loss_71, ',d') + "원"
                    )
                    button_list = build_button(["손절금액", "매수금액", "다시계산", "취소"], "trail71")
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, 2))
                    context.bot.send_message(chat_id=user_id, text=preview_text, reply_markup=show_markup, parse_mode='HTML')         

        elif menuNum == '72':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=6)
                print("commandBot[1] : ", commandBot[1])    # 매도가(현재가:0)
                print("commandBot[2] : ", commandBot[2])    # 이탈가(저가:0)
                print("commandBot[3] : ", commandBot[3])    # 비중(%)

            # 매수가(현재가:0), 이탈가(저가:0), 비중(%) 존재시
            if commandBot[1].isdecimal() and commandBot[2].isdecimal() and is_positive_int(commandBot[3]):
                year_day = datetime.now().strftime("%Y%m%d")                                                # 날짜-8자리(YYYYMMDD, 현재일자:0)
                hour_minute = datetime.now().strftime('%H%M%S')                                             # 시간-6자리(HHMMSS, 현재일시:0)
                sell_price = int(stck_prpr) if commandBot[1] == '0' else int(commandBot[1])                 # 매도가(현재가:0)
                loss_price = int(stck_lwpr) if commandBot[2] == '0' else int(commandBot[2])                 # 이탈가(저가:0)
                sell_rate = int(commandBot[3])                                                              # 비중(%)
                
                target_nicks = g_selected_accounts if g_selected_accounts else [None]

                def process_nick_72(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                    # 계좌잔고 조회
                    c = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")

                    hold_price = 0
                    hldg_qty = 0
                    hold_amt = 0

                    for i, _ in enumerate(c.index):
                        if code == c['pdno'][i]:
                            hold_price = float(c['pchs_avg_pric'][i])
                            hldg_qty = int(c['hldg_qty'][i])
                            hold_amt = int(c['pchs_amt'][i])

                    # 매매추적 보유가, 보유수량, 추적유형 조회 (스레드별 별도 DB 연결)
                    thread_conn = db.connect(conn_string)
                    try:
                        sell_qty = int(hldg_qty * sell_rate * 0.01)

                        if sell_qty > 0:
                            try:
                                with thread_conn.cursor() as cur:

                                    # 보유가
                                    base_price = int(hold_price)
                                    # 보유량 (신규 매수 시 hldg_qty=0이므로 fallback 없이 그대로 사용)
                                    base_qty = hldg_qty
                                    # 보유금액 (신규 매수 시 hold_amt=0이므로 fallback 없이 그대로 사용)
                                    base_amt = hold_amt
                                    
                                    sell_amt = int(sell_price * sell_qty)
                                    loss_amt = int((base_price - loss_price) * hldg_qty)

                                    merge_query = """
                                        WITH ins AS (
                                            INSERT INTO trading_trail (
                                                acct_no,
                                                code,
                                                name,
                                                trail_day,
                                                trail_dtm,
                                                trail_tp,
                                                stop_price,
                                                target_price,
                                                trail_plan,
                                                basic_price,
                                                basic_qty,
                                                basic_amt,
                                                proc_min,
                                                trade_tp,
                                                exit_price,
                                                loss_amt,
                                                crt_dt,
                                                mod_dt
                                            )
                                            SELECT
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                            RETURNING 1 AS flag
                                        )
                                        SELECT flag FROM ins;
                                        """
                                    cur.execute(merge_query, (
                                        t_acct_no, code, company, year_day, hour_minute, "1", loss_price, sell_price, sell_rate, base_price, base_qty, base_amt, hour_minute, 'M', loss_price, loss_amt, datetime.now(), datetime.now()
                                    ))
                                    was_updated = cur.fetchone() is not None
                                    thread_conn.commit()

                                    if was_updated:
                                        context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 보유가 : " + format(base_price, ',d') + "원, 보유량 : " + format(base_qty, ',d') + "주, 보유금액 : " + format(base_amt, ',d') + "원, 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도금액 : " + format(sell_amt, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 손실금액 : " + format(loss_amt, ',d') + "원 매매추적 처리", parse_mode='HTML')
                                    else:
                                        context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도금액 : " + format(sell_amt, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원 매매추적 미처리")
                            
                            except Exception as e:
                                thread_conn.rollback()
                                print(f"Error 발생: {e}")
                                context.bot.send_message(chat_id=user_id, text=f"-{nick}- 처리 중 오류가 발생했습니다: {e}")   

                        else:
                            context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "] 보유수량 부족 미처리")                                                                     

                    finally:
                        thread_conn.close()

                threads_72 = []
                for nick in target_nicks:
                    if nick is not None:
                        ac = account(nick)
                        t_acct_no = ac['acct_no']
                        t_access_token = ac['access_token']
                        t_app_key = ac['app_key']
                        t_app_secret = ac['app_secret']
                    else:
                        t_acct_no = acct_no
                        t_access_token = access_token
                        t_app_key = app_key
                        t_app_secret = app_secret
                    t = threading.Thread(target=process_nick_72, args=(nick if nick is not None else arguments[1], t_acct_no, t_access_token, t_app_key, t_app_secret))
                    threads_72.append(t)
                    t.start()
                for t in threads_72:
                    t.join()

            else:
                print("매도가(현재가:0), 이탈가(저가:0), 비중(%) 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가(현재가:0), 이탈가(저가:0), 비중(%) 미존재 또는 부적합")         

        elif menuNum == '81':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=6)
                print("commandBot[1] : ", commandBot[1])    # 매도가(현재가:0)
                print("commandBot[2] : ", commandBot[2])    # 이탈가(저가:0)
                print("commandBot[3] : ", commandBot[3])    # 비중(%)

            # 매도가(현재가:0), 이탈가(저가:0), 비중(%) 존재시
            if commandBot[1].isdecimal() and commandBot[2].isdecimal() and is_positive_int(commandBot[3]):
                year_day = datetime.now().strftime("%Y%m%d")                                                # 날짜-8자리(YYYYMMDD, 현재일자:0)
                hour_minute = datetime.now().strftime('%H%M%S')                                             # 시간-6자리(HHMMSS, 현재일시:0)
                sell_price = int(stck_prpr) if commandBot[1] == '0' else int(commandBot[1])                 # 매도가(현재가:0)
                loss_price = int(stck_lwpr) if commandBot[2] == '0' else int(commandBot[2])                 # 이탈가(저가:0)
                sell_rate = int(commandBot[3])                                                              # 비중(%)
                target_nicks = g_selected_accounts if g_selected_accounts else [None]

                def process_nick_81(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                    # 계좌잔고 조회
                    c = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")

                    hold_price = 0
                    hldg_qty = 0

                    for i, _ in enumerate(c.index):
                        if code == c['pdno'][i]:
                            hold_price = float(c['pchs_avg_pric'][i])
                            hldg_qty = int(c['hldg_qty'][i])

                    # 매매추적 보유가, 보유수량, 추적유형 조회 (스레드별 별도 DB 연결)
                    thread_conn = db.connect(conn_string)
                    try:
                        sell_qty = int(hldg_qty * sell_rate * 0.01)

                        if sell_qty > 0:
                            try:
                                with thread_conn.cursor() as cur:
                                    # 매매추적 update
                                    update_query1 = """
                                        UPDATE trading_trail tt SET
                                            trail_dtm = %s, trail_tp = %s, trail_plan = %s, stop_price = %s, target_price = %s, proc_min = %s, trail_price = NULL, trail_rate = NULL, trail_qty = NULL, trail_amt = NULL, volumn = NULL, mod_dt = %s
                                        WHERE acct_no = %s
                                        AND code = %s
                                        AND trail_day = %s
                                        AND trail_tp NOT IN ('4', 'Y')
                                        RETURNING 1;
                                        """
                                    cur.execute(update_query1, (hour_minute, "1", str(sell_rate), loss_price, sell_price, hour_minute, datetime.now(), t_acct_no, code, year_day))
                                    was_updated1 = cur.fetchone() is not None

                                    if was_updated1:
                                        thread_conn.commit()
                                        context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 저가 : " + format(int(stck_lwpr), ',d') + "원, 고가 : " + format(int(stck_hgpr), ',d') + "원, 보유가 : " + format(int(hold_price), ',d') + "원, 보유량 : " + format(hldg_qty, ',d') + "주, 매도가 : " + format(sell_price, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 추적변경 처리", parse_mode='HTML')
                                    else:
                                        context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "] 매도가 : " + format(sell_price, ',d') + "원, 이탈가 : " + format(loss_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도비율(%) : " + str(sell_rate) + "% 추적변경 미처리")

                            except Exception as e:
                                thread_conn.rollback()
                                print(f"Error 발생: {e}")
                                context.bot.send_message(chat_id=user_id, text=f"-{nick}- 처리 중 오류가 발생했습니다: {e}")
                        else:
                            context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "] 매도가 : " + format(sell_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도량 부족 미처리")
                    finally:
                        thread_conn.close()

                threads_81 = []
                for nick in target_nicks:
                    if nick is not None:
                        ac = account(nick)
                        t_acct_no = ac['acct_no']
                        t_access_token = ac['access_token']
                        t_app_key = ac['app_key']
                        t_app_secret = ac['app_secret']
                    else:
                        t_acct_no = acct_no
                        t_access_token = access_token
                        t_app_key = app_key
                        t_app_secret = app_secret
                    t = threading.Thread(target=process_nick_81, args=(nick if nick is not None else arguments[1], t_acct_no, t_access_token, t_app_key, t_app_secret))
                    threads_81.append(t)
                    t.start()
                for t in threads_81:
                    t.join()

            else:
                print("매도가(현재가:0), 이탈가(저가:0), 비중(%) 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매도가(현재가:0), 이탈가(저가:0), 비중(%) 미존재 또는 부적합")                         

        elif menuNum == '91':
            initMenuNum()
            if len(user_text.split(",")) > 0:
                
                commandBot = user_text.split(sep=',', maxsplit=5)
                print("commandBot[1] : ", commandBot[1])    # 매수가(현재가:0)
                print("commandBot[2] : ", commandBot[2])    # 이탈가(저가:0)
                print("commandBot[3] : ", commandBot[3])    # 매수금액
                print("commandBot[4] : ", commandBot[4])    # 손절금액

            # 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액 존재시
            if commandBot[1].isdecimal() and commandBot[2].isdecimal() and commandBot[3].isdecimal() and commandBot[4].isdecimal():
                buy_price = int(stck_prpr) if commandBot[1] == '0' else int(commandBot[1])                  # 매수가(현재가:0)
                loss_price = int(stck_lwpr) if commandBot[2] == '0' else int(commandBot[2])                 # 이탈가(저가:0)
                input_buy_amt = commandBot[3]   # 매수금액
                item_loss_sum = commandBot[4]   # 손절금액

                if buy_price <= loss_price:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(" + format(buy_price, ',d') + ")가 이탈가(" + format(loss_price, ',d') + ") 이하입니다.")
                else:
                    # 공통 손절율
                    loss_rate = round((100 - (loss_price / buy_price) * 100) * -1, 2)

                    # ① 손절금액 기준
                    loss_buy_qty = int(round(item_loss_sum / (buy_price - loss_price)))
                    loss_buy_amt = buy_price * loss_buy_qty

                    # ② 매수금액 기준
                    amt_buy_qty = int(round(input_buy_amt / buy_price))
                    amt_buy_amt = buy_price * amt_buy_qty
                    amt_item_loss = (buy_price - loss_price) * amt_buy_qty

                    preview_text = (
                        "[" + company + "(<code>" + code + "</code>)]\n"
                        "매수가: " + format(buy_price, ',d') + "원 | 이탈가: " + format(loss_price, ',d') + "원 | 손절율: " + str(loss_rate) + "%\n"
                        "─────────────────\n"
                        "  손절금액 기준\n"
                        "  매수금액: " + format(loss_buy_amt, ',d') + "원 | 매수량: " + format(loss_buy_qty, ',d') + "주 | 손실금액: " + format(item_loss_sum, ',d') + "원\n"
                        "─────────────────\n"
                        "  매수금액 기준\n"
                        "  매수금액: " + format(amt_buy_amt, ',d') + "원 | 매수량: " + format(amt_buy_qty, ',d') + "주 | 손실금액: " + format(amt_item_loss, ',d') + "원"
                    )
                    context.bot.send_message(chat_id=user_id, text=preview_text, reply_markup=show_markup, parse_mode='HTML')        
                    
                    # 매수 가능(현금) 조회
                    b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                    print("매수 가능(현금) : " + format(int(b), ',d'))

                    if int(b) < int(loss_buy_amt):  # 매수가능(현금)이 손절매수금액보다 작은 경우
                        context.bot.send_message(chat_id=user_id, text="[" + company + "{<code>"+code+"</code>}] 매수가 : " + format(buy_price, ',d') + "원, 손절가 : " + format(loss_price, ',d') + "원, 손절매수금액 : " + format(loss_buy_amt, ',d') + "원, 매수량 : " + format(loss_buy_qty, ',d') + "주, 손절율 : " + str(loss_rate) + "% 매수금액 : " + format(loss_buy_amt - int(b), ',d') +"원 부족", parse_mode='HTML')
                    if int(b) < int(amt_buy_amt):  # 매수가능(현금)이 매수금액보다 작은 경우
                        context.bot.send_message(chat_id=user_id, text="[" + company + "{<code>"+code+"</code>}] 매수가 : " + format(buy_price, ',d') + "원, 손절가 : " + format(loss_price, ',d') + "원, 매수금액 : " + format(amt_buy_amt, ',d') + "원, 매수량 : " + format(loss_buy_qty, ',d') + "주, 손절율 : " + str(loss_rate) + "% 매수금액 : " + format(amt_buy_amt - int(b), ',d') +"원 부족", parse_mode='HTML')                        
            
            else:
                print("매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액 미존재 또는 부적합")
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액 미존재 또는 부적합")

        elif menuNum == '92':
            initMenuNum()
            commandBot = user_text.split(sep=',', maxsplit=3)
            if len(commandBot) < 3 or not commandBot[1].strip().isdecimal() or not commandBot[2].strip().isdecimal() or not (1 <= int(commandBot[2].strip()) <= 100):
                context.bot.send_message(chat_id=user_id, text=f"[{company}] 매도가(현재가:0) 정수 또는 매도 비율은 1~100 사이 정수로 입력하세요.")
            else:
                sell_price_92 = int(stck_prpr) if commandBot[1].strip() == '0' else int(commandBot[1].strip())
                sell_ratio_92 = int(commandBot[2].strip())
                cb_code_92 = code
                cb_name_92 = company
                target_nicks_92 = g_selected_accounts[:] if g_selected_accounts else [None]

                def process_nick_92(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                    try:
                        # 보유 수량 조회
                        c_bal = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")
                        t_hldg_qty = 0
                        for i, _ in enumerate(c_bal.index):
                            if c_bal['pdno'][i] == cb_code_92:
                                t_hldg_qty = int(c_bal['hldg_qty'][i])
                                break
                        if t_hldg_qty == 0:
                            context.bot.send_message(chat_id=user_id, text=f"-{nick}-[{cb_name_92}(<code>{cb_code_92}</code>)] 보유 수량 없음", parse_mode='HTML')
                            return
                        t_sell_qty = max(1, int(t_hldg_qty * sell_ratio_92 / 100))
                        c_ord = order_cash(False, t_access_token, t_app_key, t_app_secret, str(t_acct_no),
                                           cb_code_92, "00", str(t_sell_qty), str(sell_price_92))
                        if c_ord is not None and c_ord['ODNO'] != "":
                            time.sleep(0.5)
                            output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret, t_acct_no, cb_code_92, c_ord['ODNO'])
                            tdf = pd.DataFrame(output1)
                            tdf.set_index('odno')
                            d = tdf[['odno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'tot_ccld_qty', 'tot_ccld_amt', 'rmn_qty']]
                            for k, _ in enumerate(d.index):
                                d_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                                d_qty = d['ord_qty'][k]
                                d_no = int(d['odno'][k])
                                context.bot.send_message(
                                    chat_id=user_id,
                                    text=f"-{nick}-[{cb_name_92}(<code>{cb_code_92}</code>)] "
                                         f"매도가:{format(int(d_price), ',d')}원 | 매도량:{format(int(d_qty), ',d')}주 "
                                         f"({sell_ratio_92}%) 매도주문 완료, 주문번호:<code>{d_no}</code>",
                                    parse_mode='HTML'
                                )
                        else:
                            context.bot.send_message(
                                chat_id=user_id,
                                text=f"-{nick}-[{cb_name_92}(<code>{cb_code_92}</code>)] "
                                     f"매도가:{format(sell_price_92, ',d')}원 | 매도량:{format(t_sell_qty, ',d')}주 매도주문 실패",
                                parse_mode='HTML'
                            )
                    except Exception as e:
                        context.bot.send_message(chat_id=user_id, text=f"-{nick}-[{cb_name_92}(<code>{cb_code_92}</code>)] 매도주문 오류: {str(e)}", parse_mode='HTML')

                threads_92 = []
                for nick in target_nicks_92:
                    if nick is not None:
                        ac_92 = account(nick)
                        t_acct_no = ac_92['acct_no']
                        t_access_token = ac_92['access_token']
                        t_app_key = ac_92['app_key']
                        t_app_secret = ac_92['app_secret']
                        t_nick = nick
                    else:
                        t_acct_no = acct_no
                        t_access_token = access_token
                        t_app_key = app_key
                        t_app_secret = app_secret
                        t_nick = arguments[1]
                    t = threading.Thread(target=process_nick_92, args=(t_nick, t_acct_no, t_access_token, t_app_key, t_app_secret))
                    threads_92.append(t)
                    t.start()
                for t in threads_92:
                    t.join()

# 텔레그램봇 응답 처리
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, echo))
dispatcher.add_handler(CommandHandler("start", start))

# 텔레그램봇 polling
updater.start_polling()
