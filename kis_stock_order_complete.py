import psycopg2 as db
from datetime import datetime, timedelta
import kis_api_resp as resp
import requests
import json
from decimal import Decimal

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# PostgreSQL 연결 설정
# conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")

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

# 일별주문체결조회
def get_my_complete(access_token, app_key, app_secret, acct_no, code, order_no, strt_dt, end_dt):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "CTSC9215R",                            # (3개월이내) TTTC0081R (3개월이전) CTSC9215R
               "custtype": "P"}
    params = {
            'CANO': acct_no,                                    # 종합계좌번호 계좌번호 체계(8-2)의 앞 8자리
            'ACNT_PRDT_CD':"01",                                # 계좌상품코드 계좌번호 체계(8-2)의 뒤 2자리
            'SORT_DVSN': "01",                                  # 00: 최근 순, 01: 과거 순, 02: 최근 순
            # 'INQR_STRT_DT': datetime.now().strftime('%Y%m%d'),  # 조회시작일(8자리) 
            # 'INQR_END_DT': datetime.now().strftime('%Y%m%d'),   # 조회종료일(8자리)
            'INQR_STRT_DT': strt_dt,  # 조회시작일(8자리) 
            'INQR_END_DT': end_dt,   # 조회종료일(8자리)
            'SLL_BUY_DVSN_CD': "00",                            # 매도매수구분코드 00 : 전체 / 01 : 매도 / 02 : 매수
            'PDNO': code,                                       # 종목번호(6자리) ""공란입력 시, 전체
            'ORD_GNO_BRNO': "",                                 # 주문채번지점번호 ""공란입력 시, 전체
            'ODNO': order_no,                                   # 주문번호 ""공란입력 시, 전체
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
def inquire_period_profit_loss(access_token, app_key, app_secret, code, strt_dt, end_dt, acct_no):

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

def stock_order_comploete_proc(access_token, app_key, app_secret, acct_no, code, proc_day):

    balance_list = []

    # 일별 잔고종목 조회
    cur101 = conn.cursor()
    cur101.execute("""
        SELECT code, name, purchase_price, purchase_qty, current_price
        FROM public.dly_stock_balance
        WHERE acct = %s AND dt = %s AND code = %s
    """, (str(acct_no), code, proc_day))

    row = cur101.fetchone()
    cur101.close()

    if row and row[0] is not None:
        balance_list.append({
            '계좌번호': str(acct_no),
            '종목코드': row[0],
            '종목명': row[1],
            '보유단가': row[2],
            '보유수량': row[3],
            '현재가': row[4],
        })

    # 잔고정보 맵 설정 : 계좌번호, 종목명
    balance_map = {
        (item['계좌번호'], item['종목명']): item
        for item in balance_list
    }

    # 일별 주문 체결 조회
    order_complete_output = get_my_complete(access_token, app_key, app_secret, acct_no, code, "", proc_day, proc_day)

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

                # 현재일 최근 체결된 해당종목의 일별체결정보 조회
                cur302 = conn.cursor()
                cur302.execute("select paid_fee, profit_loss_amt, paid_tax from \"stockOrderComplete_stock_order_complete\" A where acct_no = '" + str(acct_no) + "' and name = '" + item['prdt_name'] + "' and order_dt = '" + proc_day + "' and total_complete_qty::int > 0")
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
                    period_profit_loss_sum_output = inquire_period_profit_loss(access_token, app_key, app_secret, item['pdno'], proc_day, proc_day, acct_no)

                    for item2 in period_profit_loss_sum_output:
            
                        pfls_rate = float(item2['pfls_rt'])
                        pfls_amt = float(item2['rlzt_pfls']) - last_pfls_amt
                        paid_tax = float(item2['tl_tax']) - last_paid_tax
                        paid_fee = float(item2['fee']) - last_paid_fee

                else:
                    # 기간별손익일별합산조회
                    period_profit_loss_sum_output = inquire_period_profit_loss(access_token, app_key, app_secret, item['pdno'], proc_day, proc_day, acct_no)

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
    , (str(acct_no), proc_day))
    result_400 = cur400.fetchall()
    cur400.close()

    # 읿별주문체결정보 맵 설정 : 계좌번호, 주문일자, 주문번호, 원주문번호의 체결량, 잔여량 
    order_commplete_map = {
        (str(acct_no), proc_day, str(int(row[0])), str(int(row[1])) if row[1] != "" else ""): (int(row[2]), int(row[3]))
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

                conn.commit()  

            else:
                # 주문(주문정정) 생성 후, 주문체결정보 현행화(1분단위)되기전에 전량 체결되어 잔고정보의 보유단가와 보유수량이 0 인 경우, 
                # 보유단가 = 체결금액 - 수익금액(세금 및 수수료 포함) / 체결수량
                if new_complete_qty > 0:
                    if item['원주문번호'] != "":
                        # 원주문번호의 일별체결정보 조회
                        cur401 = conn.cursor()
                        cur401.execute("select hold_price from \"stockOrderComplete_stock_order_complete\" A where acct_no = '" + str(acct_no) + "' and order_no = '" + item['원주문번호'] + "' and order_dt = '" + proc_day + "'")
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

                conn.commit()    
    
        cur600.close()        

ac = account('yh480825')
acct_no = ac['acct_no']
access_token = ac['access_token']
app_key = ac['app_key']
app_secret = ac['app_secret']
token = ac['bot_token1']

# 일별주문체결정보 생성 처리
stock_order_comploete_proc(access_token, app_key, app_secret, acct_no, '117670', '20250210')        