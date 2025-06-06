import time, copy
import yaml
import requests
import json

import pandas as pd

from collections import namedtuple
from datetime import datetime
import psycopg2 as db

with open(r'/home/terra/Public/Batch/kisdev_vi.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

_TRENV = tuple()
_last_auth_time = datetime.now()
_autoReAuth = False
_DEBUG = True
_isPaper = False

_base_headers = {
    "Content-Type": "application/json",
    "Accept": "text/plain",
    "charset": "UTF-8",
    'User-Agent': _cfg['my_agent']
}


def _getBaseHeader():
    if _autoReAuth: reAuth()
    return copy.deepcopy(_base_headers)


def _setTRENV(cfg):
    nt1 = namedtuple('KISEnv', ['my_app','my_sec','my_acct','my_phone', 'my_prod', 'my_token', 'my_url'])
    d = {
        'my_app': cfg['my_app'],
        'my_sec': cfg['my_sec'],
        'my_acct': cfg['my_acct'],
        'my_prod': cfg['my_prod'],
        'my_phone': cfg['my_phone'],
        'my_token': cfg['my_token'],
        'my_url' : cfg['my_url']
    }

    global _TRENV
    _TRENV = nt1(**d)

def isPaperTrading():
    return _isPaper

def changeTREnv(token_key, svr='vps', product='01'):
    cfg = dict()

    global _isPaper
    if svr == 'prod':
        ak1 = 'my_app'
        ak2 = 'my_sec'
        _isPaper = False
    elif svr == 'vps':
        ak1 = 'paper_app'
        ak2 = 'paper_sec'
        _isPaper = True

    cfg['my_app'] = _cfg[ak1]
    cfg['my_sec'] = _cfg[ak2]

    if svr == 'prod' and product == '01':
        cfg['my_acct'] = _cfg['my_acct_stock']
    elif svr == 'prod' and product == '03':
        cfg['my_acct'] = _cfg['my_acct_future']
    elif svr == 'vps' and product == '01':
        cfg['my_acct'] = _cfg['my_paper_stock']
    elif svr == 'vps' and product == '03':
        cfg['my_acct'] = _cfg['my_paper_future']

    cfg['my_prod'] = product
    cfg['my_phone'] = _cfg['my_phone']
    cfg['my_token'] = token_key
    cfg['my_url'] = _cfg[svr]

    _setTRENV(cfg)


def _getResultObject(json_data):
    _tc_ = namedtuple('res', json_data.keys())

    return _tc_(**json_data)

def auth(svr='prod', product='01'): # prod : ISA계좌, vps : 모의계좌

    p = {
        "grant_type": "client_credentials",
        "personalphone": _cfg['my_phone'],
        "personalname": '이승필'
    }

    if svr == 'prod':
        ak1 = 'my_app'
        ak2 = 'my_sec'
    elif svr == 'vps':
        ak1 = 'paper_app'
        ak2 = 'paper_sec'

    p["appkey"] = _cfg[ak1]
    p["appsecret"] = _cfg[ak2]


    #url = f'{_cfg[svr]}/oauth2/tokenP'

    #res = requests.post(url, data=json.dumps(p), headers=_getBaseHeader())
    # 조만간 위의 comment 한 URI 와 post 호출방식으로 바뀔 예정(0316)
    url = f'{_cfg[svr]}/oauth2/token'

    res = requests.post(url, params=p, headers=_getBaseHeader(), verify=False)
    rescode = res.status_code
    if rescode == 200:
        my_token = _getResultObject(res.json()).access_token
    else:
        print('Get Authentification token fail!\nYou have to restart your app!!!')
        return

    changeTREnv(f"Bearer {my_token}", svr, product)

    _base_headers["authorization"] = _TRENV.my_token
    _base_headers["appkey"] = _TRENV.my_app
    _base_headers["appsecret"] = _TRENV.my_sec

    global _last_auth_time
    _last_auth_time = datetime.now()

    acct_no = _cfg['my_acct_stock']

    # PostgreSQL 연결 설정
    conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
    # DB 연결
    conn = db.connect(conn_string)
    cur0 = conn.cursor()

    update_query = 'update "stockAccount_stock_account" set access_token=%s, token_publ_date=%s where acct_no=%s'
    # insert 인자값 설정
    record_to_update = ([my_token, _last_auth_time.strftime("%Y%m%d%H%M%S"), acct_no])
    # DB 연결된 커서의 쿼리 수행
    cur0.execute(update_query, record_to_update)
    conn.commit()

    if (_DEBUG):
        print(f'[{_last_auth_time}] => get AUTH Key completed!')

#end of initialize
def reAuth(svr='prod', product='01'):
    n2 = datetime.now()
    if (n2-_last_auth_time).seconds >= 86400:
        auth(svr, product)

def getEnv():
    return _cfg
def getTREnv():
    return _TRENV

#주문 API에서 사용할 hash key값을 받아 header에 설정해 주는 함수
# Input: HTTP Header, HTTP post param
# Output: None
def set_order_hash_key(h, p):

    url = f"{getTREnv().my_url}/uapi/hashkey"

    res = requests.post(url, data=json.dumps(p), headers=h, verify=False)
    rescode = res.status_code
    if rescode == 200:
        h['hashkey'] = _getResultObject(res.json()).HASH
    else:
        print("Error:", rescode)

# import hashlib
# def set_order_hash_key(h, p):
#     hashkey = hashlib.sha256(json.dumps(p).encode('utf-8')).hexdigest()
#     h['hashkey'] = hashkey

class APIResp:
    def __init__(self, resp):
        self._rescode = resp.status_code
        self._resp = resp
        self._header = self._setHeader()
        self._body = self._setBody()
        self._err_code = self._body.rt_cd
        self._err_message = self._body.msg1

    def getResCode(self):
        return self._rescode

    def _setHeader(self):
        fld = dict()
        for x in self._resp.headers.keys():
            if x.islower():
                fld[x] = self._resp.headers.get(x)
        _th_ =  namedtuple('header', fld.keys())

        return _th_(**fld)

    def _setBody(self):
        _tb_ = namedtuple('body', self._resp.json().keys())

        return  _tb_(**self._resp.json())

    def getHeader(self):
        return self._header

    def getBody(self):
        return self._body

    def getResponse(self):
        return self._resp

    def isOK(self):
        try:
            if(self.getBody().rt_cd == '0'):
                return True
            else:
                return False
        except:
            return False

    def getErrorCode(self):
        return self._err_code

    def getErrorMessage(self):
        return self._err_message

    def printAll(self):
        #print(self._resp.headers)
        print("<Header>")
        for x in self.getHeader()._fields:
            print(f'\t-{x}: {getattr(self.getHeader(), x)}')
        print("<Body>")
        for x in self.getBody()._fields:
            print(f'\t-{x}: {getattr(self.getBody(), x)}')

    def printError(self):
        print('-------------------------------\nError in response: ', self.getResCode())
        print(self.getBody().rt_cd, self.getErrorCode(), self.getErrorMessage())
        print('-------------------------------')

# end of class APIResp


########### API call wrapping

def _url_fetch(api_url, ptr_id, params, appendHeaders=None, postFlag=False, hashFlag=False):
    url = f"{getTREnv().my_url}{api_url}"

    headers = _getBaseHeader()

    #추가 Header 설정
    tr_id = ptr_id
    if ptr_id[0] in ('T', 'J', 'C'):
        if isPaperTrading():
            tr_id = 'V' + ptr_id[1:]

    headers["tr_id"] = tr_id
    headers["custtype"] = "P"

    if appendHeaders is not None:
        if len(appendHeaders) > 0:
            for x in appendHeaders.keys():
                headers[x] = appendHeaders.get(x)

    # if(_DEBUG):
    #     print(headers)
    #     print(params)

    if(_DEBUG):
        print("< Sending Info >")
        print(f"URL: {url}, TR: {tr_id}")
        print(f"<header>\n{headers}")
        print(f"<body>\n{params}")

    if (postFlag):
        if(hashFlag): set_order_hash_key(headers, params)
        res = requests.post(url, headers=headers, data=json.dumps(params), verify=False)
    else:
        res = requests.get(url, headers=headers, params=params, verify=False)

    if res.status_code == 200:
        ar = APIResp(res)
        if (_DEBUG): ar.printAll()
        return ar
    else:
        print("Error Code : " + str(res.status_code) + " | " + res.text)
        return None


# 계좌 잔고를 DataFrame 으로 반환
# Input: None (Option) rtCashFlag=True 면 예수금 총액을 반환하게 된다
# Output: DataFrame (Option) rtCashFlag=True 면 예수금 총액을 반환하게 된다

def get_acct_balance(rtCashFlag=False):
    url = '/uapi/domestic-stock/v1/trading/inquire-balance'
    tr_id = "TTTC8434R"

    params = {
        'CANO': getTREnv().my_acct,
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

    t1 = _url_fetch(url, tr_id, params)

    if rtCashFlag and t1.isOK():
        #r2 = t1.getBody().output2
        #return int(r2[0]['dnca_tot_amt'])
        return pd.DataFrame(t1.getBody().output2)

    output1 = t1.getBody().output1
    if t1.isOK() and output1:  #body 의 rt_cd 가 0 인 경우만 성공
        tdf = pd.DataFrame(output1)
        tdf.set_index('pdno')
        tdf[['pdno', 'prdt_name', 'thdt_buyqty', 'thdt_sll_qty', 'hldg_qty', 'ord_psbl_qty', 'pchs_avg_pric', 'pchs_amt', 'evlu_amt', 'evlu_pfls_amt', 'evlu_pfls_rt', 'prpr', 'bfdy_cprs_icdc', 'fltt_rt']]

        return tdf

    else:
        t1.printError()
        return pd.DataFrame()

def get_acct_balance_sell():
    url = '/uapi/domestic-stock/v1/trading/inquire-balance'
    tr_id = "TTTC8434R"

    params = {
        'CANO': getTREnv().my_acct,
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

    t1 = _url_fetch(url, tr_id, params)

    if t1.isOK():
        tdf = pd.DataFrame(t1.getBody().output1)
        tdf.set_index('pdno')
        return tdf[['pdno', 'prdt_name','hldg_qty', 'ord_psbl_qty', 'pchs_avg_pric', 'pchs_amt', 'evlu_amt', 'evlu_pfls_amt', 'evlu_pfls_rt', 'prpr', 'bfdy_cprs_icdc', 'fltt_rt']]
    else:
        t1.printError()
        return pd.DataFrame()

#종목의 주식, ETF, 선물/옵션 등의 구분값을 반환. 현재는 무조건 주식(J)만 반환
def _getStockDiv(stock_no):
    return 'J'

# 종목별 현재가를 dict 로 반환
# Input: 종목코드
# Output: 현재가 Info dictionary. 반환된 dict 가 len(dict) < 1 경우는 에러로 보면 됨

def get_current_price(stock_no):
    url = "/uapi/domestic-stock/v1/quotations/inquire-price"
    tr_id = "FHKST01010100"

    params = {
        'FID_COND_MRKT_DIV_CODE': _getStockDiv(stock_no),
        'FID_INPUT_ISCD': stock_no
        }

    t1 = _url_fetch(url, tr_id, params)

    if t1.isOK():
        return t1.getBody().output
    else:
        t1.printError()
        return dict()

# 주문 base function
# Input: 종목코드, 주문수량, 주문가격, Buy Flag(If True, it's Buy order)
# Output: HTTP Response

def do_order(stock_code, order_qty, order_price, prd_code="01", buy_flag=False, order_type="00"):

    url = "/uapi/domestic-stock/v1/trading/order-cash"

    if buy_flag:
        tr_id = "TTTC0802U"  #buy
    else:
        tr_id = "TTTC0801U"  #sell

    params = {
        'CANO': getTREnv().my_acct,
        'ACNT_PRDT_CD': prd_code,
        'PDNO': stock_code,
        'ORD_DVSN': order_type,
        'ORD_QTY': str(order_qty),
        'ORD_UNPR': str(order_price),
        'CTAC_TLNO': '',
        'SLL_TYPE': '01',
        'ALGO_NO': ''
        }

    t1 = _url_fetch(url, tr_id, params, postFlag=True, hashFlag=True)

    if t1.isOK():
        #return t1
        return t1.getBody().output
    else:
        t1.printError()
        # return None
        return dict()

# 사자 주문. 내부적으로는 do_order 를 호출한다.
# Input: 종목코드, 주문수량, 주문가격
# Output: True, False

def do_sell(stock_code, order_qty, order_price, prd_code="01", order_type="00"):
    t1 = do_order(stock_code, order_qty, order_price, buy_flag=False, order_type=order_type)
    return t1.isOK()

# 팔자 주문. 내부적으로는 do_order 를 호출한다.
# Input: 종목코드, 주문수량, 주문가격
# Output: True, False

def do_buy(stock_code, order_qty, order_price, prd_code="01", order_type="00"):
    t1 = do_order(stock_code, order_qty, order_price, buy_flag=True, order_type=order_type)
    return t1.isOK()

# 정정취소 가능한 주문 목록을 DataFrame 으로 반환
# Input: None
# Output: DataFrame

def get_orders(prd_code='01'):
    url = "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"

    tr_id = "TTTC8036R"

    params = {
        "CANO": getTREnv().my_acct,
        "ACNT_PRDT_CD": prd_code,
        "CTX_AREA_FK100": '',
        "CTX_AREA_NK100": '',
        "INQR_DVSN_1": '0',
        "INQR_DVSN_2": '0'
        }

    t1 = _url_fetch(url, tr_id, params)
    if t1.isOK():
        tdf = pd.DataFrame(t1.getBody().output)
        tdf.set_index('odno', inplace=True)
        tdf.drop('', inplace=True)
        cf1 = ['ord_dvsn_name', 'pdno', 'prdt_name', 'rvse_cncl_dvsn_name', 'ord_qty', 'ord_unpr', 'ord_tmd', 'tot_ccld_qty', 'psbl_qty', 'sll_buy_dvsn_cd','ord_dvsn_cd']
        cf2 = ['주문구분명', '종목코드', '종목명', '정정취소구분명', '주문수량', '주문단가', '주문시간', '총체결수량', '가능수량', '매도매수구분코드', '주문구분코드']
        tdf = tdf[cf1]
        ren_dict = dict(zip(cf1, cf2))

        return tdf.rename(columns=ren_dict)

    else:
        t1.printError()
        return pd.DataFrame()


# 특정 주문 취소(01)/정정(02)
# Input: 주문 번호(get_orders 를 호출하여 얻은 DataFrame 의 index  column 값이 취소 가능한 주문번호임)
#       주문점(통상 06010), 주문수량, 주문가격, 상품코드(01), 주문유형(00), 정정구분(취소-02, 정정-01)
# Output: APIResp object

def _do_cancel_revise(order_no, order_branch, order_qty, order_price, prd_code, order_dv, cncl_dv):
    url = "/uapi/domestic-stock/v1/trading/order-rvsecncl"

    tr_id = "TTTC0803U"

    params = {
        "CANO": getTREnv().my_acct,
        "ACNT_PRDT_CD": prd_code,
        "KRX_FWDG_ORD_ORGNO": order_branch,
        "ORGN_ODNO": order_no,
        "ORD_DVSN": order_dv,
        "RVSE_CNCL_DVSN_CD": cncl_dv, #취소(02)
        "ORD_QTY": str(order_qty),
        "ORD_UNPR": str(order_price),
        "QTY_ALL_ORD_YN": "Y"
    }

    t1 = _url_fetch(url, tr_id, params=params, postFlag=True)

    if t1.isOK():
        #return t1
        return t1.getBody().output
    else:
        t1.printError()
        #return None
        return dict()

# 특정 주문 취소
#
def do_cancel(order_no, order_qty, order_price, order_branch='06010', prd_code='01', order_dv='00', cncl_dv='02'):
    return _do_cancel_revise(order_no, order_branch, order_qty, order_price, prd_code, order_dv, cncl_dv)

# 특정 주문 정정
#
def do_revise(order_no, order_qty, order_price, order_branch='06010', prd_code='01', order_dv='00', cncl_dv='01'):
    return _do_cancel_revise(order_no, order_branch, order_qty, order_price, prd_code, order_dv, cncl_dv)

# 모든 주문 취소
# Input: None
# Output: None

def do_cancel_all():
    tdf = get_orders()
    od_list = tdf.index.to_list()
    qty_list = tdf['주문수량'].to_list()
    price_list = tdf['주문가격'].to_list()
    branch_list = tdf['주문점'].to_list()
    cnt = 0
    for x in od_list:
        ar = do_cancel(x, qty_list[cnt], price_list[cnt], branch_list[cnt])
        cnt += 1
        print(ar.getErrorCode(), ar.getErrorMessage())
        time.sleep(.2)



# 내 계좌의 일별 주문 체결 조회
# Input: 시작일, 종료일 (Option)지정하지 않으면 현재일
# output: DataFrame

def get_my_complete(sdt, edt=None, prd_code='01', zipFlag=True):
    url = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    tr_id = "TTTC8001R"

    if (edt is None):
        ltdt = datetime.now().strftime('%Y%m%d')
    else:
        ltdt = edt

    params = {
        "CANO": getTREnv().my_acct,
        "ACNT_PRDT_CD": prd_code,
        "INQR_STRT_DT": sdt,
        "INQR_END_DT": ltdt,
        "SLL_BUY_DVSN_CD": '00',
        "INQR_DVSN": '00',
        "PDNO": "",
        "CCLD_DVSN": "00",
        "ORD_GNO_BRNO": "",
        "ODNO":"",
        "INQR_DVSN_3": "00",
        "INQR_DVSN_1": "",
        "INQR_DVSN_2": "",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
     }

    t1 = _url_fetch(url, tr_id, params)

    #output1 과 output2 로 나뉘어서 결과가 옴. 지금은 output1만 DF 로 변환
    if t1.isOK():
        tdf = pd.DataFrame(t1.getBody().output1)
        tdf.set_index('odno')
        #tdf.drop('', inplace=True)
        if (zipFlag):
            return tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn','tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]
        else:
            return tdf
    else:
        t1.printError()
        return pd.DataFrame()


# 매수 가능(현금) 조회
# Input: None
# Output: 매수 가능 현금 액수
def get_buyable_cash(stock_code='', qry_price=0, prd_code='01'):
    url = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    tr_id = "TTTC8908R"

    params = {
        "CANO": getTREnv().my_acct,
        "ACNT_PRDT_CD": prd_code,
        "PDNO": stock_code,
        "ORD_UNPR": str(qry_price),  #이 값을 사용하기는 하나? 아니면 현재가를 알아서 사용하나?
        "ORD_DVSN": "02",
        "CMA_EVLU_AMT_ICLD_YN": "Y", #API 설명부분 수정 필요 (YN)
        "OVRS_ICLD_YN": "N"
     }

    t1 = _url_fetch(url, tr_id, params)

    if t1.isOK():
        return int(t1.getBody().output['ord_psbl_cash'])
    else:
        t1.printError()
        return 0


# 시세 Function

# 종목별 체결 Data (현재 기준 30개만 조회 가능 - 연속이 되야...)
# Input: 종목코드
# Output: 체결 Data DataFrame
# 주식체결시간, 주식현재가, 전일대비, 전일대비부호, 체결거래량, 당일 체결강도, 전일대비율
def get_stock_completed(stock_no):
    url = "/uapi/domestic-stock/v1/quotations/inquire-ccnl"

    tr_id = "FHKST01010300"

    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_no
    }

    t1 = _url_fetch(url, tr_id, params)

    if t1.isOK():
        return pd.DataFrame(t1.getBody().output)
    else:
        t1.printError()
        return pd.DataFrame()

# 종목별 history data (현재 기준 30개만 조회 가능 - 연속이 되야...)
# Input: 종목코드, 구분(D, W, M 기본값은 D)
# output: 시세 History DataFrame
def get_stock_history(stock_no, gb_cd='D'):
    url = "/uapi/domestic-stock/v1/quotations/inquire-daily-price"
    tr_id = "FHKST01010400"

    params = {
        "FID_COND_MRKT_DIV_CODE": _getStockDiv(stock_no),
        "FID_INPUT_ISCD": stock_no,
        "FID_PERIOD_DIV_CODE": gb_cd,
        "FID_ORG_ADJ_PRC": "0000000001"
    }

    t1 = _url_fetch(url, tr_id, params)

    if t1.isOK():
        return pd.DataFrame(t1.getBody().output)
    else:
        t1.printError()
        return pd.DataFrame()

# 종목별 history data 를 표준 OHLCV DataFrame 으로 반환
# Input: 종목코드, 구분(D, W, M 기본값은 D), (Option)adVar 을 True 로 설정하면
#        OHLCV 외에 inter_volatile 과 pct_change 를 추가로 반환한다.
# output: 시세 History OHLCV DataFrame
def get_stock_history_by_ohlcv(stock_no, gb_cd='D', adVar=False):
    hdf1 = get_stock_history(stock_no, gb_cd)

    chosend_fld = ['stck_bsop_date', 'stck_oprc', 'stck_hgpr', 'stck_lwpr', 'stck_clpr', 'acml_vol']
    renamed_fld = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

    hdf1 = hdf1[chosend_fld]
    ren_dict = dict()
    i = 0
    for x in chosend_fld:
        ren_dict[x] = renamed_fld[i]
        i += 1

    hdf1.rename(columns = ren_dict, inplace=True)
    hdf1[['Date']] = hdf1[['Date']].apply(pd.to_datetime)
    hdf1[['Open','High','Low','Close','Volume']] = hdf1[['Open','High','Low','Close','Volume']].apply(pd.to_numeric)
    hdf1.set_index('Date', inplace=True)

    if(adVar):
        hdf1['inter_volatile'] = (hdf1['High']-hdf1['Low'])/hdf1['Close']
        hdf1['pct_change'] = (hdf1['Close'] - hdf1['Close'].shift(-1))/hdf1['Close'].shift(-1) * 100


    return hdf1


# 투자자별 매매 동향
# Input: 종목코드
# output: 매매 동향 History DataFrame (Date, PerBuy, ForBuy, OrgBuy) 30개 row를 반환
def get_stock_investor(stock_no):
    url = "/uapi/domestic-stock/v1/quotations/inquire-investor"
    tr_id = "FHKST01010900"

    params = {
        "FID_COND_MRKT_DIV_CODE": _getStockDiv(stock_no),
        "FID_INPUT_ISCD": stock_no
    }

    t1 = _url_fetch(url, tr_id, params)

    if t1.isOK():
        hdf1 = pd.DataFrame(t1.getBody().output)

        chosend_fld = ['stck_bsop_date', 'prsn_ntby_qty', 'frgn_ntby_qty', 'orgn_ntby_qty']
        renamed_fld = ['Date', 'PerBuy', 'ForBuy', 'OrgBuy']

        hdf1 = hdf1[chosend_fld]
        ren_dict = dict()
        i = 0
        for x in chosend_fld:
            ren_dict[x] = renamed_fld[i]
            i += 1

        hdf1.rename(columns = ren_dict, inplace=True)
        hdf1[['Date']] = hdf1[['Date']].apply(pd.to_datetime)
        hdf1[['PerBuy','ForBuy','OrgBuy']] = hdf1[['PerBuy','ForBuy','OrgBuy']].apply(pd.to_numeric)
        hdf1['EtcBuy'] = (hdf1['PerBuy'] + hdf1['ForBuy'] + hdf1['OrgBuy']) * -1
        hdf1.set_index('Date', inplace=True)
        #sum을 맨 마지막에 추가하는 경우
        #tdf.append(tdf.sum(numeric_only=True), ignore_index=True) <- index를 없애고  만드는 경우
        #tdf.loc['Total'] = tdf.sum() <- index 에 Total 을 추가하는 경우
        return hdf1
    else:
        t1.printError()
        return pd.DataFrame()


#########################################
# 해외주식
#
#########################################

# 해외 종목별 현재가를 dict 로 반환
# Input: 종목 Symbol
# Output: 현재가 Info dictionary. 반환된 dict 가 len(dict) < 1 경우는 에러로 보면 됨
# 실시간조회종목코드, 소수점자리수, 전일종가, 전일거래량, 현재가, 대비기호, 대비, 등락율, 전체 거래량, 전체 거래금액, 매수가능여부

def get_current_price_OS(symbol):
    url = "/uapi/overseas-price/v1/quotations/price"
    tr_id = "HHDFS00000300"

    params = {
        "AUTH": "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD", #사용자권한정보
        "EXCD": "NAS", #거래소코드
        "SYMB": symbol
    }

    t1 = _url_fetch(url, tr_id, params)

    if t1.isOK():
        return t1.getBody().output
    else:
        t1.printError()
        return dict()


# 해외 종목별 history data (현재 기준 100개만 조회 가능 - 연속이 되야...)
# Input: 종목코드, 구분(D, W, M 기본값은 D)
# output: 시세 History DataFrame
def get_stock_history_OS(symbol, base_dt='0', gb_cd='0'):
    url = "/uapi/overseas-price/v1/quotations/dailyprice"
    tr_id = "HHDFS76240000"

    params = {
        "AUTH": "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD", #사용자권한정보
        "EXCD": "NAS", #거래소코드
        "SYMB": symbol,
        "GUBN": "",
        "BYMD": "",
        "MODP": "0",
        "KEYB": "20220222 000000"
    }

    t1 = _url_fetch(url, tr_id, params)

    if t1.isOK():
        return pd.DataFrame(t1.getBody().output2)
    else:
        t1.printError()
        return pd.DataFrame()






############################################
# Test
############################################

def do_order_OS():
    url = '/uapi/overseas-stock/v1/trading/order'

    tr_id = 'JTTT1002U'

    params = {
        'CANO': getTREnv().my_acct,
        'ACNT_PRDT_CD': '01',
        'OVRS_EXCG_CD': 'NASD',
        'PDNO': 'AAPL',
        'ORD_QTY': '1',
        'OVRS_ORD_UNPR': '145.00',
        'CTAC_TLNO': '',
        'MGCO_APTM_ODNO': '',
        'ORD_SVR_DVSN_CD': '0',
        'ORD_DVSN': '00'
        }

    t1 = _url_fetch(url, tr_id, params, postFlag=True, hashFlag=True)

    if t1.isOK():
        print('>>>>>>>>>>> Success')
    else:
        print('>>>>>>>>>>> Fail')

# 국내주식업종기간별시세
def inquire_daily_indexchartprice(market, stock_day):

    url = '/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice'
    tr_id = "FHKUP03500100"

    params = {
        'FID_COND_MRKT_DIV_CODE': "U",  # 시장 분류 코드(J : 주식, ETF, ETN U: 업종)
        'FID_INPUT_ISCD': market,
        'FID_INPUT_DATE_1': stock_day,
        'FID_INPUT_DATE_2': stock_day,
        'FID_PERIOD_DIV_CODE': 'D'}
    
    t1 = _url_fetch(url, tr_id, params)

    if t1.isOK():
        return t1.getBody().output1
    else:
        t1.printError()
        return dict()    
    
def inquire_search_result(id, seq):

    url = '/uapi/domestic-stock/v1/quotations/psearch-result'
    tr_id = "HHKST03900400"

    params = {
            'user_id': id,
            'seq': seq
    }  

    t1 = _url_fetch(url, tr_id, params)

    if t1.isOK():
        return t1.getBody().output2
    else:
        t1.printError()
        return dict() 