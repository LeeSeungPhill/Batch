import psycopg2 as db
import sys
import requests
from datetime import datetime, timedelta
import json
import kis_api_resp as resp
import pandas as pd
import streamlit as st
import plotly.express as px

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

arguments = sys.argv

# PostgreSQL 연결 설정
# conn_string = "dbname='kis' host='192.168.50.106' port='5432' user='postgres' password='asdf1234'"
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

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
def account(nickname):

    cur01 = conn.cursor()
    cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date from \"stockAccount_stock_account\" where nick_name = '" + nickname + "'")
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

# 기간별매매손익현황조회
def inquire_period_trade_profit(access_token, app_key, app_secret, code, strt_dt, end_dt):

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
            'PDNO': code,               # ""공란입력 시, 전체
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

        # 응답에 output1이 있는지 확인
        body = ar.getBody()
        if hasattr(body, 'output1'):
            return body.output1
        else:
            print("기간별매매손익현황조회 응답이 없습니다.")
            return []  # 혹은 None

    except Exception as e:
        print("기간별매매손익현황조회 중 오류 발생:", e)
        return []

# 기간별손익일별합산조회
def inquire_period_profit(access_token, app_key, app_secret, code, strt_dt, end_dt):

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

nickname = ['phills2', 'phills75', 'yh480825', 'phills13', 'phills15']
my_choice = st.selectbox('닉네임을 선택하세요', nickname)   

ac = account(my_choice)
acct_no = ac['acct_no']
access_token = ac['access_token']
app_key = ac['app_key']
app_secret = ac['app_secret']

# 잔고정보 조회
cur01 = conn.cursor()
cur01.execute("select name, purchase_price, purchase_amount, purchase_sum, current_price, eval_sum, earnings_rate, valuation_sum from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "'")
result_one = cur01.fetchall()
cur01.close()

data0 = []
for item in result_one:

    data0.append({
        '종목명': item[0],
        '매입단가': float(item[1]),
        '매입수량': float(item[2]),
        '매입금액': float(item[3]),
        '현재가': float(item[4]),
        '평가금액': float(item[5]),
        '손익률(%)': float(item[6]),
        '손익금액': float(item[7]),
    })

cur02 = conn.cursor()
cur02.execute("select tot_evlu_amt, scts_evlu_amt, prvs_rcdl_excc_amt  from \"stockFundMng_stock_fund_mng\" where acct_no = '" + str(acct_no) + "'")
result_two = cur02.fetchall()
cur02.close()    

for item in result_two:
    
    data0.append({
        '종목명': '현금',
        '매입단가': 0,
        '매입수량': 0,
        '매입금액': 0,
        '현재가': 0,
        '평가금액': float(item[2]),
        '손익률(%)': 0,
        '손익금액': 0,
    })

df0 = pd.DataFrame(data0)

if df0.empty:
    st.warning("조회된 데이터가 없습니다. 조건을 확인해주세요.")
else:
    # Streamlit 앱 구성
    st.title("잔고정보 조회")
    # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
    if st.button('데이터프레임 보기0'):
        st.dataframe(df0)

    df_pie = df0[df0['평가금액'] > 0]
    
    fig = px.pie(
        df_pie,
        names='종목명',
        values='평가금액',
        title='종목별 평가금액 비율',
        hole=0.4  # 도넛 형태 (없애려면 0으로)
    )

    # Streamlit에 그래프 출력
    st.plotly_chart(fig)    

code = ""
# selected_date = st.slider(
#     "날짜 범위 선택",
#     min_value=datetime.today() - timedelta(days=365),
#     max_value=datetime.today(),
#     value=(datetime.today() - timedelta(days=30), datetime.today()),
#     step=timedelta(days=1),
# )

# strt_dt = selected_date[0].strftime("%Y%m%d")
# end_dt = selected_date[1].strftime("%Y%m%d")

strt_dt = (st.date_input("시작일", datetime.today() - timedelta(days=30))).strftime("%Y%m%d")
end_dt = (st.date_input("종료일", datetime.today())).strftime("%Y%m%d")

cur03 = conn.cursor()
cur03.execute("select prvs_excc_amt, pchs_amt, evlu_amt, evlu_pfls_amt dt from \"dly_acct_balance\" where acct = '" + str(acct_no) + "' and dt between '" + strt_dt + "' and '" + end_dt + "'")
result_three = cur03.fetchall()
cur03.close() 

data01 = []
for item in result_three:

    data01.append({
        '예수금': int(item[0]),
        '총구매금액': int(item[1]),
        '평가금액': int(item[2]),
        '수익금액': float(item[3]),
        '전체금액': int(item[0]) + int(item[2]),
        '일자': item[4],
    })

df01 = pd.DataFrame(data01)

if df01.empty:
    st.warning("조회된 데이터가 없습니다. 조건을 확인해주세요.")
else:
    # Streamlit 앱 구성
    st.title("기간별 잔고 현황 조회")
    # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
    if st.button('데이터프레임 보기01'):
        st.dataframe(df01)

    df01['일자'] = pd.to_datetime(df01['일자'])
    df01 = df01.dropna(subset=['일자'])               
    df01 = df01.sort_values(by='일자')
    df01 = df01[df01['전체금액'] != 0]                
    st.line_chart(df01.set_index('일자')[['전체금액']])        

# 기간별매매손익현황조회
result1 = inquire_period_trade_profit(access_token, app_key, app_secret, code, strt_dt, end_dt)   

if not result1:
    print("기간별매매손익현황조회 결과가 없습니다.")
else:

    data1 = []
    for item in result1:

        data1.append({
            '거래일자': item['trad_dt'],
            '종목코드': item['pdno'],
            '종목명': item['prdt_name'],
            '매입단가': float(item['pchs_unpr']),
            '보유수량': float(item['hldg_qty']),
            '매도단가': item['sll_pric'],
            '매수수량': item['buy_qty'],
            '매수금액': item['buy_amt'],
            '매도수량': item['sll_qty'],
            '매도금액': item['sll_amt'],
            '손익률(%)': item['pfls_rt'],
            '손익금액': float(item['rlzt_pfls']),
            '거래세': item['tl_tax'],
            '수수료': item['fee'],
        })

    df1 = pd.DataFrame(data1)

    if df1.empty:
        st.warning("기간별매매손익현황조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("기간별 매매 손익 현황 조회")
        # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
        if st.button('데이터프레임 보기1'):
            st.dataframe(df1)

        df1['거래일자'] = pd.to_datetime(df1['거래일자'], errors='coerce')
        df1 = df1.dropna(subset=['거래일자'])
        df1 = df1.sort_values(by='거래일자')
        종목리스트 = df1['종목명'].unique()
        선택종목 = st.selectbox("종목을 선택하세요", 종목리스트)
        선택_df = df1[df1['종목명'] == 선택종목].copy()
        선택_df = 선택_df.sort_values(by='거래일자')
        선택_df['누적손익금액'] = 선택_df['손익금액'].cumsum()
        st.subheader(f"{선택종목} - 누적 손익금액")
        st.line_chart(선택_df.set_index('거래일자')[['누적손익금액']])
        st.subheader(f"{선택종목} - 일자별 매입단가")
        st.line_chart(선택_df.set_index('거래일자')[['매입단가']])
        st.subheader(f"{선택종목} - 일자별 보유수량")
        st.line_chart(선택_df.set_index('거래일자')[['보유수량']])

# 기간별손익일별합산조회
result2 = inquire_period_profit(access_token, app_key, app_secret, code, strt_dt, end_dt)    

if not result2:
    print("기간별손익일별합산조회 결과가 없습니다.")
else:

    data2 = []
    for item in result2:

        data2.append({
            '거래일자': item['trad_dt'],
            '매수금액': item['buy_amt'],
            '매도금액': item['sll_amt'],
            # '손익률(%)': item['pfls_rt'],
            '손익금액': float(item['rlzt_pfls']),
            # '거래세': item['tl_tax'],
            # '수수료': item['fee'],
        })

    df2 = pd.DataFrame(data2)

    if df1.empty:
        st.warning("기간별손익일별합산조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("기간별 손익 일별 합산 조회")

        # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
        if st.button('데이터프레임 보기2'):
            st.dataframe(df2)

        # 라디오버튼 선택
        # status = st.radio('정렬을 선택하세요', ['오름차순정렬', '내림차순정렬'])

        # if status == '오름차순정렬':
        # 	# df의 petal_length 컬럼을 기준으로 오름차순으로 정렬해서 보여주세요
        # 	st.dataframe(df.sort_values('petal_length',ascending=True))
        # elif status == '내림차순정렬':
        # 	st.dataframe(df.sort_values('petal_length',ascending=False))

        df2['거래일자'] = pd.to_datetime(df2['거래일자'])
        df2 = df2.dropna(subset=['거래일자'])               # 거래일자 존재하는 대상
        df2 = df2.sort_values(by='거래일자')
        df2 = df2[df2['손익금액'] != 0]                     # 실제 손익금액 존재하는 대상
        df2['누적손익금액'] = df2['손익금액'].cumsum()       # 누적 손익금액 계산
        st.line_chart(df2.set_index('거래일자')[['누적손익금액']])
