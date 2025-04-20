import psycopg2 as db
import sys
import requests
from datetime import datetime, timedelta
import json
import kis_api_resp as resp
import pandas as pd
import streamlit as st
import plotly.express as px
import altair as alt
import plotly.graph_objects as go
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode

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

# 기간별매매손익현황 합산조회
def inquire_period_trade_profit_sum(access_token, app_key, app_secret, strt_dt, end_dt):

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
            'PDNO': "",                 # ""공란입력 시, 전체
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

        # 응답에 output2이 있는지 확인
        body = ar.getBody()
        if hasattr(body, 'output2'):
            return body.output2
        else:
            print("기간별매매손익현황 합산조회 응답이 없습니다.")
            return []  # 혹은 None

    except Exception as e:
        print("기간별매매손익현황 합산조회 중 오류 발생:", e)
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
# nickname = ['yh480825']
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

    # 전체 평가금액 기준 비중 계산
    df0['비중(%)'] = df0['평가금액'] / df0['평가금액'].sum() * 100

    # 비중 순으로 정렬
    df0.sort_values(by='비중(%)', ascending=False, inplace=True)

    # 순서 컬럼 추가 (1부터 시작)
    df0.insert(0, '순서', range(1, len(df0) + 1))

    # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
    if st.button('잔고정보 상세 데이터'):
        
        df_display = df0.copy().reset_index(drop=True)

        # Grid 옵션 생성
        gb = GridOptionsBuilder.from_dataframe(df_display)
        # 페이지당 20개 표시
        gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
        # 컬럼 폭 자동 맞춤
        gb.configure_grid_options(domLayout='autoHeight', autoSizeColumns=True)
        # 열 제목 길이에 따라 폭 자동 조절을 위한 이벤트 등록
        auto_size_columns_js = JsCode("""
            function(params) {
                let allColumnIds = [];
                params.columnApi.getAllColumns().forEach(function(column) {
                    allColumnIds.push(column.getColId());
                });
                params.columnApi.autoSizeColumns(allColumnIds);
            }
        """)
        gb.configure_grid_options(
            domLayout='autoHeight',  # 높이 자동 조정
            onFirstDataRendered=auto_size_columns_js  # 열 폭 자동 조정
        )

        # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
        number_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toLocaleString();
            }
        """)

        percent_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toFixed(2) + '%';
            }
        """)

        # 숫자 포맷을 적용할 컬럼들 설정
        for col in ['매입단가', '매입수량', '매입금액', '현재가', '평가금액', '손익금액']:
            gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js)

        gb.configure_column('손익률(%)', type=['numericColumn'], cellRenderer=percent_format_js)
        gb.configure_column('비중(%)', type=['numericColumn'], cellRenderer=percent_format_js)

        grid_options = gb.build()

        # AgGrid를 통해 데이터 출력
        AgGrid(
            df_display,
            gridOptions=grid_options,
            # fit_columns_on_grid_load=True,  # 화면 로드시 자동 폭 맞춤
            fit_columns_on_grid_load=False,   # JS에서 autoSizeColumns 사용
            allow_unsafe_jscode=True
        )

    df_pie = df0[df0['평가금액'] > 0].copy()

    # 레이블 생성: 종목명 (매입단가) or 종목명 (평가금액)
    def format_label(row):
        if row['종목명'] == '현금':
            return f"{row['비중(%)']:.1f}% {row['종목명']} ({row['평가금액']:,.0f}원)"
        else:
            profit_rate = f"{row['손익률(%)']:+.2f}%"
            return f"{row['비중(%)']:.1f}% {row['종목명']} (매입가 {row['매입단가']:,.0f}원, 손익률 {profit_rate})"

    df_pie['종목명'] = df_pie.apply(format_label, axis=1)
    df_pie['custom_평가금액'] = df_pie['평가금액'].apply(lambda x: f"{x:,.0f}원")

    df_pie.sort_values(by='비중(%)', ascending=False, inplace=True)

    # 도넛 차트 생성
    fig = go.Figure(
        data=[go.Pie(
            labels=df_pie['종목명'],
            values=df_pie['평가금액'],
            hole=0.4,
            customdata=df_pie[['custom_평가금액']],
            hovertemplate='<b>%{label}</b><br><span style="color:red">평가금액: %{customdata[0]}</span><extra></extra>'
        )]
    )

    fig.update_layout(title='종목별 평가금액 비율')

    # Streamlit에 출력
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
cur03.execute("select prvs_excc_amt, pchs_amt, evlu_amt, evlu_pfls_amt, dt from \"dly_acct_balance\" where acct = '" + str(acct_no) + "' and dt between '" + strt_dt + "' and '" + end_dt + "'")
result_three = cur03.fetchall()
cur03.close() 

data01 = []
for item in result_three:

    전체금액 = float(item[0]) + float(item[2])  # 예수금 + 평가금액
    예수금 = float(item[0])

    data01.append({
        '일자': item[4],
        '전체금액': 전체금액,
        '총구매금액': float(item[1]),
        '평가금액': float(item[2]),
        '수익금액': float(item[3]),
        '예수금': 예수금,
        '예수금비율(%)': (예수금 / 전체금액 * 100) if 전체금액 > 0 else 0,
    })

df01 = pd.DataFrame(data01)

if df01.empty:
    st.warning("조회된 데이터가 없습니다. 조건을 확인해주세요.")
else:
    # Streamlit 앱 구성
    st.title("기간별 잔고현황 조회")

    df01['일자'] = pd.to_datetime(df01['일자'])

    # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
    if st.button('기간별 잔고현황 상세 데이터'):

        df_display = df01.copy().reset_index(drop=True)

        # Grid 옵션 생성
        gb = GridOptionsBuilder.from_dataframe(df_display)
        # 페이지당 20개 표시
        gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
        # 컬럼 폭 자동 맞춤
        gb.configure_grid_options(domLayout='autoHeight', autoSizeColumns=True)
        # 열 제목 길이에 따라 폭 자동 조절을 위한 이벤트 등록
        auto_size_columns_js = JsCode("""
            function(params) {
                let allColumnIds = [];
                params.columnApi.getAllColumns().forEach(function(column) {
                    allColumnIds.push(column.getColId());
                });
                params.columnApi.autoSizeColumns(allColumnIds);
            }
        """)
        gb.configure_grid_options(
            domLayout='autoHeight',  # 높이 자동 조정
            onFirstDataRendered=auto_size_columns_js  # 열 폭 자동 조정
        )

        # 날짜 포맷 지정 (YYYY-MM-DD)
        date_formatter = JsCode("""
            function(params) {
                const date = new Date(params.value);
                return date.toISOString().split('T')[0];
            }
        """)

        # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
        number_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toLocaleString();
            }
        """)

        percent_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toFixed(2) + '%';
            }
        """)

        gb.configure_column("일자", type=["dateColumn"], cellRenderer=date_formatter)

        # 숫자 포맷을 적용할 컬럼들 설정
        for col in ['전체금액', '총구매금액', '평가금액', '수익금액', '예수금']:
            gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js)

        gb.configure_column('예수금비율(%)', type=['numericColumn'], cellRenderer=percent_format_js)

        grid_options = gb.build()

        # AgGrid를 통해 데이터 출력
        AgGrid(
            df_display,
            gridOptions=grid_options,
            # fit_columns_on_grid_load=True,  # 화면 로드시 자동 폭 맞춤
            fit_columns_on_grid_load=False,   # JS에서 autoSizeColumns 사용
            allow_unsafe_jscode=True
        )

    df01['일자'] = pd.to_datetime(df01['일자'])
    df01 = df01.dropna(subset=['일자'])               
    df01 = df01.sort_values(by='일자')
    df01 = df01[df01['전체금액'] != 0]
    # 인덱스를 'YYYY-MM-DD' 문자열로 포맷
    df01['일자_str'] = df01['일자'].dt.strftime('%Y-%m-%d')
    df01.set_index('일자_str', inplace=True)                
    
    st.line_chart(df01[['전체금액']])       

# 기간별매매손익현황조회
result1 = inquire_period_trade_profit(access_token, app_key, app_secret, code, strt_dt, end_dt)   

if not result1:
    print("기간별매매손익현황조회 결과가 없습니다.")
else:

    data1 = []
    for item in result1:

        data1.append({
            '거래일자': item['trad_dt'],
            '종목명': item['prdt_name'],
            '매입단가': float(item['pchs_unpr']),
            '보유수량': float(item['hldg_qty']),
            '매도단가': float(item['sll_pric']),
            '매수수량': float(item['buy_qty']),
            '매도수량': float(item['sll_qty']),
            '손익률(%)': float(item['pfls_rt']),
            '손익금액': float(item['rlzt_pfls']),
            '거래세': float(item['tl_tax']),
            '수수료': float(item['fee']),
        })

    df1 = pd.DataFrame(data1)

    if df1.empty:
        st.warning("기간별매매손익현황조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("기간별 매매 손익현황 조회")

        df1['거래일자'] = pd.to_datetime(df1['거래일자'])

        # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
        if st.button('기간별 매매 손익현황 상세 데이터'):

            df_display = df1.copy().reset_index(drop=True)

            # Grid 옵션 생성
            gb = GridOptionsBuilder.from_dataframe(df_display)
            # 페이지당 20개 표시
            gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
            # 컬럼 폭 자동 맞춤
            gb.configure_grid_options(domLayout='autoHeight', autoSizeColumns=True)
            # 열 제목 길이에 따라 폭 자동 조절을 위한 이벤트 등록
            auto_size_columns_js = JsCode("""
                function(params) {
                    let allColumnIds = [];
                    params.columnApi.getAllColumns().forEach(function(column) {
                        allColumnIds.push(column.getColId());
                    });
                    params.columnApi.autoSizeColumns(allColumnIds);
                }
            """)
            gb.configure_grid_options(
                domLayout='autoHeight',  # 높이 자동 조정
                onFirstDataRendered=auto_size_columns_js  # 열 폭 자동 조정
            )

            # 날짜 포맷 지정 (YYYY-MM-DD)
            date_formatter = JsCode("""
                function(params) {
                    const date = new Date(params.value);
                    return date.toISOString().split('T')[0];
                }
            """)

            # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
            number_format_js = JsCode("""
                function(params) {
                    if (params.value === null || params.value === undefined) {
                        return '';
                    }
                    return params.value.toLocaleString();
                }
            """)

            percent_format_js = JsCode("""
                function(params) {
                    if (params.value === null || params.value === undefined) {
                        return '';
                    }
                    return params.value.toFixed(2) + '%';
                }
            """)

            gb.configure_column("거래일자", type=["dateColumn"], cellRenderer=date_formatter)

            # 숫자 포맷을 적용할 컬럼들 설정
            for col in ['매입단가', '보유수량', '매도단가', '매수수량', '매도수량', '손익금액', '거래세', '수수료']:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js)

            gb.configure_column('손익률(%)', type=['numericColumn'], cellRenderer=percent_format_js)

            grid_options = gb.build()

            # AgGrid를 통해 데이터 출력
            AgGrid(
                df_display,
                gridOptions=grid_options,
                # fit_columns_on_grid_load=True,  # 화면 로드시 자동 폭 맞춤
                fit_columns_on_grid_load=False,   # JS에서 autoSizeColumns 사용
                allow_unsafe_jscode=True
            )

        df1['거래일자'] = pd.to_datetime(df1['거래일자'], errors='coerce')
        df1 = df1.dropna(subset=['거래일자'])
        df1 = df1.sort_values(by='거래일자')

        종목리스트 = df1['종목명'].unique()
        선택종목 = st.selectbox("종목을 선택하세요", 종목리스트)

        선택_df = df1[df1['종목명'] == 선택종목].copy()
        선택_df = 선택_df.sort_values(by='거래일자')
        선택_df['누적손익금액'] = 선택_df['손익금액'].cumsum()

        # 누적손익금액 - 왼쪽 Y축 (라인)
        profit_line = alt.Chart(선택_df).mark_line(color='red', strokeWidth=2).encode(
            x=alt.X('거래일자:T', title='거래일자', axis=alt.Axis(format='%Y-%m-%d')),
            y=alt.Y('누적손익금액:Q', title='누적손익금액', axis=alt.Axis(titleColor='red')),
            tooltip=[
                alt.Tooltip('거래일자:T', format='%Y-%m-%d'),
                alt.Tooltip('누적손익금액:Q', format=',')
            ]
        )

        # 보유수량 - 오른쪽 Y축 (바 차트 + 오른쪽 axis)
        qty_bar = alt.Chart(선택_df).mark_bar(color='gray', opacity=0.5).encode(
            x=alt.X('거래일자:T', axis=alt.Axis(format='%Y-%m-%d')),
            y=alt.Y('보유수량:Q', axis=alt.Axis(title='보유수량', titleColor='gray', orient='right')),
            tooltip=[
                alt.Tooltip('거래일자:T', format='%Y-%m-%d'),
                alt.Tooltip('보유수량:Q', format=',')
            ]
        )

        # 결합 차트
        combined_chart = alt.layer(
            profit_line,
            qty_bar
        ).resolve_scale(
            y='independent'
        ).properties(
            width=800,
            height=400,
            title=f"{선택종목} - 누적손익금액(좌) & 보유수량(우)"
        )

        st.altair_chart(combined_chart, use_container_width=True)

# 기간별손익일별합산조회
result2 = inquire_period_profit(access_token, app_key, app_secret, code, strt_dt, end_dt)    

if not result2:
    print("기간별손익일별합산조회 결과가 없습니다.")
else:

    data2 = []
    for item in result2:

        data2.append({
            '거래일자': item['trad_dt'],
            '매수금액': float(item['buy_amt']),
            '매도금액': float(item['sll_amt']),
            '손익률(%)': float(item['pfls_rt']),
            '손익금액': float(item['rlzt_pfls']),
            '거래세': float(item['tl_tax']),
            '수수료': float(item['fee']),
        })

    df2 = pd.DataFrame(data2)

    if df2.empty:
        st.warning("기간별손익일별합산조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("기간별 손익 일별합산 조회")

        df2['거래일자'] = pd.to_datetime(df2['거래일자'])

        # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
        if st.button('기간별 손익 일별합산 상세 데이터'):
            
            df_display = df2.copy().reset_index(drop=True)

            # Grid 옵션 생성
            gb = GridOptionsBuilder.from_dataframe(df_display)
            # 페이지당 20개 표시
            gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
            # 컬럼 폭 자동 맞춤
            gb.configure_grid_options(domLayout='autoHeight', autoSizeColumns=True)
            # 열 제목 길이에 따라 폭 자동 조절을 위한 이벤트 등록
            auto_size_columns_js = JsCode("""
                function(params) {
                    let allColumnIds = [];
                    params.columnApi.getAllColumns().forEach(function(column) {
                        allColumnIds.push(column.getColId());
                    });
                    params.columnApi.autoSizeColumns(allColumnIds);
                }
            """)
            gb.configure_grid_options(
                domLayout='autoHeight',  # 높이 자동 조정
                onFirstDataRendered=auto_size_columns_js  # 열 폭 자동 조정
            )

            # 날짜 포맷 지정 (YYYY-MM-DD)
            date_formatter = JsCode("""
                function(params) {
                    const date = new Date(params.value);
                    return date.toISOString().split('T')[0];
                }
            """)

            # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
            number_format_js = JsCode("""
                function(params) {
                    if (params.value === null || params.value === undefined) {
                        return '';
                    }
                    return params.value.toLocaleString();
                }
            """)

            percent_format_js = JsCode("""
                function(params) {
                    if (params.value === null || params.value === undefined) {
                        return '';
                    }
                    return params.value.toFixed(2) + '%';
                }
            """)

            gb.configure_column("거래일자", type=["dateColumn"], cellRenderer=date_formatter)

            # 숫자 포맷을 적용할 컬럼들 설정
            for col in ['매수금액', '매도금액', '손익금액', '거래세', '수수료']:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js)

            gb.configure_column('손익률(%)', type=['numericColumn'], cellRenderer=percent_format_js)

            grid_options = gb.build()

            # AgGrid를 통해 데이터 출력
            AgGrid(
                df_display,
                gridOptions=grid_options,
                # fit_columns_on_grid_load=True,  # 화면 로드시 자동 폭 맞춤
                fit_columns_on_grid_load=False,   # JS에서 autoSizeColumns 사용
                allow_unsafe_jscode=True
            )

        # 라디오버튼 선택
        # status = st.radio('정렬을 선택하세요', ['오름차순정렬', '내림차순정렬'])

        # if status == '오름차순정렬':
        # 	# df의 petal_length 컬럼을 기준으로 오름차순으로 정렬해서 보여주세요
        # 	st.dataframe(df.sort_values('petal_length',ascending=True))
        # elif status == '내림차순정렬':
        # 	st.dataframe(df.sort_values('petal_length',ascending=False))

        df2['거래일자'] = pd.to_datetime(df2['거래일자'])
        df2 = df2.dropna(subset=['거래일자'])
        df2 = df2.sort_values(by='거래일자')
        df2 = df2[df2['손익금액'] != 0]

        # 누적 손익금액 계산
        df2['누적손익금액'] = df2['손익금액'].cumsum()

        # Altair 바 차트 생성 - 누적손익금액 기준
        bar_chart = alt.Chart(df2).mark_bar().encode(
            x=alt.X('거래일자:T', title='거래일자', axis=alt.Axis(format='%Y-%m-%d')),
            y=alt.Y('누적손익금액:Q', axis=alt.Axis(title='누적 손익금액 (₩)')),
            color=alt.condition(
                alt.datum['누적손익금액'] > 0,
                alt.value('steelblue'),  # 이익
                alt.value('tomato')      # 손실
            ),
            tooltip=[
                alt.Tooltip('거래일자:T', format='%Y-%m-%d'),
                alt.Tooltip('누적손익금액:Q', format=',')
            ]
        ).properties(
            width=800,
            height=400,
            title='거래일자별 누적 손익금액 바 차트'
        )

        # Streamlit에 표시
        st.altair_chart(bar_chart, use_container_width=True)

# 기간별손익 합산조회
result3 = inquire_period_trade_profit_sum(access_token, app_key, app_secret, strt_dt, end_dt)        

if not result3:
    print("손익합산조회 결과가 없습니다.")
else:

    data3 = []
    
    data3.append({
        '매수정산금액 합계': float(result3['buy_excc_amt_smtl']),    # 매수정산금액 합계
        '매도정산금액 합계': float(result3['sll_excc_amt_smtl']),    # 매도정산금액 합계
        '총정산금액': float(result3['tot_excc_amt']),                # 총정산금액
        '총실현손익': float(result3['tot_rlzt_pfls']),        # 총실현손익
        '총수수료': float(result3['tot_fee']),                       # 총수수료
        '총제세금': float(result3['tot_tltx']),                      # 총제세금
    })

    df3 = pd.DataFrame(data3)

    if df3.empty:
        st.warning("손익합산조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("손익 합산 조회")

        # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
        if st.button('손익 합산 상세 데이터'):

            df_display = df3.copy().reset_index(drop=True)

            # Grid 옵션 생성
            gb = GridOptionsBuilder.from_dataframe(df_display)
            # 페이지당 20개 표시
            gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
            # 컬럼 폭 자동 맞춤
            gb.configure_grid_options(domLayout='autoHeight', autoSizeColumns=True)

           # 열 제목 길이에 따라 폭 자동 조절을 위한 이벤트 등록
            auto_size_columns_js = JsCode("""
                function(params) {
                    let allColumnIds = [];
                    params.columnApi.getAllColumns().forEach(function(column) {
                        allColumnIds.push(column.getColId());
                    });
                    params.columnApi.autoSizeColumns(allColumnIds);
                }
            """)
            gb.configure_grid_options(
                domLayout='autoHeight',  # 높이 자동 조정
                onFirstDataRendered=auto_size_columns_js  # 열 폭 자동 조정
            )

            # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
            number_format_js = JsCode("""
                function(params) {
                    if (params.value === null || params.value === undefined) {
                        return '';
                    }
                    return params.value.toLocaleString();
                }
            """)

            # 숫자 포맷을 적용할 컬럼들 설정
            for col in ['매수정산금액 합계', '매도정산금액 합계', '총정산금액', '총실현손익', '총수수료', '총제세금']:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js)

            grid_options = gb.build()

            # AgGrid를 통해 데이터 출력
            AgGrid(
                df_display,
                gridOptions=grid_options,
                # fit_columns_on_grid_load=True,  # 화면 로드시 자동 폭 맞춤
                fit_columns_on_grid_load=False,   # JS에서 autoSizeColumns 사용
                allow_unsafe_jscode=True
            )