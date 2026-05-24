import psycopg2 as db
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import kis_api_resp as resp
from psycopg2.extras import execute_values
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater
import traceback
import time

# 기본 DB 연결 정보
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")
# today = '20260508'

def auth(APP_KEY, APP_SECRET):
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False, timeout=10)
    return res.json()["access_token"]

def account(nickname):
    cur01 = conn.cursor()
    cur01.execute("""
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day, bot_token1, bot_token2, chat_id
        FROM "stockAccount_stock_account"
        WHERE nick_name = %s
    """, (nickname,))
    result_two = cur01.fetchone()
    cur01.close()

    acct_no, access_token, app_key, app_secret, token_publ_date, token_day, bot_token1, bot_token2, chat_id = result_two
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
        'bot_token1': bot_token1,
        'bot_token2': bot_token2,
        'chat_id': chat_id
    }

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

def get_prev_day_price_info(access_token, app_key, app_secret, code, prev_date):
    """전일 종가(현재가 근사)와 전일 저가를 반환. 실패 시 (0, 0)."""
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": app_key,
        "appSecret": app_secret,
        "tr_id": "FHKST01010400"
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
        "FID_PERIOD_DIV_CODE": "D",
        "FID_ORG_ADJ_PRC": "0"
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-price"
    URL = f"{URL_BASE}/{PATH}"
    try:
        res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
        ar = resp.APIResp(res)
        if ar.isOK():
            output = ar.getBody().output
            if output:
                for row in output:
                    if row.get('stck_bsop_date', '') == prev_date:
                        return int(row.get('stck_clpr') or 0), int(row.get('stck_lwpr') or 0)
                first = output[0]
                return int(first.get('stck_clpr') or 0), int(first.get('stck_lwpr') or 0)
    except Exception as e:
        print(f"[get_prev_day_price_info] {code} 오류: {e}")
    return 0, 0

# nickname_list = ['chichipa', 'phills2', 'phills75', 'yh480825', 'phills13', 'phills15', 'mamalong', 'honeylong', 'worry106']
nickname_list = ['phills2', 'yh480825', 'phills13', 'phills15', 'mamalong', 'worry106']

for nick in nickname_list:
    try:
        ac = account(nick)
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']
        token = ac['bot_token2']
        chat_id = ac['chat_id']

        updater = Updater(token=token, use_context=True)
        bot = updater.bot

        business_day = datetime.now().strftime("%Y-%m-%d")
        # business_day = '2026-05-08'
        trail_day = post_business_day_char(business_day)
        prev_date = get_previous_business_day((datetime.strptime(business_day, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"))

        # 관심종목 interest_day 갱신 (전 영업일 이후 proc_yn=Y 대상 → 오늘 날짜로 갱신)
        try:
            cur_iday = conn.cursor()
            cur_iday.execute("""
                UPDATE public."interestItem_interest_item"
                SET interest_day = %s, interest_dtm = %s
                WHERE acct_no = %s AND proc_yn = 'Y' AND length(code) > 4
                  AND interest_day >= %s
            """, (today, datetime.now().strftime('%H%M%S'), str(acct_no), prev_date))
            conn.commit()
            print(f"[{nick}] 관심종목 interest_day 갱신: {cur_iday.rowcount}건")
            cur_iday.close()
        except Exception as e_iday:
            print(f"[{nick}] 관심종목 interest_day 갱신 오류: {e_iday}")

        # 코스피 코스닥 대상 interest_day 갱신(오늘 날짜로 갱신)
        try:
            cur_iday2 = conn.cursor()
            cur_iday2.execute("""
                UPDATE public."interestItem_interest_item"
                SET interest_day = %s, interest_dtm = %s
                WHERE acct_no = %s AND proc_yn = 'Y' AND code IN ('0001', '1001')
            """, (today, datetime.now().strftime('%H%M%S'), str(acct_no)))
            conn.commit()
            print(f"[{nick}] 코스피 코스닥 interest_day 갱신: {cur_iday2.rowcount}건")
            cur_iday2.close()
        except Exception as e_iday2:
            print(f"[{nick}] 코스피 코스닥 interest_day 갱신 오류: {e_iday2}")

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

            if int(c['hldg_qty'][i]) >  0:
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
                        exit_price
                    FROM trading_trail
                    WHERE acct_no = {acct_no}                            
                    AND trail_day = '{prev_date}'
                    AND trail_tp IN ('1','2','3','L','P','C','U')
                ) t
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
                COALESCE(S.volumn, 0) AS volumn,
                COALESCE(S.stop_price, 0) AS stop_price,
                COALESCE(S.target_price, 0) AS target_price,
                '090000' AS proc_min,
                COALESCE(S.trade_tp, 'M') AS trade_tp,
                COALESCE(S.exit_price, 0) AS exit_price,
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
                AND T.trail_dtm = CASE WHEN S.trail_day = '{trail_day}' THEN S.trail_dtm ELSE '090000' END
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

            execute_values(
                cur200,
                balance_sql,
                balance_rows,
                template="(%s, %s, %s, %s, %s)"
            )

            trading_trail_create_list = cur200.fetchall()
            cur200.close()

            inserted_count = 0
            replace_candidates = []
            if not trading_trail_create_list or len(trading_trail_create_list) < 1:
                print(f"[{nick}] No trading simulation data found.")
            else:
                insert_query1 = """
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
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (acct_no, code, trail_day, trail_dtm, trail_tp) DO NOTHING
                """
                
                cur201 = conn.cursor()
                inserted_rows_info = []
                for row in trading_trail_create_list:
                    acct_no, name, code, trail_day, trail_dtm, trail_tp, basic_price, basic_qty, volumn, stop_price, target_price, proc_min, trade_tp, exit_price, crt_dt, mod_dt = row
                    try:
                        current_price, prev_day_low = get_prev_day_price_info(access_token, app_key, app_secret, code, prev_date)
                        time.sleep(0.2)
                        if current_price > 0 and stop_price > current_price and prev_day_low > 0:
                            print(f"[{nick}] {code} stop_price({stop_price}) > 현재가({current_price}) → 전일저가({prev_day_low})로 설정")
                            stop_price = prev_day_low
                        cur201.execute(insert_query1, (
                            acct_no, name, code, trail_day, trail_dtm, trail_tp, basic_price, 0 if basic_qty is None else basic_qty, 0 if basic_qty is None else basic_price*basic_qty, volumn, stop_price, target_price, proc_min, trade_tp, exit_price, (basic_price-exit_price)*basic_qty, crt_dt, mod_dt
                        ))
                        if cur201.rowcount > 0:
                            inserted_rows_info.append({
                                'code': code, 'name': name, 'trail_tp': trail_tp,
                                'trail_day': trail_day, 'trail_dtm': trail_dtm,
                                'basic_price': float(basic_price or 0),
                                'stop_price': float(stop_price or 0),
                                'target_price': float(target_price or 0),
                                'exit_price': float(exit_price or 0),
                                'current_price': float(current_price),
                            })
                        inserted_count += cur201.rowcount
                    except Exception as e:
                        print(f"[{nick}] Error trading_trail inserting row {row}: {e}")

                conn.commit()
                cur201.close()

                # 종목 교체 고려 대상 분석
                for info in inserted_rows_info:
                    i_code      = info['code']
                    i_name      = info['name']
                    i_trail_tp  = info['trail_tp']
                    i_basic     = info['basic_price']
                    i_stop      = info['stop_price']
                    i_target    = info['target_price']
                    i_exit      = info['exit_price']
                    i_cur       = info['current_price']

                    if i_trail_tp == '1':
                        try:
                            cur_oc = conn.cursor()
                            cur_oc.execute("""
                                SELECT order_dt FROM public."stockOrderComplete_stock_order_complete"
                                WHERE name = %s AND acct_no = %s
                                  AND order_type LIKE '%%매수%%'
                                  AND total_complete_qty::int > 0
                                  AND order_dt >= COALESCE(
                                      (SELECT MAX(order_dt)
                                       FROM public."stockOrderComplete_stock_order_complete"
                                       WHERE name = %s AND acct_no = %s
                                         AND order_type LIKE '%%매도%%'
                                         AND total_complete_qty::int > 0),
                                      '00000000'
                                  )
                                ORDER BY order_dt DESC LIMIT 1
                            """, (i_name, str(acct_no), i_name, str(acct_no)))
                            oc_row = cur_oc.fetchone()
                            cur_oc.close()
                        except Exception as e_oc:
                            print(f"[{nick}] {i_name} 매수주문 조회 오류: {e_oc}")
                            oc_row = None

                        reason = None
                        if oc_row:
                            try:
                                order_date = datetime.strptime(str(oc_row[0])[:8], '%Y%m%d')
                                days_since_buy = (datetime.now() - order_date).days
                                if i_stop > 0 and i_cur > i_stop and i_basic > 0 and i_cur < i_basic * 0.95:
                                    drop_pct = round((i_basic - i_cur) / i_basic * 100, 1)
                                    reason = f"매수가:{int(i_basic):,}원 대비 {drop_pct}% 하락→현재가:{int(i_cur):,}원"
                                    plain = f"{drop_pct}% 하락"
                                elif days_since_buy >= 3 and i_target > 0 and i_cur > 0 and i_cur < i_target:
                                    reason = f"{days_since_buy}일전 매수 목표가:{int(i_target):,}원 미달성→현재가:{int(i_cur):,}원"
                                    plain = f"{days_since_buy}일 소요"
                            except Exception as e_dt:
                                print(f"[{nick}] {i_code} 날짜 파싱 오류: {e_dt}")

                        if reason:
                            replace_candidates.append({
                                'nick': nick,
                                'acct_no': acct_no,
                                'token': token,
                                'chat_id': chat_id,
                                'name': i_name,
                                'code': i_code,
                                'trail_day': info['trail_day'],
                                'trail_dtm': info['trail_dtm'],
                                'trail_tp': i_trail_tp,
                                'display': f"  - {i_name}(<code>{i_code}</code>): {reason}",
                                'display_plain': f"-{plain}",
                            })

                    elif i_trail_tp == 'L':
                        reason = None
                        if i_cur > 0:
                            if i_exit > 0 and i_cur < i_exit:
                                reason = f"최종이탈가({int(i_exit):,}원) 하회(현재가:{int(i_cur):,}원)"
                                plain = f"{int(i_exit):,}원 최종이탈가 하회"
                            elif i_basic > 0 and i_cur < i_basic:
                                reason = f"매수가({int(i_basic):,}원) 하회(현재가:{int(i_cur):,}원)"
                                plain = f"{int(i_basic):,}원 매수가 하회"
                        if reason:
                            replace_candidates.append({
                                'nick': nick,
                                'acct_no': acct_no,
                                'token': token,
                                'chat_id': chat_id,
                                'name': i_name,
                                'code': i_code,
                                'trail_day': info['trail_day'],
                                'trail_dtm': info['trail_dtm'],
                                'trail_tp': i_trail_tp,
                                'display': f"  - {i_name}(<code>{i_code}</code>)[장기]: {reason}",
                                'display_plain': f"[장기]-{plain}",
                            })

            skipped_count = len(trading_trail_create_list) - inserted_count

            message = (
                f"[{today}-{nick}] 추적준비 등록 \n"
                f"(전체 : {len(trading_trail_create_list)}건, "
                f"생성 : {inserted_count}건, "
                f"미생성 : {skipped_count}건)"
            )
            if replace_candidates:
                message += "\n\n[종목 교체 고려 대상]\n" + "\n".join(c['display'] for c in replace_candidates)

            # i/h 제외 종목 요약 및 교체 고려 대상 매도 후 현금비율 계산
            try:
                b_all = stock_balance(access_token, app_key, app_secret, acct_no, "all")
                u_prvs_rcdl_excc_amt = 0
                for _bi, _ in enumerate(b_all.index):
                    u_prvs_rcdl_excc_amt = int(b_all['prvs_rcdl_excc_amt'][_bi])

                with conn.cursor() as cur_sbm:
                    cur_sbm.execute(
                        """SELECT code, trading_plan FROM public."stockBalance_stock_balance"
                           WHERE acct_no = %s AND proc_yn = 'Y'""",
                        (str(acct_no),)
                    )
                    sb_tp_map = {row[0]: row[1] for row in cur_sbm.fetchall()}

                market_ratio_v = None
                with conn.cursor() as cur_mrv:
                    cur_mrv.execute(
                        'SELECT market_ratio FROM public."stockFundMng_stock_fund_mng" WHERE acct_no = %s',
                        (str(acct_no),)
                    )
                    row_mrv = cur_mrv.fetchone()
                    if row_mrv:
                        market_ratio_v = float(row_mrv[0])

                filtered_scts_evlu = sum(
                    int(c['evlu_amt'][i])
                    for i, _ in enumerate(c.index)
                    if int(c['hldg_qty'][i]) > 0 and sb_tp_map.get(c['pdno'][i]) not in ('i', 'h')
                )
                filtered_tot_evlu = u_prvs_rcdl_excc_amt + filtered_scts_evlu

                mr_str = ""
                if market_ratio_v is not None and filtered_tot_evlu > 0:
                    current_ratio_v = u_prvs_rcdl_excc_amt / filtered_tot_evlu * 100
                    mr_str = f", 시장비율:{market_ratio_v:.0f}%, 현재비율:{current_ratio_v:.1f}%"
                message += (
                    f"\n\n* 총평가금액:{format(filtered_tot_evlu, ',d')}원, 잔고금액:{format(filtered_scts_evlu, ',d')}원, "
                    f"가정산금:{format(u_prvs_rcdl_excc_amt, ',d')}원{mr_str}"
                )

                if replace_candidates and filtered_tot_evlu > 0:
                    c_qty_map = {c['pdno'][i]: int(c['hldg_qty'][i]) for i, _ in enumerate(c.index)}
                    replace_sell_amt = sum(
                        int(rc['current_price']) * c_qty_map.get(rc['code'], 0)
                        for rc in replace_candidates
                    )
                    cash_after_sell = u_prvs_rcdl_excc_amt + replace_sell_amt
                    market_amt_v = int(filtered_tot_evlu * market_ratio_v / 100) if market_ratio_v is not None else 0
                    diff_amt = cash_after_sell - market_amt_v
                    message += (
                        f"\n* 교체대상 매도금액:{format(replace_sell_amt, ',d')}원 → "
                        f"매도후현금:{format(cash_after_sell, ',d')}원, "
                        f"시장비율금액:{format(market_amt_v, ',d')}원, "
                        f"차이금액:{format(diff_amt, ',d')}원"
                    )
            except Exception as e_summary:
                print(f"[{nick}] 요약 계산 오류: {e_summary}")

            print(message)
            bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='HTML'
            )
            if replace_candidates:
                tp_buttons = [
                    [InlineKeyboardButton(
                        f"{c['name']}{c['display_plain']}",
                        callback_data=f"tp:{c['acct_no']}:{c['name']}:{c['code']}:{c['trail_day']}:{c['trail_dtm']}:{c['trail_tp']}"
                    )]
                    for c in replace_candidates
                ]
                bot.send_message(
                    chat_id=chat_id,
                    text="[종목 교체 고려 대상 매도비율 설정] 종목을 선택하세요:",
                    reply_markup=InlineKeyboardMarkup(tp_buttons)
                )

        time.sleep(3)                                              

    except Exception as e:
        error_msg = (
            f"[{today}-{nick}] 추적준비 등록 에러\n\n"
            f"Error: {str(e)}\n\n"
            f"Traceback:\n{traceback.format_exc()}"
        )
        print(error_msg)
        try:
            if bot is not None:
                bot.send_message(
                    chat_id=chat_id,
                    text=error_msg[:4000]  # Telegram 메시지 길이 제한 대비
                )
        except Exception as te:
            print(f"[{nick}] Telegram error send failed: {te}")

# 연결 종료
conn.close()
