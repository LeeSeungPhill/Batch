import psycopg2 as db
from datetime import datetime

# PostgreSQL 연결 설정
# conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

today = datetime.now().strftime("%Y%m%d")

# DB 연결
conn = db.connect(conn_string)

def is_business_day(check_date: datetime, conn) -> bool:
    """
    DB 기준 영업일 여부 확인
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT is_business_day(%s)",
        (check_date,)
    )
    result = cur.fetchone()
    cur.close()

    return bool(result[0])

# 영업일 확인용 임시 연결 (스레드 진입 전 단일 사용)
_conn_check = db.connect(conn_string)
try:
    _is_business = is_business_day(today, _conn_check)
finally:
    _conn_check.close()

if _is_business:
    cur1 = conn.cursor()
    cur1.execute("CALL upd_dly_stock_item(%s, %s);", ['', ''])
    result = cur1.fetchall()
    cur1.close()
    conn.commit()

    if result != None:
        for i in result:
            print(i[0])
            print(i[1])

conn.close()

