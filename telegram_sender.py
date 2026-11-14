"""
텔레그램 메시지 중복 전송 방지 헬퍼

- telegram_msg_send 테이블에 (전송일, 전송시스템, 봇ID, chat_id, 중복키) 단위로 선점 기록 후 발송
- 동일 키가 당일 이미 발송(Y)/발송중(N)이면 skip, 이전 발송 실패(E)는 재발송 허용
- dedup_key 미지정시 메시지 본문 sha256 해시를 키로 사용
- 호출측 트랜잭션 오염 방지를 위해 별도 DB 연결(autocommit) 사용
- DB 장애시에는 알림 유실 방지를 위해 중복체크 없이 발송 (fail-open)
"""
import hashlib
import threading
from datetime import datetime

import psycopg2 as db

conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

# 'N'(발송중) 상태로 남은 행을 재선점 허용하는 시간 (프로세스 비정상 종료 대비)
_STALE_MINUTES = 5


_ALERT_NICK = 'kwphills75'     # fnguidePerformbot.py 가 사용하는 봇 계정
_alert_target = None
_alert_lock = threading.Lock()


def get_alert_target():
    """알림 전송용 통합 봇 (token, chat_id) 조회 : fnguidePerformbot 과 동일한 bot_token1 / chat_id (프로세스당 1회 조회)"""
    global _alert_target
    with _alert_lock:
        if _alert_target is None:
            conn = db.connect(conn_string, connect_timeout=5)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        'SELECT bot_token1, chat_id FROM "stockAccount_stock_account" WHERE nick_name = %s',
                        (_ALERT_NICK,))
                    row = cur.fetchone()
            finally:
                conn.close()
            if row is None or not row[0] or not row[1]:
                raise RuntimeError(f"알림 봇 계정({_ALERT_NICK}) 의 bot_token1/chat_id 미존재")
            _alert_target = (row[0], row[1])
        return _alert_target


def _bot_id(bot):
    # 봇 토큰 '123456:ABC...' 의 앞부분이 봇 ID (네트워크 호출 없이 추출)
    try:
        return str(bot.token).split(':')[0]
    except Exception:
        return ''


def send_once(bot, chat_id, text, system, dedup_key=None, **kwargs):
    """
    당일 동일 키 메시지가 없을 때만 텔레그램 발송.
    반환값 : True(발송) / False(중복 skip 또는 발송 실패)
    kwargs : bot.send_message 로 그대로 전달 (parse_mode, reply_markup 등)
    """
    key = dedup_key if dedup_key else hashlib.sha256(text.encode('utf-8')).hexdigest()
    key = str(key)[:200]
    now = datetime.now()

    conn = None
    row_id = None
    try:
        conn = db.connect(conn_string, connect_timeout=5)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO telegram_msg_send(send_day, send_time, send_system, bot_id, chat_id, dedup_key, msg_content)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (send_day, send_system, bot_id, chat_id, dedup_key)
                DO UPDATE SET send_yn = 'N', send_time = EXCLUDED.send_time,
                              msg_content = EXCLUDED.msg_content, udate = now()
                 WHERE telegram_msg_send.send_yn = 'E'
                    OR (telegram_msg_send.send_yn = 'N'
                        AND telegram_msg_send.udate < now() - make_interval(mins => %s))
                RETURNING id
            """, (now.strftime('%Y%m%d'), now.strftime('%H%M%S'), system, _bot_id(bot),
                  str(chat_id), key, text, _STALE_MINUTES))
            row = cur.fetchone()
            if row is None:
                print(f"[중복 메시지 skip] {system} key={key}")
                return False
            row_id = row[0]
    except Exception as de:
        print(f"[telegram_msg_send] 중복체크 DB 오류 → 그대로 발송: {de}")
        row_id = None

    try:
        bot.send_message(chat_id=chat_id, text=text, **kwargs)
        ok, err = True, None
    except Exception as te:
        print(f"텔레그램 발송 실패: {te}")
        ok, err = False, str(te)

    if conn is not None and row_id is not None:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE telegram_msg_send
                       SET send_yn = %s, send_cnt = send_cnt + 1, err_msg = %s, udate = now()
                     WHERE id = %s
                """, ('Y' if ok else 'E', err, row_id))
        except Exception as de:
            print(f"[telegram_msg_send] 발송결과 갱신 오류: {de}")

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

    return ok
