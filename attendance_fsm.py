import redis
import json
from datetime import datetime, timedelta, date

from . import posgres_redis

# -----------------------------
# Redis Setup
# -----------------------------
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# -----------------------------
# Constants
# -----------------------------
STATE_KEY = "attendance_fsm"  # per employee: attendance_fsm:{company_id}:{emp_id}
COOLDOWN_SECONDS = 10  # prevent double events

# -----------------------------
# FSM States
# -----------------------------
STATES = ['OUT', 'IN', 'BREAK']
EVENTS = ['CHECK_IN', 'CHECK_OUT', 'BREAK_OUT', 'BREAK_IN']

# -----------------------------
# FSM Helper Functions
# -----------------------------
def _get_state_key(company_id, emp_id):
    return f"{STATE_KEY}:{company_id}:{emp_id}"

def _now():
    return datetime.now()

def _read_state(company_id, emp_id):
    key = _get_state_key(company_id, emp_id)
    data = r.get(key)
    if data:
        return json.loads(data)
    # default state
    return {'state': 'OUT', 'last_ts': None, 'check_in_id': None, 'break_start': None}

def _write_state(company_id, emp_id, state_data):
    key = _get_state_key(company_id, emp_id)
    r.set(key, json.dumps(state_data))

# -----------------------------
# Event Processing
# -----------------------------
def process_event(emp_id, company_id, detected_event, camera_name=None, image_url=None):
    """
    Handles a single event and triggers DB logging
    """
    state_data = _read_state(company_id, emp_id)
    prev_state = state_data['state']
    last_ts = state_data['last_ts']
    now = _now()

    # 1️⃣ Throttle repeated events
    if last_ts:
        elapsed = (now - datetime.fromisoformat(last_ts)).total_seconds()
        if elapsed < COOLDOWN_SECONDS:
            return False  # ignore duplicates

    # 2️⃣ Determine next state
    new_state = prev_state
    check_in_id = state_data.get('check_in_id')
    break_start = state_data.get('break_start')

    if detected_event == 'IN':
        if prev_state == 'OUT':
            new_state = 'IN'
            check_in_id = None  # will be updated after DB insert
        elif prev_state == 'BREAK':
            new_state = 'IN'
            break_duration = (now - datetime.fromisoformat(break_start)).total_seconds()
            # Log break duration in attendance1
            if check_in_id:
                with posgres_redis.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "UPDATE attendance1 SET duration = COALESCE(duration,0) + %s WHERE id = %s",
                            (break_duration, check_in_id)
                        )
                        conn.commit()
            break_start = None

    elif detected_event == 'OUT':
        if prev_state in ['IN', 'BREAK']:
            new_state = 'OUT'
            # Compute total duration
            if check_in_id:
                with posgres_redis.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "SELECT attendance_time FROM attendance1 WHERE id = %s", (check_in_id,)
                        )
                        row = cursor.fetchone()
                        if row:
                            start_time = row[0]
                            duration = (now - datetime.combine(datetime.today(), start_time)).total_seconds()
                            cursor.execute(
                                "UPDATE attendance1 SET duration = %s WHERE id = %s",
                                (duration, check_in_id)
                            )
                            conn.commit()
            check_in_id = None
            break_start = None

    elif detected_event == 'BREAK_OUT' and prev_state == 'IN':
        new_state = 'BREAK'
        break_start = now.isoformat()

    elif detected_event == 'BREAK_IN' and prev_state == 'BREAK':
        new_state = 'IN'
        # Log break duration
        if check_in_id and break_start:
            break_duration = (now - datetime.fromisoformat(break_start)).total_seconds()
            with posgres_redis.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE attendance1 SET duration = COALESCE(duration,0) + %s WHERE id = %s",
                        (break_duration, check_in_id)
                    )
                    conn.commit()
        break_start = None

    # 3️⃣ Log attendance if state changed to IN or OUT
    new_check_in_id = check_in_id
    if prev_state != new_state:
        if new_state == 'IN':
            # Insert a new attendance record
            with posgres_redis.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO attendance1(emp_id, company_id, first_name, last_name, attendance_date,
                        attendance_time, check_type, camera_name, image_url)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
                    """, (
                        emp_id, company_id, *posgres_redis.get_user_info(emp_id, company_id)[:2],
                        date.today(), now.time(), 'in', camera_name, image_url
                    ))
                    new_check_in_id = cursor.fetchone()[0]
                    conn.commit()

        elif new_state == 'OUT' and check_in_id:
            # Log check-out in DB
            posgres_redis.log_attendance(
                emp_id, company_id, check_type='out', camera_name=camera_name, image_url=image_url
            )
            new_check_in_id = None

    # 4️⃣ Save updated FSM state
    _write_state(company_id, emp_id, {
        'state': new_state,
        'last_ts': now.isoformat(),
        'check_in_id': new_check_in_id,
        'break_start': break_start
    })

    return True
