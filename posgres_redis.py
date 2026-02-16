import os
import psycopg2
import requests
import traceback
import time
import socket
import uuid
from datetime import datetime
from contextlib import contextmanager, closing


last_processed_time = {}
# --- CONFIGURATION ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "redis"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin@123"),
    "port": os.getenv("DB_PORT", "5432")
}

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://localhost:5001/api/add-entry")
WEBHOOK_HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': f"Bearer {os.getenv('WEBHOOK_TOKEN', '9ff8ed8f-f8e7-4c1d-96da-b731da7b2fb9')}"
}

COMPANY_ID = os.getenv("COMPANY_ID", "267e6b1a-bc2e-4d18-b92b-7a37e3e4af7a")
WEBHOOK_RETRIES = 3
WEBHOOK_TIMEOUT = 10

_db_initialized = False

@contextmanager
def get_db_connection():
    global _db_initialized
    conn = None
    try:
        if not _db_initialized:
            _init_db_and_migrate()
            _db_initialized = True
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
    except Exception as e:
        print(f"DB Connection Error: {e}")
    finally:
        if conn: conn.close()

def _init_db_and_migrate():
    """Ensures tables exist without dummy data."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                -- =========================================
                -- 1️⃣ Employee Table
                -- =========================================
                CREATE TABLE IF NOT EXISTS user_data (
                    emp_id VARCHAR(255) NOT NULL,
                    company_id UUID NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    image_path TEXT,
                    PRIMARY KEY (emp_id, company_id)
                );

                -- Index for faster lookups per employee
                CREATE INDEX IF NOT EXISTS idx_user_company ON user_data(company_id);

                -- =========================================
                -- 2️⃣ Attendance Raw Events Table
                -- =========================================
                CREATE TABLE IF NOT EXISTS attendance1 (
                    id SERIAL PRIMARY KEY,
                    emp_id VARCHAR(255) NOT NULL,
                    company_id UUID NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    attendance_date DATE NOT NULL,
                    attendance_time TIME NOT NULL,
                    check_type TEXT NOT NULL,        -- 'in', 'out', 'break_in', 'break_out'
                    image_url TEXT,
                    camera_name TEXT,
                    duration INTERVAL,               -- total working duration for 'out' events
                    check_in_id INTEGER,             -- links 'out' to corresponding 'in'
                    sent_to_webhook BOOLEAN DEFAULT FALSE,
                    webhook_response TEXT,
                    FOREIGN KEY(emp_id, company_id) REFERENCES user_data(emp_id, company_id) ON DELETE CASCADE
                );

                -- Index for fast per-employee daily queries
                CREATE INDEX IF NOT EXISTS idx_attendance_emp_date
                ON attendance1(emp_id, attendance_date);

                CREATE INDEX IF NOT EXISTS idx_attendance_company_date
                ON attendance1(company_id, attendance_date);

                -- =========================================
                -- 3️⃣ FSM Audit / History Table
                -- =========================================
                CREATE TABLE IF NOT EXISTS attendance_fsm_history (
                    id SERIAL PRIMARY KEY,
                    emp_id VARCHAR(255) NOT NULL,
                    company_id UUID NOT NULL,
                    event_type TEXT NOT NULL,        -- CHECK_IN, CHECK_OUT, BREAK_OUT, BREAK_IN
                    prev_state TEXT,
                    new_state TEXT,
                    processed_at TIMESTAMP DEFAULT now(),
                    success BOOLEAN,
                    error_message TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_fsm_emp_date
                ON attendance_fsm_history(emp_id, processed_at);

                -- =========================================
                -- 4️⃣ Daily Summary Table
                -- =========================================
                CREATE TABLE IF NOT EXISTS attendance_summary (
                    emp_id VARCHAR(255) NOT NULL,
                    company_id UUID NOT NULL,
                    attendance_date DATE NOT NULL,
                    total_work_duration INTERVAL DEFAULT '0',
                    total_break_duration INTERVAL DEFAULT '0',
                    PRIMARY KEY (emp_id, company_id, attendance_date)
                );

                CREATE INDEX IF NOT EXISTS idx_summary_company_date
                ON attendance_summary(company_id, attendance_date);

                 """)
                conn.commit()
    except Exception as e:
        print(f"Init DB Error: {e}")

# --- WEBHOOK LOGIC ---

def prepare_webhook_payload(record, image_path):
    """
    Standardizes payload using tuple indices.
    Order from SELECT: 0:id, 1:emp_id, 2:company_id, 3:first, 4:last, 5:date, 6:time, 7:type, 8:cam
    """
    return {
        "id": record[0],
        "emp_id": str(record[1]),
        "company_id": str(record[2]),
        "first_name": record[3],
        "last_name": record[4],
        "attendance_date": record[5].isoformat() if hasattr(record[5], 'isoformat') else str(record[5]),
        "attendance_time": record[6].isoformat() if hasattr(record[6], 'isoformat') else str(record[6]),
        "check_type": record[7],
        "camera_name": record[8],
        "image_url": image_path
    }

def send_to_webhook(payload):
    print(f"DEBUG: Attempting to send to {WEBHOOK_URL}...")
    for attempt in range(WEBHOOK_RETRIES):
        try:
            # Added verify=False in case of local SSL issues
            response = requests.post(
                WEBHOOK_URL, 
                headers=WEBHOOK_HEADERS, 
                json=payload, 
                timeout=WEBHOOK_TIMEOUT,
                verify=False 
            )
            print(f"DEBUG: Webhook Response Status: {response.status_code}")
            
            if response.status_code in [200, 201]:
                return True, response.text
            else:
                print(f"DEBUG: Webhook Error Body: {response.text}")
                
        except requests.exceptions.ConnectionError:
            print(f"DEBUG: Connection Refused! Is the API running on {WEBHOOK_URL}?")
        except requests.exceptions.Timeout:
            print("DEBUG: Webhook timed out.")
        except Exception as e:
            print(f"DEBUG: Unexpected Webhook Error: {e}")
            
        time.sleep(1)
    return False, "Max retries reached"

def mark_attendance_sent(attendance_id, response_text):
    with get_db_connection() as conn:
        if not conn: return
        with conn.cursor() as cursor:
            cursor.execute("UPDATE attendance1 SET sent_to_webhook = True, webhook_response = %s WHERE id = %s", 
                           (str(response_text)[:500], attendance_id))
            conn.commit()

# --- CORE LOGIC ---

def get_user_info(emp_id, company_id):
    """Retrieves user details. Returns 4 values for probe.py compatibility."""
    with get_db_connection() as conn:
        if not conn: return (None, None, None, None)
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT first_name, last_name, emp_id, image_path 
                FROM user_data WHERE emp_id = %s AND company_id = %s
            """, (str(emp_id), str(company_id)))
            user = cursor.fetchone()
            # Index-based: 0=first, 1=last, 2=id, 3=path
            if user:
                return (user[0], user[1], user[2], user[3])
            return (None, None, None, None)

def log_attendance(emp_id, company_id, **kwargs):
    """
    Logs attendance for an employee safely:
      - Skips unknown users
      - Handles string or date/time objects
      - Queue-safe (**kwargs)
      - Inserts into attendance1
      - Logs FSM events in attendance_fsm_history
      - Updates daily summary in attendance_summary
      - Sends webhook
    """
    import time
    from datetime import datetime, date, time as time_cls

    # --- 1. THROTTLING ---
    current_ts = time.time()
    if emp_id in last_processed_time and (current_ts - last_processed_time[emp_id] < 30):
        return False

    # --- 2. EXTRACT PARAMS ---
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    attendance_date = kwargs.get("attendance_date")
    attendance_time = kwargs.get("attendance_time")
    check_type = kwargs.get("check_type", "in")
    camera_name = kwargs.get("camera_name", "Entrance")
    image_url = kwargs.get("image_url")

    # --- 3. FETCH USER INFO if missing ---
    if not first_name or not last_name or not image_url:
        user = get_user_info(emp_id, company_id)
        if not user[0]:
            # Skip unknown employee
            print(f"EVENT IGNORED: ID {emp_id} not in user_data.")
            return False
        first_name, last_name, _, image_url = user

    # --- 4. SAFE DATE/TIME CONVERSION ---
    now = datetime.now()

    if isinstance(attendance_date, str):
        try:
            attendance_date = datetime.fromisoformat(attendance_date).date()
        except Exception:
            attendance_date = now.date()
    elif attendance_date is None:
        attendance_date = now.date()

    if isinstance(attendance_time, str):
        try:
            attendance_time = datetime.fromisoformat(attendance_time).time()
        except Exception:
            attendance_time = now.time()
    elif attendance_time is None:
        attendance_time = now.time()

    # --- 5. DATABASE LOGIC ---
    with get_db_connection() as conn:
        if not conn:
            return False
        try:
            with conn.cursor() as cursor:
                # --- 5a. Auto-detect 'in' vs 'out' ---
                cursor.execute("""
                    SELECT id FROM attendance1 
                    WHERE emp_id = %s AND company_id = %s AND attendance_date = %s 
                    AND check_type = 'in' AND id NOT IN (
                        SELECT check_in_id FROM attendance1 WHERE check_in_id IS NOT NULL
                    )
                    ORDER BY attendance_time DESC LIMIT 1
                """, (str(emp_id), str(company_id), attendance_date))
                
                open_checkin = cursor.fetchone()
                final_check_type = 'in'
                parent_id = None
                if open_checkin:
                    final_check_type = 'out'
                    parent_id = open_checkin[0]

                # --- 5b. Insert into attendance1 ---
                cursor.execute("""
                    INSERT INTO attendance1 
                    (emp_id, company_id, first_name, last_name, attendance_date, attendance_time,
                     check_type, camera_name, image_url, check_in_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (str(emp_id), str(company_id), first_name, last_name,
                      attendance_date, attendance_time, final_check_type, camera_name, image_url, parent_id))
                
                new_db_id = cursor.fetchone()[0]

                # --- 5c. Log FSM event ---
                event_type = 'CHECK_IN' if final_check_type == 'in' else 'CHECK_OUT'
                prev_state = None
                # Optionally, fetch last state
                cursor.execute("""
                    SELECT new_state FROM attendance_fsm_history
                    WHERE emp_id = %s AND company_id = %s
                    ORDER BY processed_at DESC LIMIT 1
                """, (str(emp_id), str(company_id)))
                last_state_row = cursor.fetchone()
                if last_state_row:
                    prev_state = last_state_row[0]

                new_state = final_check_type.upper()
                cursor.execute("""
                    INSERT INTO attendance_fsm_history
                    (emp_id, company_id, event_type, prev_state, new_state, success)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (str(emp_id), str(company_id), event_type, prev_state, new_state, True))

                # --- 5d. Update daily summary ---
                # Ensure a summary row exists
                cursor.execute("""
                    INSERT INTO attendance_summary (emp_id, company_id, attendance_date)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (emp_id, company_id, attendance_date) DO NOTHING
                """, (str(emp_id), str(company_id), attendance_date))

                if final_check_type == 'out' and parent_id:
                    # Calculate duration in seconds
                    cursor.execute("SELECT attendance_time FROM attendance1 WHERE id = %s", (parent_id,))
                    check_in_time = cursor.fetchone()[0]
                    duration_seconds = (datetime.combine(attendance_date, attendance_time) -
                                        datetime.combine(attendance_date, check_in_time)).total_seconds()
                    cursor.execute("""
                        UPDATE attendance_summary
                        SET total_work_duration = total_work_duration + make_interval(secs => %s)
                        WHERE emp_id = %s AND company_id = %s AND attendance_date = %s
                    """, (int(duration_seconds), str(emp_id), str(company_id), attendance_date))

                # Commit all DB changes together
                conn.commit()

                # --- 6. TRIGGER WEBHOOK ---
                last_processed_time[emp_id] = current_ts
                payload = {
                    "emp_id": str(emp_id),
                    "company_id": str(company_id),
                    "first_name": first_name,
                    "last_name": last_name,
                    "check_type": final_check_type,
                    "check_in_id": parent_id,
                    "attendance_date": attendance_date.isoformat(),
                    "attendance_time": attendance_time.isoformat(),
                    "image_url": image_url
                }
                
                success, resp = send_to_webhook(payload)
                if success:
                    mark_attendance_sent(new_db_id, resp)

                return True

        except Exception as e:
            print(f"Logging Error: {e}")
            return False
