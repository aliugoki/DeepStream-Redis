import os
import datetime
import random
import asyncio
import logging
import json
import threading
from typing import Annotated, List, Optional
from uuid import uuid4, UUID
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse
from pathlib import Path

# Database imports
from databases import Database
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime, select, update, delete, Date, Boolean

from dotenv import load_dotenv
from fastapi import FastAPI, Request, status, WebSocket, WebSocketDisconnect, Query, HTTPException, Depends, Form, Response, Header
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import uvicorn
import socketio
import httpx
from starlette.websockets import WebSocketState
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from psycopg2.extras import DictCursor

import redis

redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True
)

logger = logging.getLogger("RedisConsumer")

CONSUMER_NAME = "consumer_1"

async def redis_xreadgroup(*args, **kwargs):
    return await asyncio.to_thread(redis_client.xreadgroup, *args, **kwargs)

async def redis_xack(stream, group, message_id):
    return await asyncio.to_thread(redis_client.xack, stream, group, message_id)


async def redis_stream_listener():
    logger.info("🚀 Starting async Redis listener...")
    while True:
        try:
            response = await redis_xreadgroup(
                groupname="attendance_group",
                consumername=CONSUMER_NAME,
                streams={"attendance_stream": ">"},
                count=10,
                block=5000
            )

            if not response:
                await asyncio.sleep(0.1)  # small sleep to prevent busy loop
                continue

            for stream_name, messages in response:
                for message_id, data in messages:
                    logger.info(f"[REDIS STREAM RECEIVED] ID={message_id} DATA={data}")
                    try:
                        await process_attendance_event(data)
                        await redis_xack("attendance_stream", "attendance_group", message_id)
                        logger.info(f"[REDIS STREAM ACKED] ID={message_id}")
                    except Exception as e:
                        logger.error(f"Processing failed for message {message_id}: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"Redis listener error: {e}", exc_info=True)
            await asyncio.sleep(2)  # backoff on error

def create_consumer_group():
    try:
        redis_client.xgroup_create(
            name="attendance_stream",
            groupname="attendance_group",
            id="0",
            mkstream=True
        )
        print("Consumer group created.")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print("Consumer group already exists.")
        else:
            raise


async def process_attendance_event(data):
    company_id = data["company_id"]

    # 🔹 Emit new attendance entry
    await sio.emit(
        "new_attendance_entry",
        data,
        room=str(company_id)
    )

    # 🔹 Update stats
    stats = {
        "total_present_today": await get_total_present_today(company_id),
        "total_registered_employees": await get_total_registered_employees(company_id),
        "total_on_time_today": await get_total_on_time_today(company_id),
        "total_late_today": await get_total_late_today(company_id)
    }

    await sio.emit(
        "attendance_statistics_update",
        stats,
        room=str(company_id)
    )

    # 🔹 Send to Oracle-compatible API
    payload_external = {
        "emp_id": data["emp_id"],
        "first_name": data["first_name"],
        "last_name": data["last_name"],
        "check_type": data["check_type"],
        "attendance_time": data["attendance_time"],
        "attendance_date": data["attendance_date"],
        "image_url": data.get("image_url")
    }

    await send_single_record_to_external_api(payload_external, company_id)

async def claim_pending():
    try:
        pending = await asyncio.to_thread(
            redis_client.xpending_range,
            "attendance_stream",
            "attendance_group",
            min="-",
            max="+",
            count=50
        )

        for p in pending:
            await asyncio.to_thread(
                redis_client.xclaim,
                "attendance_stream",
                "attendance_group",
                CONSUMER_NAME,
                min_idle_time=0,
                message_ids=[p["message_id"]]
            )
        logger.info(f"Claimed {len(pending)} pending messages (if any).")
    except Exception as e:
        logger.error(f"Error claiming pending messages: {e}", exc_info=True)



# Load environment variables from .env file

load_dotenv()

# --- Enhanced Logging Configuration ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
# 5 MB rotating logs (fixed: was 5 * 124 * 1024 earlier)
file_handler = RotatingFileHandler('fastapi_app.log', maxBytes=5 * 1024 * 1024, backupCount=2)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# --- App Configuration ---
SECRET_KEY = os.getenv('SECRET_KEY', 'default_secret_key')
APP_DEBUG = os.getenv('APP_DEBUG', 'False').lower() == 'true'

# --- Authentication Configuration ---
SESSION_COOKIE_NAME = "session_token"
# DEEPSTREAM_API_KEY = os.getenv('DEEPSTREAM_API_KEY', {'6b17e44e-c22a-45a4-a301-c366e68c7c10'})

# --- Database Configuration (PostgreSQL) ---
# The DATABASE_URL must be set in your .env file
# Example: postgresql://user:password@host:port/dbname
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:admin%40123@localhost:5432/redis")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set. Please configure your PostgreSQL connection string.")

# Initialize the async database connection
database = Database(DATABASE_URL, min_size=5, max_size=20)


# REMOTE_DATABASE_URL = os.getenv("REMOTE_DATABASE_URL", "postgresql://postgres:admin%40123@103.149.33.102:5432/facial_recognition_db")
# if not REMOTE_DATABASE_URL:
#     raise ValueError("REMOTE_DATABASE_URL environment variable is not set. Please configure your PostgreSQL connection string.")

# # Initialize the async database connection
# remote_database = Database(REMOTE_DATABASE_URL, min_size=5, max_size=20)

# SQLAlchemy MetaData and table definitions
metadata = MetaData()

# Define the 'companies' table
companies = Table(
    'companies', metadata,
    Column('company_id', String, unique=True, index=True),
    Column('company_name', String),    
    Column('admin_username', String),
    Column('admin_password_hash', String),
    Column('company_image_folder', String, default=None),
    Column('registration_date', DateTime, default=datetime.datetime.now),
    Column('session_token', String, nullable=True),
    Column('api_key', String, nullable=True),
    Column('status', String, nullable=True),
    Column('rtsp_url', String, nullable=True),
    Column('webrtc_url', String, nullable=True),
    Column('attendance_api', String, nullable=True),
)

# Define the 'attendance' table
attendance = Table(
    'attendance_logs', metadata,
    Column('id', Integer, primary_key=True),
    Column('company_id', String, index=True),
    Column('emp_id', String, nullable=False),
    Column('first_name', String, nullable=False),
    Column('last_name', String, nullable=False),
    Column('attendance_time', DateTime, default=datetime.datetime.now),
    Column('attendance_date', Date, nullable=False),
    Column('check_type', String, nullable=False, default='in'),
    Column('check_in_id', Integer, nullable=True),
    Column('image_url', String, nullable=True),
    Column('camera_name', String, nullable=True),
    Column('duration', String, nullable=True),
    Column('sent_to_webhook', Boolean, nullable=True),
    Column('webhook_response', String, nullable=True),    
    Column('status', String, nullable=True, default='Unknown')
)

# Define the 'user_data' table
user_data = Table(
    'user_data', metadata,
    Column('user_id', String, primary_key=True),
    Column('company_id', String, index=True),
    # FIX: explicitly type emp_id as String
    Column('emp_id', String, unique=True, index=True),  # Note: if you want uniqueness per company, use a composite unique index.
    Column('first_name', String, nullable=False),
    Column('last_name', String, nullable=False),
    Column('image_path', String),
    Column('feature_path', String),
    Column('registration_date', DateTime, default=datetime.datetime.now)
)

# Define a standard start time for "On Time" calculation (e.g., 9:00 AM)
STANDARD_START_TIME = datetime.time(9, 0, 0)

# --- External Server Sync Configuration ---
EXTERNAL_SYNC_TIMEOUT = int(os.getenv('EXTERNAL_SYNC_TIMEOUT', '10'))

# --- FastAPI App and Socket.IO Server Setup ---
# sio = socketio.AsyncServer(
#     async_mode='asgi',
#     cors_allowed_origins='*',
#     socketio_path='socket.io'
# )

mgr = socketio.AsyncRedisManager("redis://localhost:6379/0")

sio = socketio.AsyncServer(
    async_mode='asgi',
    client_manager=mgr,
    cors_allowed_origins='*',
    socketio_path='socket.io',
    ping_timeout=60,
    ping_interval=25
)


app = FastAPI(debug=APP_DEBUG)

@app.on_event("startup")
async def startup():
    logger.info("🔌 FastAPI Startup: Connecting to PostgreSQL...")
    await database.connect()
    logger.info("✅ PostgreSQL connected.")

    await claim_pending()
    create_consumer_group()  # This can stay sync

    # Start async Redis listener in background
    asyncio.create_task(redis_stream_listener())
    logger.info("🚀 Redis listener task started.")
    
    # logger.info("🚀 Connecting to Remote PostgreSQL...")
    # await remote_database.connect()
    # logger.info("✅ Remote PostgreSQL connected.")
    

@app.on_event("shutdown")
async def shutdown():
    logger.info("🔌 Disconnecting from PostgreSQL...")
    await database.disconnect()
    logger.info("✅ PostgreSQL disconnected.")
    # logger.info("🔌 Disconnecting from Remote PostgreSQL...")
    # await remote_database.disconnect()
    # logger.info("✅ Remote PostgreSQL disconnected.")

# Static files that are NOT company-specific (e.g., CSS, JS, general images)
GLOBAL_STATIC_DIR = "static"
GLOBAL_STATIC_URL_PREFIX = "/static"

if os.path.exists(GLOBAL_STATIC_DIR) and os.path.isdir(GLOBAL_STATIC_DIR):
    app.mount(GLOBAL_STATIC_URL_PREFIX, StaticFiles(directory=GLOBAL_STATIC_DIR), name="global_static")
    logger.info(f"Mounted global static directory: '{GLOBAL_STATIC_DIR}' to URL prefix: '{GLOBAL_STATIC_URL_PREFIX}'")
else:
    logger.warning(f"Global static directory not found at '{GLOBAL_STATIC_DIR}'. CSS, JS, and fallback images might not be served.")

socketio_asgi_app = socketio.ASGIApp(sio, app)
templates = Jinja2Templates(directory="templates")

# --- Pydantic Models for Data Validation and Serialization ---
class AttendanceEntryPayload(BaseModel):
    emp_id: str
    first_name: str
    last_name: str
    check_type: str
    image_url: Optional[str] = None

# --- Database Functions (Asynchronous with databases) ---
def _prepare_entry_for_client(entry: dict, company_id: str) -> dict:
    """
    Helper function to process an attendance entry dictionary for client-side display.
    This function handles:
    1. Converting UUID and datetime objects to strings.
    2. Formatting the image URL for client display.
    """
    client_friendly_entry = dict(entry)
    
    # 1. Convert UUID and datetime objects to strings
    for key, value in client_friendly_entry.items():
        if isinstance(value, UUID):
            client_friendly_entry[key] = str(value)
        elif isinstance(value, (datetime.datetime, datetime.time, datetime.date)):
            client_friendly_entry[key] = value.isoformat()
    
    # 2. Format the image URL for client display
    original_image_url = client_friendly_entry.get('image_url')
    if original_image_url:
        image_filename = os.path.basename(original_image_url)
        client_friendly_entry['image_url'] = f"/company_images/{company_id}/{image_filename}"
    else:
        client_friendly_entry['image_url'] = None
    
    return client_friendly_entry

async def fetch_all_attendance(company_id: str, request: Request):
    """
    Fetches all attendance records for a given company and formats the image URL
    to be usable by the client.
    """
    query = attendance.select().where(attendance.c.company_id == company_id).order_by(attendance.c.attendance_date.desc())
    rows = await database.fetch_all(query)
    processed_rows = []
    for row in rows:
        processed_rows.append(_prepare_entry_for_client(dict(row), company_id))
    return processed_rows

async def add_attendance_entry(company_id: str, emp_id: str, first_name: str, last_name: str, check_type: str, image_url: Optional[str] = None):
    current_time = datetime.datetime.now()
    attendance_date = current_time.date()

    clean_image_url = None
    if image_url:
        parsed_url = urlparse(image_url)
        clean_image_url = os.path.basename(parsed_url.path)

    if check_type.lower() == 'in':
        today_date = current_time.date()
        query_check = select(attendance.c.id).where(
            (attendance.c.company_id == company_id) &
            (attendance.c.check_type == 'in') &
            (attendance.c.attendance_date >= today_date) &
            (attendance.c.emp_id == str(emp_id))
        )
        existing_check_in = await database.fetch_one(query_check)
        if existing_check_in:
            logger.warning(f"Employee {emp_id} already has a check-in record for today. Skipping.")
            return None

        attendance_status = "On Time" if current_time.time() <= STANDARD_START_TIME else "Late"
        insert_values = dict(
            company_id=company_id,
            emp_id=str(emp_id),
            first_name=first_name,
            last_name=last_name,
            attendance_time=current_time,
            attendance_date=attendance_date,
            check_type='in',
            image_url=clean_image_url,
            status=attendance_status
        )

        # insert into local
        query_local = attendance.insert().values(**insert_values)
        last_insert_id = await database.execute(query_local)

        # insert into remote
        # try:
        #     query_remote = attendance.insert().values(**insert_values)
        #     await remote_database.execute(query_remote)
        # except Exception as e:
        #     logger.error(f"Remote DB insert failed for check-in {emp_id}: {e}")

        query_select = attendance.select().where(attendance.c.id == last_insert_id)
        row = await database.fetch_one(query_select)
        return dict(row) if row else None

    elif check_type.lower() == 'out':
        query_check_in = select(attendance.c.id, attendance.c.attendance_time).where(
            (attendance.c.company_id == company_id) &
            (attendance.c.check_type == 'in') &
            (attendance.c.check_in_id.is_(None)) &
            (attendance.c.emp_id == str(emp_id))
        ).order_by(attendance.c.attendance_time.desc()).limit(1)

        check_in_record = await database.fetch_one(query_check_in)
        if not check_in_record:
            logger.warning(f"No open check-in found for employee {emp_id}. Skipping check-out.")
            return None

        check_in_id = check_in_record['id']
        insert_values = dict(
            company_id=company_id,
            emp_id=str(emp_id),
            first_name=first_name,
            last_name=last_name,
            attendance_time=current_time,
            attendance_date=attendance_date,
            check_type='out',
            check_in_id=check_in_id,
            image_url=clean_image_url,
            status="Exit"
        )

        query_local = attendance.insert().values(**insert_values)
        last_insert_id = await database.execute(query_local)

        # insert into remote
        # try:
        #     query_remote = attendance.insert().values(**insert_values)
        #     await remote_database.execute(query_remote)
        # except Exception as e:
        #     logger.error(f"Remote DB insert failed for check-out {emp_id}: {e}")

        query_select = attendance.select().where(attendance.c.id == last_insert_id)
        row = await database.fetch_one(query_select)
        return dict(row) if row else None

    else:
        logger.warning(f"Invalid check_type '{check_type}'. Must be 'in' or 'out'.")
        return None


async def get_total_registered_employees(company_id: str):
    query = select(user_data.c.user_id).where(user_data.c.company_id == company_id)
    result = await database.fetch_all(query)
    return len(result)

async def get_total_present_today(company_id: str):
    today_date = datetime.date.today()
    query = select(attendance.c.emp_id).distinct().where(
        (attendance.c.company_id == company_id) &
        (attendance.c.check_type == 'in') &
        (attendance.c.attendance_date >= today_date)
    )
    result = await database.fetch_all(query)
    return len(result)

async def get_total_on_time_today(company_id: str):
    today_date = datetime.date.today()
    query = select(attendance.c.emp_id).distinct().where(
        (attendance.c.company_id == company_id) &
        (attendance.c.check_type == 'in') &
        (attendance.c.status == 'On Time') &
        (attendance.c.attendance_date >= today_date)
    )
    result = await database.fetch_all(query)
    return len(result)

async def get_total_late_today(company_id: str):
    today_date = datetime.date.today()
    query = select(attendance.c.emp_id).distinct().where(
        (attendance.c.company_id == company_id) &
        (attendance.c.check_type == 'in') &
        (attendance.c.status == 'Late') &
        (attendance.c.attendance_date >= today_date)
    )
    result = await database.fetch_all(query)
    return len(result)

async def clear_all_attendance_records(company_id: str):
    query = delete(attendance).where(attendance.c.company_id == company_id)
    await database.execute(query)
    logger.info(f"All attendance records have been cleared for company ID: {company_id}.")


async def send_single_record_to_external_api(record: dict, company_id: str):
    try:
        # 1️⃣ Fetch the API URL and API key for this company
        query = select(companies.c.attendance_api, companies.c.api_key).where(companies.c.company_id == company_id)
        row = await database.fetch_one(query)
        if not row or not row["attendance_api"] or not row["api_key"]:
            logger.error(f"No external API or API key configured for company {company_id}")
            return False, "No external API or API key configured"

        external_url = row["attendance_api"]
        api_key = row["api_key"]
    except Exception as e:
        logger.error(f"Error fetching API config for company {company_id}: {e}", exc_info=True)
        return False, str(e)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    # ✅ Build payload manually (same way as in sync_all_to_external_api)
    payload = {
        "emp_id": str(record.get("emp_id")),
        "first_name": record.get("first_name"),
        "last_name": record.get("last_name"),
        "check_type": record.get("check_type"),
        "attendance_time": record.get("attendance_time").isoformat() if isinstance(record.get("attendance_time"), (datetime.datetime, datetime.date, datetime.time)) else record.get("attendance_time"),
        "attendance_date": record.get("attendance_date").isoformat() if isinstance(record.get("attendance_date"), (datetime.datetime, datetime.date)) else record.get("attendance_date"),
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                external_url,
                json=payload,
                headers=headers,
                timeout=float(EXTERNAL_SYNC_TIMEOUT)
            )
            response.raise_for_status()
            response_json = {}
            if response.content:
                try:
                    response_json = response.json()
                except json.decoder.JSONDecodeError:
                    logger.warning(
                        f"External API returned {response.status_code} for emp_id {payload.get('emp_id')}, "
                        f"but response body was not valid JSON. Content: '{response.text}'"
                    )
            else:
                logger.info(f"External API returned {response.status_code} for emp_id {payload.get('emp_id')}, but response body was empty.")

            logger.info(f"Successfully sent record for emp_id {payload.get('emp_id')} to external API. Response Status: {response.status_code}. Response Data: {response_json}")
            return True, response_json

    except httpx.RequestError as exc:
        logger.error(f"HTTPX Request Error sending record for emp_id {payload.get('emp_id')} to external API. Type: {type(exc)}. Details: {exc}", exc_info=True)
        return False, f"Network or connection error: {exc.__class__.__name__}"
    except httpx.HTTPStatusError as exc:
        error_details = exc.response.text
        try:
            error_details = exc.response.json()
        except json.decoder.JSONDecodeError:
            pass
        logger.error(f"HTTPX Status Error sending record for emp_id {payload.get('emp_id')} to external API. Status: {exc.response.status_code}. Response: {error_details}", exc_info=True)
        return False, error_details
    except Exception as e:
        logger.error(f"Unexpected error sending record for emp_id {payload.get('emp_id')} to external API. Type: {type(e)}. Details: {e}", exc_info=True)
        return False, f"An unexpected error occurred: {e.__class__.__name__}"

def format_attendance_date(value):
    """Return DD-MMM-YYYY (e.g., 09-SEP-2025). Works for date, datetime, or string."""
    if value is None:
        return None

    # If it's already a datetime/date object
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.strftime("%d-%b-%Y").upper()
    
    # If it's a string
    if isinstance(value, str):
        # Try multiple formats
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.datetime.strptime(value, fmt)
                return dt.strftime("%d-%b-%Y").upper()
            except ValueError:
                continue
        # If all parsing fails, try splitting first 10 characters (YYYY-MM-DD)
        try:
            dt = datetime.datetime.strptime(value[:10], "%Y-%m-%d")
            return dt.strftime("%d-%b-%Y").upper()
        except Exception:
            pass

    # If all else fails, return value as-is
    return value

async def sync_all_to_external_api(company_id: str, request: Request):
    all_attendance = await fetch_all_attendance(company_id, request)
    if not all_attendance:
        logger.info("No attendance records to sync to external API.")
        return 0, 0, False
    success_count = 0
    total_count = len(all_attendance)
    for record in all_attendance:
        payload = {
    "emp_id": str(record.get("emp_id")),
    "first_name": record.get("first_name"),
    "last_name": record.get("last_name"),
    "check_type": record.get("check_type"),
    "attendance_time": (
        record.get("attendance_time").strftime("%d-%b-%Y %H:%M:%S").upper()
        if isinstance(record.get("attendance_time"), datetime.datetime)
        else record.get("attendance_time")
    ),
    "attendance_date": format_attendance_date(record.get("attendance_date")),
}

        # FIX: pass company_id (was DEEPSTREAM_API_KEY)
        success, response_data = await send_single_record_to_external_api(payload, company_id)
        if success:
            success_count += 1
        else:
            logger.error(f"Failed to sync record for emp_id {record.get('emp_id')} (Local ID: {record.get('id')}) to external API: {response_data}")
    if success_count == total_count and total_count > 0:
        await clear_all_attendance_records(company_id)
        logger.info(f"All records successfully synced to external API and local database cleared for company {company_id}.")
        return success_count, total_count, True
    else:
        logger.info(f"External API sync complete for company {company_id}. Successfully sent {success_count} of {total_count} records. Local database not cleared due to partial success or no records.")
        return success_count, total_count, False

# --- Dependency to get the current company context ---
async def get_company_context_for_fastapi(request: Request):
    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_token:
        logger.warning(f"Unauthenticated access attempt from {request.client.host} to '{request.url.path}'. No session token found. Redirecting to login.")
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            detail="Not authenticated",
            headers={"Location": "/login"}
        )

    try:
        query = select(companies).where(companies.c.session_token == session_token)
        company_info = await database.fetch_one(query)

        if not company_info:
            logger.error(f"Session token found but no associated company_id in DB. Invalidating session.")
            raise HTTPException(
                status_code=status.HTTP_303_SEE_OTHER,
                detail="Session invalid, please log in again.",
                headers={"Location": "/login"}
            )

        request.state.company_id = company_info['company_id']
        request.state.company_image_folder = company_info['company_image_folder']
        request.state.company_name = company_info['company_name']
        request.state.webrtc_url = company_info['webrtc_url']

    except Exception as e:
        logger.error(f"Error loading company context for FastAPI for session token {session_token}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to load company context."
        )

# Dependency for /api/add-entry that allows either session token or API key
async def get_company_context_for_api_entry(
    request: Request,
    authorization: Annotated[str | None, Header()] = None
):
    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    api_key_provided = None

    if authorization and authorization.startswith("Bearer "):
        api_key_provided = authorization.split(" ")[1]

    company_info = None
    try:
        if session_token:
            query = select(companies).where(companies.c.session_token == session_token)
            company_info = await database.fetch_one(query)
            if company_info:
                logger.debug(f"Authenticated via session token for company ID: {company_info['company_id']}")

        if not company_info and api_key_provided:
            query = select(companies).where(companies.c.api_key == api_key_provided)
            company_info = await database.fetch_one(query)
            if company_info:
                logger.debug(f"Authenticated via API key for company ID: {company_info['company_id']}")
            else:
                logger.warning(f"Invalid API key provided: {api_key_provided[:8]}...")

        if not company_info:
            logger.warning(f"Authentication failed for /api/add-entry. No valid session token or API key.")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated. Provide valid session token or API key."
            )

        request.state.company_id = company_info['company_id']
        request.state.company_image_folder = company_info['company_image_folder']
        request.state.company_name = company_info['company_name']        

        return company_info['company_id']

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error loading company context for /api/add-entry: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to load company context for API entry."
        )

# --- Login Routes ---
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, message: str | None = None):
    return templates.TemplateResponse("login.html", {"request": request, "message": message})

@app.post("/login")
async def perform_login(response: Response, username: str = Form(...), password: str = Form(...)):
    try:
        query = select(companies).where(companies.c.admin_username == username)
        company_record = await database.fetch_one(query)

        if company_record and check_password_hash(company_record['admin_password_hash'], password):
            company_id = company_record['company_id']
            session_token = str(uuid4())

            update_query = update(companies).where(companies.c.company_id == company_id).values(session_token=session_token)
            await database.execute(update_query)

            redirect_response = RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
            # FIX: Corrected typo from 'httpy_only' to 'httponly'
            redirect_response.set_cookie(key=SESSION_COOKIE_NAME, value=session_token, httponly=True, samesite="strict", expires=3600)
            logger.info(f"Successful login for user '{username}' (Company ID: {company_id}).")
            return redirect_response
        else:
            logger.warning(f"Failed login attempt for user '{username}'. Invalid credentials.")
            return RedirectResponse(url="/login?message=Invalid credentials", status_code=status.HTTP_302_FOUND)
    except Exception as e:
        logger.error(f"Error during login verification: {e}", exc_info=True)
        return RedirectResponse(url="/login?message=An internal error occurred during login.", status_code=status.HTTP_302_FOUND)

@app.get("/logout")
async def perform_logout(request: Request, response: Response):
    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    if session_token:
        try:
            update_query = update(companies).where(companies.c.session_token == session_token).values(session_token=None)
            await database.execute(update_query)
            logger.info(f"Session token cleared from database.")
        except Exception as e:
            logger.error(f"Error clearing session token on logout: {e}", exc_info=True)

    response.delete_cookie(key=SESSION_COOKIE_NAME)
    logger.info("User logged out successfully.")
    return RedirectResponse(url="/login?message=You have been logged out.", status_code=status.HTTP_302_FOUND)

# --- Protected FastAPI Routes ---
@app.get("/", response_class=HTMLResponse, dependencies=[Depends(get_company_context_for_fastapi)])
async def read_root(request: Request):
    socket_io_url = str(request.base_url).rstrip('/')
    company_id = request.state.company_id
    company_name = request.state.company_name

    total_present_today = await get_total_present_today(company_id)
    total_on_time_today = await get_total_on_time_today(company_id)
    total_late_today = await get_total_late_today(company_id)
    total_registered_employees = await get_total_registered_employees(company_id)
    session_token = request.cookies.get(SESSION_COOKIE_NAME)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "socket_io_url": socket_io_url,
        "total_present_today": total_present_today,
        "total_registered_employees": total_registered_employees,
        "total_on_time_today": total_on_time_today,
        "total_late_today": total_late_today,
        "company_name": company_name,
        "session_token": session_token
    })

@app.get("/grid", response_class=HTMLResponse, dependencies=[Depends(get_company_context_for_fastapi)])
async def read_grid_page(request: Request):
    socket_io_url = str(request.base_url).rstrip('/')
    company_id = request.state.company_id
    company_name = request.state.company_name

    total_present_today = await get_total_present_today(company_id)
    total_on_time_today = await get_total_on_time_today(company_id)
    total_late_today = await get_total_late_today(company_id)
    total_registered_employees = await get_total_registered_employees(company_id)
    session_token = request.cookies.get(SESSION_COOKIE_NAME)

    return templates.TemplateResponse("attendance_grid.html", {
        "request": request,
        "socket_io_url": socket_io_url,
        "total_present_today": total_present_today,
        "total_registered_employees": total_registered_employees,
        "total_on_time_today": total_on_time_today,
        "total_late_today": total_late_today,
        "company_name": company_name,
        "session_token": session_token
    })

@app.get("/stream")
async def read_stream_page(
    request: Request,
    _: None = Depends(get_company_context_for_fastapi)
):
    socket_io_url = str(request.base_url).rstrip('/')
    company_name = request.state.company_name
    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    webrtc_url = request.state.webrtc_url
    return templates.TemplateResponse(
        "stream.html",
        {
            "request": request,
            "socket_io_url": socket_io_url,
            "company_name": company_name,
            "session_token": session_token,
            "webrtc_url": webrtc_url,
        }
    )


@app.get("/company_images/{company_id_from_path}/{filename}", dependencies=[Depends(get_company_context_for_fastapi)])
async def serve_company_image(
    company_id_from_path: str,
    filename: str,
    request: Request,
):
    if company_id_from_path != str(request.state.company_id):
        logger.warning(f"Access forbidden: Requested company_id '{company_id_from_path}' does not match authenticated company_id '{request.state.company_id}'.")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access to specified company's images forbidden.")

    base_path = Path(request.state.company_image_folder)
    file_path = (base_path / secure_filename(filename)).resolve()

    # FIX: Python 3.8+ safe relative check
    try:
        file_path.relative_to(base_path.resolve())
    except ValueError:
        logger.error(f"Directory traversal attempt detected: {file_path}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid path.")

    if not file_path.exists() or not file_path.is_file():
        logger.error(f"Image not found at expected path: {file_path}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Image not found.")

    response = FileResponse(file_path)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.post("/api/add-entry", status_code=status.HTTP_201_CREATED)
async def add_entry_endpoint(
    entry: AttendanceEntryPayload,
    request: Request,
    company_id: str = Depends(get_company_context_for_api_entry),
):
    try:
        # Add entry to local database
        new_entry = await add_attendance_entry(
            company_id=company_id,
            emp_id=entry.emp_id,
            first_name=entry.first_name,
            last_name=entry.last_name,
            check_type=entry.check_type,
            image_url=entry.image_url
        )

        if not new_entry:
            logger.warning(f"Attempted to add duplicate/invalid entry for Emp ID {entry.emp_id}")
            return JSONResponse(
                content={"message": "Entry could not be created (duplicate, invalid, or no open check-in)."},
                status_code=status.HTTP_409_CONFLICT
            )

        # Prepare entry for client
        client_entry = _prepare_entry_for_client(new_entry, company_id)
        await sio.emit('new_attendance_entry', client_entry, room=str(company_id))

        # Emit updated statistics
        stats = {
            "total_present_today": await get_total_present_today(company_id),
            "total_registered_employees": await get_total_registered_employees(company_id),
            "total_on_time_today": await get_total_on_time_today(company_id),
            "total_late_today": await get_total_late_today(company_id)
        }
        await sio.emit('attendance_statistics_update', stats, room=str(company_id))

        # --- Prepare payload for Oracle-compatible external API ---
        attendance_time = new_entry['attendance_time']
        attendance_date = new_entry['attendance_date']

        payload_external = {
            "emp_id": new_entry['emp_id'],
            "first_name": new_entry['first_name'],
            "last_name": new_entry['last_name'],
            "check_type": new_entry['check_type'],
            "attendance_time": attendance_time.strftime("%H:%M:%S") if isinstance(attendance_time, datetime.datetime) else str(attendance_time),
            "attendance_date": attendance_date.strftime("%d-%b-%Y").upper() if isinstance(attendance_date, datetime.date) else str(attendance_date),
            "image_url": new_entry.get('image_url')
        }

        # Send record to external API
        success, response_data = await send_single_record_to_external_api(payload_external, company_id)

        # Update database with webhook result
        update_query = update(attendance).where(attendance.c.id == new_entry['id']).values(
            sent_to_webhook=bool(success) if success is not None else None,
            webhook_response=json.dumps(response_data) if not isinstance(response_data, str) else response_data
        )
        await database.execute(update_query)

        return JSONResponse(content={
            "message": "Entry added and broadcasted successfully",
            "entry": client_entry,
            "stats": stats
        })

    except Exception as e:
        logger.error(f"Error adding entry via API: {e}", exc_info=True)
        return JSONResponse(content={"message": f"Error adding entry: {e}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@app.get("/api/attendance-stats", response_class=JSONResponse, dependencies=[Depends(get_company_context_for_fastapi)])
async def get_attendance_stats_endpoint(request: Request):
    try:
        company_id = request.state.company_id
        total_present_today = await get_total_present_today(company_id)
        total_on_time_today = await get_total_on_time_today(company_id)
        total_late_today = await get_total_late_today(company_id)
        total_registered_employees = await get_total_registered_employees(company_id)
        # FIX: JSONResponse has no 'room' argument
        return JSONResponse(content={
            "total_present_today": total_present_today,
            "total_registered_employees": total_registered_employees,
            "total_on_time_today": total_on_time_today,
            "total_late_today": total_late_today
        })
    except Exception as e:
        logger.error(f"Error fetching attendance statistics: {e}", exc_info=True)
        return JSONResponse(content={"message": f"Error fetching statistics: {e}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

@app.delete("/api/clear-attendance", status_code=status.HTTP_200_OK, dependencies=[Depends(get_company_context_for_fastapi)])
async def clear_attendance_endpoint(
    request: Request,
    confirm: bool = Query(False, description="Set to true to confirm deletion of all attendance records."),
):
    if not confirm:
        return JSONResponse(
            content={"message": "Confirmation required. Add '?confirm=true' to the request URL to clear all records."},
            status_code=status.HTTP_400_BAD_REQUEST
        )
    try:
        company_id = request.state.company_id
        await clear_all_attendance_records(company_id)
        await sio.emit('attendance_data_cleared', {'message': 'All attendance records cleared.'})
        total_present_today = await get_total_present_today(company_id)
        total_on_time_today = await get_total_on_time_today(company_id)
        total_late_today = await get_total_late_today(company_id)
        total_registered_employees = await get_total_registered_employees(company_id)
        await sio.emit('attendance_statistics_update', {
            "total_present_today": total_present_today,
            "total_registered_employees": total_registered_employees,
            "total_on_time_today": total_on_time_today,
            "total_late_today": total_late_today
        })
        return JSONResponse(content={"message": "All attendance records cleared successfully.", "stats": {"total_present_today": total_present_today, "total_registered_employees": total_registered_employees, "total_on_time_today": total_on_time_today, "total_late_today": total_late_today}})
    except Exception as e:
        logger.error(f"Error clearing attendance records: {e}", exc_info=True)
        return JSONResponse(content={"message": f"Error clearing records: {e}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

@app.post("/api/sync-all-to-external", status_code=status.HTTP_200_OK, dependencies=[Depends(get_company_context_for_fastapi)])
async def sync_all_to_external_endpoint(request: Request):
    try:
        company_id = request.state.company_id
        logger.info(f"Syncing all attendance data for company {company_id} to external server...")

        all_attendance = await fetch_all_attendance(company_id, request)
        if not all_attendance:
            return JSONResponse(content={"message": "No attendance records to sync.", "success": True})

        success_count = 0
        total_count = len(all_attendance)
        
        for record in all_attendance:
            payload = {
                "emp_id": str(record['emp_id']),
                "first_name": record['first_name'],
                "last_name": record['last_name'],
                "check_type": record['check_type'],
                "attendance_time": record['attendance_time'],
                "attendance_date": record['attendance_date'],
                "image_url": record.get('image_url')
            }
            # FIX: pass company_id (was request)
            success, _ = await send_single_record_to_external_api(payload, company_id)
            if success:
                success_count += 1

        cleared = False
        if success_count == total_count and total_count > 0:
            await clear_all_attendance_records(company_id)
            cleared = True

        message = f"Successfully synced {success_count} of {total_count} records."
        if cleared:
            message += " Local database cleared."

        await sio.emit('external_sync_status', {"message": message, "success": success_count == total_count and cleared})
        return JSONResponse(content={"message": message, "success_count": success_count, "total_count": total_count, "cleared_local": cleared})

    except Exception as e:
        logger.error(f"Error syncing all to external: {e}", exc_info=True)
        await sio.emit('external_sync_status', {"message": f"Error initiating sync: {e}", "success": False})
        return JSONResponse(content={"message": f"Error initiating sync: {e}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@sio.event
async def connect(sid, environ, auth):
    logger.info(f"Socket.IO client connected: {sid}")
    try:
        session_token_from_auth = None
        api_key_from_auth = None
        company_id_for_sio = None

        if auth:
            session_token_from_auth = auth.get(SESSION_COOKIE_NAME)
            api_key_from_auth = auth.get("api_key")

        # --- Check session token first ---
        if session_token_from_auth:
            query = select(companies.c.company_id).where(companies.c.session_token == session_token_from_auth)
            company_info_sio = await database.fetch_one(query)

            if company_info_sio:
                company_id_for_sio = company_info_sio["company_id"]
                logger.info(f"Socket.IO client {sid} authenticated with session token for company ID: {company_id_for_sio}")
            else:
                logger.warning(f"Socket.IO client {sid} provided invalid session token.")
                await sio.emit("error", {"message": "Invalid session token. Please re-login."}, room=sid)
                return

        # --- If no session token, check API key ---
        elif api_key_from_auth:
            query = select(companies).where(companies.c.api_key == api_key_from_auth)
            company_info_sio = await database.fetch_one(query)

            if company_info_sio:
                company_id_for_sio = company_info_sio["company_id"]
                logger.info(f"Socket.IO client {sid} authenticated with API key for company ID: {company_id_for_sio}")
            else:
                logger.warning(f"Socket.IO client {sid} provided invalid API key.")
                await sio.emit("error", {"message": "Invalid API key."}, room=sid)
                return

        # --- No valid auth found ---
        if not company_id_for_sio:
            logger.warning(f"Socket.IO client {sid} connected without valid authentication.")
            await sio.emit("error", {"message": "No valid authentication provided."}, room=sid)
            return

        # --- Join room for this company ---
        await sio.save_session(sid, {"company_id": company_id_for_sio})
        await sio.enter_room(sid, str(company_id_for_sio))
        logger.info(f"Socket.IO client {sid} joined room for company {company_id_for_sio}")

        # --- Send initial attendance data ---
        dummy_request = Request(scope={"type": "http", "asgi": {"version": "3.0"}})
        dummy_request.state.company_id = company_id_for_sio

        all_attendance = await fetch_all_attendance(company_id_for_sio, dummy_request)
        await sio.emit("initial_attendance_data", all_attendance, room=sid)

        total_present_today = await get_total_present_today(company_id_for_sio)
        total_on_time_today = await get_total_on_time_today(company_id_for_sio)
        total_late_today = await get_total_late_today(company_id_for_sio)
        total_registered_employees = await get_total_registered_employees(company_id_for_sio)

        await sio.emit(
            "attendance_statistics_update",
            {
                "total_present_today": total_present_today,
                "total_registered_employees": total_registered_employees,
                "total_on_time_today": total_on_time_today,
                "total_late_today": total_late_today,
            },
            room=sid,
        )
        logger.info(f"Sent initial attendance data + statistics to {sid} for company {company_id_for_sio}")

    except Exception as e:
        logger.error(f"Unexpected error in Socket.IO connect for {sid}: {e}", exc_info=True)
        await sio.emit("error", {"message": "An unexpected server error occurred during connection."}, room=sid)

@sio.event
async def disconnect(sid):
    try:
        session = await sio.get_session(sid)
        company_id = session.get("company_id") if session else None

        if company_id:
            await sio.leave_room(sid, str(company_id))
            logger.info(f"Socket.IO client {sid} disconnected and left room for company {company_id}")
        else:
            logger.info(f"Socket.IO client {sid} disconnected (no associated company room)")

    except Exception as e:
        logger.error(f"Error during disconnect for {sid}: {e}", exc_info=True)


if __name__ == "__main__":
    uvicorn.run(socketio_asgi_app, host="0.0.0.0", port=5002, reload=APP_DEBUG)