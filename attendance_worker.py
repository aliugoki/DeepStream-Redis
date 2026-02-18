# utils/attendance_worker.py
import os
import logging
import multiprocessing
from .posgres_redis import log_attendance
from .attendance_fsm import process_event

logger = logging.getLogger("AttendanceWorker")

def attendance_worker(queue: multiprocessing.Queue):
    logger.info(f"Attendance worker started (pid={os.getpid()})")

    while True:
        task = queue.get()
        if task is None: # Shutdown signal
            break

        try:
            # 1. Normalize Event
            direction = str(task.get("check_type", "IN")).upper()
            event = "CHECK_OUT" if direction == "OUT" else "CHECK_IN"

            # 2. FSM Gatekeeper (Redis)
            # We do this here so the DeepStream probe stays fast!
            accepted = process_event(
                company_id=task["company_id"],
                emp_id=task["emp_id"],
                detected_event=event,
                camera_name=task.get("camera_name"),
                image_url=task.get("image_url")
            )

            # 3. Database Persistence
            if accepted:
                log_attendance(
                    emp_id=task["emp_id"],
                    company_id=task["company_id"],
                    first_name=task.get("first_name"),
                    last_name=task.get("last_name"),
                    attendance_date=task.get("attendance_date"),
                    attendance_time=task.get("attendance_time"),
                    check_type=direction,
                    camera_name=task.get("camera_name"),
                    image_url=task.get("image_url")
                )
                logger.info(f"✅ Logged {event} for Emp: {task['emp_id']}")
            else:
                logger.warning(f"⚠️ FSM Rejected {event} for Emp: {task['emp_id']} (Invalid Sequence)")

        except Exception as e:
            logger.error(f"Worker Error: {e}", exc_info=True)