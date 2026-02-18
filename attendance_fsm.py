# utils/attendance_fsm.py

import logging
from utils.posgres_redis import redis_client

logger = logging.getLogger("AttendanceFSM")


def get_last_state(emp_id, company_id):
    key = f"fsm:{company_id}:{emp_id}"
    state = redis_client.get(key)

    if state is None:
        logger.info(f"[REDIS MISS] {key}")
    else:
        logger.info(f"[REDIS READ] {key} = {state}")

    return state


def set_last_state(emp_id, company_id, new_state):
    key = f"fsm:{company_id}:{emp_id}"
    redis_client.setex(key, 86400, new_state)

    logger.info(f"[REDIS WRITE] {key} → {new_state} (TTL=86400)")


def process_event(emp_id, company_id, detected_event, camera_name=None, image_url=None):
    detected_event = detected_event.upper()

    logger.info(
        f"[FSM EVENT] emp={emp_id} company={company_id} event={detected_event}"
    )

    last_state = get_last_state(emp_id, company_id)

    allowed = False
    new_state = last_state

    if detected_event == "CHECK_IN":
        if last_state != "IN":
            allowed = True
            new_state = "IN"

    elif detected_event == "CHECK_OUT":
        if last_state == "IN":
            allowed = True
            new_state = "OUT"

    if allowed:
        set_last_state(emp_id, company_id, new_state)
        logger.info(
            f"[FSM ACCEPTED] emp={emp_id} {last_state} → {new_state}"
        )
    else:
        logger.warning(
            f"[FSM REJECTED] emp={emp_id} last_state={last_state} event={detected_event}"
        )

    return allowed
