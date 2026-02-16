import os
import sys
import time
import logging
import traceback
import multiprocessing
import numpy as np
from datetime import datetime, date

# GStreamer and DeepStream Imports
try:
    import gi
    gi.require_version('Gst', '1.0')
    from gi.repository import Gst
    import pyds
except Exception as e:
    print(f"Failed to import GStreamer/DeepStream dependencies: {e}")
    sys.exit(1)

# Application Imports
from . import posgres_redis

# ------------------------------------------------------------
# 1. Configuration & Constants
# ------------------------------------------------------------
get_user_info = posgres_redis.get_user_info
log_attendance = posgres_redis.log_attendance
COMPANY_ID = posgres_redis.COMPANY_ID

# Recognition Params
PGIE_CONFIDENCE_THRESHOLD = 0.6
RECOGNITION_THRESHOLD = 0.4
FEATURE_VECTOR_SIZE = 512

# Cooldowns & Retention
ATTENDANCE_COOLDOWN_SECONDS = 10
GLOBAL_STATUS_COOLDOWN = 5
FACE_SAVE_RETENTION_DAYS = 2
FACE_SAVE_CLEAN_INTERVAL = 300

# Geometry
DETECTION_AREA = {'x1': 0, 'y1': 250, 'x2': 2500, 'y2': 2000}
DETECTION_LINE = {'x1': 0, 'y1': 250, 'x2': 1920, 'y2': 250}

# Display Constraints
MAX_DISPLAY_LABELS = 16
MAX_DISPLAY_LINES = 16
MAX_DISPLAY_RECTS = 16

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DeepStream")

# ------------------------------------------------------------
# 2. Global State & Memory Management
# ------------------------------------------------------------
CAMERA_STATE = {}
TRACKED_OBJECTS = {}
FACE_SAVE_STATE = {}
FACE_SAVE_LOCK = multiprocessing.Lock()

_LAST_FACE_CLEANUP_TS = 0
_OBJ_ENC_CTX = None

def get_obj_encoder(gpu_id=0):
    global _OBJ_ENC_CTX
    if _OBJ_ENC_CTX is None:
        _OBJ_ENC_CTX = pyds.nvds_obj_enc_create_context(gpu_id)
        logger.info(f"NvDs object encoder context created on GPU {gpu_id}")
    return _OBJ_ENC_CTX

def cleanup_face_save_state():
    """Removes old face-save entries to prevent memory leaks."""
    global _LAST_FACE_CLEANUP_TS
    now = time.time()
    if now - _LAST_FACE_CLEANUP_TS < FACE_SAVE_CLEAN_INTERVAL:
        return
    
    today_ord = date.today().toordinal()
    cutoff_ord = today_ord - FACE_SAVE_RETENTION_DAYS
    with FACE_SAVE_LOCK:
        keys_to_del = [k for k in FACE_SAVE_STATE if k[2].toordinal() < cutoff_ord]
        for k in keys_to_del: del FACE_SAVE_STATE[k]
        if keys_to_del:
            logger.info(f"🧹 Cleaned {len(keys_to_del)} stale face-save entries.")
    _LAST_FACE_CLEANUP_TS = now

def should_save_face(source_id, emp_id, tracker_id):
    """Throttles face saving to once per employee/camera/day."""
    today = date.today()
    key = (source_id, emp_id, today)
    with FACE_SAVE_LOCK:
        if key in FACE_SAVE_STATE:
            return False
        FACE_SAVE_STATE[key] = {"tracker_id": tracker_id, "timestamp": datetime.now()}
        return True

# ------------------------------------------------------------
# 3. Drawing & UI Utilities
# ------------------------------------------------------------

def add_label(frame_meta, batch_meta, text, x, y, color=(1.0, 1.0, 1.0, 1.0), bg=(0.0, 0.0, 0.0, 0.6)):
    """Generic helper to add text labels to a frame."""
    display_meta = pyds.nvds_acquire_display_meta_from_pool(batch_meta)
    if display_meta.num_labels < MAX_DISPLAY_LABELS:
        txt = display_meta.text_params[display_meta.num_labels]
        txt.display_text = text
        txt.x_offset, txt.y_offset = int(x), int(max(0, y))
        txt.font_params.font_name = "Serif"
        txt.font_params.font_size = 15
        txt.font_params.font_color.set(*color)
        txt.set_bg_clr = 1
        txt.text_bg_clr.set(*bg)
        display_meta.num_labels += 1
        pyds.nvds_add_display_meta_to_frame(frame_meta, display_meta)
    else:
        pyds.nvds_release_display_meta(display_meta)

def draw_static_overlays(frame_meta, batch_meta):
    """Draws the detection zone and crossing line on the frame."""
    display_meta = pyds.nvds_acquire_display_meta_from_pool(batch_meta)
    
    # Detection Rect
    if display_meta.num_rects < MAX_DISPLAY_RECTS:
        rect = display_meta.rect_params[display_meta.num_rects]
        rect.left, rect.top = DETECTION_AREA['x1'], DETECTION_AREA['y1']
        rect.width = DETECTION_AREA['x2'] - DETECTION_AREA['x1']
        rect.height = DETECTION_AREA['y2'] - DETECTION_AREA['y1']
        rect.border_width = 2
        rect.border_color.set(0.0, 1.0, 0.0, 0.5)
        display_meta.num_rects += 1
        
    # Crossing Line
    if display_meta.num_lines < MAX_DISPLAY_LINES:
        line = display_meta.line_params[display_meta.num_lines]
        line.x1, line.y1 = DETECTION_LINE['x1'], DETECTION_LINE['y1']
        line.x2, line.y2 = DETECTION_LINE['x2'], DETECTION_LINE['y2']
        line.line_width = 3
        line.line_color.set(1.0, 0.0, 0.0, 0.8)
        display_meta.num_lines += 1
        
    pyds.nvds_add_display_meta_to_frame(frame_meta, display_meta)

# ------------------------------------------------------------
# 4. Geometry & Logic Helpers
# ------------------------------------------------------------

def is_in_area(obj_meta):
    r = obj_meta.rect_params
    cx, cy = r.left + r.width / 2, r.top + r.height / 2
    return (DETECTION_AREA['x1'] <= cx <= DETECTION_AREA['x2'] and 
            DETECTION_AREA['y1'] <= cy <= DETECTION_AREA['y2'])

def process_line_crossing(obj_meta, obj_id, now):
    """Detects if an object bottom-center crossed the horizontal detection line."""
    bottom_y = obj_meta.rect_params.top + obj_meta.rect_params.height
    line_y = DETECTION_LINE['y1']
    
    if obj_id not in TRACKED_OBJECTS:
        TRACKED_OBJECTS[obj_id] = {'last_y': bottom_y, 'last_cross_ts': None}
    
    prev_y = TRACKED_OBJECTS[obj_id]['last_y']
    TRACKED_OBJECTS[obj_id]['last_y'] = bottom_y
    
    direction = None
    if prev_y < line_y <= bottom_y: direction = "IN"
    elif prev_y > line_y >= bottom_y: direction = "OUT"
    
    if direction:
        last_t = TRACKED_OBJECTS[obj_id]['last_cross_ts']
        if last_t is None or (now - last_t).total_seconds() > ATTENDANCE_COOLDOWN_SECONDS:
            TRACKED_OBJECTS[obj_id]['last_cross_ts'] = now
            return direction
    return None

# ------------------------------------------------------------
# 5. Recognition & Vector Helpers
# ------------------------------------------------------------

def get_face_feature(obj_meta):
    """Extracts 512d normalized vector from SGIE Tensor Metadata."""
    l_user = obj_meta.obj_user_meta_list
    while l_user:
        user_meta = pyds.NvDsUserMeta.cast(l_user.data)
        if user_meta.base_meta.meta_type == pyds.NvDsMetaType.NVDSINFER_TENSOR_OUTPUT_META:
            tensor = pyds.NvDsInferTensorMeta.cast(user_meta.user_meta_data)
            layer = pyds.get_nvds_LayerInfo(tensor, 0)
            # Efficient extraction via list comprehension
            features = [pyds.get_detections(layer.buffer, i) for i in range(FEATURE_VECTOR_SIZE)]
            res = np.array(features, dtype=np.float32)
            norm = np.linalg.norm(res)
            return (res / norm).reshape((1, FEATURE_VECTOR_SIZE)) if norm > 0 else None
        l_user = l_user.next
    return None

def match_faces(feature, loaded_faces):
    """Cosine similarity matching against dictionary of loaded face vectors."""
    best_id, best_score = None, -1.0
    feat_flat = feature.flatten()
    for uid, known in loaded_faces.items():
        score = np.dot(feat_flat, known.flatten())
        if score > best_score:
            best_id, best_score = uid, score
    return best_id, best_score

# ------------------------------------------------------------
# 6. Core Probes
# ------------------------------------------------------------

def pgie_src_filter_probe(pad, info, u_data):
    """Filters low confidence detections at the PGIE output."""
    gst_buffer = info.get_buffer()
    if not gst_buffer: return Gst.PadProbeReturn.OK
    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    
    l_frame = batch_meta.frame_meta_list
    while l_frame:
        frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        l_obj = frame_meta.obj_meta_list
        to_remove = []
        while l_obj:
            obj_meta = pyds.NvDsObjectMeta.cast(l_obj.data)
            if obj_meta.confidence < PGIE_CONFIDENCE_THRESHOLD:
                to_remove.append(obj_meta)
            l_obj = l_obj.next
        for o in to_remove:
            pyds.nvds_remove_obj_meta_from_frame(frame_meta, o)
        l_frame = l_frame.next
    return Gst.PadProbeReturn.OK

def sgie_feature_extract_probe(pad, info, user_data):
    """Primary logic probe: Recognition, Line Crossing, and FSM Attendance."""
    cleanup_face_save_state()
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        return Gst.PadProbeReturn.OK
    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    
    loaded_faces = user_data.get('loaded_faces', {})
    q = user_data.get('attendance_queue')
    sources = user_data.get('sources', [])

    l_frame = batch_meta.frame_meta_list
    while l_frame:
        frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        src_id = frame_meta.source_id
        now = datetime.now()
        
        # Initialize Camera State
        if src_id not in CAMERA_STATE:
            CAMERA_STATE[src_id] = {
                'in': 0, 'out': 0, 'status': 'Standby', 
                'status_ts': now, 'last_date': date.today()
            }
        
        cam = CAMERA_STATE[src_id]
        if cam['last_date'] != date.today():
            cam.update({'in': 0, 'out': 0, 'last_date': date.today()})

        draw_static_overlays(frame_meta, batch_meta)
        
        l_obj = frame_meta.obj_meta_list
        while l_obj:
            obj_meta = pyds.NvDsObjectMeta.cast(l_obj.data)
            
            # 1. Line Crossing Logic
            direction = process_line_crossing(obj_meta, obj_meta.object_id, now)
            if direction:
                cam[direction.lower()] += 1
                cam['status'], cam['status_ts'] = direction, now

            # 2. Face Recognition Logic
            feat = get_face_feature(obj_meta)
            name_label = "Unknown"
            
            if feat is not None:
                uid, score = match_faces(feat, loaded_faces)
                if uid and score >= RECOGNITION_THRESHOLD:
                    fname, lname, emp_id, img_path = get_user_info(uid, COMPANY_ID)
                    name_label = f"{fname} {lname}"
                    
                    # 3. Attendance & FSM Logic
                    if is_in_area(obj_meta) and should_save_face(src_id, uid, obj_meta.object_id):
                        # Construct save path
                        save_dir = os.path.join("/workspace/data/attendance_faces", f"Cam_{src_id}", str(date.today()))
                        os.makedirs(save_dir, exist_ok=True)
                        save_path = os.path.join(save_dir, f"{uid}_{now.strftime('%H%M%S')}.jpg")
                        
                        # Process NvDs Object Crop
                        enc_args = pyds.NvDsObjEncUsrArgs()
                        enc_args.saveImg, enc_args.fileNameImg, enc_args.quality = 1, save_path, 90
                        pyds.nvds_obj_enc_process(get_obj_encoder(), enc_args, hash(gst_buffer), obj_meta, frame_meta)
                        
                        # Queue for DB worker
                        try:
                            q.put_nowait({
                                "company_id": COMPANY_ID,
                                "emp_id": uid,
                                "first_name": fname,
                                "last_name": lname,
                                "image_url": img_path,
                                "attendance_date": now.strftime("%d-%m-%Y"),
                                "attendance_time": now.strftime("%H:%M:%S"),
                                "check_type": direction or "auto",
                                "camera_name": f"Camera_{src_id}"
                            })
                        except multiprocessing.queues.Full:
                            logger.warning(
                                f"Attendance queue full – skipped emp_id={uid} cam={src_id}"
                            )

                        # --- FSM Event Trigger ---
                        try:
                            from utils.attendance_fsm import process_event
                            process_event(
                                emp_id=uid,
                                company_id=COMPANY_ID,
                                detected_event=direction or "IN",
                                camera_name=f"Camera_{src_id}",
                                image_url=img_path
                            )
                        except Exception as e:
                            logger.error(f"FSM Error: {e}")


            add_label(frame_meta, batch_meta, name_label, obj_meta.rect_params.left, obj_meta.rect_params.top - 20)
            l_obj = l_obj.next
        
        # Dashboard UI Update
        if (now - cam['status_ts']).total_seconds() > GLOBAL_STATUS_COOLDOWN:
            cam['status'] = 'Standby'
        
        dashboard_txt = f"STATUS: {cam['status']} | IN: {cam['in']} OUT: {cam['out']}"
        add_label(frame_meta, batch_meta, dashboard_txt, 10, 10, color=(1, 1, 0, 1), bg=(0, 0, 0, 0.8))

        l_frame = l_frame.next
    return Gst.PadProbeReturn.OK

# ------------------------------------------------------------
# 7. Lifecycle & Entry Points
# ------------------------------------------------------------

from utils.attendance_fsm import process_event


def attendance_worker(q):
    logger.info(f"Worker process {os.getpid()} online.")

    while True:
        task = q.get()
        if task is None:
            break

        try:
            # 1️⃣ Map direction → FSM event
            direction = task.get("check_type")

            if direction == "IN":
                event = "CHECK_IN"
            elif direction == "OUT":
                event = "CHECK_OUT"
            else:
                event = "CHECK_IN"  # fallback

            # 2️⃣ Run FSM in Redis
            accepted = process_event(
                company_id=task["company_id"],
                emp_id=task["emp_id"],
                detected_event=event
            )

            # 3️⃣ Only write to DB if FSM accepted transition
            if accepted:
                log_attendance(**task)

        except Exception as e:
            logger.error(f"Worker Error: {e}")


def attach_sgie_probe(sgie, loaded_faces, attendance_queue, sources):
    """Attaches the refactored sgie probe to the pipeline."""
    pad = sgie.get_static_pad("src")
    if pad:
        user_data = {
            "loaded_faces": loaded_faces, 
            "attendance_queue": attendance_queue, 
            "sources": sources
        }
        pad.add_probe(Gst.PadProbeType.BUFFER, sgie_feature_extract_probe, user_data)
        logger.info("SGIE src probe attached successfully.")
    else:
        logger.error("Failed to acquire SGIE src pad.")
