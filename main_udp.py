import sys
import os
import traceback
import time
import threading
import hashlib
import logging
import multiprocessing
import gi

os.environ['GIO_USE_VFS'] = 'local'
os.environ['GIO_USE_PROXY_RESOLVER'] = 'dummy'
os.environ['GIO_MODULE_DIR'] = '/nonexistent'
os.environ['no_proxy'] = '*'
os.environ['ALL_PROXY'] = ''
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''
os.environ['FTP_PROXY'] = ''

gi.require_version('Gst', '1.0')
gi.require_version('GstRtspServer', '1.0')
from gi.repository import Gst, GObject, GLib, GstRtspServer

from utils.probe_redis import pgie_src_filter_probe, attach_sgie_probe, attendance_worker
from utils.parser_cfg import parse_args, set_property, set_tracker_properties, load_faces
from utils.bus_call import bus_call

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeepStream")

# ===== Hot reload for faces =====
def get_dir_signature(directory):
    h = hashlib.md5()
    for root, _, files in os.walk(directory):
        for f in sorted(files):
            p = os.path.join(root, f)
            try:
                st = os.stat(p)
                h.update(f.encode())
                h.update(str(st.st_mtime).encode())
                h.update(str(st.st_size).encode())
            except Exception:
                continue
    return h.hexdigest()

def start_face_reloader(path, shared_faces, interval=5):
    sig = [get_dir_signature(path)]
    def loop():
        while True:
            try:
                new_sig = get_dir_signature(path)
                if new_sig != sig[0]:
                    logger.info("Hot reload: faces updated")
                    new_faces = load_faces(path) or {}
                    shared_faces.clear()
                    shared_faces.update(new_faces)
                    sig[0] = new_sig
            except Exception:
                traceback.print_exc()
            time.sleep(interval)
    threading.Thread(target=loop, daemon=True).start()

# ===== Logging helpers =====
def log_error(message):
    sys.stderr.write(f"ERROR: {message}\n")
    sys.stdout.flush()
def log_info(message):
    print(f"INFO: {message}")
    sys.stdout.flush()

# ===== Source bin helpers =====
def cb_newpad(decodebin, decoder_src_pad, data):
    try:
        source_bin = data
        queue = source_bin.get_by_name("queue")
        if not queue:
            log_error("Queue not found in source bin")
            return
        sink_pad = queue.get_static_pad("sink")
        if sink_pad.is_linked():
            return
        if decoder_src_pad.link(sink_pad) != Gst.PadLinkReturn.OK:
            log_error("Failed to link decodebin to queue")
        else:
            log_info("Linked decodebin → queue")
    except Exception as e:
        log_error(f"Exception in cb_newpad: {e}\n{traceback.format_exc()}")

def decodebin_child_added(child_proxy, obj, name, user_data):
    if "decodebin" in name:
        obj.connect("child-added", decodebin_child_added, user_data)
    if "source" in name:
        obj.set_property("drop-on-latency", True)

def create_source_bin(index, uri):
    bin = Gst.Bin.new(f"source-bin-{index}")

    decodebin = Gst.ElementFactory.make("uridecodebin", f"decodebin-{index}")
    decodebin.set_property("uri", uri)
    decodebin.connect("pad-added", cb_newpad, bin)
    decodebin.connect("child-added", decodebin_child_added, bin)

    queue = Gst.ElementFactory.make("queue", "queue")
    queue.set_property("leaky", 2)
    queue.set_property("max-size-buffers", 30)

    nvvidconv = Gst.ElementFactory.make("nvvideoconvert", f"nvvidconv-{index}")

    capsfilter = Gst.ElementFactory.make("capsfilter", f"caps-{index}")
    capsfilter.set_property(
        "caps",
        Gst.Caps.from_string("video/x-raw(memory:NVMM), format=NV12")
    )

    bin.add(decodebin)
    bin.add(queue)
    bin.add(nvvidconv)
    bin.add(capsfilter)

    queue.link(nvvidconv)
    nvvidconv.link(capsfilter)

    ghost_pad = Gst.GhostPad.new("src", capsfilter.get_static_pad("src"))
    bin.add_pad(ghost_pad)

    return bin

# ===== RTSP server =====
def start_rtsp_server_from_cfg(cfg, num_cams):
    rtsp_cfg = cfg["rtsp_server"]
    udp_host = rtsp_cfg["udpsink-host"]
    udp_port_base = int(rtsp_cfg["udpsink-port"])
    port = int(rtsp_cfg["port"])

    server = GstRtspServer.RTSPServer()
    server.props.service = str(port)
    mounts = server.get_mount_points()

    # Tiled stream
    factory = GstRtspServer.RTSPMediaFactory()
    factory.set_launch(
        f"( udpsrc address={udp_host} port={udp_port_base} caps="
        f"\"application/x-rtp,media=video,encoding-name=H264,payload=96\" "
        f"! rtph264depay ! rtph264pay name=pay0 pt=96 )"
    )
    factory.set_shared(True)
    mounts.add_factory(rtsp_cfg["mount-point"], factory)
    log_info(f"Tiled RTSP stream: rtsp://{udp_host}:{port}{rtsp_cfg['mount-point']}")

    # Individual streams
    for i in range(num_cams):
        mount = f"/cam{i}"
        factory = GstRtspServer.RTSPMediaFactory()
        factory.set_launch(
            f"( udpsrc address={udp_host} port={udp_port_base + i} caps="
            f"\"application/x-rtp,media=video,encoding-name=H264,payload=96\" "
            f"! rtph264depay ! rtph264pay name=pay0 pt=96 )"
        )
        factory.set_shared(True)
        mounts.add_factory(mount, factory)
        log_info(f"Individual RTSP stream: rtsp://{udp_host}:{port}{mount}")

    server.attach(None)

# ===== UDP branch (per camera) =====
# ===== UDP + RTSP branch for multiple cameras =====
def add_udp_rtsp_branches(pipeline, tee, cfg):
    rtsp_cfg = cfg["rtsp_server"]
    codec = rtsp_cfg.get("codec", "H264")
    udp_ports = rtsp_cfg.get("udpsink-ports", [int(rtsp_cfg["udpsink-port"])])
    mount_points = rtsp_cfg.get("mount-points", [rtsp_cfg["mount-point"]])
    udp_host = rtsp_cfg.get("udpsink-host", "127.0.0.1")

    server = GstRtspServer.RTSPServer()
    server.props.service = str(rtsp_cfg.get("port", 8555))
    mounts = server.get_mount_points()

    for i, src_cfg in enumerate(cfg["sources"]):
        udp_port = udp_ports[i % len(udp_ports)]
        mount_point = mount_points[i % len(mount_points)]

        # GStreamer elements
        queue = Gst.ElementFactory.make("queue")
        nvvidconv = Gst.ElementFactory.make("nvvideoconvert")
        capsfilter = Gst.ElementFactory.make("capsfilter")
        capsfilter.set_property("caps", Gst.Caps.from_string(
            "video/x-raw(memory:NVMM), width=1280, height=720, format=I420"
        ))

        if codec.upper() == "H265":
            encoder = Gst.ElementFactory.make("nvv4l2h265enc")
            pay = Gst.ElementFactory.make("rtph265pay")
        else:
            encoder = Gst.ElementFactory.make("nvv4l2h264enc")
            pay = Gst.ElementFactory.make("rtph264pay")

        encoder.set_property("bitrate", 4000000)
        encoder.set_property("iframeinterval", 30)
        pay.set_property("config-interval", 1)

        udpsink = Gst.ElementFactory.make("udpsink")
        udpsink.set_property("host", udp_host)
        udpsink.set_property("port", udp_port)
        udpsink.set_property("async", False)
        udpsink.set_property("sync", False)
        udpsink.set_property("qos", False)

        # Add elements to pipeline
        for e in [queue, nvvidconv, capsfilter, encoder, pay, udpsink]:
            pipeline.add(e)

        # Link the branch
        tee_src_pad = tee.get_request_pad(f"src_%u")
        tee_src_pad.link(queue.get_static_pad("sink"))
        queue.link(nvvidconv)
        nvvidconv.link(capsfilter)
        capsfilter.link(encoder)
        encoder.link(pay)
        pay.link(udpsink)

        # Add RTSP mount for this camera
        factory = GstRtspServer.RTSPMediaFactory()
        factory.set_launch(
            f"( udpsrc address={udp_host} port={udp_port} caps="
            f"\"application/x-rtp,media=video,encoding-name=H264,payload=96\" "
            f"! rtph264depay ! rtph264pay name=pay0 pt=96 )"
        )
        factory.set_shared(True)
        mounts.add_factory(mount_point, factory)

        print(f"INFO: Camera {i} mounted at rtsp://{udp_host}:{server.props.service}{mount_point}, UDP port {udp_port}")

    server.attach(None)

# ===== Main =====
def main(cfg):
    multiprocessing.set_start_method("spawn", force=True)
    Gst.init(None)

    known_dir = cfg["pipeline"]["known_face_dir"]
    known_faces = load_faces(known_dir) or {}

    attendance_q = multiprocessing.Queue()
    attendance_p = multiprocessing.Process(target=attendance_worker, args=(attendance_q,), daemon=True)
    attendance_p.start()

    pipeline = Gst.Pipeline.new("ds-pipeline")
    streammux = Gst.ElementFactory.make("nvstreammux")
    pgie = Gst.ElementFactory.make("nvinfer")
    tracker = Gst.ElementFactory.make("nvtracker")
    nvvidconv_sgie = Gst.ElementFactory.make("nvvideoconvert")
    caps_sgie = Gst.ElementFactory.make("capsfilter")
    sgie = Gst.ElementFactory.make("nvinfer")
    tiler = Gst.ElementFactory.make("nvmultistreamtiler")
    nvvidconv = Gst.ElementFactory.make("nvvideoconvert")
    nvosd = Gst.ElementFactory.make("nvdsosd")
    tee = Gst.ElementFactory.make("tee")

    for e in [streammux, pgie, tracker, nvvidconv_sgie, caps_sgie, sgie,
              tiler, nvvidconv, nvosd, tee]:
        pipeline.add(e)

    is_live = False
    for i, src in enumerate(cfg["sources"]):
        uri = src["uri"]
        if uri.startswith(("rtsp://", "rtspt://")):
            is_live = True
        sb = create_source_bin(i, uri)
        pipeline.add(sb)
        sinkpad = streammux.get_request_pad(f"sink_{i}")
        sb.get_static_pad("src").link(sinkpad)

    streammux.set_property("live-source", is_live)
    set_property(cfg, streammux, "streammux")
    set_property(cfg, pgie, "pgie")
    set_property(cfg, sgie, "sgie")
    set_property(cfg, tiler, "tiler")
    set_property(cfg, nvosd, "nvosd")
    set_tracker_properties(tracker, cfg["tracker"]["config-file-path"])

    # Main pipeline link
    streammux.link(pgie)
    pgie.link(tracker)
    tracker.link(nvvidconv_sgie)
    nvvidconv_sgie.link(caps_sgie)
    caps_sgie.link(sgie)
    sgie.link(tiler)
    tiler.link(nvvidconv)
    nvvidconv.link(nvosd)
    nvosd.link(tee)

    # Display branch
    q_disp = Gst.ElementFactory.make("queue")
    sink_disp = Gst.ElementFactory.make("nveglglessink" if cfg["pipeline"]["display"] else "fakesink")
    pipeline.add(q_disp); pipeline.add(sink_disp)
    tee.get_request_pad("src_%u").link(q_disp.get_static_pad("sink"))
    q_disp.link(sink_disp)

    attach_sgie_probe(sgie, known_faces, attendance_q, cfg["sources"])
    pgie.get_static_pad("src").add_probe(Gst.PadProbeType.BUFFER, pgie_src_filter_probe, None)
    start_face_reloader(known_dir, known_faces, int(cfg["pipeline"].get("reload_interval", 5)))

    # UDP branches + RTSP server    
    add_udp_rtsp_branches(pipeline, tee, cfg)
    #start_rtsp_server_from_cfg(cfg, len(cfg["sources"]))

    # Run
    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)

    pipeline.set_state(Gst.State.PLAYING)
    logger.info("Pipeline PLAYING")

    try:
        loop.run()
    finally:
        pipeline.set_state(Gst.State.NULL)
        attendance_q.put(None)
        attendance_p.join(3)

    return 0

if __name__ == "__main__":
    cfg = parse_args("config/config_pipeline.toml")
    sys.exit(main(cfg))
