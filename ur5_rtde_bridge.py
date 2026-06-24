#!/usr/bin/env python3
"""
ur5_rtde_bridge.py — RTDE → WebSocket bridge for UR5 digital shadow

Reads live joint data from your UR5 via ur_rtde and broadcasts it
over a WebSocket so the browser-based 3D viewer can mirror the robot
in real time. Includes a live config dashboard served on HTTP.

Requirements:
    pip install ur-rtde websockets pyyaml aiohttp

Usage:
    python ur5_rtde_bridge.py                                           # defaults
    python ur5_rtde_bridge.py --config config.yaml                      # load yaml
    python ur5_rtde_bridge.py --robot-ip 192.168.1.237 --ws-port 9090  # override
"""

import argparse
import asyncio
import json
import math
import signal
import sys
import threading
import time
import os
from pathlib import Path
from typing import Optional

import yaml

try:
    from rtde_receive import RTDEReceiveInterface as RTDEReceive
    RTDE_AVAILABLE = True
except ImportError:
    print("WARNING: ur-rtde not installed. Running in SIMULATION mode.")
    print("         Run:  pip install ur-rtde   to connect a real robot.\n")
    RTDE_AVAILABLE = False

try:
    import websockets
    from websockets.server import serve as ws_serve
except ImportError:
    print("ERROR: websockets not installed. Run:  pip install websockets")
    sys.exit(1)

try:
    from aiohttp import web as aio_web
    AIOHTTP_AVAILABLE = True
except ImportError:
    print("WARNING: aiohttp not installed. Dashboard will not be available.")
    print("         Run:  pip install aiohttp\n")
    AIOHTTP_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────
#  Default configuration (also written to config.yaml on first run)
# ─────────────────────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "robot": {
        "ip": "192.168.1.237",
        "frequency_hz": 10.0,
        "reconnect_delay_s": 5.0,
        "stale_data_warn_s": 1.0,
    },
    "websocket": {
        "host": "0.0.0.0",
        "port": 9090,
        "broadcast_hz": 10.0,
    },
    "dashboard": {
        "host": "0.0.0.0",
        "port": 8080,
        "enabled": True,
    },
    "identity": {
        "name":     "UR5 #1",
        "model":    "Universal Robots UR5 (CB3)",
        "serial":   "SN-XXXX-XXXX",
        "location": "Halle A / Montagestation 3",
        "asset_id": "ROBOT-001",
    },
    "logging": {
        "console_hz": 1.0,
        "log_to_file": False,
        "log_file": "ur5_bridge.log",
    },
}

CONFIG_FILE = Path("config.yaml")


# ─────────────────────────────────────────────────────────────────
#  Global shared state (protected by lock where needed)
# ─────────────────────────────────────────────────────────────────
packet_lock = threading.Lock()
latest_packet: Optional[dict] = None

config_lock = threading.Lock()
config: dict = {}          # live config, can be hot-reloaded

ws_clients: set = set()

running = True             # set False to stop all loops
rtde_r = None

stats = {
    "rtde_connected": False,
    "rtde_reconnects": 0,
    "packets_sent": 0,
    "packets_dropped": 0,   # stale / invalid
    "ws_clients_peak": 0,
    "start_time": time.time(),
    "last_packet_time": None,
    "sim_mode": not RTDE_AVAILABLE,
}


# ─────────────────────────────────────────────────────────────────
#  Fault injection + automatic detection
# ─────────────────────────────────────────────────────────────────
fault_lock   = threading.Lock()
active_faults = []            # list of {"type","idx","error","severity","value"}
fault_manual  = False         # True → demo faults pinned, auto-detect paused

FAULT_ERRORS = ["overtemperature", "overcurrent", "following_error",
                "overspeed", "lost_workpiece"]
FAULT_LABELS_DE = {
    "overtemperature": "Übertemperatur",
    "overcurrent":     "Überstrom",
    "following_error": "Schleppfehler",
    "overspeed":       "Überdrehzahl",
    "lost_workpiece":  "Werkstück verloren",
    "protective_stop": "Schutzstopp",
}
PART_NAMES_DE = {
    "joint": ["Gelenk 1 (Basis)", "Gelenk 2 (Schulter)", "Gelenk 3 (Ellbogen)",
              "Gelenk 4 (Handgelenk 1)", "Gelenk 5 (Handgelenk 2)", "Gelenk 6 (Flansch)"],
    "link":  ["Arm 1", "Arm 2", "Arm 3", "Arm 4", "Arm 5", "Arm 6"],
}

# Threshold rules. Each watches one per-joint array in the packet.
#   warn  -> amber,  fault -> red.   abs() is taken so signs don't matter.
# These are DEMO starting values — calibrate against logged real ranges.
FAULT_RULES = [
    {"error": "overcurrent",     "field": "joint_current", "warn": 2.0,  "fault": 3.5,  "unit": "A"},
    {"error": "overtemperature", "field": "joint_temp",    "warn": 55.0, "fault": 70.0, "unit": "°C"},
    {"error": "following_error", "field": "follow_err",    "warn": 0.05, "fault": 0.10, "unit": "rad"},
    {"error": "overspeed",       "field": "qd",            "warn": 2.0,  "fault": 3.0,  "unit": "rad/s"},
]
_SEV_RANK = {"warn": 1, "fault": 2}


def detect_faults(packet: dict) -> list:
    """Evaluate one packet against all rules; return ALL violations (empty = ok)."""
    if not packet:
        return []
    found = []

    # whole-robot: protective / safety stop overrides everything
    if packet.get("safety_mode", 1) and packet.get("safety_mode", 1) >= 3:
        found.append({"type": "robot", "idx": -1, "error": "protective_stop",
                      "severity": "fault", "value": packet.get("safety_mode")})

    for rule in FAULT_RULES:
        arr = packet.get(rule["field"])
        if not arr:
            continue
        for i, v in enumerate(arr[:6]):
            av = abs(v)
            if av >= rule["fault"]:
                sev = "fault"
            elif av >= rule["warn"]:
                sev = "warn"
            else:
                continue
            found.append({"type": "joint", "idx": i, "error": rule["error"],
                          "severity": sev, "value": round(av, 3)})

    # sort: faults before warns, then by rule order
    found.sort(key=lambda f: (0 if f["severity"] == "fault" else 1, f.get("idx", -1)))
    return found


# ─────────────────────────────────────────────────────────────────
#  Config helpers
# ─────────────────────────────────────────────────────────────────
def load_config(path: Path) -> dict:
    if path.exists():
        with open(path) as f:
            user = yaml.safe_load(f) or {}
        return deep_merge(DEFAULT_CONFIG, user)
    else:
        save_config(DEFAULT_CONFIG, path)
        print(f"[CFG]  Created default config: {path}")
    return DEFAULT_CONFIG.copy()


def save_config(cfg: dict, path: Path = CONFIG_FILE):
    with open(path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)


def deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def get_cfg(*keys, default=None):
    """Thread-safe nested config getter: get_cfg('robot', 'ip')"""
    with config_lock:
        node = config
        for k in keys:
            if not isinstance(node, dict) or k not in node:
                return default
            node = node[k]
        return node


def set_cfg(value, *keys):
    """Thread-safe nested config setter + auto-save."""
    with config_lock:
        node = config
        for k in keys[:-1]:
            node = node.setdefault(k, {})
        node[keys[-1]] = value
    save_config(config)


# ─────────────────────────────────────────────────────────────────
#  Data validation
# ─────────────────────────────────────────────────────────────────
def is_valid_packet(packet: dict) -> bool:
    if not packet:
        return False
    q = packet.get("q", [])
    if len(q) != 6:
        return False
    if any(math.isnan(v) or math.isinf(v) for v in q):
        return False
    return True


# ─────────────────────────────────────────────────────────────────
#  SIMULATION mode (fake robot data for testing without hardware)
# ─────────────────────────────────────────────────────────────────
def simulate_rtde(frequency: float):
    """Generate fake sinusoidal joint data when no robot is connected."""
    global latest_packet, running
    seq = 0
    t0 = time.time()
    interval = 1.0 / frequency

    print("[SIM]  Simulation mode active — generating fake joint data\n")

    while running:
        t = time.time() - t0
        q = [math.sin(t * 0.5 + i * math.pi / 3) * 0.5 for i in range(6)]
        qd = [math.cos(t * 0.5 + i * math.pi / 3) * 0.25 for i in range(6)]
        tcp = [math.sin(t * 0.3) * 0.2, math.cos(t * 0.3) * 0.2, 0.5,
               0.0, 0.0, math.sin(t * 0.1)]

        seq += 1
        packet = {
            "seq": seq,
            "timestamp": round(t, 4),
            "q": [round(v, 6) for v in q],
            "qd": [round(v, 6) for v in qd],
            "tcp_pose": [round(v, 6) for v in tcp],
            "tcp_speed": [0.0] * 6,
            "tcp_force": [0.0] * 6,
            "joint_current": [round(0.5 + 0.3 * math.sin(t * 0.3 + i), 4) for i in range(6)],
            "joint_temp":    [round(30.0 + i * 2 + 3 * math.sin(t * 0.1 + i), 2) for i in range(6)],
            "follow_err":    [round(abs(math.sin(t * 0.2 + i) * 0.008), 5) for i in range(6)],
            "robot_mode": 7,
            "safety_mode": 1,
            "sim": True,
        }

        with packet_lock:
            latest_packet = packet

        stats["last_packet_time"] = time.time()
        time.sleep(interval)


# ─────────────────────────────────────────────────────────────────
#  RTDE reader thread (with auto-reconnect)
# ─────────────────────────────────────────────────────────────────
def read_rtde_blocking():
    global latest_packet, rtde_r, running

    robot_ip = get_cfg("robot", "ip")
    frequency = get_cfg("robot", "frequency_hz")

    print(f"\n{'='*62}")
    print(f"  UR5 RTDE → WebSocket Bridge")
    print(f"{'='*62}")
    print(f"  Robot IP   : {robot_ip}")
    print(f"  RTDE freq  : {frequency:.1f} Hz")
    print(f"  WS port    : {get_cfg('websocket', 'port')}")
    print(f"  Dashboard  : http://localhost:{get_cfg('dashboard', 'port')}")
    print(f"{'='*62}\n")

    while running:
        try:
            robot_ip = get_cfg("robot", "ip")        # re-read in case hot-updated
            frequency = get_cfg("robot", "frequency_hz")
            interval = 1.0 / frequency

            print(f"[RTDE] Connecting to {robot_ip} ...")
            rtde_r = RTDEReceive(robot_ip, frequency=frequency)
            stats["rtde_connected"] = True
            print(f"[RTDE] Connected ✓\n")

            seq = 0
            log_interval = max(1, int(frequency / max(0.1, get_cfg("logging", "console_hz"))))

            while running:
                try:
                    ts = rtde_r.getTimestamp()
                    q = list(rtde_r.getActualQ())
                    tcp = list(rtde_r.getActualTCPPose())
                    speed = list(rtde_r.getActualTCPSpeed())
                    force = list(rtde_r.getActualTCPForce())
                    qd = list(rtde_r.getActualQd())
                    current = list(rtde_r.getActualCurrent())
                    mode = rtde_r.getRobotMode()
                    safety = rtde_r.getSafetyMode()
                    # extra signals for fault detection (standard ur_rtde methods)
                    try:
                        temp = list(rtde_r.getJointTemperatures())
                    except Exception:
                        temp = [0.0] * 6
                    try:
                        target_q = list(rtde_r.getTargetQ())
                    except Exception:
                        target_q = list(q)
                    follow_err = [abs(target_q[i] - q[i]) for i in range(min(len(q), len(target_q)))]

                    seq += 1
                    packet = {
                        "seq": seq,
                        "timestamp": round(ts, 4),
                        "q": [round(v, 6) for v in q],
                        "qd": [round(v, 6) for v in qd],
                        "tcp_pose": [round(v, 6) for v in tcp],
                        "tcp_speed": [round(v, 6) for v in speed],
                        "tcp_force": [round(v, 4) for v in force],
                        "joint_current": [round(v, 4) for v in current],
                        "joint_temp": [round(v, 2) for v in temp],
                        "follow_err": [round(v, 5) for v in follow_err],
                        "robot_mode": mode,
                        "safety_mode": safety,
                    }

                    if is_valid_packet(packet):
                        with packet_lock:
                            latest_packet = packet
                        stats["last_packet_time"] = time.time()
                    else:
                        stats["packets_dropped"] += 1
                        print(f"[RTDE] Invalid packet #{seq} — skipped")

                    # Console log at reduced rate
                    if seq % log_interval == 0:
                        q_deg = [f"{math.degrees(v):7.1f}" for v in q]
                        tcp_mm = [f"{v*1000:8.1f}" for v in tcp[:3]]
                        spd = [f"{v*1000:7.1f}" for v in speed[:3]]
                        print(
                            f"[{ts:9.3f}] "
                            f"q(°)=[{', '.join(q_deg)}]  "
                            f"TCP(mm)=[{', '.join(tcp_mm)}]  "
                            f"v(mm/s)=[{', '.join(spd)}]  "
                            f"clients={len(ws_clients)}"
                        )

                except Exception as e:
                    print(f"[RTDE] Read error: {e}")

                time.sleep(interval)

        except Exception as e:
            stats["rtde_connected"] = False
            delay = get_cfg("robot", "reconnect_delay_s")
            stats["rtde_reconnects"] += 1
            print(f"[RTDE] Connection failed: {e}")
            print(f"[RTDE] Retrying in {delay}s... (attempt #{stats['rtde_reconnects']})\n")
            if rtde_r:
                try:
                    rtde_r.disconnect()
                except Exception:
                    pass
                rtde_r = None
            for _ in range(int(delay * 10)):
                if not running:
                    break
                time.sleep(0.1)

    # Cleanup
    stats["rtde_connected"] = False
    if rtde_r:
        print("\n[RTDE] Disconnecting...")
        try:
            rtde_r.disconnect()
        except Exception:
            pass
        print("[RTDE] Disconnected ✓")


# ─────────────────────────────────────────────────────────────────
#  WebSocket server
# ─────────────────────────────────────────────────────────────────
async def ws_handler(websocket):
    addr = websocket.remote_address
    ws_clients.add(websocket)
    peak = stats["ws_clients_peak"]
    stats["ws_clients_peak"] = max(peak, len(ws_clients))
    print(f"[WS]   Client connected: {addr}  (total: {len(ws_clients)})")

    try:
        async for msg in websocket:
            pass   # ignore incoming messages
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        ws_clients.discard(websocket)
        print(f"[WS]   Client disconnected: {addr}  (total: {len(ws_clients)})")


async def broadcast_loop():
    global active_faults
    last_seq = -1

    while running:
        hz = get_cfg("websocket", "broadcast_hz")
        interval = 1.0 / max(0.1, hz)

        with packet_lock:
            pkt = latest_packet

        if pkt and pkt["seq"] != last_seq:
            # Stale data warning
            age = time.time() - (stats["last_packet_time"] or 0)
            warn_age = get_cfg("robot", "stale_data_warn_s")
            if age > warn_age:
                print(f"[WS]   WARNING: Data is {age:.1f}s stale!")

            last_seq = pkt["seq"]
            # automatic detection (all violations) unless demo faults are pinned
            with fault_lock:
                if not fault_manual:
                    active_faults = detect_faults(pkt)
                f_list = list(active_faults)
            msg_data = dict(pkt)
            msg_data["faults"]   = f_list   # always present; empty list = no fault
            msg_data["identity"] = get_cfg("identity") or {}  # robot metadata from config
            msg = json.dumps(msg_data)

            if ws_clients:
                results = await asyncio.gather(
                    *[c.send(msg) for c in ws_clients.copy()],
                    return_exceptions=True,
                )
                for r in results:
                    if isinstance(r, Exception):
                        print(f"[WS]   Send error: {r}")
                    else:
                        stats["packets_sent"] += 1

        await asyncio.sleep(interval)


# ─────────────────────────────────────────────────────────────────
#  HTTP Dashboard (aiohttp)
# ─────────────────────────────────────────────────────────────────
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>UR5 Bridge — Control Panel</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;700&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0a0c0f;
    --surface: #111418;
    --surface2: #181c22;
    --border: #23292f;
    --accent: #00e5ff;
    --accent2: #ff6b35;
    --green: #39ff14;
    --red: #ff2d55;
    --yellow: #ffd60a;
    --text: #d4dce8;
    --muted: #5a6478;
    --font-mono: 'JetBrains Mono', monospace;
    --font-display: 'Syne', sans-serif;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; background: var(--bg); color: var(--text); font-family: var(--font-mono); font-size: 13px; }

  /* Scanline texture overlay */
  body::before {
    content: '';
    position: fixed; inset: 0; z-index: 0; pointer-events: none;
    background: repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0,229,255,0.015) 2px, rgba(0,229,255,0.015) 4px);
  }

  .app { position: relative; z-index: 1; display: grid; grid-template-rows: auto 1fr; min-height: 100vh; }

  /* Header */
  header {
    padding: 16px 28px;
    border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 20px;
    background: var(--surface);
  }
  header .logo {
    font-family: var(--font-display);
    font-size: 20px; font-weight: 800; letter-spacing: -0.5px;
    color: var(--accent);
    text-shadow: 0 0 20px rgba(0,229,255,0.4);
  }
  header .logo span { color: var(--text); font-weight: 400; }
  header .status-bar { display: flex; gap: 16px; margin-left: auto; align-items: center; }
  .badge {
    display: flex; align-items: center; gap: 6px;
    padding: 4px 10px; border-radius: 4px;
    background: var(--surface2); border: 1px solid var(--border);
    font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;
  }
  .badge .dot {
    width: 7px; height: 7px; border-radius: 50%;
    animation: pulse 2s infinite;
  }
  .dot.green { background: var(--green); box-shadow: 0 0 6px var(--green); }
  .dot.red   { background: var(--red);   box-shadow: 0 0 6px var(--red); }
  .dot.yellow{ background: var(--yellow);box-shadow: 0 0 6px var(--yellow); }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

  /* Main layout */
  main { display: grid; grid-template-columns: 320px 1fr; gap: 0; }

  /* Sidebar */
  .sidebar {
    background: var(--surface);
    border-right: 1px solid var(--border);
    overflow-y: auto;
    padding: 20px;
    display: flex; flex-direction: column; gap: 20px;
  }

  .section-title {
    font-family: var(--font-display);
    font-size: 13px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 1.5px;
    color: var(--text); margin-bottom: 12px;
    border-bottom: 1px solid var(--border);
    padding-bottom: 6px;
  }

  .card {
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 16px;
  }

  /* Form fields */
  .field { margin-bottom: 14px; }
  .field:last-child { margin-bottom: 0; }
  label {
    display: block; font-size: 12px; color: var(--text);
    text-transform: uppercase; letter-spacing: 0.5px;
    margin-bottom: 5px;
  }
  input[type=text], input[type=number] {
    width: 100%; padding: 8px 10px;
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 4px; color: var(--text);
    font-family: var(--font-mono); font-size: 13px;
    transition: border-color 0.2s;
    outline: none;
  }
  input:focus { border-color: var(--accent); box-shadow: 0 0 0 2px rgba(0,229,255,0.1); }
  input.changed { border-color: var(--yellow); }
  input.saved   { border-color: var(--green); }

  .range-wrap { display: flex; align-items: center; gap: 10px; }
  input[type=range] {
    flex: 1; -webkit-appearance: none; height: 3px;
    background: var(--border); border-radius: 2px; outline: none;
  }
  input[type=range]::-webkit-slider-thumb {
    -webkit-appearance: none; width: 14px; height: 14px;
    border-radius: 50%; background: var(--accent);
    cursor: pointer; box-shadow: 0 0 6px rgba(0,229,255,0.5);
  }
  .range-val {
    min-width: 42px; text-align: right;
    color: var(--accent); font-weight: 600;
  }

  .btn {
    display: block; width: 100%; padding: 9px 14px;
    border: 1px solid var(--accent); border-radius: 4px;
    background: transparent; color: var(--accent);
    font-family: var(--font-mono); font-size: 12px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 1px;
    cursor: pointer; transition: all 0.15s;
  }
  .btn:hover { background: rgba(0,229,255,0.08); }
  .btn:active { background: rgba(0,229,255,0.18); }
  .btn.danger { border-color: var(--red); color: var(--red); }
  .btn.danger:hover { background: rgba(255,45,85,0.08); }
  .btn.success { border-color: var(--green); color: var(--green); }
  .btn + .btn { margin-top: 8px; }

  /* YAML panel */
  textarea.yaml-box {
    width: 100%; min-height: 200px;
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 4px; color: var(--green);
    font-family: var(--font-mono); font-size: 11px;
    padding: 10px; resize: vertical; outline: none;
    line-height: 1.6;
  }
  textarea.yaml-box:focus { border-color: var(--accent); }

  /* Right panel */
  .panel {
    padding: 20px;
    display: flex; flex-direction: column; gap: 16px;
    overflow-y: auto;
  }

  /* Stats grid */
  .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 10px;
  }
  .stat-card {
    background: var(--surface2); border: 1px solid var(--border);
    border-radius: 6px; padding: 14px;
  }
  .stat-label { font-size: 12px; color: var(--text); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
  .stat-value { font-size: 22px; font-weight: 700; font-family: var(--font-display); color: var(--accent); }
  .stat-value.red { color: var(--red); }
  .stat-value.green { color: var(--green); }
  .stat-value.yellow { color: var(--yellow); }
  .stat-sub { font-size: 10px; color: var(--muted); margin-top: 2px; }

  /* Joint bars */
  .joint-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
  .joint-card {
    background: var(--surface2); border: 1px solid var(--border);
    border-radius: 6px; padding: 12px;
  }
  .joint-name { font-size: 12px; color: var(--text); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
  .joint-val { font-size: 18px; font-weight: 700; color: var(--text); margin-bottom: 6px; }
  .joint-val span { font-size: 11px; color: var(--muted); font-weight: 400; }
  .bar-track { height: 3px; background: var(--border); border-radius: 2px; overflow: hidden; }
  .bar-fill { height: 100%; background: var(--accent); border-radius: 2px; transition: width 0.12s ease; min-width: 2px; }
  .bar-fill.warn { background: var(--yellow); }
  .bar-fill.danger { background: var(--red); }

  /* TCP table */
  .tcp-grid { display: grid; grid-template-columns: repeat(6, 1fr); gap: 8px; }
  .tcp-cell {
    background: var(--surface2); border: 1px solid var(--border);
    border-radius: 6px; padding: 12px 8px; text-align: center;
  }
  .tcp-label { font-size: 12px; color: var(--text); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
  .tcp-value { font-size: 15px; font-weight: 600; color: var(--accent2); }

  /* Live packet */
  .packet-box {
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 6px; padding: 14px;
    font-size: 11px; line-height: 1.7; color: var(--green);
    max-height: 280px; overflow-y: auto;
    white-space: pre;
  }

  /* Notification toast */
  #toast {
    position: fixed; bottom: 20px; right: 20px;
    padding: 10px 18px; border-radius: 6px;
    font-size: 12px; font-weight: 600;
    background: var(--surface2); border: 1px solid var(--border);
    color: var(--text);
    opacity: 0; transition: opacity 0.3s;
    z-index: 100;
    pointer-events: none;
  }
  #toast.show { opacity: 1; }
  #toast.ok   { border-color: var(--green); color: var(--green); }
  #toast.err  { border-color: var(--red);   color: var(--red); }

  /* Toggle */
  .toggle-row { display: flex; align-items: center; justify-content: space-between; margin-bottom: 14px; }
  .toggle { position: relative; width: 36px; height: 20px; }
  .toggle input { opacity: 0; width: 0; height: 0; }
  .toggle-slider {
    position: absolute; inset: 0; background: var(--border);
    border-radius: 20px; cursor: pointer; transition: background 0.2s;
  }
  .toggle-slider::before {
    content: ''; position: absolute;
    left: 3px; top: 3px; width: 14px; height: 14px;
    border-radius: 50%; background: var(--muted);
    transition: transform 0.2s, background 0.2s;
  }
  .toggle input:checked + .toggle-slider { background: rgba(0,229,255,0.2); border: 1px solid var(--accent); }
  .toggle input:checked + .toggle-slider::before { transform: translateX(16px); background: var(--accent); }
  .toggle-label { font-size: 11px; color: var(--text); }

  .uptime { font-size: 11px; color: var(--muted); text-align: center; margin-top: 4px; }

  /* Fault panel */
  .fault-card { transition: border-color 0.3s, box-shadow 0.3s; }
  .fault-card.active {
    border-color: var(--red) !important;
    box-shadow: 0 0 12px rgba(255,45,85,0.25);
  }
  .fault-part  { font-size: 11px; color: var(--muted); margin-bottom: 3px; transition: color 0.3s; }
  .fault-error { font-size: 16px; font-weight: 700; color: var(--text); min-height: 22px; transition: color 0.3s; }
  .fault-card.active .fault-part  { color: var(--red); }
  .fault-card.active .fault-error { color: var(--red); }
  .fault-display {
    padding: 10px; border-radius: 4px; background: var(--bg);
    text-align: center; margin-bottom: 12px;
  }
</style>
</head>
<body>
<div class="app">
  <header>
    <div class="logo">UR5<span> Bridge</span></div>
    <div class="badge" id="rtde-badge">
      <span class="dot" id="rtde-dot"></span>
      <span id="rtde-label">RTDE</span>
    </div>
    <div class="badge" id="ws-badge">
      <span class="dot yellow"></span>
      <span id="ws-label">WS</span>
    </div>
    <div class="badge">
      <span class="dot green"></span>
      <span id="uptime-badge">--</span>
    </div>
  </header>

  <main>
    <!-- ── Sidebar ── -->
    <aside class="sidebar">

      <!-- Fehler-Simulation -->
      <div>
        <p class="section-title">Fehler-Simulation</p>
        <div class="card fault-card" id="fault-card">
          <div class="fault-display">
            <div class="fault-part"  id="fault-part-label">kein aktiver Fehler</div>
            <div class="fault-error" id="fault-error-label"></div>
          </div>
          <button class="btn danger" onclick="triggerFault()">⚡ Random Fehler</button>
          <button class="btn" onclick="clearFault()" style="margin-top:8px;">✕ Fehler löschen</button>
        </div>
      </div>

      <!-- Actions -->
      <div>
        <p class="section-title">Actions</p>
        <button class="btn success" onclick="applyConfig()">⟳ Apply Config</button>
        <button class="btn" onclick="loadYaml()">↓ Load from YAML</button>
        <button class="btn" onclick="downloadYaml()">↑ Download YAML</button>
      </div>

      <!-- Roboter-Identität -->
      <div>
        <p class="section-title" style="cursor:pointer" onclick="toggleSection('sec-identity',this)">▾ Roboter-Identität</p>
        <div id="sec-identity">
          <div class="card">
            <div class="field"><label>Bezeichnung</label>
              <input type="text" id="identity_name" placeholder="UR5 #1"></div>
            <div class="field"><label>Modell</label>
              <input type="text" id="identity_model" placeholder="UR5 (CB3)"></div>
            <div class="field"><label>Seriennummer</label>
              <input type="text" id="identity_serial" placeholder="SN-XXXX"></div>
            <div class="field"><label>Standort</label>
              <input type="text" id="identity_location" placeholder="Halle A / Station 3"></div>
            <div class="field"><label>Asset-ID</label>
              <input type="text" id="identity_asset_id" placeholder="ROBOT-001"></div>
            <button class="btn success" style="margin-top:4px" onclick="saveIdentity()">⟳ Speichern</button>
          </div>
        </div>
      </div>

      <!-- Robot config -->
      <div>
        <p class="section-title" style="cursor:pointer" onclick="toggleSection('sec-robot',this)">▾ Robot</p>
        <div id="sec-robot">
          <div class="card">
            <div class="field">
              <label>Robot IP</label>
              <input type="text" id="robot_ip" placeholder="192.168.1.237">
            </div>
            <div class="field">
              <label>RTDE Frequency (Hz)</label>
              <div class="range-wrap">
                <input type="range" id="robot_frequency_hz" min="1" max="125" step="1">
                <span class="range-val" id="robot_frequency_hz_val">--</span>
              </div>
            </div>
            <div class="field">
              <label>Reconnect Delay (s)</label>
              <input type="number" id="robot_reconnect_delay_s" min="1" max="60" step="1">
            </div>
            <div class="field">
              <label>Stale Data Warn (s)</label>
              <input type="number" id="robot_stale_data_warn_s" min="0.1" max="10" step="0.1">
            </div>
          </div>
        </div>
      </div>

      <!-- WebSocket config -->
      <div>
        <p class="section-title" style="cursor:pointer" onclick="toggleSection('sec-ws',this)">▾ WebSocket</p>
        <div id="sec-ws">
          <div class="card">
            <div class="field">
              <label>Bind Host</label>
              <input type="text" id="websocket_host" placeholder="0.0.0.0">
            </div>
            <div class="field">
              <label>Port</label>
              <input type="number" id="websocket_port" min="1024" max="65535">
            </div>
            <div class="field">
              <label>Broadcast Hz</label>
              <div class="range-wrap">
                <input type="range" id="websocket_broadcast_hz" min="1" max="125" step="1">
                <span class="range-val" id="websocket_broadcast_hz_val">--</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Logging config -->
      <div>
        <p class="section-title" style="cursor:pointer" onclick="toggleSection('sec-log',this)">▾ Logging</p>
        <div id="sec-log">
          <div class="card">
            <div class="field">
              <label>Console Hz</label>
              <div class="range-wrap">
                <input type="range" id="logging_console_hz" min="0.1" max="30" step="0.1">
                <span class="range-val" id="logging_console_hz_val">--</span>
              </div>
            </div>
            <div class="toggle-row">
              <span class="toggle-label">Log to File</span>
              <label class="toggle">
                <input type="checkbox" id="logging_log_to_file">
                <span class="toggle-slider"></span>
              </label>
            </div>
            <div class="field">
              <label>Log File</label>
              <input type="text" id="logging_log_file" placeholder="ur5_bridge.log">
            </div>
          </div>
        </div>
      </div>

      <!-- YAML editor -->
      <div>
        <p class="section-title" style="cursor:pointer" onclick="toggleSection('sec-yaml',this)">▸ YAML Editor</p>
        <div id="sec-yaml" style="display:none">
          <textarea class="yaml-box" id="yaml-editor" rows="14" spellcheck="false"></textarea>
          <button class="btn" style="margin-top:8px" onclick="applyYaml()">Apply YAML</button>
        </div>
      </div>

      <p class="uptime" id="uptime-text">--</p>
    </aside>

    <!-- ── Right panel ── -->
    <section class="panel">

      <!-- Stats row -->
      <div>
        <p class="section-title">Statistics</p>
        <div class="stats-grid">
          <div class="stat-card">
            <div class="stat-label">Packets Sent</div>
            <div class="stat-value green" id="stat-sent">--</div>
          </div>
          <div class="stat-card">
            <div class="stat-label">Dropped</div>
            <div class="stat-value" id="stat-dropped">--</div>
          </div>
          <div class="stat-card">
            <div class="stat-label">WS Clients</div>
            <div class="stat-value" id="stat-clients">--</div>
            <div class="stat-sub" id="stat-peak">peak --</div>
          </div>
          <div class="stat-card">
            <div class="stat-label">Reconnects</div>
            <div class="stat-value yellow" id="stat-reconnects">--</div>
          </div>
          <div class="stat-card">
            <div class="stat-label">Data Age</div>
            <div class="stat-value" id="stat-age">--</div>
            <div class="stat-sub">seconds</div>
          </div>
          <div class="stat-card">
            <div class="stat-label">Mode</div>
            <div class="stat-value" id="stat-mode">--</div>
          </div>
          <div class="stat-card" id="fault-stat-card" style="transition: border-color 0.3s; grid-column: span 2;">
            <div class="stat-label">Aktive Fehler</div>
            <div id="fault-list-mini" style="margin-top:4px; font-size:11px; color:var(--muted);">—</div>
          </div>
        </div>
      </div>

      <!-- Aktive Fehler -->
      <div>
        <p class="section-title">Fehler-Liste</p>
        <div class="card" id="fault-list-card" style="min-height:60px;">
          <div id="fault-list"><div style="color:var(--muted);font-size:11px;text-align:center;padding:4px">Kein aktiver Fehler</div></div>
        </div>
      </div>

      <!-- Gelenk-Monitoring -->
      <div>
        <p class="section-title">Gelenk-Monitoring</p>
        <div style="overflow-x:auto">
          <table style="width:100%;border-collapse:collapse;font-size:13px;" id="jmon-table">
            <thead>
              <tr style="color:var(--text);text-align:left">
                <th style="padding:6px 8px">Gelenk</th>
                <th style="padding:6px 8px">Strom (A)</th>
                <th style="padding:6px 8px">Temp (°C)</th>
                <th style="padding:6px 8px">Schleppf. (mrad)</th>
                <th style="padding:6px 8px">Geschw. (rad/s)</th>
              </tr>
            </thead>
            <tbody id="jmon-body">
            </tbody>
          </table>
        </div>
      </div>

      <!-- Joint positions -->
      <div>
        <p class="section-title">Joint Positions</p>
        <div class="joint-grid" id="joint-grid">
          <!-- filled by JS -->
        </div>
      </div>

      <!-- TCP Pose -->
      <div>
        <p class="section-title">TCP Pose</p>
        <div class="tcp-grid" id="tcp-grid">
          <!-- filled by JS -->
        </div>
      </div>

      <!-- Live packet -->
      <div>
        <p class="section-title">Live Packet</p>
        <div class="packet-box" id="packet-box">Waiting for data...</div>
      </div>

    </section>
  </main>
</div>

<div id="toast"></div>

<script>
// ── Setup ──
const JOINTS = ['J1','J2','J3','J4','J5','J6'];
const TCP_LABELS = ['X (mm)','Y (mm)','Z (mm)','Rx','Ry','Rz'];
let ws, pollTimer;

// Build joint cards
const jGrid = document.getElementById('joint-grid');
JOINTS.forEach((n,i) => {
  jGrid.innerHTML += `
    <div class="joint-card">
      <div class="joint-name">${n}</div>
      <div class="joint-val" id="jv${i}">-- <span>°</span></div>
      <div class="bar-track"><div class="bar-fill" id="jb${i}" style="width:50%"></div></div>
    </div>`;
});

// Build TCP cells
const tGrid = document.getElementById('tcp-grid');
TCP_LABELS.forEach((l,i) => {
  tGrid.innerHTML += `
    <div class="tcp-cell">
      <div class="tcp-label">${l}</div>
      <div class="tcp-value" id="tv${i}">--</div>
    </div>`;
});

// ── Connect WebSocket for live data ──
function connectWS() {
  const port = document.getElementById('websocket_port').value || 9090;
  ws = new WebSocket(`ws://${location.hostname}:${port}`);
  document.getElementById('ws-label').textContent = 'WS connecting…';

  ws.onopen = () => {
    document.getElementById('ws-label').textContent = `WS :${port}`;
    document.getElementById('ws-badge').querySelector('.dot').className = 'dot green';
  };
  ws.onclose = () => {
    document.getElementById('ws-label').textContent = 'WS offline';
    document.getElementById('ws-badge').querySelector('.dot').className = 'dot red';
    setTimeout(connectWS, 3000);
  };
  ws.onmessage = (e) => {
    const d = JSON.parse(e.data);
    updateLiveData(d);
    updateJointMonitor(d);
    if (d.faults) updateFaultDisplay(d.faults);
  };
}

function updateLiveData(d) {
  // Joints
  (d.q || []).forEach((v,i) => {
    const deg = (v * 180 / Math.PI);
    document.getElementById(`jv${i}`).innerHTML = `${deg.toFixed(1)} <span>°</span>`;
    const pct = Math.min(100, Math.max(0, (deg + 180) / 360 * 100));
    const bar = document.getElementById(`jb${i}`);
    bar.style.width = pct + '%';
    bar.className = 'bar-fill' + (Math.abs(deg) > 150 ? ' danger' : Math.abs(deg) > 120 ? ' warn' : '');
  });

  // TCP
  (d.tcp_pose || []).forEach((v,i) => {
    const disp = i < 3 ? (v*1000).toFixed(1) : v.toFixed(4);
    document.getElementById(`tv${i}`).textContent = disp;
  });

  // Packet box
  document.getElementById('packet-box').textContent = JSON.stringify(d, null, 2);
}

// ── Poll /api/status ──
async function pollStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    const st = d.stats;

    // RTDE badge
    const dot = document.getElementById('rtde-dot');
    const lbl = document.getElementById('rtde-label');
    if (st.sim_mode) {
      dot.className = 'dot yellow'; lbl.textContent = 'SIM';
    } else if (st.rtde_connected) {
      dot.className = 'dot green'; lbl.textContent = 'RTDE ✓';
    } else {
      dot.className = 'dot red'; lbl.textContent = 'RTDE ✗';
    }

    // Stats
    document.getElementById('stat-sent').textContent = fmt(st.packets_sent);
    const dropped = document.getElementById('stat-dropped');
    dropped.textContent = st.packets_dropped;
    dropped.className = 'stat-value' + (st.packets_dropped > 0 ? ' red' : '');
    document.getElementById('stat-clients').textContent = st.ws_clients;
    document.getElementById('stat-peak').textContent = `peak ${st.ws_clients_peak}`;
    document.getElementById('stat-reconnects').textContent = st.rtde_reconnects;

    const age = st.data_age_s;
    const ageEl = document.getElementById('stat-age');
    ageEl.textContent = age !== null ? age.toFixed(2) : '--';
    ageEl.className = 'stat-value' + (age > 2 ? ' red' : age > 0.5 ? ' yellow' : ' green');

    const modeEl = document.getElementById('stat-mode');
    modeEl.textContent = st.sim_mode ? 'SIM' : (st.rtde_connected ? 'LIVE' : 'OFF');
    modeEl.className = 'stat-value' + (st.sim_mode ? ' yellow' : st.rtde_connected ? ' green' : ' red');

    // Uptime
    const up = Math.floor(st.uptime_s);
    const h = Math.floor(up/3600), m = Math.floor((up%3600)/60), s = up%60;
    const upStr = `${h}h ${m}m ${s}s`;
    document.getElementById('uptime-badge').textContent = upStr;
    document.getElementById('uptime-text').textContent = `Uptime: ${upStr}`;

  } catch(e) {
    console.warn('Status poll failed', e);
  }
}

function fmt(n) {
  return n >= 1e6 ? (n/1e6).toFixed(1)+'M' : n >= 1e3 ? (n/1e3).toFixed(1)+'K' : String(n);
}

// ── Config ──
async function loadConfig() {
  const r = await fetch('/api/config');
  const cfg = await r.json();
  applyConfigToUI(cfg);
}

function applyConfigToUI(cfg) {
  // Map flat keys to nested config
  const fields = {
    'robot_ip':               ['robot','ip'],
    'robot_frequency_hz':     ['robot','frequency_hz'],
    'robot_reconnect_delay_s':['robot','reconnect_delay_s'],
    'robot_stale_data_warn_s':['robot','stale_data_warn_s'],
    'websocket_host':         ['websocket','host'],
    'websocket_port':         ['websocket','port'],
    'websocket_broadcast_hz': ['websocket','broadcast_hz'],
    'logging_console_hz':     ['logging','console_hz'],
    'logging_log_to_file':    ['logging','log_to_file'],
    'logging_log_file':       ['logging','log_file'],
  };

  for (const [id, path] of Object.entries(fields)) {
    let val = cfg;
    for (const k of path) val = (val||{})[k];
    const el = document.getElementById(id);
    if (!el) continue;
    if (el.type === 'checkbox') el.checked = !!val;
    else el.value = val ?? '';
    updateRangeLabel(id);
  }

  // YAML editor
  document.getElementById('yaml-editor').value = toYaml(cfg);
}

function updateRangeLabel(id) {
  const el = document.getElementById(id);
  const lbl = document.getElementById(id + '_val');
  if (el && lbl) lbl.textContent = el.value;
}

// Attach range listeners
['robot_frequency_hz','websocket_broadcast_hz','logging_console_hz'].forEach(id => {
  const el = document.getElementById(id);
  if (el) el.addEventListener('input', () => updateRangeLabel(id));
});

async function applyConfig() {
  const body = {
    robot: {
      ip: document.getElementById('robot_ip').value,
      frequency_hz: parseFloat(document.getElementById('robot_frequency_hz').value),
      reconnect_delay_s: parseFloat(document.getElementById('robot_reconnect_delay_s').value),
      stale_data_warn_s: parseFloat(document.getElementById('robot_stale_data_warn_s').value),
    },
    websocket: {
      host: document.getElementById('websocket_host').value,
      port: parseInt(document.getElementById('websocket_port').value),
      broadcast_hz: parseFloat(document.getElementById('websocket_broadcast_hz').value),
    },
    logging: {
      console_hz: parseFloat(document.getElementById('logging_console_hz').value),
      log_to_file: document.getElementById('logging_log_to_file').checked,
      log_file: document.getElementById('logging_log_file').value,
    }
  };

  try {
    const r = await fetch('/api/config', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(body)
    });
    if (r.ok) {
      toast('Config applied & saved ✓', 'ok');
      document.getElementById('yaml-editor').value = toYaml(body);
    } else toast('Apply failed', 'err');
  } catch(e) { toast('Network error', 'err'); }
}

async function applyYaml() {
  const yamlStr = document.getElementById('yaml-editor').value;
  try {
    const r = await fetch('/api/config/yaml', {
      method: 'POST', headers: {'Content-Type':'text/plain'},
      body: yamlStr
    });
    if (r.ok) {
      toast('YAML applied ✓', 'ok');
      await loadConfig();
    } else toast('Invalid YAML', 'err');
  } catch(e) { toast('Network error', 'err'); }
}

async function loadYaml() {
  await loadConfig();
  toast('Config loaded from server ✓', 'ok');
}

function downloadYaml() {
  const txt = document.getElementById('yaml-editor').value;
  const a = document.createElement('a');
  a.href = 'data:text/plain;charset=utf-8,' + encodeURIComponent(txt);
  a.download = 'ur5_bridge_config.yaml';
  a.click();
  toast('YAML downloaded ✓', 'ok');
}

// ── Minimal YAML serializer ──
function toYaml(obj, indent=0) {
  let out = '';
  for (const [k,v] of Object.entries(obj)) {
    const pad = '  '.repeat(indent);
    if (typeof v === 'object' && v !== null) {
      out += `${pad}${k}:\n${toYaml(v, indent+1)}`;
    } else {
      out += `${pad}${k}: ${v}\n`;
    }
  }
  return out;
}

// ── Toast ──
function toast(msg, type='ok') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = `show ${type}`;
  setTimeout(() => { el.className = ''; }, 2500);
}

// ── Collapsible sections ─────────────────────────────────────────
function toggleSection(id, title) {
  const el = document.getElementById(id);
  if (!el) return;
  const open = el.style.display !== 'none';
  el.style.display = open ? 'none' : '';
  title.textContent = title.textContent.replace(/[▾▸]/, open ? '▸' : '▾');
}

// ── Robot identity ───────────────────────────────────────────────
async function loadIdentity() {
  const r = await fetch('/api/config');
  const cfg = await r.json();
  const id = cfg.identity || {};
  ['name','model','serial','location','asset_id'].forEach(k => {
    const el = document.getElementById('identity_'+k);
    if (el) el.value = id[k] || '';
  });
}
async function saveIdentity() {
  const id = {};
  ['name','model','serial','location','asset_id'].forEach(k => {
    const el = document.getElementById('identity_'+k);
    if (el) id[k] = el.value;
  });
  await fetch('/api/config', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({identity: id})});
  toast('Identität gespeichert ✓', 'ok');
}

// ── Joint monitoring table ────────────────────────────────────────
const JNAMES = ['Basis','Schulter','Ellbogen','HG 1','HG 2','Flansch'];
const jmonBody = document.getElementById('jmon-body');
JNAMES.forEach((n,i) => {
  jmonBody.innerHTML += `<tr id="jmon-row${i}" style="border-top:1px solid var(--border)">
    <td style="padding:7px 8px;color:var(--text);font-size:13px">${n}</td>
    <td style="padding:7px 8px"><div style="display:flex;align-items:center;gap:6px">
      <div style="flex:1;height:4px;background:var(--border);border-radius:2px;overflow:hidden">
        <div id="jmon-ci${i}" style="height:100%;width:0%;background:var(--accent);transition:width 0.1s"></div></div>
      <span id="jmon-cv${i}" style="min-width:42px;font-size:12px">--</span></div></td>
    <td style="padding:7px 8px"><div style="display:flex;align-items:center;gap:6px">
      <div style="flex:1;height:4px;background:var(--border);border-radius:2px;overflow:hidden">
        <div id="jmon-ti${i}" style="height:100%;width:0%;background:var(--accent);transition:width 0.1s"></div></div>
      <span id="jmon-tv${i}" style="min-width:42px;font-size:12px">--</span></div></td>
    <td style="padding:7px 8px"><div style="display:flex;align-items:center;gap:6px">
      <div style="flex:1;height:4px;background:var(--border);border-radius:2px;overflow:hidden">
        <div id="jmon-fi${i}" style="height:100%;width:0%;background:var(--accent);transition:width 0.1s"></div></div>
      <span id="jmon-fv${i}" style="min-width:52px;font-size:12px">--</span></div></td>
    <td style="padding:7px 8px"><div style="display:flex;align-items:center;gap:6px">
      <div style="flex:1;height:4px;background:var(--border);border-radius:2px;overflow:hidden">
        <div id="jmon-si${i}" style="height:100%;width:0%;background:var(--accent);transition:width 0.1s"></div></div>
      <span id="jmon-sv${i}" style="min-width:42px;font-size:12px">--</span></div></td>
  </tr>`;
});

function setBar(barId, valId, pct, val, warn, danger) {
  const bar = document.getElementById(barId);
  const valEl = document.getElementById(valId);
  if (!bar || !valEl) return;
  bar.style.width = Math.min(100, pct) + '%';
  bar.style.background = pct >= danger ? 'var(--red)' : pct >= warn ? 'var(--yellow)' : 'var(--accent)';
  valEl.textContent = val;
  valEl.style.color = pct >= danger ? 'var(--red)' : pct >= warn ? 'var(--yellow)' : 'var(--text)';
}

function updateJointMonitor(d) {
  for (let i = 0; i < 6; i++) {
    const ci = (d.joint_current||[])[i];
    if (ci !== undefined) setBar(`jmon-ci${i}`,`jmon-cv${i}`, Math.abs(ci)/5*100, ci.toFixed(2)+'A', 40, 70);
    const ti = (d.joint_temp||[])[i];
    if (ti !== undefined) setBar(`jmon-ti${i}`,`jmon-tv${i}`, ti/80*100, ti.toFixed(1)+'°C', 69, 88);
    const fi = (d.follow_err||[])[i];
    if (fi !== undefined) setBar(`jmon-fi${i}`,`jmon-fv${i}`, fi/0.15*100, (fi*1000).toFixed(1)+'mr', 33, 67);
    const si = (d.qd||[])[i];
    if (si !== undefined) setBar(`jmon-si${i}`,`jmon-sv${i}`, Math.abs(si)/3*100, Math.abs(si).toFixed(2)+'r/s', 67, 100);
  }
}

// ── Fault simulation ─────────────────────────────────────────────
const FAULT_PARTS_DE = {
  joint: ['Gelenk 1 (Basis)', 'Gelenk 2 (Schulter)', 'Gelenk 3 (Ellbogen)',
          'Gelenk 4 (Handgelenk 1)', 'Gelenk 5 (Handgelenk 2)', 'Gelenk 6 (Flansch)'],
  link:  ['Arm 1', 'Arm 2', 'Arm 3', 'Arm 4', 'Arm 5', 'Arm 6'],
  tcp:   ['Werkzeug / TCP'],
  robot: ['Gesamter Roboter'],
};
const FAULT_ERRORS_DE = {
  overtemperature: 'Übertemperatur', overcurrent: 'Überstrom',
  following_error: 'Schleppfehler',  overspeed: 'Überdrehzahl',
  lost_workpiece:  'Werkstück verloren', protective_stop: 'Schutzstopp',
};

function faultPartLabel(f) {
  if (!f) return null;
  const arr = FAULT_PARTS_DE[f.type] || [];
  return (f.type === 'tcp' || f.type === 'robot') ? arr[0] : (arr[f.idx] || f.type+' '+f.idx);
}
function faultErrLabel(f) {
  if (!f) return null;
  let s = FAULT_ERRORS_DE[f.error] || f.error || '';
  if (f.value !== null && f.value !== undefined) s += ' ('+f.value+')';
  if (f.severity === 'warn') s = '⚠ ' + s;
  return s;
}

function updateFaultDisplay(faults) {
  const card     = document.getElementById('fault-card');
  const partEl   = document.getElementById('fault-part-label');
  const errEl    = document.getElementById('fault-error-label');
  const listEl   = document.getElementById('fault-list');
  const miniEl   = document.getElementById('fault-list-mini');
  const sFCard   = document.getElementById('fault-stat-card');

  if (!faults || !faults.length) {
    if (partEl)  partEl.textContent = 'kein aktiver Fehler';
    if (errEl)   errEl.textContent  = '';
    if (card)    card.classList.remove('active');
    if (listEl)  listEl.innerHTML   = '<div style="color:var(--muted);font-size:11px;text-align:center;padding:4px">Kein aktiver Fehler</div>';
    if (miniEl)  miniEl.textContent = '—';
    if (sFCard)  sFCard.style.borderColor = '';
    return;
  }

  const primary = faults[0];
  const pName   = faultPartLabel(primary);
  const eName   = faultErrLabel(primary);
  if (partEl) { partEl.textContent = pName; }
  if (errEl)  { errEl.textContent  = faults.length > 1 ? eName + ' (+' + (faults.length-1) + ' weitere)' : eName; }
  if (card)   card.classList.add('active');
  if (sFCard) sFCard.style.borderColor = 'var(--red)';
  if (miniEl) miniEl.innerHTML = faults.map(f =>
    `<span style="color:var(${f.severity==='warn'?'--yellow':'--red'})">${faultPartLabel(f)}: ${FAULT_ERRORS_DE[f.error]||f.error}</span>`
  ).join('<br/>');
  if (listEl) listEl.innerHTML = faults.map(f => {
    const sev = f.severity === 'warn' ? 'var(--yellow)' : 'var(--red)';
    return `<div style="border-left:3px solid ${sev};padding:6px 10px;margin-bottom:5px;background:var(--bg);border-radius:3px">
      <div style="color:${sev};font-size:11px;font-weight:700">${FAULT_ERRORS_DE[f.error]||f.error}</div>
      <div style="color:var(--text);font-size:11px">${faultPartLabel(f)}</div>
      ${f.value!==null&&f.value!==undefined?`<div style="color:var(--muted);font-size:10px">Wert: ${f.value}</div>`:''}
    </div>`;
  }).join('');
}

async function triggerFault() {
  try {
    const r = await fetch('/api/fault/trigger', {method:'POST'});
    const d = await r.json();
    if (d.ok) { updateFaultDisplay(d.faults); toast('⚡ '+faultPartLabel(d.faults[d.faults.length-1])+': '+faultErrLabel(d.faults[d.faults.length-1]), 'err'); }
  } catch(e) { toast('Trigger fehlgeschlagen', 'err'); }
}

async function clearFault() {
  try {
    const r = await fetch('/api/fault/clear', {method:'POST'});
    if ((await r.json()).ok) { updateFaultDisplay([]); toast('Alle Fehler gelöscht ✓', 'ok'); }
  } catch(e) { toast('Löschen fehlgeschlagen', 'err'); }
}

async function syncFault() {
  try {
    const d = await (await fetch('/api/fault')).json();
    updateFaultDisplay(d.faults || []);
  } catch(e) {}
}
setInterval(syncFault, 3000);
syncFault();

// ── Init ──
loadConfig();
loadIdentity();
connectWS();
pollStatus();
setInterval(pollStatus, 1000);
</script>
</body>
</html>
"""


async def dashboard_health(request):
    with packet_lock:
        pkt = latest_packet
    age = (time.time() - stats["last_packet_time"]) if stats["last_packet_time"] else None
    return aio_web.json_response({
        "status": "ok",
        "rtde_connected": stats["rtde_connected"],
        "data_age_s": round(age, 3) if age is not None else None,
        "ws_clients": len(ws_clients),
    })


async def dashboard_status(request):
    age = (time.time() - stats["last_packet_time"]) if stats["last_packet_time"] else None
    return aio_web.json_response({
        "stats": {
            **stats,
            "uptime_s": round(time.time() - stats["start_time"], 1),
            "ws_clients": len(ws_clients),
            "data_age_s": round(age, 3) if age is not None else None,
        }
    })


async def dashboard_get_config(request):
    with config_lock:
        cfg = json.loads(json.dumps(config))  # deep copy
    return aio_web.json_response(cfg)


async def dashboard_post_config(request):
    try:
        new_cfg = await request.json()
        with config_lock:
            config.update(deep_merge(config, new_cfg))
        save_config(config)
        print(f"[CFG]  Config updated via dashboard")
        return aio_web.json_response({"ok": True})
    except Exception as e:
        return aio_web.json_response({"ok": False, "error": str(e)}, status=400)


async def dashboard_post_yaml(request):
    try:
        text = await request.text()
        new_cfg = yaml.safe_load(text)
        if not isinstance(new_cfg, dict):
            raise ValueError("Not a valid YAML mapping")
        with config_lock:
            config.update(deep_merge(config, new_cfg))
        save_config(config)
        print(f"[CFG]  Config updated via YAML editor")
        return aio_web.json_response({"ok": True})
    except Exception as e:
        return aio_web.json_response({"ok": False, "error": str(e)}, status=400)


# ── Fault injection API ──────────────────────────────────────────

async def api_fault_get(request):
    with fault_lock:
        f = list(active_faults)
    return aio_web.json_response({"faults": f, "manual": fault_manual})


async def api_fault_trigger(request):
    global active_faults, fault_manual
    import random as _rnd
    error = _rnd.choice(FAULT_ERRORS)
    if error == "lost_workpiece":
        ftype, idx = "tcp", 5
    else:
        ftype, idx = "joint", _rnd.randint(0, 5)
    f = {"type": ftype, "idx": idx, "error": error, "severity": "fault", "value": None}
    with fault_lock:
        # avoid exact duplicate (same type+idx+error)
        active_faults = [x for x in active_faults
                         if not (x["type"]==f["type"] and x["idx"]==f["idx"] and x["error"]==f["error"])]
        active_faults.append(f)
        fault_manual = True
    part  = PART_NAMES_DE.get(ftype, ["TCP"])[idx] if ftype in PART_NAMES_DE else "TCP"
    label = FAULT_LABELS_DE.get(error, error)
    print(f"[FAULT] ⚡ Demo: {part} — {label}  (gesamt: {len(active_faults)})")
    return aio_web.json_response({"ok": True, "faults": active_faults})


async def api_fault_clear(request):
    global active_faults, fault_manual
    with fault_lock:
        active_faults = []
        fault_manual  = False
    print("[FAULT] ✓ Alle gelöscht — Auto-Erkennung aktiv")
    return aio_web.json_response({"ok": True})


async def dashboard_index(request):
    return aio_web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def start_dashboard(host: str, port: int):
    if not AIOHTTP_AVAILABLE:
        return
    app = aio_web.Application()
    app.router.add_get("/", dashboard_index)
    app.router.add_get("/health", dashboard_health)
    app.router.add_get("/api/status", dashboard_status)
    app.router.add_get("/api/config", dashboard_get_config)
    app.router.add_post("/api/config", dashboard_post_config)
    app.router.add_post("/api/config/yaml", dashboard_post_yaml)
    app.router.add_get( "/api/fault",         api_fault_get)
    app.router.add_post("/api/fault/trigger", api_fault_trigger)
    app.router.add_post("/api/fault/clear",   api_fault_clear)

    runner = aio_web.AppRunner(app)
    await runner.setup()
    site = aio_web.TCPSite(runner, host, port)
    await site.start()
    print(f"[HTTP] Dashboard ready → http://localhost:{port}\n")


# ─────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────
async def main_async():
    global running

    loop = asyncio.get_event_loop()

    # Start RTDE reader (or simulator) in background thread
    if RTDE_AVAILABLE:
        rtde_future = loop.run_in_executor(None, read_rtde_blocking)
    else:
        rtde_future = loop.run_in_executor(
            None, simulate_rtde, get_cfg("robot", "frequency_hz")
        )

    # Start WebSocket server
    ws_host = get_cfg("websocket", "host")
    ws_port = get_cfg("websocket", "port")
    print(f"[WS]   Starting WebSocket server on ws://{ws_host}:{ws_port}")
    async with ws_serve(ws_handler, ws_host, ws_port):
        print(f"[WS]   Server ready ✓\n")

        # Start dashboard
        if get_cfg("dashboard", "enabled"):
            await start_dashboard(
                get_cfg("dashboard", "host"),
                get_cfg("dashboard", "port"),
            )

        # Start broadcast loop
        broadcast_task = asyncio.create_task(broadcast_loop())

        try:
            await rtde_future
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[MAIN] Fatal error: {e}")
        finally:
            running = False
            broadcast_task.cancel()


def main():
    global config, running

    parser = argparse.ArgumentParser(
        description="UR5 RTDE → WebSocket bridge with live config dashboard"
    )
    parser.add_argument("--config", default="config.yaml",
                        help="Path to YAML config file (default: config.yaml)")
    parser.add_argument("--robot-ip", help="Override robot IP from config")
    parser.add_argument("--ws-port", type=int, help="Override WebSocket port from config")
    parser.add_argument("--hz", type=float, help="Override RTDE frequency from config")
    args = parser.parse_args()

    # Load config
    config = load_config(Path(args.config))

    # CLI overrides
    if args.robot_ip:
        config["robot"]["ip"] = args.robot_ip
    if args.ws_port:
        config["websocket"]["port"] = args.ws_port
    if args.hz:
        config["robot"]["frequency_hz"] = args.hz
        config["websocket"]["broadcast_hz"] = args.hz

    # Graceful shutdown
    def signal_handler(sig, frame):
        global running
        print("\n[MAIN] Shutting down gracefully...")
        running = False
        # Allow asyncio to clean up properly — no sys.exit here

    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass
    print("[MAIN] Bye ✓")


if __name__ == "__main__":
    main()