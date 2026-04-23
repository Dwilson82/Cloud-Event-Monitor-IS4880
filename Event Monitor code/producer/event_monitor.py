# This script is hardware dependant and requires the raspberry pi and the DS18B20 to produce output

import glob
import json
import logging
import os
import queue
import random
import socket
import threading
import time
import tkinter as tk
from tkinter import ttk
from datetime import datetime

try:
    from google.cloud import pubsub_v1
except Exception:
    pubsub_v1 = None

# Combined monitor:
# - Live mode (radio): DS18B20 on 1-wire
# - Simulated mode (radio): random +/- 5C changes
# - Publish mode (toggle button): publish to Pub/Sub

GCP_PROJECT_ID = "cloud-event-monitor-v2"
PUBSUB_TOPIC_ID = "telemetry-topic"
DEVICE_ID_DEFAULT = f"{socket.gethostname()}-pi"
CONFIG_FILE = "config.json"

DEFAULT_CONFIG = {
    "device_id_live": "rpi-1",
    "device_id_sim": "rpi-sim",
    "publish_interval": 10,
    "temp_high_threshold": 30,
    "temp_low_threshold": -10,
    "sim_min_temp": -20,
    "sim_max_temp": 35,
    "alerts_enabled": True,
    "duplicate_sim_enabled": False,
    "duplicate_rate_percent": 5,
    "id_counters": {},
}

EVENT_TYPE_CODE_MAP = {
    "TEMP_READING": "TEMP",
    "TEMP_THRESHOLD_EXCEEDED": "ALERT",
    "TEMP_THRESHOLD_RECOVERED": "REC",
}

BASE_TEMP_C = 22.0
MAX_VARIATION_C = 5.0
LIVE_INTERVAL_S = 1.0
SPOOL_FILE = "spool_unsent_events.jsonl"

_device_path = None
_rom = None
_ds18_initialized = False


def load_config(log):
    config = DEFAULT_CONFIG.copy()

    if not os.path.exists(CONFIG_FILE):
        save_config(config, log)
        return config

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as config_file:
            loaded = json.load(config_file)

        if isinstance(loaded, dict):
            config.update(loaded)
            if not isinstance(config.get("id_counters"), dict):
                config["id_counters"] = {}

            missing = [key for key in DEFAULT_CONFIG if key not in loaded]
            if missing:
                save_config(config, log)
        else:
            log.error("Invalid config format. Recreating with defaults")
            save_config(config, log)
    except Exception as exc:
        log.error("Failed to read config file. Using defaults: %s", exc)
        save_config(config, log)

    return config


def save_config(config, log):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as config_file:
            json.dump(config, config_file, indent=2)
    except Exception as exc:
        log.error("Failed to write config file: %s", exc)


def next_series_value(series):
    if not isinstance(series, str) or len(series) != 4:
        return "AA01"

    letters = series[:2]
    digits = series[2:]

    if not letters.isalpha() or not digits.isdigit():
        return "AA01"

    number = int(digits)
    if number < 1 or number > 99:
        return "AA01"

    if number < 99:
        return f"{letters.upper()}{number + 1:02d}"

    first = ord(letters[0].upper()) - ord("A")
    second = ord(letters[1].upper()) - ord("A")
    if first < 0 or first > 25 or second < 0 or second > 25:
        return "AA01"

    second += 1
    if second > 25:
        second = 0
        first += 1
        if first > 25:
            first = 0

    return f"{chr(first + ord('A'))}{chr(second + ord('A'))}01"


def init_ds18b20(log):
    global _device_path, _rom, _ds18_initialized

    if _ds18_initialized:
        return True

    if os.name == "nt":
        log.info("Live mode is not supported on Windows")
        return False

    try:
        os.system("modprobe w1-gpio")
        os.system("modprobe w1-therm")

        base_dir = "/sys/bus/w1/devices/"
        matches = glob.glob(base_dir + "28*")
        if not matches:
            log.error("No DS18B20 device found under %s", base_dir)
            return False

        _device_path = matches[0]
        _rom = _device_path.split("/")[-1]
        _ds18_initialized = True
        log.info("DS18B20 initialized rom=%s path=%s", _rom, _device_path)
        return True
    except Exception as exc:
        log.error("DS18B20 init failed: %s", exc)
        return False


def read_temp_raw_live():
    with open(_device_path + "/w1_slave", "r", encoding="utf-8") as sensor_file:
        valid, temp = sensor_file.readlines()
    return valid, temp


def read_temp_live(log):
    valid, temp = read_temp_raw_live()
    while "YES" not in valid:
        time.sleep(0.2)
        valid, temp = read_temp_raw_live()

    pos = temp.index("t=")
    temp_string = temp[pos + 2 :]
    temp_c = float(temp_string) / 1000.0
    temp_f = temp_c * (9.0 / 5.0) + 32.0
    log.info("rom=%s temp_c=%.3f temp_f=%.3f", _rom, temp_c, temp_f)
    return temp_c, temp_f


def build_event_payload(device_id, mode, temp_c, temp_f, sequence, message_id, event_type="TEMP_READING", extra_fields=None):
    payload = {
        "message_id": message_id,
        "device_id": device_id,
        "mode": mode,
        "temp_c": temp_c,
        "temp_f": temp_f,
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "sequence": sequence,
        "event_type": event_type,
    }
    if extra_fields:
        payload.update(extra_fields)
    return payload


def spool_event(event_payload, log):
    try:
        with open(SPOOL_FILE, "a", encoding="utf-8") as spool_file:
            spool_file.write(json.dumps(event_payload) + "\n")
    except Exception as exc:
        log.error("Failed writing spool file: %s", exc)


def build_logger():
    log = logging.getLogger("sensor_logger")
    log.setLevel(logging.INFO)
    if not log.handlers:
        formatter = logging.Formatter("%(asctime)s %(message)s")
        handler = logging.FileHandler("sensor_readings.log")
        handler.setFormatter(formatter)
        log.addHandler(handler)
    return log


def publisher_worker(running_event, publish_queue, output_queue, log, is_publish_enabled):
    publisher = None
    topic_path = None
    fallback_spool_only = False
    fallback_status_sent = False
    published_ids = set()

    if pubsub_v1 is None:
        fallback_spool_only = True
    else:
        try:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)
        except Exception as exc:
            fallback_spool_only = True
            log.error("Pub/Sub unavailable; falling back to spool-only mode: %s", exc)

    while running_event.is_set() or not publish_queue.empty():
        try:
            event_payload = publish_queue.get(timeout=0.2)
        except queue.Empty:
            continue

        is_duplicate = event_payload["message_id"] in published_ids

        if fallback_spool_only:
            spool_event(event_payload, log)
            if not fallback_status_sent:
                output_queue.put(("status", "Pub/Sub unavailable: spooling enabled"))
                fallback_status_sent = True
            publish_queue.task_done()
            continue

        if not is_publish_enabled():
            spool_event(event_payload, log)
            output_queue.put(("status", "Publish disabled: event spooled"))
            log.info("Publish disabled; spooled message_id=%s", event_payload["message_id"])
            publish_queue.task_done()
            continue

        try:
            data = json.dumps(event_payload).encode("utf-8")
            future = publisher.publish(
                topic_path,
                data,
                message_id=event_payload["message_id"],
                device_id=event_payload["device_id"],
                mode=event_payload["mode"],
                event_type=event_payload["event_type"],
            )
            pubsub_id = future.result(timeout=5)

            if is_duplicate:
                output_queue.put(("status", f"Published duplicate event {event_payload['message_id']}"))
            else:
                output_queue.put(("status", f"Published event {event_payload['message_id']}"))

            log.info(
                "Published pubsub_id=%s message_id=%s sequence=%s",
                pubsub_id,
                event_payload["message_id"],
                event_payload["sequence"],
            )
            published_ids.add(event_payload["message_id"])
        except Exception as exc:
            spool_event(event_payload, log)
            output_queue.put(("status", "Publish failed: event spooled"))
            log.error("Publish failed for message_id=%s: %s", event_payload["message_id"], exc)
            output_queue.put(("status", f"Pub/Sub init error: {exc}"))
        finally:
            publish_queue.task_done()


def temp_worker(running_event, output_queue, publish_queue, log, is_sim_mode, is_publish_enabled, get_config, next_message_id):
    current_temp_c = BASE_TEMP_C
    last_mode = None
    sequence = 0
    in_alert_state = False
    sim_trend = 1
    excursion_cycles = 0
    recovery_cycles = 0
    excursion_direction = 1
    excursion_target = None

    while running_event.is_set():
        sim_mode = is_sim_mode()

        if os.name == "nt" and not sim_mode:
            output_queue.put(("force_sim", "Live mode is not supported on Windows; switched to Simulated"))
            sim_mode = True

        if sim_mode != last_mode:
            mode_text = "Simulated mode active" if sim_mode else "Live mode active"
            output_queue.put(("status", mode_text))
            log.info(mode_text)
            last_mode = sim_mode

        try:
            config = get_config()
            sim_min_temp = float(config["sim_min_temp"])
            sim_max_temp = float(config["sim_max_temp"])
            threshold_low = float(config["temp_low_threshold"])
            threshold_high = float(config["temp_high_threshold"])
            alerts_enabled = bool(config.get("alerts_enabled", True))
            duplicate_enabled = bool(config.get("duplicate_sim_enabled", False))
            duplicate_rate_percent = max(0.0, min(100.0, float(config.get("duplicate_rate_percent", 5))))
            publish_interval = max(0.1, float(config["publish_interval"]))

            if sim_mode:
                normal_mid = (threshold_low + threshold_high) / 2.0

                if excursion_cycles > 0 and excursion_target is not None:
                    direction = 1 if excursion_target > current_temp_c else -1
                    temp_step = direction * random.uniform(1.0, 1.8)
                    excursion_cycles -= 1
                    crossed_threshold = current_temp_c < threshold_low or current_temp_c > threshold_high
                    reached_target = abs(excursion_target - current_temp_c) <= 0.8
                    if crossed_threshold or reached_target or excursion_cycles == 0:
                        recovery_cycles = random.randint(4, 8)
                        excursion_target = None
                        excursion_cycles = 0
                elif recovery_cycles > 0:
                    direction = 1 if normal_mid > current_temp_c else -1
                    temp_step = direction * random.uniform(0.5, 0.9)
                    recovery_cycles -= 1
                else:
                    if random.random() < 0.15:
                        sim_trend *= -1
                    temp_step = (sim_trend * random.uniform(0.1, 0.45)) + random.uniform(-0.2, 0.2)

                    can_breach_high = threshold_high < sim_max_temp
                    can_breach_low = threshold_low > sim_min_temp
                    if alerts_enabled and not in_alert_state and random.random() < 0.12 and (can_breach_high or can_breach_low):
                        options = [d for d in [1, -1] if (d == 1 and can_breach_high) or (d == -1 and can_breach_low)]
                        excursion_direction = random.choice(options)
                        if excursion_direction > 0:
                            excursion_target = min(sim_max_temp - 0.2, threshold_high + random.uniform(1.0, 2.5))
                        else:
                            excursion_target = max(sim_min_temp + 0.2, threshold_low - random.uniform(1.0, 2.5))
                        excursion_cycles = random.randint(10, 18)
                        temp_step += excursion_direction * random.uniform(0.8, 1.2)

                current_temp_c += temp_step

                if current_temp_c > sim_max_temp:
                    current_temp_c = sim_max_temp - (current_temp_c - sim_max_temp) * 0.4
                    sim_trend = -1
                elif current_temp_c < sim_min_temp:
                    current_temp_c = sim_min_temp + (sim_min_temp - current_temp_c) * 0.4
                    sim_trend = 1

                temp_c = current_temp_c
                temp_f = temp_c * (9.0 / 5.0) + 32.0
                output_queue.put(("temp", temp_c, temp_f))
                log.info("sim temp_c=%.3f temp_f=%.3f", temp_c, temp_f)
                device_id = config["device_id_sim"]
                mode = "sim"
            else:
                if not init_ds18b20(log):
                    output_queue.put(("status", "Live mode: DS18B20 not available"))
                    time.sleep(LIVE_INTERVAL_S)
                    continue

                temp_c, temp_f = read_temp_live(log)
                output_queue.put(("temp", temp_c, temp_f))
                device_id = config["device_id_live"]
                mode = "live"

            sequence += 1
            message_id = next_message_id(mode, "TEMP_READING")
            event_payload = build_event_payload(device_id, mode, temp_c, temp_f, sequence, message_id)
            publish_queue.put(event_payload)

            if duplicate_enabled and random.random() * 100.0 < duplicate_rate_percent:
                publish_queue.put(dict(event_payload))
                output_queue.put(("status", f"Queued duplicate event {event_payload['message_id']}"))

            if alerts_enabled:
                is_out_of_threshold = temp_c < threshold_low or temp_c > threshold_high

                if not in_alert_state and is_out_of_threshold:
                    in_alert_state = True
                    sequence += 1
                    threshold_message_id = next_message_id(mode, "TEMP_THRESHOLD_EXCEEDED")
                    threshold_payload = build_event_payload(
                        device_id,
                        mode,
                        temp_c,
                        temp_f,
                        sequence,
                        threshold_message_id,
                        event_type="TEMP_THRESHOLD_EXCEEDED",
                        extra_fields={
                            "temperature_c": temp_c,
                            "temperature_f": temp_f,
                            "temp_low_threshold": threshold_low,
                            "temp_high_threshold": threshold_high,
                            "alerts_enabled": alerts_enabled,
                        },
                    )
                    publish_queue.put(threshold_payload)
                    output_queue.put(
                        (
                            "status",
                            f"{device_id} TEMP_THRESHOLD_EXCEEDED temp={temp_c:.1f}C low={threshold_low:g} high={threshold_high:g}",
                        )
                    )

                    if duplicate_enabled and random.random() * 100.0 < duplicate_rate_percent:
                        publish_queue.put(dict(threshold_payload))
                        output_queue.put(("status", f"Queued duplicate event {threshold_payload['message_id']}"))

                elif in_alert_state and not is_out_of_threshold:
                    in_alert_state = False
                    sequence += 1
                    recover_message_id = next_message_id(mode, "TEMP_THRESHOLD_RECOVERED")
                    recover_payload = build_event_payload(
                        device_id,
                        mode,
                        temp_c,
                        temp_f,
                        sequence,
                        recover_message_id,
                        event_type="TEMP_THRESHOLD_RECOVERED",
                        extra_fields={
                            "temperature_c": temp_c,
                            "temperature_f": temp_f,
                            "temp_low_threshold": threshold_low,
                            "temp_high_threshold": threshold_high,
                            "alerts_enabled": alerts_enabled,
                        },
                    )
                    publish_queue.put(recover_payload)
                    output_queue.put(("status", f"{device_id} TEMP_THRESHOLD_RECOVERED temp={temp_c:.1f}C"))

                    if duplicate_enabled and random.random() * 100.0 < duplicate_rate_percent:
                        publish_queue.put(dict(recover_payload))
                        output_queue.put(("status", f"Queued duplicate event {recover_payload['message_id']}"))

            time.sleep(publish_interval)

        except Exception as exc:
            output_queue.put(("status", "Temp read error"))
            log.error("Temp worker error: %s", exc)
            time.sleep(1)


def main():
    log = build_logger()
    config = load_config(log)
    output_queue = queue.Queue()
    publish_queue = queue.Queue()

    temp_running = threading.Event()
    publish_running = threading.Event()
    temp_thread = None
    publish_thread = None

    mode_lock = threading.Lock()
    sim_mode_enabled = True

    publish_lock = threading.Lock()
    publish_enabled = False

    config_lock = threading.Lock()

    last_temp = None
    last_status = None

    root = tk.Tk()
    root.title("Event Monitor")
    root.geometry("880x620")

    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass

    title_font = ("Segoe UI", 16, "bold")
    subtitle_font = ("Segoe UI", 10)
    temp_font = ("Segoe UI", 24, "bold")
    status_font = ("Segoe UI", 10)
    log_font = ("Consolas", 10)

    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)

    container = ttk.Frame(root, padding=14)
    container.grid(row=0, column=0, sticky="nsew")
    container.columnconfigure(0, weight=1)
    container.rowconfigure(5, weight=1)

    header_frame = ttk.Frame(container)
    header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
    header_frame.columnconfigure(0, weight=1)

    title_label = ttk.Label(header_frame, text="Event Monitor", font=title_font)
    title_label.grid(row=0, column=0, sticky="w")

    subtitle_label = ttk.Label(header_frame, text="Telemetry Publisher", font=subtitle_font)
    subtitle_label.grid(row=1, column=0, sticky="w", pady=(2, 0))

    controls_frame = ttk.Frame(header_frame)
    controls_frame.grid(row=0, column=1, rowspan=2, sticky="e")

    mode_label = ttk.Label(controls_frame, text="Mode:")
    mode_label.grid(row=0, column=0, padx=(0, 6), sticky="w")

    mode_var = tk.StringVar(value="sim")
    ttk.Radiobutton(controls_frame, text="Live", variable=mode_var, value="live").grid(row=0, column=1, padx=(0, 6))
    ttk.Radiobutton(controls_frame, text="Simulated", variable=mode_var, value="sim").grid(row=0, column=2, padx=(0, 12))

    publish_button = ttk.Button(controls_frame, text="Enable Publishing")
    publish_button.grid(row=0, column=3, padx=(0, 8))

    publish_state_label = ttk.Label(controls_frame, text="Publishing: OFF")
    publish_state_label.grid(row=0, column=4, sticky="w")

    mode_state_label = ttk.Label(container, text="Mode: Simulated", font=subtitle_font)
    mode_state_label.grid(row=1, column=0, sticky="w")

    temp_label = ttk.Label(container, text="--.- C / --.- F", font=temp_font, anchor="center")
    temp_label.grid(row=2, column=0, sticky="ew", pady=(8, 6))

    status_label = ttk.Label(container, text="Status: Simulated mode selected", font=status_font, anchor="center")
    status_label.grid(row=3, column=0, sticky="ew", pady=(0, 10))

    ttk.Separator(container, orient="horizontal").grid(row=4, column=0, sticky="ew", pady=(0, 10))

    log_group = ttk.LabelFrame(container, text="Change Log", padding=8)
    log_group.grid(row=5, column=0, sticky="nsew")
    log_group.columnconfigure(0, weight=1)
    log_group.rowconfigure(0, weight=1)

    log_text = tk.Text(log_group, height=12, state="disabled", wrap="word", font=log_font, relief="flat")
    log_text.grid(row=0, column=0, sticky="nsew")

    log_scrollbar = ttk.Scrollbar(log_group, orient="vertical", command=log_text.yview)
    log_scrollbar.grid(row=0, column=1, sticky="ns")
    log_text.configure(yscrollcommand=log_scrollbar.set)

    button_frame = ttk.Frame(container)
    button_frame.grid(row=6, column=0, sticky="e", pady=(12, 0))

    menubar = tk.Menu(root)
    file_menu = tk.Menu(menubar, tearoff=0)
    file_menu.add_command(label="Exit", command=root.quit)
    menubar.add_cascade(label="File", menu=file_menu)

    settings_menu = tk.Menu(menubar, tearoff=0)
    menubar.add_cascade(label="Settings", menu=settings_menu)

    help_menu = tk.Menu(menubar, tearoff=0)
    help_menu.add_command(label="About", command=lambda: output_queue.put(("status", "Event Monitor Telemetry Publisher")))
    menubar.add_cascade(label="Help", menu=help_menu)

    root.config(menu=menubar)

    def append_log(message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_text.configure(state="normal")
        log_text.insert("end", "{time} {message}\n".format(time=timestamp, message=message))
        log_text.see("end")
        log_text.configure(state="disabled")

    def is_sim_mode():
        with mode_lock:
            return sim_mode_enabled

    def is_publish_enabled():
        with publish_lock:
            return publish_enabled

    def get_config():
        with config_lock:
            return dict(config)

    def set_config(new_config):
        with config_lock:
            config.update(new_config)

    def next_message_id(mode, event_type):
        with config_lock:
            device_prefix = "sim" if mode == "sim" else "rpi"
            type_code = EVENT_TYPE_CODE_MAP.get(event_type, "EVT")
            counter_key = f"{device_prefix}-{type_code}"

            id_counters = config.setdefault("id_counters", {})
            if not isinstance(id_counters, dict):
                id_counters = {}
                config["id_counters"] = id_counters

            current_series = id_counters.get(counter_key, "AA00")
            next_series = next_series_value(current_series)
            id_counters[counter_key] = next_series
            save_config(config, log)

            return f"{device_prefix}-{type_code}-{next_series}"

    def open_config_window():
        config_snapshot = get_config()
        config_window = tk.Toplevel(root)
        config_window.title("Publisher Configuration")
        config_window.geometry("460x620")
        config_window.transient(root)
        config_window.grab_set()

        body = tk.Frame(config_window, padx=12, pady=12)
        body.pack(fill="both", expand=True)

        def build_section(title, row_start, fields):
            tk.Label(body, text=title, font=("Segoe UI", 10, "bold")).grid(
                row=row_start, column=0, columnspan=2, sticky="w", pady=(8, 4)
            )
            entries = {}
            row = row_start + 1
            for key, label_text in fields:
                tk.Label(body, text=label_text).grid(row=row, column=0, sticky="w", pady=2)
                entry = tk.Entry(body, width=28)
                entry.insert(0, str(config_snapshot[key]))
                entry.grid(row=row, column=1, sticky="ew", padx=(10, 0), pady=2)
                entries[key] = entry
                row += 1
            return entries, row

        body.columnconfigure(1, weight=1)

        all_entries = {}
        entries, next_row = build_section(
            "Device Settings",
            0,
            [
                ("device_id_live", "Live Device ID"),
                ("device_id_sim", "Simulated Device ID"),
            ],
        )
        all_entries.update(entries)

        entries, next_row = build_section(
            "Publishing",
            next_row,
            [("publish_interval", "Publish Interval (seconds)")],
        )
        all_entries.update(entries)

        entries, next_row = build_section(
            "Thresholds",
            next_row,
            [
                ("temp_high_threshold", "High Temperature Threshold (°C)"),
                ("temp_low_threshold", "Low Temperature Threshold (°C)"),
            ],
        )
        all_entries.update(entries)

        entries, next_row = build_section(
            "Simulation",
            next_row,
            [
                ("sim_min_temp", "Simulation Minimum Temperature (°C)"),
                ("sim_max_temp", "Simulation Maximum Temperature (°C)"),
            ],
        )
        all_entries.update(entries)

        tk.Label(body, text="Alerts", font=("Segoe UI", 10, "bold")).grid(
            row=next_row, column=0, columnspan=2, sticky="w", pady=(8, 4)
        )
        alerts_enabled_var = tk.BooleanVar(value=bool(config_snapshot.get("alerts_enabled", True)))
        tk.Checkbutton(body, text="Enable Temperature Alerts", variable=alerts_enabled_var).grid(
            row=next_row + 1, column=0, columnspan=2, sticky="w", pady=2
        )

        tk.Label(body, text="Duplicate Simulation", font=("Segoe UI", 10, "bold")).grid(
            row=next_row + 2, column=0, columnspan=2, sticky="w", pady=(8, 4)
        )
        duplicate_enabled_var = tk.BooleanVar(value=bool(config_snapshot.get("duplicate_sim_enabled", False)))
        tk.Checkbutton(body, text="Enable Duplicate Simulation", variable=duplicate_enabled_var).grid(
            row=next_row + 3, column=0, columnspan=2, sticky="w", pady=2
        )

        tk.Label(body, text="Duplicate Rate (%)").grid(row=next_row + 4, column=0, sticky="w", pady=2)
        duplicate_rate_entry = tk.Entry(body, width=28)
        duplicate_rate_entry.insert(0, str(config_snapshot.get("duplicate_rate_percent", 5)))
        duplicate_rate_entry.grid(row=next_row + 4, column=1, sticky="ew", padx=(10, 0), pady=2)

        button_row = tk.Frame(body)
        button_row.grid(row=next_row + 5, column=0, columnspan=2, sticky="e", pady=(16, 0))

        def save_and_close():
            try:
                duplicate_rate_percent = float(duplicate_rate_entry.get())
                updated_config = {
                    "device_id_live": all_entries["device_id_live"].get().strip() or DEFAULT_CONFIG["device_id_live"],
                    "device_id_sim": all_entries["device_id_sim"].get().strip() or DEFAULT_CONFIG["device_id_sim"],
                    "publish_interval": float(all_entries["publish_interval"].get()),
                    "temp_high_threshold": float(all_entries["temp_high_threshold"].get()),
                    "temp_low_threshold": float(all_entries["temp_low_threshold"].get()),
                    "sim_min_temp": float(all_entries["sim_min_temp"].get()),
                    "sim_max_temp": float(all_entries["sim_max_temp"].get()),
                    "alerts_enabled": bool(alerts_enabled_var.get()),
                    "duplicate_sim_enabled": bool(duplicate_enabled_var.get()),
                    "duplicate_rate_percent": duplicate_rate_percent,
                }
            except ValueError:
                output_queue.put(("status", "Invalid configuration values"))
                return

            if updated_config["sim_min_temp"] > updated_config["sim_max_temp"]:
                output_queue.put(("status", "Simulation min temp cannot exceed max temp"))
                return

            if updated_config["duplicate_rate_percent"] < 0 or updated_config["duplicate_rate_percent"] > 100:
                output_queue.put(("status", "Duplicate rate must be between 0 and 100"))
                return

            set_config(updated_config)
            save_config(get_config(), log)
            output_queue.put(("status", "Publisher configuration updated"))
            config_window.destroy()

        tk.Button(button_row, text="Save", command=save_and_close, width=10).pack(side="right", padx=(8, 0))
        tk.Button(button_row, text="Cancel", command=config_window.destroy, width=10).pack(side="right")

    settings_menu.add_command(label="Configure Publisher", command=open_config_window)

    def update_mode_selection():
        nonlocal sim_mode_enabled
        requested_mode = mode_var.get()
        requested_sim = requested_mode == "sim"

        if os.name == "nt" and not requested_sim:
            mode_var.set("sim")
            requested_sim = True
            output_queue.put(("status", "Live mode is not supported on Windows; staying in Simulated"))

        with mode_lock:
            sim_mode_enabled = requested_sim

        mode_text = "Simulated mode selected" if requested_sim else "Live mode selected"
        mode_state_label.config(text="Mode: {}".format("Simulated" if requested_sim else "Live"))
        output_queue.put(("status", mode_text))

    def set_publish_ui_state():
        if is_publish_enabled():
            publish_button.config(text="Disable Publishing")
            publish_state_label.config(text="Publishing: ON")
        else:
            publish_button.config(text="Enable Publishing")
            publish_state_label.config(text="Publishing: OFF")

    def toggle_publish_state():
        nonlocal publish_enabled
        with publish_lock:
            publish_enabled = not publish_enabled
            now_enabled = publish_enabled
        set_publish_ui_state()
        if now_enabled:
            output_queue.put(("status", "Publish enabled"))
        else:
            output_queue.put(("status", "Publish disabled: events will be spooled"))

    mode_var.trace_add("write", lambda *_: update_mode_selection())
    publish_button.config(command=toggle_publish_state)
    set_publish_ui_state()

    def ensure_publisher_thread():
        nonlocal publish_thread
        if publish_running.is_set() and publish_thread is not None and publish_thread.is_alive():
            return

        publish_running.set()
        publish_thread = threading.Thread(
            target=publisher_worker,
            args=(publish_running, publish_queue, output_queue, log, is_publish_enabled),
            daemon=True,
        )
        publish_thread.start()

    def start_temp():
        nonlocal temp_thread
        if temp_running.is_set():
            return

        ensure_publisher_thread()

        temp_running.set()
        temp_thread = threading.Thread(
            target=temp_worker,
            args=(temp_running, output_queue, publish_queue, log, is_sim_mode, is_publish_enabled, get_config, next_message_id),
            daemon=True,
        )
        temp_thread.start()
        status_label.config(text="Status: Temp sensor started at {time}".format(time=datetime.now().strftime("%H:%M:%S")))

    def stop_temp():
        if temp_running.is_set():
            temp_running.clear()
            status_label.config(text="Status: Temp sensor stopped at {time}".format(time=datetime.now().strftime("%H:%M:%S")))

    def process_queue():
        nonlocal last_temp
        nonlocal last_status

        try:
            while True:
                message = output_queue.get_nowait()
                if not message:
                    continue

                if message[0] == "temp":
                    _, temp_c, temp_f = message
                    temp_label.config(text="{temp_c:.3f} C / {temp_f:.3f} F".format(temp_c=temp_c, temp_f=temp_f))
                    current_temp = (round(temp_c, 3), round(temp_f, 3))
                    if current_temp != last_temp:
                        append_log("Temp changed to {temp_c:.3f} C / {temp_f:.3f} F".format(temp_c=temp_c, temp_f=temp_f))
                        last_temp = current_temp

                elif message[0] == "status":
                    _, status = message
                    if status != last_status:
                        status_label.config(text="Status: {status}".format(status=status))
                        append_log(status)
                        log.info("status=%s", status)
                        last_status = status

                elif message[0] == "force_sim":
                    _, status = message
                    mode_var.set("sim")
                    with mode_lock:
                        sim_mode_enabled = True
                    mode_state_label.config(text="Mode: Simulated")
                    status_label.config(text="Status: {status}".format(status=status))
                    append_log(status)
                    log.info("status=%s", status)
                    last_status = status

        except queue.Empty:
            pass

        root.after(200, process_queue)

    def shutdown():
        temp_running.clear()
        publish_running.clear()

        if temp_thread is not None and temp_thread.is_alive():
            temp_thread.join(timeout=1.5)

        if publish_thread is not None and publish_thread.is_alive():
            publish_thread.join(timeout=1.5)

        root.destroy()

    ttk.Button(button_frame, text="Start Temp", command=start_temp).grid(row=0, column=0, padx=(0, 8))
    ttk.Button(button_frame, text="Stop Temp", command=stop_temp).grid(row=0, column=1, padx=(0, 8))
    ttk.Button(button_frame, text="Quit", command=shutdown).grid(row=0, column=2)

    root.after(200, process_queue)
    root.protocol("WM_DELETE_WINDOW", shutdown)
    root.mainloop()


if __name__ == "__main__":
    main()