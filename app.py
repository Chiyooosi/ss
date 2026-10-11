"""
Server cloud UTS IoT: HTTP (Flask) + subscriber MQTT (broker publik) -> satu log CSV.
Deploy di Render dengan start command:
    gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4
PENTING: --workers 1, supaya hanya ada satu subscriber MQTT (kalau 2 worker, data tercatat dobel).

Endpoint:
  POST /data      terima data dari ESP32 (HTTP)
  GET  /health    cek server hidup + jumlah data masuk per protokol/skenario
  GET  /log.csv   unduh seluruh data
  GET  /reset     kosongkan data (dipakai sebelum tiap run)
"""
import csv
import json
import os
import threading
import time
from collections import Counter

import paho.mqtt.client as mqtt
from flask import Flask, Response, jsonify, request

MQTT_HOST = "broker.hivemq.com"
MQTT_PORT = 8883                                  # TLS, setara dengan HTTPS
MQTT_TOPIC = "iot/unsika/uts-rafi/hcsr04"         # HARUS sama dengan di sketch ESP32
LOG_FILE = "log.csv"
HEADER = ["proto", "scn", "seq", "t_send_ms", "t_recv_ms", "dist_cm", "bytes"]
REQUIRED = ("proto", "scn", "seq", "t_send", "dist_cm")

app = Flask(__name__)
lock = threading.Lock()


def init_log():
    with lock, open(LOG_FILE, "w", newline="") as f:
        csv.writer(f).writerow(HEADER)


def simpan(raw: bytes) -> bool:
    t_recv = int(time.time() * 1000)
    try:
        d = json.loads(raw)
        if not all(k in d for k in REQUIRED):
            return False
    except Exception:
        return False
    with lock, open(LOG_FILE, "a", newline="") as f:
        csv.writer(f).writerow(
            [d["proto"], d["scn"], d["seq"], d["t_send"], t_recv, d["dist_cm"], len(raw)]
        )
    return True


if not os.path.exists(LOG_FILE):
    init_log()


# ---------------- HTTP ----------------
@app.route("/data", methods=["POST"])
def data():
    return ("OK", 200) if simpan(request.get_data()) else ("BAD", 400)


@app.route("/health")
def health():
    with lock, open(LOG_FILE, newline="") as f:
        rows = list(csv.DictReader(f))
    hitung = Counter(f'{r["proto"]}/{r["scn"]}' for r in rows)
    return jsonify(status="ok", total=len(rows), per_proto_skenario=hitung)


@app.route("/log.csv")
def log_csv():
    with lock, open(LOG_FILE) as f:
        isi = f.read()
    return Response(
        isi,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=log.csv"},
    )


@app.route("/reset")
def reset():
    init_log()
    return "log dikosongkan"


# ---------------- MQTT ----------------
def on_connect(client, userdata, flags, reason_code, properties):
    print("MQTT terhubung:", reason_code, flush=True)
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    simpan(msg.payload)


_started = False


def start_mqtt():
    global _started
    if _started:
        return
    _started = True
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="render-sub-uts-rafi")
    c.tls_set()                       # pakai CA bawaan sistem
    c.on_connect = on_connect
    c.on_message = on_message
    c.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
    c.loop_start()                    # thread sendiri + auto-reconnect


start_mqtt()

if __name__ == "__main__":            # untuk tes lokal saja
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
