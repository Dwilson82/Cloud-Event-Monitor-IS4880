from flask import Flask, render_template, jsonify, request
import pymysql
import os

app = Flask(__name__)

def get_db_connection():
    return pymysql.connect(
        unix_socket="/cloudsql/project-e3a6924b-8583-4f8a-b9d:us-east1:cloudservereventmonitor",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=os.environ["DB_NAME"],
        cursorclass=pymysql.cursors.DictCursor
    )

def get_current_settings(cursor):
    cursor.execute("""
        SELECT warning_temp_f, critical_temp_f, alert_enabled
        FROM dashboard_settings
        ORDER BY id DESC
        LIMIT 1
    """)
    row = cursor.fetchone()
    if not row:
        return {
            "warning_temp_f": 80.0,
            "critical_temp_f": 90.0,
            "alert_enabled": True
        }
    row["alert_enabled"] = bool(row["alert_enabled"])
    return row

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/events")
def get_events():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    message_id AS id,
                    device_id,
                    temp_c AS temp,
                    DATE_FORMAT(timestamp_utc, '%H:%i:%s') AS time,
                    is_duplicate AS duplicate
                FROM messages
                ORDER BY timestamp_utc DESC
               
            """)
            rows = cursor.fetchall()

        for row in rows:
            row["duplicate"] = bool(row["duplicate"])

        return jsonify(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()

@app.route("/api/metrics")
def get_metrics():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            settings = get_current_settings(cursor)

            cursor.execute("SELECT COUNT(*) AS total FROM messages")
            total = cursor.fetchone()["total"]

            cursor.execute("SELECT COUNT(*) AS dups FROM messages WHERE is_duplicate = 1")
            dups = cursor.fetchone()["dups"]

            cursor.execute("SELECT AVG(temp_f) AS avg_temp_f FROM messages")
            avg_temp = cursor.fetchone()["avg_temp_f"]

            cursor.execute("""
                SELECT COUNT(*) AS warning_count
                FROM messages
                WHERE temp_f >= %s AND temp_f < %s
            """, (settings["warning_temp_f"], settings["critical_temp_f"]))
            warning_count = cursor.fetchone()["warning_count"]

            cursor.execute("""
                SELECT COUNT(*) AS critical_count
                FROM messages
                WHERE temp_f >= %s
            """, (settings["critical_temp_f"],))
            critical_count = cursor.fetchone()["critical_count"]

            cursor.execute("""
                SELECT MAX(temp_f) AS max_temp_f
                FROM messages
            """)
            max_temp = cursor.fetchone()["max_temp_f"]

        out_of_range = warning_count + critical_count
        out_of_range_pct = round((out_of_range / total) * 100, 1) if total > 0 else 0

        return jsonify({
            "total": total,
            "dups": dups,
            "avg_temp_f": round(avg_temp, 1) if avg_temp is not None else 0,
            "warning_count": warning_count,
            "critical_count": critical_count,
            "max_temp_f": round(max_temp, 1) if max_temp is not None else 0,
            "out_of_range_pct": out_of_range_pct
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()

@app.route("/api/chart-data")
def chart_data():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    DATE_FORMAT(timestamp_utc, '%H:%i:%s') AS label,
                    temp_f AS temp
                FROM messages
                ORDER BY timestamp_utc DESC
                LIMIT 20
            """)
            rows = cursor.fetchall()

        rows.reverse()

        return jsonify({
            "labels": [r["label"] for r in rows],
            "temps": [float(r["temp"]) for r in rows]
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()

@app.route("/api/status")
def get_status():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            settings = get_current_settings(cursor)

            cursor.execute("""
                SELECT
                    device_id,
                    temp_f,
                    DATE_FORMAT(timestamp_utc, '%H:%i:%s') AS time
                FROM messages
                ORDER BY timestamp_utc DESC
                LIMIT 1
            """)
            latest = cursor.fetchone()

        if not latest:
            return jsonify({
                "status": "NO DATA",
                "color": "#9ca3af",
                "message": "No telemetry received yet",
                "latest_temp_f": None,
                "device_id": "--",
                "time": "--"
            })

        temp_f = float(latest["temp_f"])
        alerts_enabled = settings["alert_enabled"]

        if not alerts_enabled:
            status = "MONITORING"
            color = "#60a5fa"
            message = "Alerts disabled. Monitoring only."
        elif temp_f >= settings["critical_temp_f"]:
            status = "CRITICAL"
            color = "#ef4444"
            message = f"Temperature exceeds critical threshold ({settings['critical_temp_f']} °F)"
        elif temp_f >= settings["warning_temp_f"]:
            status = "WARNING"
            color = "#f59e0b"
            message = f"Temperature exceeds warning threshold ({settings['warning_temp_f']} °F)"
        else:
            status = "NORMAL"
            color = "#22c55e"
            message = "Temperature within configured range"

        return jsonify({
            "status": status,
            "color": color,
            "message": message,
            "latest_temp_f": round(temp_f, 1),
            "device_id": latest["device_id"],
            "time": latest["time"]
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()

@app.route("/api/settings", methods=["GET"])
def get_settings():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            settings = get_current_settings(cursor)
        return jsonify(settings)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()

@app.route("/api/settings", methods=["POST"])
def save_settings():
    conn = None
    try:
        data = request.get_json(force=True)

        warning_temp_f = float(data["warning_temp_f"])
        critical_temp_f = float(data["critical_temp_f"])
        alert_enabled = bool(data["alert_enabled"])

        if warning_temp_f >= critical_temp_f:
            return jsonify({"error": "Warning threshold must be lower than critical threshold."}), 400

        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dashboard_settings (
                    warning_temp_f,
                    critical_temp_f,
                    alert_enabled
                ) VALUES (%s, %s, %s)
            """, (warning_temp_f, critical_temp_f, alert_enabled))

        conn.commit()

        return jsonify({
            "success": True,
            "warning_temp_f": warning_temp_f,
            "critical_temp_f": critical_temp_f,
            "alert_enabled": alert_enabled
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()
