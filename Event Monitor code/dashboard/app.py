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


def f_to_c(temp_f):
    return (float(temp_f) - 32.0) * 5.0 / 9.0


def c_to_f(temp_c):
    return (float(temp_c) * 9.0 / 5.0) + 32.0


def get_current_settings(cursor):
    cursor.execute("""
        SELECT warning_temp_f, critical_temp_f, alert_enabled
        FROM dashboard_settings
        ORDER BY id DESC
        LIMIT 1
    """)
    row = cursor.fetchone()

    if not row:
        warning_c = f_to_c(80.0)
        critical_c = f_to_c(90.0)
        return {
            "warning_temp": round(warning_c, 1),
            "critical_temp": round(critical_c, 1),
            "warning_temp_f": round(warning_c, 1),   # compatibility alias for current frontend
            "critical_temp_f": round(critical_c, 1), # compatibility alias for current frontend
            "alert_enabled": True
        }

    warning_c = f_to_c(row["warning_temp_f"])
    critical_c = f_to_c(row["critical_temp_f"])

    return {
        "warning_temp": round(warning_c, 1),
        "critical_temp": round(critical_c, 1),
        "warning_temp_f": round(warning_c, 1),   # compatibility alias for current frontend
        "critical_temp_f": round(critical_c, 1), # compatibility alias for current frontend
        "alert_enabled": bool(row["alert_enabled"])
    }


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
                LIMIT 300
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

            cursor.execute("SELECT AVG(temp_c) AS avg_temp FROM messages")
            avg_temp = cursor.fetchone()["avg_temp"]

            cursor.execute("""
                SELECT COUNT(*) AS warning_count
                FROM messages
                WHERE temp_c >= %s AND temp_c < %s
            """, (settings["warning_temp"], settings["critical_temp"]))
            warning_count = cursor.fetchone()["warning_count"]

            cursor.execute("""
                SELECT COUNT(*) AS critical_count
                FROM messages
                WHERE temp_c >= %s
            """, (settings["critical_temp"],))
            critical_count = cursor.fetchone()["critical_count"]

            cursor.execute("""
                SELECT MAX(temp_c) AS max_temp
                FROM messages
            """)
            max_temp = cursor.fetchone()["max_temp"]

        out_of_range = warning_count + critical_count
        out_of_range_pct = round((out_of_range / total) * 100, 1) if total > 0 else 0

        avg_temp_val = round(avg_temp, 1) if avg_temp is not None else 0
        max_temp_val = round(max_temp, 1) if max_temp is not None else 0

        return jsonify({
            "total": total,
            "dups": dups,
            "avg_temp": avg_temp_val,
            "avg_temp_f": avg_temp_val,  # compatibility alias for current frontend
            "warning_count": warning_count,
            "critical_count": critical_count,
            "max_temp": max_temp_val,
            "max_temp_f": max_temp_val,  # compatibility alias for current frontend
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
                    temp_c AS temp
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
                    temp_c,
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
                "latest_temp": None,
                "latest_temp_f": None,  # compatibility alias for current frontend
                "device_id": "--",
                "time": "--"
            })

        temp_c = float(latest["temp_c"])
        alerts_enabled = settings["alert_enabled"]

        if not alerts_enabled:
            status = "MONITORING"
            color = "#60a5fa"
            message = "Alerts disabled. Monitoring only."
        elif temp_c >= settings["critical_temp"]:
            status = "CRITICAL"
            color = "#ef4444"
            message = f"Temperature exceeds critical threshold ({settings['critical_temp']} °C)"
        elif temp_c >= settings["warning_temp"]:
            status = "WARNING"
            color = "#f59e0b"
            message = f"Temperature exceeds warning threshold ({settings['warning_temp']} °C)"
        else:
            status = "NORMAL"
            color = "#22c55e"
            message = "Temperature within configured range"

        latest_temp_val = round(temp_c, 1)

        return jsonify({
            "status": status,
            "color": color,
            "message": message,
            "latest_temp": latest_temp_val,
            "latest_temp_f": latest_temp_val,  # compatibility alias for current frontend
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

        # Accept either new Celsius names or current frontend compatibility names
        warning_temp_c = data.get("warning_temp", data.get("warning_temp_f"))
        critical_temp_c = data.get("critical_temp", data.get("critical_temp_f"))
        alert_enabled = bool(data["alert_enabled"])

        warning_temp_c = float(warning_temp_c)
        critical_temp_c = float(critical_temp_c)

        if warning_temp_c >= critical_temp_c:
            return jsonify({"error": "Warning threshold must be lower than critical threshold."}), 400

        # Store in existing DB schema (Fahrenheit columns) for compatibility
        warning_temp_f_db = c_to_f(warning_temp_c)
        critical_temp_f_db = c_to_f(critical_temp_c)

        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dashboard_settings (
                    warning_temp_f,
                    critical_temp_f,
                    alert_enabled
                ) VALUES (%s, %s, %s)
            """, (warning_temp_f_db, critical_temp_f_db, alert_enabled))

        conn.commit()

        return jsonify({
            "success": True,
            "warning_temp": round(warning_temp_c, 1),
            "critical_temp": round(critical_temp_c, 1),
            "warning_temp_f": round(warning_temp_c, 1),   # compatibility alias for current frontend
            "critical_temp_f": round(critical_temp_c, 1), # compatibility alias for current frontend
            "alert_enabled": alert_enabled
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()