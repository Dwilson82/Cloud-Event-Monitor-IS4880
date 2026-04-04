from flask import Flask, render_template, jsonify
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

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/events")
def get_events():
    conn = None
    try:
        print("=== API CALLED ===")

        conn = get_db_connection()
        print("DB connection successful")

        with conn.cursor() as cursor:
            print("Running query...")

            cursor.execute("""
                SELECT
                    message_id AS id,
                    temp_c AS temp,
                    DATE_FORMAT(timestamp_utc, '%H:%i:%s') AS time,
                    is_duplicate AS duplicate
                FROM messages
                ORDER BY timestamp_utc DESC
                LIMIT 20
            """)

            rows = cursor.fetchall()
            print(f"Rows fetched: {len(rows)}")

        for row in rows:
            row["duplicate"] = bool(row["duplicate"])

        return jsonify(rows)

    except Exception as e:
        print("🔥 FULL ERROR:", str(e))
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()
    