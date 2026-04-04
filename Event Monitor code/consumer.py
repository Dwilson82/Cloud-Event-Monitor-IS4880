import base64
import json
import os
import pymysql


def processCloudEvent(request):
    envelope = request.get_json(silent=True)
    print("FULL REQUEST:", envelope)

    if not envelope or "message" not in envelope:
        print("Invalid Pub/Sub format:", envelope)
        return ("Bad Request", 400)

    pubsub_message = envelope["message"]

    if "data" in pubsub_message:
        try:
            decoded = base64.b64decode(pubsub_message["data"]).decode("utf-8")
        except Exception as e:
            print("Decode error:", e)
            return ("Bad Request", 400)
    else:
        print("Missing data field in Pub/Sub message")
        return ("Bad Request", 400)

    print("Decoded message:", decoded)

    try:
        payload_json = json.loads(decoded)
    except Exception as e:
        print("JSON parse error:", e)
        return ("Bad Request", 400)

    message_id = payload_json.get("message_id")
    if not message_id:
        print("Missing message_id in payload:", payload_json)
        return ("Bad Request", 400)

    print("Using message_id:", message_id)

    device_id = payload_json.get("device_id")
    temp_c = payload_json.get("temp_c")
    temp_f = payload_json.get("temp_f")
    timestamp_utc = payload_json.get("timestamp_utc")

    if timestamp_utc:
        try:
            timestamp_utc = timestamp_utc.replace("T", " ").replace("Z", "")
            if "." in timestamp_utc:
                timestamp_utc = timestamp_utc.split(".")[0]
        except Exception as e:
            print("Timestamp conversion error:", e)
            return ("Bad Request", 400)

    connection = None

    try:
        print("Connecting to database...")

        connection = pymysql.connect(
            unix_socket="/cloudsql/project-e3a6924b-8583-4f8a-b9d:us-east1:cloudservereventmonitor",
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASS"],
            database=os.environ["DB_NAME"],
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS cnt FROM messages WHERE message_id = %s",
                (message_id,)
            )
            result = cursor.fetchone()
            is_duplicate = 1 if result["cnt"] > 0 else 0

            sql = """
            INSERT INTO messages (
                message_id,
                device_id,
                temp_c,
                temp_f,
                timestamp_utc,
                is_duplicate
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """

            cursor.execute(
                sql,
                (
                    message_id,
                    device_id,
                    temp_c,
                    temp_f,
                    timestamp_utc,
                    is_duplicate
                )
            )

        connection.commit()
        print(f"Insert successful, duplicate={is_duplicate}")

    except Exception as e:
        print("DATABASE ERROR:", str(e))
        return ("Internal Server Error", 500)

    finally:
        if connection:
            connection.close()

    return ("", 204)