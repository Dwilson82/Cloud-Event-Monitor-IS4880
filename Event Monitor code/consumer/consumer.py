import base64
import json
import os
import pymysql


def process_cloud_event(cloud_event):
    connection = None

    try:
        envelope = cloud_event.data
        print("RAW EVENT TYPE:", type(envelope))
        print("RAW EVENT:", envelope)

        if isinstance(envelope, bytes):
            envelope = envelope.decode("utf-8")

        if isinstance(envelope, str):
            envelope = json.loads(envelope)

        if not isinstance(envelope, dict):
            print("Unexpected event format:", envelope)
            return

        print("PARSED EVENT:", envelope)

        message = envelope.get("message", {})
        data_b64 = message.get("data")

        if not data_b64:
            print("Missing data field in Pub/Sub message")
            return

        decoded = base64.b64decode(data_b64).decode("utf-8")
        print("Decoded message:", decoded)

        payload_json = json.loads(decoded)

        message_id = payload_json.get("message_id")
        if not message_id:
            print("Missing message_id in payload:", payload_json)
            return

        device_id = payload_json.get("device_id")
        temp_c = payload_json.get("temp_c")
        temp_f = payload_json.get("temp_f")
        timestamp_utc = payload_json.get("timestamp_utc")

        if timestamp_utc:
            timestamp_utc = timestamp_utc.replace("T", " ").replace("Z", "")
            if "." in timestamp_utc:
                timestamp_utc = timestamp_utc.split(".")[0]

        print("Connecting to database...")

        connection = pymysql.connect(
            unix_socket=f"/cloudsql/{os.environ['INSTANCE_CONNECTION_NAME']}",
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASS"],
            database=os.environ["DB_NAME"],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False
        )

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS cnt FROM messages WHERE message_id = %s",
                (message_id,)
            )
            result = cursor.fetchone()
            is_duplicate = 1 if result["cnt"] > 0 else 0

            insert_sql = """
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
                insert_sql,
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
        print(f"Insert successful: message_id={message_id}, duplicate={is_duplicate}")

    except Exception as e:
        print("DATABASE ERROR:", str(e))
        raise

    finally:
        if connection:
            connection.close()