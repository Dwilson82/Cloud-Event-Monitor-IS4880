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

        message = envelope.get("message", {})
        attributes = message.get("attributes", {})
        data_b64 = message.get("data")

        decoded_json = {}
        if data_b64:
            decoded = base64.b64decode(data_b64).decode("utf-8")
            print("Decoded message:", decoded)

            try:
                decoded_json = json.loads(decoded)
            except json.JSONDecodeError:
                print("Decoded payload is not valid JSON")
                decoded_json = {}

        # Prefer payload JSON, then fall back to Pub/Sub attributes
        message_id = decoded_json.get("message_id") or attributes.get("message_id")
        device_id = decoded_json.get("device_id") or attributes.get("device_id")
        temp_c = decoded_json.get("temp_c")
        temp_f = decoded_json.get("temp_f")
        timestamp_utc = decoded_json.get("timestamp_utc")
        event_type = decoded_json.get("event_type") or attributes.get("event_type")
        mode = decoded_json.get("mode") or attributes.get("mode")

        if not message_id:
            print("Missing message_id in both payload and attributes")
            return

        if timestamp_utc:
            timestamp_utc = timestamp_utc.replace("T", " ").replace("Z", "")
            if "." in timestamp_utc:
                timestamp_utc = timestamp_utc.split(".")[0]

        print("Extracted values:")
        print("message_id =", message_id)
        print("device_id =", device_id)
        print("temp_c =", temp_c)
        print("temp_f =", temp_f)
        print("timestamp_utc =", timestamp_utc)
        print("event_type =", event_type)
        print("mode =", mode)

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

            cursor.execute("""
                INSERT INTO messages (
                    message_id,
                    device_id,
                    temp_c,
                    temp_f,
                    timestamp_utc,
                    is_duplicate
                )
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                message_id,
                device_id,
                temp_c,
                temp_f,
                timestamp_utc,
                is_duplicate
            ))

        connection.commit()
        print(f"Insert successful: message_id={message_id}, duplicate={is_duplicate}")

    except Exception as e:
        print("DATABASE ERROR:", str(e))
        raise

    finally:
        if connection:
            connection.close()