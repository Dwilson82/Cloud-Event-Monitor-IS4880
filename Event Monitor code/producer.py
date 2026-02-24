from google.cloud import pubsub_v1
import json

project_id = "project-e3a6924b-8583-4f8a-b9d"
topic_id = "cloudevent-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

message_data = {
    "messageId": "MSG-1001",
    "eventType": "TEST_EVENT"
}

message_json = json.dumps(message_data)
message_bytes = message_json.encode("utf-8")

future = publisher.publish(topic_path, message_bytes)
print("Message published with ID:", future.result())
                                                     
