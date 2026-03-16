from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "green-trips",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="green-trip-consumer-2",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=1000
)

count = 0

for message in consumer:
    trip = message.value

    if trip["trip_distance"] > 5:
        count += 1

print("Trips > 5km:", count)