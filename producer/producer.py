import os
import json
import time
import random
import utils
from kafka import KafkaProducer


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("TOPIC", "game-events")

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        compression_type='gzip'
    )

    try:
        while True:
            event_type = random.choice(["init", "match", "purchase"])
            match event_type:
                case "init":
                    event = utils.generate_init_event()
                    producer.send("init_events", event)
                case "match":
                    event = utils.generate_match_event()
                    producer.send("match_events", event)
                case "purchase":
                    event = utils.generate_inapp_purchase_event()
                    producer.send("iap_events", event)
            print(event)
            time.sleep(random.uniform(0, 1.0))

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        # Safely close the producer
        producer.close()

if __name__ == "__main__":
    main()
