import os
import json
import time
import random
import utils
from kafka import KafkaProducer

# Set up Kafka broker and topic from environment variables, with defaults
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("TOPIC", "game-events")

def main():
    # Initialize Kafka producer with specific configurations
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serialize values to JSON
        retries=3, # Number of retries on failure
        compression_type='gzip' # Compress messages with gzip
    )

    try:
        while True:
            # Randomly select an event type to generate
            event_type = random.choice(["init", "match", "purchase"])
            match event_type:
                case "init":
                    # Generate and send an init event
                    event = utils.generate_init_event()
                    producer.send("init_events", event)
                case "match":
                    # Generate and send a match event
                    event = utils.generate_match_event()
                    producer.send("match_events", event)
                case "purchase":
                    # Generate and send an in-app purchase event
                    event = utils.generate_inapp_purchase_event()
                    producer.send("iap_events", event)

            # Sleep for a random time between 0 to 1 second            
            time.sleep(random.uniform(0, 1.0))

    except KeyboardInterrupt:
        # Gracefully handle a keyboard interrupt (e.g., Ctrl+C)
        print("Stopping producer...")
    finally:
        # Safely close the Kafka producer
        producer.close()

if __name__ == "__main__":
    main()
