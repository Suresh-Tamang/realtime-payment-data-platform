import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker
fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 1)
)

def generate_payment():
    return{
        "transaction_id": f"TXN{random.randint(100000, 999999)}",
        "ts_event": datetime.now(timezone.utc).isoformat(),
        "card_hash": fake.sha1(),
        "merchant_id": f"M{random.randint(1000,9999)}",
        "amount": round(random.uniform(1,15000), 2),
        "currency": "USD",
        "mcc": "5411",
        "channel": random.choice(["POS", "ECOM"]),
        "status": random.choice(["APPROVED", "DECLINED"]),
        "location": random.choice(["USA", "UK", "India"])
    }
  
  
if __name__ == "__main__":
    topic_name = 'payments.raw'
    print(f"Sending transactions to topic: {topic_name}, Press Ctrl+C to Stop \n")  

    try:
        print(f"Producing event to topic {topic_name}: \n")
        while True:
            event = generate_payment()
            # Send the event to Kafka topic
            producer.send(topic_name, event)
            print(event, "\n")
            #flush the producer to ensure the event is sent
            time.sleep(0.5)  # Simulate a delay between events
    except KeyboardInterrupt:
        print("Stopping payment producer.") 
    finally:
        producer.flush()
        producer.close()