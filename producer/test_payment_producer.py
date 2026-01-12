import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# Use 'localhost:9092' if running from host, or 'kafka:29092' if inside Docker network
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_base_payment(card_hash=None, amount=None, merchant=None, location=None, auth="APPROVED"):
    return {
        "transaction_id": f"TXN{random.randint(100000, 999999)}",
        "ts_event": datetime.now(timezone.utc).isoformat(),
        "card_hash": card_hash if card_hash else fake.sha1(),
        "merchant_id": merchant if merchant else f"M{random.randint(1000, 9999)}",
        "amount": amount if amount is not None else round(random.uniform(1, 1000), 2),
        "currency": "USD",
        "mcc": "5411",
        "channel": "POS",
        "auth_result": auth,
        "location": location if location else "USA"
    }


def produce_test_scenarios(topic):
    while True:
        mode = random.choice(
            ["NORMAL", "HIGH_AMOUNT", "BLACKLIST", "VELOCITY", "CROSS_BORDER", "DECLINE"])

        if mode == "NORMAL":
            event = generate_base_payment()
            producer.send(topic, event)
            print(f"✅ NORMAL: {event['transaction_id']}")
            time.sleep(1)

        elif mode == "HIGH_AMOUNT":
            # Triggers Rule 1
            event = generate_base_payment(amount=12000.00)
            producer.send(topic, event)
            print(f"🚩 FRAUD (Rule 1: High Amount): {event['transaction_id']}")
            time.sleep(1)

        elif mode == "BLACKLIST":
            # Triggers Rule 4
            event = generate_base_payment(merchant="M9999")
            producer.send(topic, event)
            print(f"🚩 FRAUD (Rule 4: Blacklist): {event['transaction_id']}")
            time.sleep(1)

        elif mode == "VELOCITY":
            # Triggers Rule 2 (>5 txns/min)
            card = fake.sha1()
            print(f"⚡ Starting Velocity Burst for card: {card[:8]}")
            for _ in range(7):
                event = generate_base_payment(card_hash=card)
                producer.send(topic, event)
                time.sleep(0.5)  # Fast succession
            time.sleep(2)

        elif mode == "CROSS_BORDER":
            # Triggers Rule 3 (Multi-location)
            card = fake.sha1()
            countries = ["USA", "UK", "India"]
            print(f"🌍 Starting Cross-Border Burst for card: {card[:8]}")
            for country in countries:
                event = generate_base_payment(card_hash=card, location=country)
                producer.send(topic, event)
                time.sleep(1)

        elif mode == "DECLINE":
            # Triggers Rule 5 (>50% declines in last 10)
            card = fake.sha1()
            print(f"❌ Starting Decline Burst for card: {card[:8]}")
            for i in range(12):
                auth = "DECLINED" if i < 8 else "APPROVED"
                event = generate_base_payment(card_hash=card, auth=auth)
                producer.send(topic, event)
                time.sleep(0.5)


if __name__ == "__main__":
    topic_name = 'payments.raw'
    try:
        produce_test_scenarios(topic_name)
    except KeyboardInterrupt:
        print("Stopping producer.")
    finally:
        producer.close()
