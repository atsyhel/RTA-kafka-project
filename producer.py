import time
import signal
import sys
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from data_generator import generate_order

# —————————————————————————————————————————————————————————————————————
# Konfiguracja Kafka Producer
# —————————————————————————————————————————————————————————————————————
producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    acks='all',              # potwierdzenie od wszystkich brokerów
    retries=5,               # liczba prób ponowienia wysyłki
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def shutdown(signum, frame):
    """Graceful shutdown przy SIGINT/SIGTERM"""
    print("\n[INFO] Zamykanie producenta…")
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# —————————————————————————————————————————————————————————————————————
# Główna pętla wysyłająca zamówienia
# —————————————————————————————————————————————————————————————————————
if __name__ == '__main__':
    order_id = 1
    topic = 'zamowienia_elektronika'

    while True:
        # Generowanie kolejnego zamówienia
        order = generate_order(order_id)
        print(f"[SEND] {order}")

        # Wysłanie do Kafka z kluczem store_id dla partycjonowania
        producer.send(
            topic,
            key=order['store_id'],
            value=order
        )

        # Przygotowanie kolejnego identyfikatora
        order_id += 1

        # Regulacja tempa generacji: częściej w godzinach szczytu
        hour = datetime.now().hour
        if 9 <= hour < 12 or 17 <= hour < 20:
            time.sleep(random.uniform(0.5, 1.5))
        else:
            time.sleep(random.uniform(2.0, 5.0))