import json
import csv
import signal
import sys
from datetime import datetime
from kafka import KafkaConsumer
from collections import defaultdict
# —————————————————————————————————————————————————————————————————————
# Graceful shutdown
# —————————————————————————————————————————————————————————————————————
running = True

def shutdown(signum, frame):
    """Zamyka konsumenta przy SIGINT/SIGTERM"""
    global running
    print("\n[INFO] Zamykanie konsumenta…")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# —————————————————————————————————————————————————————————————————————
# Konfiguracja KafkaConsumer
# —————————————————————————————————————————————————————————————————————
consumer = KafkaConsumer(
    'zamowienia_elektronika',
    bootstrap_servers='broker:9092',
    group_id='consumer_group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    key_deserializer=lambda x: x.decode('utf-8') if x else None,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# —————————————————————————————————————————————————————————————————————
# Słownik do obliczenia KPI dla każdej minuty
# —————————————————————————————————————————————————————————————————————
stats = {
    'count': 0,
    'sum_brutto': 0.0,
    'sum_netto': 0.0,
    'ilosc_total': 0,
    'per_region_orders': defaultdict(int),
    'per_region_brutto': defaultdict(float),
    'per_region_netto': defaultdict(float),
    'per_brand_qty': defaultdict(int),
    'start_minute': datetime.now().strftime('%Y-%m-%d %H:%M')
}

# —————————————————————————————————————————————————————————————————————
# Przygotowanie CSV (rotacja dzienna) oraz KPI
# —————————————————————————————————————————————————————————————————————
today = datetime.now().strftime("%Y-%m-%d")
file_path = f"orders_{today}.csv"
fieldnames = [
    'id', 'timestamp', 'store_id', 'region', 'customer_id',
    'payment_method', 'nazwa_produktu', 'marka',
    'kategoria_produktu', 'ilosc', 'cena_netto', 'wartosc_brutto'
]

with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    print(f"[INFO] Zapisuję zamówienia do {file_path}")

    while running:
        # Pobranie partii wiadomości
        records = consumer.poll(timeout_ms=1000, max_records=20)
        for tp, msgs in records.items():
            for msg in msgs:
                order = msg.value

                # Podsumowanie KPI dla każdej minuty
                now_minute = datetime.now().strftime('%Y-%m-%d %H:%M')
                if now_minute != stats['start_minute'] and stats['count'] > 0:
                    avg = stats['sum_brutto'] / stats['count']
                    avg_netto = stats['sum_netto'] / stats['count']
                    print(f"\n[KPI] {stats['start_minute']}")
                    print(f"  Zamówień: {stats['count']}")
                    print(f"  Średnia wartość zamówienia brutto: {avg:.2f} zł")
                    print(f"  Średnia wartość zamówienia netto: {avg_netto:.2f} zł")
                    print(f"  Całkowita wartość sprzedaży brutto: {stats['sum_brutto']:.2f} zł")
                    print(f"  Całkowita wartość sprzedaży netto: {stats['sum_netto']:.2f} zł")
                    print(f"  Całkowita liczba sprzedanych towarów: {stats['ilosc_total']} szt.\n")  
                    print("[KPI] Liczba zamówień per region:")
                    for region, val in stats['per_region_orders'].items():
                        print(f"  {region}: {val} zamówień")

                    print("[KPI] Suma sprzedaży brutto per region:")
                    for region, val in stats['per_region_brutto'].items():
                        print(f"  {region}: {val:.2f} zł")

                    print("[KPI] Suma sprzedaży netto per region:")
                    for region, val in stats['per_region_netto'].items():
                        print(f"  {region}: {val:.2f} zł")

                    print("[KPI] Liczba sprzedanych towarów wg marek:")
                    for region, val in stats['per_brand_qty'].items():
                        print(f"  {region}: {val} szt.")

                    top_brand = max(stats['per_brand_qty'], key=stats['per_brand_qty'].get, default='brak')
                    print(f"[KPI] Najpopularniejsza marka: {top_brand} ({stats['per_brand_qty'][top_brand]} sprzedanych szt.)")
                    
                    # Wyświetlenie alertów i informacji o liczbie zamówień - różne progi w zależnosci od godzin szczytu
                    hour = datetime.now().hour
                    if 9 <= hour < 12 or 17 <= hour < 20:
                        low_threshold = 50
                        high_threshold = 100
                    else:
                        low_threshold = 10
                        high_threshold = 30
                        
                    if stats['count'] < low_threshold:
                        print(f"[ALERT] Obniżona aktywność zakupowa: ({stats['count']}) w tej minucie!")
                    elif stats['count'] > high_threshold:
                        print(f"[ALERT] Ponadprzeciętna liczba zamówień: ({stats['count']}) w tej minucie!")
                    else:
                        print(f"[INFO] Liczba zamówień w normie: {stats['count']}")
                    print("-" * 60)
                    
                    # Reset statystyk dla nowej minuty
                    stats = {
                        'count': 0,
                        'sum_brutto': 0.0,
                        'sum_netto': 0.0,
                        'ilosc_total': 0,
                        'per_region_orders': defaultdict(int),
                        'per_region_brutto': defaultdict(float),
                        'per_region_netto': defaultdict(float),
                        'per_brand_qty': defaultdict(int),
                        'start_minute': now_minute
                    }
                
                # Aktualizacja statystyk
                try:
                    brutto = float(order.get('wartosc_brutto', 0))
                    netto = float(order.get('cena_netto', 0))
                    ilosc = int(order.get('ilosc', 1))
    
                    brand = order.get('marka', 'unknown')
                    region = order.get('region', 'unknown')
                
                    stats['count'] += 1
                    stats['sum_brutto'] += brutto
                    stats['sum_netto'] += netto * ilosc
                    stats['ilosc_total'] += ilosc
                
                    stats['per_region_orders'][region] += 1
                    stats['per_region_brutto'][region] += brutto
                    stats['per_region_netto'][region] += netto * ilosc
                
                    stats['per_brand_qty'][brand] += ilosc
                except (ValueError, TypeError):
                    print(f"[WARN] Nieprawidłowa wartość w zamówieniu: {order}")
                
                # Wybór potrzebnych pól
                row = {fn: order.get(fn) for fn in fieldnames}
                try:
                    writer.writerow(row)
                    csvfile.flush()
                    print(f"[SAVED] order_id={row['id']}")
                except Exception as e:
                    print(f"[ERROR] podczas zapisu zamówienia {row.get('id')}: {e}")

    consumer.close()
    print("[INFO] Konsument zamknięty.")
