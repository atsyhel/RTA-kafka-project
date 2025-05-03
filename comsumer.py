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
last_report_minute = -1
global_stats = {
    'count': 0,
    'sum_brutto': 0.0,
    'sum_netto': 0.0,
    'ilosc_total': 0,
    'per_region_orders': defaultdict(int),
    'per_region_brutto': defaultdict(float),
    'per_region_netto': defaultdict(float),
    'per_brand_qty': defaultdict(int),
    'start_time': datetime.now() 
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
                
                try:
                    brutto = float(order.get('wartosc_brutto', 0))
                    netto = float(order.get('cena_netto', 0))
                    ilosc = int(order.get('ilosc', 1))
    
                    brand = order.get('marka', 'unknown')
                    region = order.get('region', 'unknown')
                
                    global_stats['count'] += 1
                    global_stats['sum_brutto'] += brutto
                    global_stats['sum_netto'] += netto * ilosc
                    global_stats['ilosc_total'] += ilosc
                
                    global_stats['per_region_orders'][region] += 1
                    global_stats['per_region_brutto'][region] += brutto
                    global_stats['per_region_netto'][region] += netto * ilosc
                    global_stats['per_brand_qty'][brand] += ilosc
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
                    
        # Co 1 minutę — pokaż KPI
        now_minute = datetime.now().minute
        if last_report_minute != now_minute:
            last_report_minute = now_minute
            print(f"\n=== [KPI AGREGOWANE] Od {global_stats['start_time']} do {datetime.now()} ===")
            print(f" Wszystkich zamówień: {global_stats['count']}")
            print(f" Średnia wartość zamówienia brutto {global_stats['sum_brutto'] / global_stats['count']:.2f} zł")
            print(f" Średnia wartość zamówienia netto: {global_stats['sum_netto'] / global_stats['count']:.2f} zł")
            print(f" Całkowita wartość sprzedaży brutto: {global_stats['sum_brutto']:.2f} zł")
            print(f" Całkowita wartość sprzedaży netto: {global_stats['sum_netto']:.2f} zł")
            print(f" Całkowita liczba sprzedanych towarów: {global_stats['ilosc_total']}")

            print("\n[KPI] Liczba zamówień per region:")
            for region, val in global_stats['per_region_orders'].items():
                print(f"  {region}: {val} zamówień")

            print("[KPI] Suma sprzedaży brutto per region:")
            for region, val in global_stats['per_region_brutto'].items():
                print(f"  {region}: {val:.2f} zł")

            print("[KPI] Suma sprzedaży netto per region:")
            for region, val in global_stats['per_region_netto'].items():
                print(f"  {region}: {val:.2f} zł")

            print("[KPI] Liczba sprzedanych towarów wg marek:")
            for marka, val in global_stats['per_brand_qty'].items():
                print(f"  {marka}: {val} szt.")

            top_brand = max(global_stats['per_brand_qty'], key=global_stats['per_brand_qty'].get, default='brak')
            print(f"[KPI] Najpopularniejsza marka: {top_brand} ({global_stats['per_brand_qty'][top_brand]} szt.)")
            print("-" * 60)

    consumer.close()
    print("[INFO] Konsument zamknięty.")
