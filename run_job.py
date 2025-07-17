import os
import psycopg2
import sys

# Pobierz URL bazy danych ze zmiennych środowiskowych Railway
DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    print("BŁĄD: Zmienna środowiskowa DATABASE_URL nie jest ustawiona.")
    sys.exit(1)

conn = None
try:
    # Połącz się z bazą danych
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Wykonaj funkcję
    print("Uruchamiam funkcję process_async_jobs()...")
    cur.execute("SELECT process_async_jobs();")

    print("Odświeżam dane zagregowane...")
    cur.execute("SELECT refresh_all_aggregated_data();")

    # Zatwierdź transakcję
    conn.commit()

    print("Funkcja wykonana pomyślnie.")

    # Zamknij połączenie
    cur.close()

except Exception as e:
    print(f"Wystąpił błąd: {e}")
    sys.exit(1) # Zakończ z kodem błędu, aby Railway wiedziało o problemie
finally:
    if conn is not None:
        conn.close()
