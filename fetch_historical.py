import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

# =========================
# KONFIGURASI
# =========================
BASE_CURRENCY = "USD"
TARGET_CURRENCIES = ["EUR", "JPY", "IDR"]
DAYS_BACK = 90                               # rentang hari ke belakang (disarankan: min 3, max ~90)
HISTORY_OUTPUT_DIR = "data/output/history"   # folder yang sama dengan spark_streaming_job.py


def fetch_historical_range(start_date, end_date):
    """
    Panggil endpoint time-series Frankfurter:
    https://api.frankfurter.app/{start}..{end}?from=USD&to=EUR,JPY,IDR
    """
    url = f"https://api.frankfurter.app/{start_date}..{end_date}"
    params = {
        "from": BASE_CURRENCY,
        "to": ",".join(TARGET_CURRENCIES),
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def to_long_format(data):
    """
    Ubah response Frankfurter (per tanggal -> dict currency:rate)
    menjadi long format: base, currency, rate, event_date, ingestion_time.
    Skema ini dibuat SAMA PERSIS dengan output spark_streaming_job.py
    supaya bisa langsung digabung/dibaca bareng oleh train_model.py & app.py.
    """
    rows = []
    base = data.get("base", BASE_CURRENCY)
    rates_by_date = data.get("rates", {})

    for date_str, currency_rates in rates_by_date.items():
        # pakai jam 12:00 UTC sebagai waktu representatif tiap hari
        ingestion_time = datetime.strptime(date_str, "%Y-%m-%d").replace(
            hour=12, tzinfo=timezone.utc
        )
        for currency, rate in currency_rates.items():
            rows.append({
                "base": base,
                "currency": currency,
                "rate": float(rate),
                "event_date": date_str,
                "ingestion_time": ingestion_time,
            })

    return pd.DataFrame(rows)


def main():
    os.makedirs(HISTORY_OUTPUT_DIR, exist_ok=True)

    if DAYS_BACK < 3:
        print(f"[WARNING] DAYS_BACK={DAYS_BACK} terlalu kecil, disarankan minimal 3 hari "
              f"supaya cukup untuk fitur lag (rate t-1, t-2).")
    elif DAYS_BACK > 90:
        print(f"[WARNING] DAYS_BACK={DAYS_BACK} cukup besar (>90 hari, ~3 bulan). "
              f"Tetap bisa jalan, hanya butuh waktu fetch sedikit lebih lama.")

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=DAYS_BACK)

    print(f"[INFO] Mengambil data historis dari {start_date} sampai {end_date}...")
    data = fetch_historical_range(start_date.isoformat(), end_date.isoformat())

    df = to_long_format(data)
    print(f"[INFO] Diperoleh {len(df)} baris data ({df['event_date'].nunique()} hari unik).")

    if df.empty:
        print("[ERROR] Tidak ada data yang diperoleh. Cek koneksi internet / parameter tanggal.")
        return

    # Tulis sebagai file parquet baru di folder history (menyatu dengan file
    # yang sudah ditulis spark_streaming_job.py, karena Spark/pandas akan
    # membaca SEMUA file parquet dalam folder itu sebagai satu tabel).
    output_path = os.path.join(HISTORY_OUTPUT_DIR, "historical_bootstrap.parquet")
    df.to_parquet(output_path, index=False)

    print(f"[DONE] Data historis disimpan di: {output_path}")
    print("[INFO] Sekarang kamu bisa langsung jalankan: python train_model.py")


if __name__ == "__main__":
    main()