import os
import requests
import pandas as pd
from datetime import datetime, timezone

BASE_CURRENCY = "USD"
TARGET_CURRENCIES = ["EUR", "JPY", "IDR"]

HISTORY_DIR = "data/output/history"
LATEST_DIR = "data/output/latest"


def fetch_current_rates():
    url = "https://api.frankfurter.app/latest"
    params = {"from": BASE_CURRENCY, "to": ",".join(TARGET_CURRENCIES)}
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def build_row_dataframe(data, ingestion_time):
    rows = []
    base = data.get("base", BASE_CURRENCY)
    for currency, rate in data.get("rates", {}).items():
        rows.append({
            "base": base,
            "currency": currency,
            "rate": float(rate),
            "event_date": data.get("date"),
            "ingestion_time": ingestion_time,
        })
    return pd.DataFrame(rows)


def append_to_history(new_df):
    os.makedirs(HISTORY_DIR, exist_ok=True)
    filename = f"ci_update_{new_df['ingestion_time'].iloc[0].strftime('%Y%m%d_%H%M%S')}.parquet"
    filepath = os.path.join(HISTORY_DIR, filename)
    new_df.to_parquet(filepath, index=False)
    print(f"[INFO] Baris baru ditambahkan ke history: {filepath}")


def overwrite_latest(new_df):
    os.makedirs(LATEST_DIR, exist_ok=True)
    # Kosongkan dulu isi lama, lalu tulis snapshot baru
    for f in os.listdir(LATEST_DIR):
        os.remove(os.path.join(LATEST_DIR, f))
    filepath = os.path.join(LATEST_DIR, "latest.parquet")
    new_df.to_parquet(filepath, index=False)
    print(f"[INFO] Snapshot latest diperbarui: {filepath}")


def main():
    ingestion_time = datetime.now(timezone.utc)
    print(f"[INFO] Mengambil data kurs terbaru pada {ingestion_time.isoformat()}...")

    data = fetch_current_rates()
    new_df = build_row_dataframe(data, ingestion_time)

    if new_df.empty:
        print("[WARNING] Tidak ada data yang diperoleh dari API.")
        return

    append_to_history(new_df)
    overwrite_latest(new_df)
    print("[DONE] Update data selesai.")


if __name__ == "__main__":
    main()