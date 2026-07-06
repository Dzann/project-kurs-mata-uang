import os
import json
import time
import requests
from datetime import datetime, timezone

# =========================
# KONFIGURASI - EDIT DI SINI
# =========================
API_PROVIDER = "frankfurter"          # pilihan: "frankfurter" atau "twelvedata"
TWELVEDATA_API_KEY = "YOUR_API_KEY_HERE"  # isi kalau pakai twelvedata

BASE_CURRENCY = "USD"
TARGET_CURRENCIES = ["EUR", "GBP", "JPY", "IDR", "AUD"]

POLL_INTERVAL_SECONDS = 60            # jarak antar polling (detik)
OUTPUT_DIR = "data/stream_input"      # folder yang akan dibaca Spark Streaming


# =========================
# FUNGSI FETCH PER PROVIDER
# =========================
def fetch_frankfurter():
    """
    Ambil data dari Frankfurter API (gratis, tanpa key, tanpa rate limit resmi).
    Catatan: data ini update harian (ECB reference rate), bukan per-detik.
    """
    url = "https://api.frankfurter.app/latest"
    params = {
        "from": BASE_CURRENCY,
        "to": ",".join(TARGET_CURRENCIES),
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    return {
        "source": "frankfurter",
        "base": data.get("base", BASE_CURRENCY),
        "date": data.get("date"),
        "rates": data.get("rates", {}),
    }


def fetch_twelvedata():
    """
    Ambil data dari Twelvedata API (perlu API key gratis).
    Endpoint exchange_rate hanya bisa 1 pair per request,
    jadi kita loop untuk setiap target currency.
    """
    rates = {}
    for target in TARGET_CURRENCIES:
        url = "https://api.twelvedata.com/exchange_rate"
        params = {
            "symbol": f"{BASE_CURRENCY}/{target}",
            "apikey": TWELVEDATA_API_KEY,
        }
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if "rate" in data:
            rates[target] = float(data["rate"])
        else:
            # Twelvedata mengembalikan pesan error dalam field "message"/"code"
            print(f"[WARNING] Gagal ambil {BASE_CURRENCY}/{target}: {data}")

    return {
        "source": "twelvedata",
        "base": BASE_CURRENCY,
        "date": datetime.now(timezone.utc).isoformat(),
        "rates": rates,
    }


def fetch_rates():
    """Router: panggil fungsi fetch sesuai API_PROVIDER yang dipilih."""
    if API_PROVIDER == "frankfurter":
        return fetch_frankfurter()
    elif API_PROVIDER == "twelvedata":
        return fetch_twelvedata()
    else:
        raise ValueError(f"API_PROVIDER tidak dikenal: {API_PROVIDER}")


# =========================
# MAIN LOOP
# =========================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[INFO] Producer dimulai. Provider: {API_PROVIDER}")
    print(f"[INFO] Menulis data ke folder: {OUTPUT_DIR}")
    print(f"[INFO] Interval polling: {POLL_INTERVAL_SECONDS} detik")
    print("[INFO] Tekan CTRL+C untuk berhenti.\n")

    while True:
        try:
            payload = fetch_rates()
            timestamp = datetime.now(timezone.utc)
            payload["ingestion_timestamp"] = timestamp.isoformat()

            filename = f"rates_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.json"
            filepath = os.path.join(OUTPUT_DIR, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)

            print(f"[OK] {timestamp.isoformat()} -> {filename} | rates: {payload['rates']}")

        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Gagal fetch data: {e}")
        except Exception as e:
            print(f"[ERROR] Terjadi kesalahan tak terduga: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()