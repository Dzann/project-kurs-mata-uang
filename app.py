import os
import time
from datetime import timedelta

import pandas as pd
import streamlit as st
import plotly.express as px

# =========================
# KONFIGURASI
# =========================
HISTORY_DIR = "data/output/history"
LATEST_DIR = "data/output/latest"
PREDICTIONS_DIR = "data/output/predictions"

DEFAULT_REFRESH_SECONDS = 15

# Nama lengkap tiap kode mata uang, biar lebih jelas di dropdown
CURRENCY_NAMES = {
    "USD": "Dolar Amerika Serikat",
    "EUR": "Euro",
    "JPY": "Yen Jepang",
    "IDR": "Rupiah Indonesia",
    "GBP": "Poundsterling Inggris",
    "AUD": "Dolar Australia",
    "SGD": "Dolar Singapura",
    "CNY": "Yuan Tiongkok",
    "MYR": "Ringgit Malaysia",
    "KRW": "Won Korea Selatan",
    "CHF": "Franc Swiss",
    "INR": "Rupee India",
    "THB": "Baht Thailand",
    "CAD": "Dolar Kanada",
    "HKD": "Dolar Hong Kong",
    "NZD": "Dolar Selandia Baru",
}

PREFERRED_DEFAULT_CURRENCY = "IDR"  # mata uang yang ditampilkan pertama kali

st.set_page_config(
    page_title="Dashboard Kurs Mata Uang",
    page_icon="💱",
    layout="wide",
)


# =========================
# HELPER - BACA DATA
# =========================
def safe_read_parquet(path):
    """Baca folder parquet, kembalikan DataFrame kosong kalau belum ada data."""
    if not os.path.exists(path) or len(os.listdir(path)) == 0:
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as e:
        st.warning(f"Gagal membaca {path}: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=10)
def load_data():
    latest_df = safe_read_parquet(LATEST_DIR)
    history_df = safe_read_parquet(HISTORY_DIR)
    predictions_df = safe_read_parquet(PREDICTIONS_DIR)

    for df in (latest_df, history_df, predictions_df):
        if not df.empty and "ingestion_time" in df.columns:
            df["ingestion_time"] = pd.to_datetime(df["ingestion_time"])

    return latest_df, history_df, predictions_df


latest_df, history_df, predictions_df = load_data()
available_currencies = sorted(history_df["currency"].unique()) if not history_df.empty else []


# =========================
# SIDEBAR - SEMUA KONTROL DI SINI
# =========================
with st.sidebar:
    st.title("💱 Pengaturan")

    selected_currency = None
    if available_currencies:
        # Urutkan supaya IDR (atau currency default lain) muncul PALING ATAS/PERTAMA
        ordered_currencies = sorted(
            available_currencies,
            key=lambda c: (c != PREFERRED_DEFAULT_CURRENCY, c)
        )
        selected_currency = st.selectbox(
            "Mata uang",
            ordered_currencies,
            format_func=lambda code: f"{code} — {CURRENCY_NAMES.get(code, 'Tidak diketahui')}",
        )
    else:
        st.caption("Belum ada data mata uang tersedia.")

    st.divider()

    st.subheader("Rentang waktu")
    range_option = st.radio(
        "Tampilkan data untuk:",
        ["24 jam terakhir", "7 hari terakhir", "30 hari terakhir", "Semua data"],
        index=3,
        label_visibility="collapsed",
    )

    st.divider()

    st.subheader("Refresh")
    auto_refresh = st.toggle("Auto-refresh", value=True)
    refresh_interval = st.slider(
        "Interval refresh (detik)", min_value=5, max_value=60,
        value=DEFAULT_REFRESH_SECONDS, step=5, disabled=not auto_refresh,
    )
    manual_refresh = st.button("🔄 Refresh sekarang", use_container_width=True)

    st.divider()

    show_raw_data = st.checkbox("Tampilkan data mentah")

    st.caption(
        "Data diambil berkala dari API kurs, diproses lewat Spark "
        "Structured Streaming."
    )


# =========================
# FILTER DATA SESUAI SIDEBAR
# =========================
def apply_time_filter(df, option):
    if df.empty or "ingestion_time" not in df.columns:
        return df
    now = df["ingestion_time"].max()
    if option == "24 jam terakhir":
        cutoff = now - timedelta(hours=24)
    elif option == "7 hari terakhir":
        cutoff = now - timedelta(days=7)
    elif option == "30 hari terakhir":
        cutoff = now - timedelta(days=30)
    else:
        return df
    return df[df["ingestion_time"] >= cutoff]


history_filtered = apply_time_filter(history_df, range_option)
predictions_filtered = apply_time_filter(predictions_df, range_option)


# =========================
# HALAMAN UTAMA
# =========================
st.title("Dashboard Monitoring & Prediksi Kurs Mata Uang")

if latest_df.empty and history_df.empty:
    st.warning(
        "Belum ada data masuk. Pastikan `producer.py` dan `spark_streaming_job.py` "
        "sudah dijalankan dan dibiarkan berjalan di terminal terpisah, atau jalankan "
        "`fetch_historical.py` untuk mengisi data historis awal."
    )
else:
    # ---- Ringkasan kurs terbaru (metric cards) ----
    if not latest_df.empty:
        cols = st.columns(min(len(latest_df), 6))
        for i, (_, row) in enumerate(latest_df.iterrows()):
            with cols[i % len(cols)]:
                currency_label = CURRENCY_NAMES.get(row['currency'], row['currency'])
                st.metric(
                    label=f"{row['base']} → {row['currency']} ({currency_label})",
                    value=f"{row['rate']:.4f}",
                )

    tab_tren, tab_prediksi = st.tabs(["📈 Tren Kurs", "🔮 Prediksi vs Aktual"])

    with tab_tren:
        if not history_filtered.empty and selected_currency:
            chart_df = (
                history_filtered[history_filtered["currency"] == selected_currency]
                .sort_values("ingestion_time")
            )
            if not chart_df.empty:
                fig = px.line(
                    chart_df,
                    x="ingestion_time",
                    y="rate",
                    title=f"{chart_df['base'].iloc[0]} → {selected_currency} ({range_option})",
                    markers=True,
                )
                fig.update_layout(xaxis_title="Waktu", yaxis_title="Rate", height=420)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Tidak ada data pada rentang waktu yang dipilih.")
        else:
            st.info("Data historis belum cukup untuk ditampilkan sebagai grafik.")

    with tab_prediksi:
        if not predictions_filtered.empty and selected_currency:
            pred_chart_df = (
                predictions_filtered[predictions_filtered["currency"] == selected_currency]
                .sort_values("ingestion_time")
            )
            if not pred_chart_df.empty:
                fig2 = px.line(
                    pred_chart_df,
                    x="ingestion_time",
                    y=["rate", "prediction"],
                    title=f"Prediksi vs Aktual - {selected_currency} ({range_option})",
                    markers=True,
                )
                fig2.update_layout(xaxis_title="Waktu", yaxis_title="Rate", height=420)
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info(f"Belum ada hasil prediksi untuk {selected_currency} pada rentang ini.")
        else:
            st.info(
                "Belum ada hasil prediksi. Jalankan `train_model.py` setelah data "
                "historis cukup banyak terkumpul."
            )

    if show_raw_data:
        st.divider()
        st.subheader("Data Mentah")
        col_a, col_b = st.columns(2)
        with col_a:
            st.caption("Latest snapshot")
            st.dataframe(latest_df, use_container_width=True)
        with col_b:
            st.caption("History (100 baris terakhir)")
            st.dataframe(
                history_df.sort_values("ingestion_time", ascending=False).head(100),
                use_container_width=True,
            )


# =========================
# AUTO-REFRESH SEDERHANA
# =========================
if manual_refresh:
    st.cache_data.clear()
    st.rerun()
elif auto_refresh:
    time.sleep(refresh_interval)
    st.cache_data.clear()
    st.rerun()