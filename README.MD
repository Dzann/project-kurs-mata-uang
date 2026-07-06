# Dashboard Monitoring & Prediksi Kurs Mata Uang (PySpark Streaming)

Project ini membangun pipeline **streaming data kurs mata uang** memakai
PySpark Structured Streaming, dengan model ML sederhana untuk prediksi kurs,
dan dashboard Streamlit sebagai output akhir.

## Latar Belakang Masalah

Fluktuasi kurs mata uang terjadi terus-menerus dan sulit dipantau secara
manual, terutama bagi individu/pelaku usaha kecil (importir, freelancer yang
dibayar dalam mata uang asing, traveler) yang perlu memutuskan kapan waktu
yang tepat untuk menukar uang. Project ini membangun sistem yang:
1. Mengambil data kurs secara berkala dari API eksternal (mensimulasikan stream)
2. Memproses data tersebut secara streaming dengan PySpark
3. Melatih model prediksi sederhana dari data historis yang terkumpul
4. Menyajikan semuanya dalam dashboard yang mudah dipantau

## Arsitektur

```
[API Kurs] --> producer.py --> data/stream_input/*.json
                                        |
                                        v
                          spark_streaming_job.py (Spark Structured Streaming)
                                        |
                     +------------------+------------------+
                     v                                      v
          data/output/history/                   data/output/latest/
          (semua data historis)                   (snapshot terbaru)
                     |
                     v
              train_model.py (batch, dijalankan berkala)
                     |
                     v
          data/output/predictions/
                     |
                     v
                  app.py (Streamlit) <-- baca semua folder output di atas
```

`fetch_historical.py` dan `ci_update_data.py` adalah jalur tambahan untuk
mengisi/menyegarkan `data/output/history` dan `data/output/latest` tanpa
harus menunggu streaming lokal berjalan lama (lihat penjelasan di bagian
masing-masing di bawah).

## Isi File

| File | Fungsi |
|---|---|
| `producer.py` | Polling API kurs (Frankfurter/Twelvedata), tulis JSON ke `data/stream_input/` |
| `spark_streaming_job.py` | Spark Structured Streaming: baca JSON stream, hasilkan history + latest snapshot |
| `train_model.py` | Batch training model regresi (prediksi rate berikutnya dari data historis) |
| `fetch_historical.py` | Ambil data historis asli (bukan simulasi) dari Frankfurter untuk mem-bootstrap `data/output/history` supaya training model punya variasi data yang cukup |
| `ci_update_data.py` | Script ringan (pandas, tanpa Spark) yang dijalankan GitHub Actions secara terjadwal untuk menyegarkan data di deployment publik |
| `app.py` | Dashboard Streamlit: metric kurs terbaru, grafik tren, grafik prediksi vs aktual |
| `test_spark.py` | Script kecil untuk memverifikasi instalasi PySpark berjalan benar |
| `requirements.txt` | Daftar dependency Python |
| `.github/workflows/update_data.yml` | Workflow GitHub Actions untuk menjalankan `ci_update_data.py` terjadwal |
| `.gitignore` | Mengecualikan `venv/`, checkpoint, dan file sementara dari git |

---

## Setup Awal (Windows) — lakukan sekali di awal

### A. Install Python 3.11 dan JDK 17
- Python 3.11: https://www.python.org/downloads/release/python-3119/
  (saat instalasi, centang **"Add python.exe to PATH"**, dan pastikan opsi
  **"py launcher"** ikut terinstall)
- Eclipse Temurin JDK 17: https://adoptium.net
- Set `JAVA_HOME` di Environment Variables mengarah ke folder install JDK,
  lalu tambahkan `%JAVA_HOME%\bin` ke `Path`

### B. Matikan Windows App Execution Alias untuk Python
Tanpa langkah ini, Spark akan gagal memanggil Python worker dengan error
`Python was not found... Microsoft Store`.
1. Buka **Settings > Apps > Advanced app settings > App execution aliases**
2. Matikan toggle untuk `python.exe` dan `python3.exe`

### C. Install winutils.exe (dibutuhkan Spark Structured Streaming di Windows)
1. Download `winutils.exe` dan `hadoop.dll` dari https://github.com/cdarlint/winutils
   (folder `hadoop-3.3.6/bin`)
2. Taruh keduanya di `C:\hadoop\bin\`
3. Copy juga `hadoop.dll` ke `C:\Windows\System32\`

(Script `spark_streaming_job.py` dan `train_model.py` sudah punya fallback
otomatis untuk `HADOOP_HOME` mengarah ke `C:\hadoop`, jadi tidak perlu set
Environment Variable manual untuk ini — cukup pastikan filenya ada di path itu.)

### D. Buat virtual environment & install dependency
```powershell
py -3.11 -m venv venv
venv\Scripts\activate
python --version
```
Pastikan output-nya **Python 3.11.x**, baru lanjut:
```powershell
pip install --no-cache-dir -r requirements.txt
```
Flag `--no-cache-dir` penting untuk menghindari pip memakai wheel cache
yang salah versi Python.

### E. Pilih interpreter yang benar di VSCode
`Ctrl+Shift+P` → **Python: Select Interpreter** → pilih yang path-nya
`.\venv\Scripts\python.exe`

### F. Verifikasi instalasi
```powershell
python -c "import numpy; import pandas; import pyarrow; import streamlit; import plotly; import sklearn; print('Semua OK')"
python test_spark.py
```
`test_spark.py` harus menampilkan tabel kecil tanpa error fatal (warning
soal `winutils`/`ShutdownHookManager` saat proses selesai itu normal dan
aman diabaikan).

---

## Cara Menjalankan (Lokal)

Butuh beberapa terminal terpisah, semua dengan venv aktif (`venv\Scripts\activate`)
di folder project yang sama.

**Langkah 1 — Bootstrap data historis (sekali saja, supaya training model punya variasi data)**
```powershell
python fetch_historical.py
```

**Terminal 1 — Producer (biarkan tetap berjalan)**
```powershell
python producer.py
```

**Terminal 2 — Spark Streaming Job (biarkan tetap berjalan, bersamaan dengan Terminal 1)**
```powershell
python spark_streaming_job.py
```

**Terminal 3 — Training Model** (jalankan setelah data cukup, bisa diulang kapan saja untuk retrain)
```powershell
python train_model.py
```

**Terminal 4 — Dashboard**
```powershell
streamlit run app.py
```
Buka browser ke `http://localhost:8501`


## Catatan untuk Laporan/PPT

- "Streaming" pada project ini dibuktikan lewat `spark_streaming_job.py`
  yang benar-benar memakai Spark Structured Streaming (`readStream`,
  micro-batch, checkpointing) — sertakan screenshot/video singkat terminal
  yang menunjukkan batch bertambah (`[Batch 0]`, `[Batch 1]`, dst.) sebagai
  bukti di laporan.
- Data sumber (Frankfurter, gratis) update kurs 1x/hari, sehingga sesi
  streaming singkat akan menghasilkan nilai yang identik antar polling.
  `fetch_historical.py` dipakai untuk bootstrap data historis asli supaya
  training model punya variasi nilai yang bermakna.
- Dashboard yang di-deploy publik (Opsi B) berstatus **semi-realtime**
  (update berkala tiap beberapa jam via GitHub Actions cron), bukan
  streaming per detik — jelaskan perbedaan ini secara eksplisit di laporan
  supaya penilaian dosen tidak salah paham antara "arsitektur streaming
  yang dibangun" vs "cara data disegarkan di versi yang di-deploy".
- Model prediksi memakai pendekatan lag features (rate t-1, t-2) dengan
  Linear Regression sebagai baseline; bisa disebut sebagai saran
  pengembangan lanjutan ke model time-series yang lebih canggih
  (ARIMA, LSTM, dst.) di bagian kesimpulan.
