# Dashboard Monitoring & Prediksi Kurs Mata Uang (PySpark Streaming)

Project ini mensimulasikan pipeline **streaming data kurs mata uang** memakai
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

## Isi File

| File | Fungsi |
|---|---|
| `producer.py` | Polling API kurs (Frankfurter/Twelvedata), tulis JSON ke `data/stream_input/` |
| `spark_streaming_job.py` | Spark Structured Streaming: baca JSON stream, hasilkan history + latest snapshot |
| `train_model.py` | Batch training model regresi (prediksi rate berikutnya dari data historis) |
| `app.py` | Dashboard Streamlit: metric kurs terbaru, grafik tren, grafik prediksi vs aktual |
| `requirements.txt` | Daftar dependency Python |

## Setup Awal (Windows) - PENTING, lakukan sebelum langkah lain

### A. Install Python 3.11 dan JDK 17
- Python 3.11: https://www.python.org/downloads/release/python-3119/ (centang "Add to PATH")
- Eclipse Temurin JDK 17: https://adoptium.net
- Set `JAVA_HOME` di Environment Variables mengarah ke folder install JDK,
  lalu tambahkan `%JAVA_HOME%\bin` ke `Path`

### B. Matikan Windows App Execution Alias untuk Python
Tanpa langkah ini, Spark akan gagal memanggil Python worker dengan error
`Python was not found... Microsoft Store`.
1. Buka **Settings > Apps > Advanced app settings > App execution aliases**
2. Matikan toggle untuk `python.exe` dan `python3.exe`

### C. Buat virtual environment & install dependency
```bash
py -3.11 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```
Kalau proses ini gagal di tengah jalan (misal error build pandas), jalankan
lagi `pip install -r requirements.txt` setelah memastikan versi Python 3.11
terpakai di venv.

### D. Pilih interpreter yang benar di VSCode
`Ctrl+Shift+P` → **Python: Select Interpreter** → pilih yang path-nya
`.\venv\Scripts\python.exe`

### E. Tes instalasi Spark
```bash
python test_spark.py
```
Harus muncul tabel kecil tanpa error. Kalau masih ada
`Python worker failed to connect back`, cek lagi langkah B.

## Cara Menjalankan (Setelah Setup Awal Selesai)

### 1. Setup awal (sekali saja)
```bash
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

### 2. Jalankan pipeline (butuh 3 terminal berbeda, semua di folder project yang sama)

**Terminal 1 — Producer (ambil data dari API)**
```bash
venv\Scripts\activate
python producer.py
```

**Terminal 2 — Spark Streaming Job**
```bash
venv\Scripts\activate
python spark_streaming_job.py
```

Biarkan kedua terminal ini jalan selama beberapa saat (semakin lama, semakin
banyak data historis yang terkumpul untuk training model & grafik tren).

**Setelah data terkumpul cukup banyak (misal setelah 15-30 menit / puluhan data poin):**
```bash
venv\Scripts\activate
python train_model.py
```
Jalankan ini kapan saja untuk retrain model dengan data terbaru.

**Terminal 3 — Dashboard**
```bash
venv\Scripts\activate
streamlit run app.py
```
Buka browser ke `http://localhost:8501`

### 3. Deploy (opsional, untuk demo publik)
Setelah semuanya jalan lancar di lokal, `app.py` bisa di-deploy ke
**Streamlit Community Cloud** (gratis) dengan meng-push project ke GitHub
lalu menghubungkannya di https://streamlit.io/cloud.
Catatan: untuk deploy publik, bagian producer + Spark streaming job perlu
dijalankan di tempat lain (server/VM) karena Streamlit Cloud hanya meng-host
aplikasi Streamlit-nya saja, bukan proses background lain.

## Deployment Publik (Opsi B — Semi-Realtime via GitHub Actions)

Supaya dashboard yang di-deploy tetap "hidup" (data terus diperbarui) tanpa
perlu menjalankan server 24/7, dipakai pendekatan: GitHub Actions
menjalankan `ci_update_data.py` secara terjadwal, commit hasilnya ke repo,
lalu Streamlit Cloud otomatis re-deploy dengan data terbaru.

### 1. Push project ke GitHub
```bash
git init
git add .
git commit -m "Initial commit - project kurs mata uang"
git branch -M main
git remote add origin https://github.com/<username>/<nama-repo>.git
git push -u origin main
```
(Buat repo baru dulu di github.com kalau belum ada — boleh public atau private)

### 2. Pastikan struktur ini ikut ter-push
```
.github/workflows/update_data.yml   <- workflow terjadwal
ci_update_data.py                   <- script ringan (pandas, tanpa Spark)
data/output/history/                <- data historis (hasil fetch_historical.py / streaming lokal)
data/output/latest/
app.py
requirements.txt
```

### 3. Cek jadwal di `update_data.yml`
Defaultnya jalan **tiap 6 jam** (`cron: "0 */6 * * *"`, waktu UTC). Sesuaikan
kalau mau lebih sering/jarang. Catatan: GitHub Actions free tier untuk repo
public tidak terbatas menit; untuk repo private ada kuota bulanan gratis
(cukup untuk kebutuhan tugas).

### 4. Tes manual dulu sebelum menunggu jadwal
Di GitHub: tab **Actions** → pilih workflow **"Update Currency Data"** →
klik **"Run workflow"** → jalankan manual. Cek apakah muncul commit baru
otomatis di repo setelah job selesai (biasanya beberapa menit).

### 5. Deploy ke Streamlit Community Cloud
1. Buka https://share.streamlit.io, login dengan akun GitHub
2. Klik **"New app"** → pilih repo project ini
3. Main file path: `app.py`
4. Klik **Deploy**
5. Tunggu build selesai, dapat link publik untuk dibagikan ke dosen

Setiap kali GitHub Actions melakukan commit baru (sesuai jadwal), Streamlit
Cloud akan otomatis mendeteksi perubahan di repo dan re-deploy dengan data
terbaru — dashboard jadi ter-update berkala tanpa perlu server nyala terus.

### Catatan penting untuk laporan/PPT
- `ci_update_data.py` sengaja TIDAK memakai Spark (hanya pandas) karena
  GitHub Actions runner bersifat sementara, tidak cocok untuk proses
  long-running seperti Spark Structured Streaming.
- Bukti konsep **streaming** yang sebenarnya tetap di `spark_streaming_job.py`,
  dijalankan lokal (sertakan screenshot/video singkat sebagai bukti di laporan).
- Dashboard yang di-deploy publik ini statusnya **semi-realtime** (update
  berkala tiap beberapa jam via cron), bukan streaming per detik — jelaskan
  perbedaan ini secara eksplisit supaya penilaian dosen tidak salah paham.



## Catatan Umum untuk Laporan/PPT

- Jelaskan bahwa "streaming" di sini disimulasikan lewat polling berkala +
  file source di Spark Structured Streaming, karena API kurs gratis yang
  dipakai tidak menyediakan data tick-by-tick real-time.
- Model prediksi memakai pendekatan lag features (rate t-1, t-2) dengan
  Linear Regression sebagai baseline; bisa dikembangkan lebih lanjut ke
  model time-series yang lebih canggih (ARIMA, LSTM, dst.) sebagai bagian
  dari saran pengembangan di kesimpulan.