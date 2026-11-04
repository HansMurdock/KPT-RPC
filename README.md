# KPT-RPC: Sistem Antrian Pengolahan Citra Terdistribusi Berbasis gRPC

Proyek Mini Mata Kuliah **Komputasi Paralel dan Terdistribusi (KPT)**.  
**Program Studi Informatika, Fakultas Teknik, Universitas Siliwangi (2026).**  
**Studi Kasus 5**: *Antrian Pengolahan Gambar (Client mengunggah gambar per chunk, Server mengubahnya menjadi grayscale secara asinkron)*.

---

## 👥 Tim Pengembang & Pembagian Tugas

| No | Nama | Peran | Tanggung Jawab Utama |
| :---: | :--- | :--- | :--- |
| 1 | **Zaki** | Backend & Processing Engineer | • Implementasi `server/server.py` dan `server/worker.py`<br>• Manajemen antrean thread-safe (`queue.Queue`) & worker pool<br>• Konversi citra ke Grayscale (`Pillow` mode `'L'`)<br>• Penanganan *concurrency* & locking status (`threading.Lock()`) |
| 2 | **Rafa** | Interface & Client Engineer | • Desain kontrak antarmuka `protos/image_service.proto` (6 RPCs & multi-filter `ProcessingType`)<br>• Implementasi `client/client.py` & Client Interceptor `client/interceptor.py` (AOP Pattern)<br>• CLI Interaktif & visual (`Rich` terminal UI, batch upload, live progress bar streaming)<br>• Pengunggahan & pengunduhan citra per chunk 64 KB (Streaming RPC)<br>• Toleransi kegagalan: Timeout, Retry (Exponential Backoff), & Deduplikasi UUID |
| 3 | **Khansa Rasendriya Aji (Hans)** *(Lead)* | Benchmark, DevOps & Reporting Lead | • Setup repositori Git & arsitektur proyek<br>• Kontainerisasi multi-mesin (`docker/docker-compose.yml`)<br>• Pengujian beban kinerja (`benchmark/benchmark.py` untuk 1, 5, 20 client)<br>• Visualisasi grafik latensi & throughput (`matplotlib`)<br>• Koordinasi Laporan Akhir & Video Demo |

---

## 🏛️ Arsitektur Sistem

Sistem ini memisahkan proses penerimaan unggahan gambar dari proses konversi citra menggunakan antrean asinkron thread-safe:

![Diagram Arsitektur Sistem](docs/diagram_arsitektur.png)

### Alur Kerja Layanan (4 Metode RPC):
1. **`UploadGambar` (Client Streaming RPC)**: Client mengirimkan potongan byte gambar berukuran 64 KB per chunk bersama `request_id` (UUIDv4). Server menulis stream ke file sementara, memvalidasi metadata, memindahkan ke storage, dan memasukkan pekerjaan ke dalam `queue.Queue`, lalu langsung mengembalikan `STATUS_QUEUED`.
2. **`CekStatus` (Unary RPC / Async Future)**: Client mengecek status terkini berdasarkan `job_id` secara non-blocking menggunakan `.future()`.
3. **`PantauStatus` (Server Streaming RPC)**: Server mengirimkan aliran update progres secara real-time (`10% -> 35% -> 70% -> 100%`) tanpa client perlu melakukan polling berulang.
4. **`UnduhGambar` (Server Streaming RPC)**: Setelah konversi selesai, client mengunduh citra hasil grayscale per potongan 64 KB hingga tuntas.

---

## 🎨 Hasil Konversi Citra

Berikut perbandingan visual citra uji berwarna (RGB) yang diunggah oleh client dengan citra hasil konversi grayscale yang diproses oleh worker server:

![Perbandingan Citra Input RGB vs Output Grayscale](benchmark/results/perbandingan_grayscale.png)

---

## 📁 Struktur Direktori Proyek

```text
KPT-RPC/
├── protos/
│   ├── image_service.proto       # Kontrak IDL Protocol Buffers (6 RPCs & ProcessingType)
│   ├── image_service_pb2.py      # Stub kelas pesan Protobuf
│   └── image_service_pb2_grpc.py # Stub client & servicer gRPC
├── server/
│   ├── server.py                 # Implementasi server gRPC & Servicer
│   ├── worker.py                 # ImageJobManager, queue.Queue & Worker Pool
│   └── storage/                  # Penyimpanan sementara file citra
│       ├── input/                # Citra asli hasil upload
│       └── output/               # Citra hasil konversi grayscale
├── client/
│   ├── client.py                 # Client CLI Interaktif (Rich TUI, Upload, Status, Monitor, Download, Batch)
│   ├── interceptor.py            # gRPC Client Interceptor (AOP: Logging & Latency Tracker)
│   ├── samples/                  # Berkas citra sampel RGB untuk pengujian
│   └── downloads/               # Direktori hasil unduhan citra grayscale
├── benchmark/
│   ├── benchmark.py              # Skrip pengujian beban (1, 5, 20 client) & plot
│   └── results/                  # Grafik PNG & tabel CSV metrik evaluasi
├── web/
│   ├── index.html                # Dashboard simulator RPC bertema gelap
│   ├── styles.css                # Layout responsif dan komponen visual
│   └── app.js                    # Upload, antrean, worker, job, dan benchmark simulasi
├── docker/
│   ├── Dockerfile.server         # Konfigurasi container untuk server gRPC
│   ├── Dockerfile.client         # Konfigurasi container untuk client gRPC
│   └── docker-compose.yml        # Orkestrasi 2 container dalam bridge network
├── docs/
│   ├── architecture.md           # Dokumentasi arsitektur dan diagram alur
│   ├── diagram_arsitektur.png    # Gambar visual arsitektur sistem
│   └── screenshot_proyek_kelompok.png # Tangkapan layar terminal eksekusi
├── requirements.txt              # Daftar pustaka Python
├── .gitignore
└── README.md
```

---

## 🚀 Panduan Instalasi & Eksekusi

### 1. Prasyarat Lingkungan
* Python 3.10 ke atas
* Docker & Docker Compose *(opsional untuk demo multi-container)*

### 2. Setup Virtual Environment
```bash
# Clone repositori
git clone https://github.com/HansMurdock/KPT-RPC.git
cd KPT-RPC

# Buat dan aktifkan virtual environment
python -m venv venv

# Windows:
.\venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

# Install dependensi
pip install -r requirements.txt
```

### 3. Kompilasi Kontrak Protocol Buffers (.proto)
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. protos/image_service.proto
```

---

## 💻 Menjalankan Server & Client Secara Lokal

### Jalankan Server
```bash
python -m server.server
```
*Server aktif dan mendengarkan pada port TCP `50051`.*

### Jalankan Client
```bash
# 1. Mode Interaktif (TUI Visual & Menu Interaktif):
python -m client.client

# 2. Menjalankan pipeline langsung (dengan filter: grayscale, blur, sepia, invert, edge, sharpen):
python -m client.client --action full-pipeline --file client/samples/sample.jpg --filter sepia

# 3. Atau jalankan perintah spesifik:
# Unggah gambar dengan filter tertentu:
python -m client.client --action upload --file client/samples/sample.jpg --filter blur

# Cek status pekerjaan:
python -m client.client --action status --job-id <JOB_ID>

# Pantau status secara real-time (Server Streaming):
python -m client.client --action monitor --job-id <JOB_ID>

# Unduh gambar hasil pemrosesan:
python -m client.client --action download --job-id <JOB_ID> --output client/downloads/hasil.jpg

# Lihat daftar seluruh pekerjaan di server:
python -m client.client --action list-jobs

# Batalkan pekerjaan di antrean:
python -m client.client --action cancel --job-id <JOB_ID>
```

---

## 🖥️ Web Simulator (Dashboard Gelap)

Web simulator adalah dashboard untuk mempelajari alur proyek melalui browser. Simulator menyediakan halaman **Overview**, **History**, **Benchmark**, dan **RPC Architecture**. Pengolahan gambar berjalan di browser menggunakan Canvas API; antrean, worker, status job, telemetry RPC, dan benchmark dimodelkan untuk demonstrasi.

> **Batasan penting:** Web simulator tidak mengirim request ke server gRPC Python, tidak membaca data dari `server/storage`, dan tidak menyimpan riwayat setelah halaman dimuat ulang. Menyalakan server gRPC dan web simulator secara bersamaan tidak menghubungkan keduanya. Untuk menguji backend gRPC yang sebenarnya, gunakan perintah CLI pada bagian **Menjalankan Server & Client Secara Lokal** dan benchmark aktual pada bagian **Hasil Evaluasi Kinerja**.

### Menjalankan web simulator

Jalankan dari root repositori:

```bash
python -m http.server 8080 --directory web
```

Buka browser dan akses `http://localhost:8080`. Biarkan terminal server HTTP tetap berjalan selama simulator digunakan. Untuk menghentikannya, tekan `Ctrl+C` di terminal tersebut.

Jika port `8080` sedang digunakan, jalankan di port lain, misalnya:

```bash
python -m http.server 8081 --directory web
```

Lalu buka `http://localhost:8081`. Perintah di atas hanya menyajikan file statis dalam folder `web`; tidak perlu mengaktifkan virtual environment atau menginstal dependensi Python proyek terlebih dahulu.

### Alur penggunaan Overview

1. Buka menu **Overview**. Empat kartu ringkasan menunjukkan jumlah job, job yang menunggu, worker aktif dibanding jumlah worker yang dikonfigurasi, dan job selesai pada sesi browser saat ini.
2. Tambahkan satu atau beberapa gambar melalui tombol **Browse** atau dengan menyeret file ke kotak upload. Simulator menerima berkas bertipe gambar yang dapat dibaca browser; format umum seperti PNG, JPG/JPEG, dan WEBP disarankan. Baris pratinjau menampilkan pilihan file. Tombol **×** menghapus pilihan.
3. Pilih satu filter sebelum mengirim. Filter yang tersedia adalah **Grayscale**, **Gaussian blur**, **Sepia**, **Invert**, **Edge detect**, dan **Sharpen**. Filter yang dipilih berlaku untuk setiap file dalam satu batch.
4. Atur **Request ID** bila ingin mengamati deduplikasi. UUID dibuat otomatis untuk upload tunggal jika kolom dibiarkan kosong. Tombol **New UUID** membuat UUID baru. Untuk mengulang request yang sama, gunakan kembali UUID yang sama: simulator akan menunjuk ke job yang sudah terdaftar, alih-alih menambahkan job duplikat. Pada upload beberapa gambar sekaligus, simulator membuat UUID terpisah untuk setiap file.
5. Secara opsional, aktifkan **Simulasikan timeout pertama** sebelum upload. Simulator memperagakan ACK yang hilang, jeda retry, dan pemakaian ulang UUID agar request tidak membuat job ganda.
6. Klik **Queue image job**. Progress akan memperagakan pengiriman metadata lalu potongan data berukuran 64 KB. Setelah ACK simulasi diterima, job masuk antrean FIFO dan diproses oleh worker browser. File yang diproses diskalakan bila sisi terpanjangnya melebihi 1.500 piksel agar hasil tetap ringan.
7. Panel **LIVE JOB STATUS** menampilkan job terpilih dan timeline `Queued → Processing → Completed`. Gunakan:
   - **CekStatus** untuk memperagakan permintaan status unary.
   - **Pantau stream** untuk menyalakan atau menghentikan simulasi aliran update status.
   - **Download** untuk menyimpan hasil setelah job berstatus `COMPLETED`.
   - **Cancel** untuk membatalkan job yang masih `QUEUED` atau `PROCESSING`.
8. Panel **BACKGROUND PROCESSING** memperlihatkan worker yang sedang sibuk dan panjang antrean. Pilih jumlah worker `2`, `3`, `4`, `6`, atau `8` saat tidak ada worker yang sedang menjalankan job. Worker yang aktif membuat pilihan jumlah thread dinonaktifkan sementara.
9. Panel **RPC telemetry** mencatat panggilan simulasi seperti upload, cek status, pantau status, unduh, daftar job, dan pembatalan, beserta tipe, hasil, serta latensi contoh. Angkanya bukan log dari interceptor gRPC yang berjalan di Python.

### Menggunakan halaman History

1. Pilih **History** dari sidebar, atau klik tombol **History** pada panel status.
2. Daftar menampilkan job yang dibuat selama halaman browser ini masih terbuka. Pilih satu baris untuk menampilkan **IMAGE OUTPUT** (perbandingan input dan hasil) serta **SELECTED RECORD** (ID job, request UUID, filter, status, ukuran, jumlah percobaan/potongan, waktu, dan worker/pesan).
3. Gunakan kolom pencarian untuk mencari ID job atau nama file. Menu status memfilter berdasarkan status; centang **Active only** untuk menampilkan job yang masih menunggu atau diproses.
4. **Refresh history** memperagakan RPC `ListJobs` dan merender kembali registry lokal simulator. Tombol **Status**, **↓**, dan **×** pada baris masing-masing menjalankan simulasi cek status, unduhan output, dan pembatalan. Tindakan juga tersedia pada panel LIVE JOB STATUS di Overview.
5. Tombol unduh aktif setelah pemrosesan selesai. Nama berkas hasil mengikuti pola `<nama-file>-<filter>.png` dan disimpan melalui mekanisme unduhan browser.

### Menggunakan halaman Benchmark

1. Buka menu **Benchmark** lalu klik **Run simulation**.
2. Halaman membandingkan tiga skenario: `1`, `5`, dan `20` client, masing-masing dengan model `20` job. Grafik menampilkan throughput serta rata-rata dan P95 latency; tabel juga merangkum total waktu, nilai minimum/maksimum, dan jumlah error model.
3. Hasil dihitung oleh model JavaScript di browser dengan asumsi 8 image worker dan variasi acak kecil. Hasil ini untuk demonstrasi antarmuka dan konsep concurrency; **jangan kutip sebagai hasil pengukuran server**.
4. Untuk menjalankan benchmark terhadap server gRPC sungguhan, aktifkan server terlebih dahulu lalu jalankan `python -m benchmark.benchmark` dari root repositori. Hasil benchmark Python disimpan di folder `benchmark/results/`.

### Menggunakan halaman RPC Architecture

Buka **RPC Architecture** untuk melihat diagram alur client → server gRPC → antrean thread-safe → worker gambar, ringkasan enam metode RPC, daftar filter yang tersedia, mekanisme keselamatan/concurrency, dan topologi Docker Compose. Halaman ini merupakan dokumentasi visual; diagram tidak mengendalikan proses server atau container.

### Reset dan sesi browser

- Klik **Reset simulation** di Overview untuk menghapus job, antrean, telemetry, penghitung deduplikasi, pilihan request ID, serta hasil grafik benchmark dari simulator.
- Muat ulang atau tutup tab juga menghapus state simulator karena data disimpan di memori halaman, bukan di server atau penyimpanan permanen.
- Reset tidak menghapus file apa pun pada `server/storage/` atau `client/downloads/`, sebab simulator browser tidak mengakses direktori tersebut.

### Kendala umum

- Jika `python` tidak dikenali, gunakan instalasi Python 3 dan pastikan perintah Python tersedia di terminal.
- Jika halaman tidak terbuka, pastikan server HTTP masih berjalan dan URL menggunakan port yang sama dengan perintah, misalnya `http://localhost:8080`.
- Jika file tidak dapat diproses, coba gambar lain dalam format umum yang didukung browser dan pastikan ukuran file tidak nol.
- Jika hasil benchmark web berbeda dari tabel hasil proyek, itu wajar: web menjalankan model demo, sedangkan `benchmark/benchmark.py` melakukan panggilan ke backend gRPC.

---

## 🐳 Menjalankan di 2 Container Berbeda (Docker Compose)

Untuk memenuhi ketentuan demo minimal 2 mesin/container terpisah:
```bash
cd docker
docker compose up --build
```
Container `kpt-rpc-server` dan `kpt-rpc-client` akan otomatis berjalan di jaringan bridge terisolasi `kpt-rpc-net`, di mana client berkomunikasi ke server melalui hostname `grpc-image-server:50051`.

---

## 📊 Hasil Evaluasi Kinerja (1, 5, dan 20 Client Bersamaan)

Pengujian beban dilakukan menggunakan `benchmark/benchmark.py` dengan beban 20 pekerjaan penuh (80 pemanggilan RPC) pada tingkat konkurensi 1, 5, dan 20 client simultan:

```bash
python -m benchmark.benchmark
```

### Tabel Hasil Pengukuran:

| Jumlah Client | Total Beban Uji | Waktu Total (s) | Latensi Rata-rata (ms) | Latensi P95 (ms) | Throughput (Job/s \| RPC/s) | Error / Data Rusak |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1 Client** | 20 Job (80 RPC) | `1.930 s` | `96.41 ms` | `101.03 ms` | `10.36 job/s` (`41.46 RPC/s`) | `0` (100% Aman) |
| **5 Client** | 20 Job (80 RPC) | `0.420 s` | `103.77 ms` | `108.05 ms` | `47.65 job/s` (`190.60 RPC/s`) | `0` (100% Aman) |
| **20 Client** | 20 Job (80 RPC) | `0.223 s` | `159.61 ms` | `209.98 ms` | `89.68 job/s` (`358.72 RPC/s`) | `0` (100% Aman) |

### Grafik Throughput & Latensi:

![Grafik Kinerja Benchmark](benchmark/results/grafik_kinerja_kelompok.png)

### Tangkapan Layar Eksekusi Terminal:

![Tangkapan Layar Terminal](docs/screenshot_proyek_kelompok.png)

---

## 🛡️ Toleransi Kegagalan & Concurrency Safety
* **Deduplikasi Request ID (At-Most-Once)**: Client menyertakan UUID unik di setiap permintaan unggah. Saat terjadi timeout jaringan dan client melakukan retry, server mendeteksi `request_id` yang sama dan tidak memproses ulang antrean.
* **Mutex Lock (`threading.Lock`)**: Melindungi kamus data pekerjaan dari race condition ketika banyak worker thread gRPC dan background processing thread mengakses state secara bersamaan.
* **Exponential Backoff**: Jeda pengulangan otomatis `0.5s`, `1.0s`, `2.0s` saat menghadapi galat `DEADLINE_EXCEEDED` atau `UNAVAILABLE`.
