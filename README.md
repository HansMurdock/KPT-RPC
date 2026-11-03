# KPT-RPC: Sistem Antrian Pengolahan Citra Terdistribusi Berbasis gRPC

Proyek Mini Mata Kuliah **Komputasi Paralel dan Terdistribusi (KPT)**.  
Studi Kasus 5: _Antrian Pengolahan Gambar (Client mengunggah gambar, Server mengubahnya menjadi grayscale secara asinkron)_.

---

## 👥 Tim Pengembang & Pembagian Tugas

| No  | Nama             | Peran                              | Tanggung Jawab Utama                                                                                                                                                                                                                                                                          |
| :-: | :--------------- | :--------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|  1  | **Zaki** _(Aku)_ | Backend & Processing Engineer      | • Implementasi `server/server.py` dan `server/worker.py`<br>• Manajemen antrean thread-safe (`queue.Queue`) & worker pool<br>• Konversi citra ke Grayscale (`Pillow`)<br>• Penanganan _concurrency_ & locking status (`threading.Lock()`)                                                     |
|  2  | **Rafa**         | Interface & Client Engineer        | • Desain kontrak antarmuka `protos/image_service.proto`<br>• Implementasi `client/client.py`<br>• Pengunggahan & pengunduhan citra per chunk (Streaming RPC)<br>• Toleransi kegagalan: Timeout, Retry (Exponential Backoff), & Deduplikasi UUID                                               |
|  3  | **Hans** _(Aku)_ | Benchmark, DevOps & Reporting Lead | • Setup repositori Git & arsitektur proyek<br>• Kontainerisasi multi-mesin (`docker/docker-compose.yml`)<br>• Pengujian beban kinerja (`benchmark/benchmark.py` untuk 1, 5, 20 client)<br>• Visualisasi grafik latensi & throughput (`matplotlib`)<br>• Koordinasi Laporan Akhir & Video Demo |

---

## 📁 Struktur Direktori Proyek

```text
KPT-RPC/
├── protos/
│   └── image_service.proto       # Kontrak IDL Protocol Buffers (4 RPCs)
├── server/
│   ├── server.py                 # Implementasi server gRPC & Servicer
│   ├── worker.py                 # Background thread worker untuk pemrosesan citra
│   └── storage/                  # Penyimpanan sementara file citra
│       ├── input/                # Citra asli hasil upload
│       └── output/               # Citra hasil konversi grayscale
├── client/
│   ├── client.py                 # Aplikasi client CLI (Upload, Status, Download)
│   ├── samples/                  # Gambar sampel untuk pengujian
│   └── downloads/               # Direktori hasil unduhan citra grayscale
├── benchmark/
│   ├── benchmark.py              # Pengujian kinerja (1, 5, 20 client konkuren)
│   └── results/                  # Grafik PNG & tabel metrik kinerja
├── docker/
│   ├── Dockerfile.server         # Konfigurasi container untuk server
│   ├── Dockerfile.client         # Konfigurasi container untuk client
│   └── docker-compose.yml        # Orkestrasi 2 container dalam 1 bridge network
├── docs/
│   └── architecture.md           # Diagram alur dan arsitektur sistem
├── requirements.txt              # Dependensi pustaka Python
├── .gitignore
└── README.md
```

---

## 🚀 Panduan Instalasi & Menjalankan Sistem

### 1. Prasyarat Lingkungan

- Python minimal versi 3.10
- Docker & Docker Compose _(opsional untuk pengujian multi-container)_

### 2. Setup Virtual Environment

```bash
# Buat virtual environment
python -m venv venv

# Aktivasi virtual environment
# Windows:
.\venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

# Install dependensi
pip install -r requirements.txt
```

### 3. Kompilasi Kontrak Protocol Buffers (.proto)

Jalankan perintah berikut dari direktori root proyek (`KPT-RPC/`):

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. protos/image_service.proto
```

Perintah ini akan menghasilkan dua berkas stub:

- `protos/image_service_pb2.py`
- `protos/image_service_pb2_grpc.py`

---

## 💻 Menjalankan Server & Client Secara Lokal

### Jalankan Server

```bash
python -m server.server
```

_Server akan berjalan di port `50051`._

### Jalankan Client

```bash
# Unggah dan proses gambar
python -m client.client --action upload --file client/samples/sample.jpg

# Cek status pekerjaan
python -m client.client --action status --job-id <JOB_ID>

# Pantau status secara real-time (Server Streaming)
python -m client.client --action monitor --job-id <JOB_ID>

# Unduh gambar hasil grayscale
python -m client.client --action download --job-id <JOB_ID> --output client/downloads/hasil.jpg
```

---

## 🐳 Menjalankan di 2 Container Berbeda (Docker Compose)

Untuk memenuhi ketentuan demo minimal 2 mesin/container terpisah:

```bash
cd docker
docker-compose up --build
```

---

## 📊 Menjalankan Benchmark Kinerja

Untuk mengukur latensi rata-rata dan throughput pada 1, 5, dan 20 client bersamaan:

```bash
python -m benchmark.benchmark
```

Hasil pengujian, tabel statistik, dan grafik visual akan otomatis disimpan di folder `benchmark/results/`.
