# Arsitektur Sistem Antrian Pengolahan Citra Terdistribusi Berbasis gRPC

## 1. Gambaran Umum Studi Kasus 5
Sistem ini mengimplementasikan layanan pengolahan citra terdistribusi berbasis **gRPC** dan **Protocol Buffers (proto3)**. Client mengirimkan berkas gambar berwarna (RGB) secara bertahap per potongan byte (*chunk* 64 KB) melalui *Client Streaming RPC*. Server menyimpan berkas ke penyimpanan lokal, mencatat pekerjaan di dalam struktur data yang dilindungi *mutex lock* (`threading.Lock()`), lalu memasukkan `job_id` ke dalam antrean tugas asinkron (`queue.Queue`). Sekumpulan *background worker thread* (`ImageWorkerPool`) mengambil tugas dari antrean, mengonversi citra menjadi *grayscale* (`Pillow`), dan memperbarui status progres secara *thread-safe*.

## 2. Diagram Alur Komponen & Pemanggilan RPC

```mermaid
sequenceDiagram
    autonumber
    participant C as Client (Rafa)
    participant S as gRPC Server Servicer (Zaki)
    participant Q as Thread-Safe Queue & Lock
    participant W as Background Worker Pool (4 Threads)

    C->>S: 1. UploadGambar(stream UploadChunk [Metadata + 64KB Chunks])
    S->>Q: Cek Deduplikasi request_id (with Lock) & Simpan File
    Q-->>S: Kembalikan job_id & Enqueue Tugas
    S-->>C: UploadResponse (job_id, STATUS_QUEUED)

    par Pemrosesan Asinkron di Latar Belakang
        Q->>W: Ambil job_id dari Queue
        W->>Q: Update STATUS_PROCESSING (35% -> 70%)
        W->>W: Konversi Citra RGB ke Grayscale (Pillow)
        W->>Q: Update STATUS_COMPLETED (100%)
    and Pemantauan Status oleh Client
        C->>S: 2. CekStatus(job_id) [Unary / Async Future]
        S-->>C: StatusResponse
        C->>S: 3. PantauStatus(job_id) [Server Streaming]
        S-->>C: stream StatusResponse (10% -> 35% -> 70% -> 100%)
    end

    C->>S: 4. UnduhGambar(job_id) [Server Streaming]
    S-->>C: stream DownloadChunk (Potongan 64KB Citra Grayscale)
```

## 3. Spesifikasi Keempat Metode RPC (`protos/image_service.proto`)

| No | Nama RPC | Pola Komunikasi | Input Message | Output Message | Fungsi & Alasan Pemilihan Pola |
| :-: | :--- | :--- | :--- | :--- | :--- |
| 1 | `UploadGambar` | **Client Streaming** | `stream UploadChunk` | `UploadResponse` | Mengunggah citra berukuran besar per *chunk* (64 KB) tanpa membebani RAM sekaligus; dilengkapi `request_id` untuk deduplikasi *At-Most-Once*. |
| 2 | `CekStatus` | **Unary RPC** *(Async Future)* | `StatusRequest` | `StatusResponse` | Mengecek status pekerjaan secara cepat (*idempoten*); mendukung pemanggilan non-blocking via `.future()`. |
| 3 | `PantauStatus` | **Server Streaming** | `StatusRequest` | `stream StatusResponse` | Mendorong pembaruan persentase progres (`10% -> 35% -> 70% -> 100%`) secara *real-time* dari server ke client tanpa *polling* berulang. |
| 4 | `UnduhGambar` | **Server Streaming** | `DownloadRequest` | `stream DownloadChunk` | Mengunduh citra hasil konversi *grayscale* secara bertahap per *chunk* (64 KB) hingga byte terakhir diterima utuh oleh client. |
