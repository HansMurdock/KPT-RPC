"""
==============================================================
MATA KULIAH : KOMPUTASI PARALEL & TERDISTRIBUSI
PROYEK MINI : SISTEM ANTRIAN PENGOLAHAN CITRA BERBASIS gRPC
FILE        : server/worker.py (Background Worker & Thread-Safe State)
TIM         : Zaki, Rafa, Khansa Rasendriya Aji (247006111132)
==============================================================
"""

import os
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from PIL import Image, ImageFilter
import numpy as np

from protos import image_service_pb2 as pb


# Peta nama filter yang mudah dibaca untuk log dan pesan
PROCESSING_TYPE_NAMES: Dict[int, str] = {
    pb.PROC_GRAYSCALE: "Grayscale",
    pb.PROC_BLUR: "Gaussian Blur",
    pb.PROC_SEPIA: "Sepia",
    pb.PROC_INVERT: "Invert (Negatif)",
    pb.PROC_EDGE_DETECT: "Edge Detect",
    pb.PROC_SHARPEN: "Sharpen",
}


@dataclass
class JobRecord:
    """Representasi state pekerjaan pengolahan citra di memori server."""
    job_id: str
    request_id: str
    filename: str
    file_format: str
    input_path: str
    output_path: str = ""
    status: int = pb.STATUS_QUEUED
    progress_percent: int = 0
    message: str = "Pekerjaan masuk ke dalam antrean (Queued)"
    input_size: int = 0
    output_size: int = 0
    processing_type: int = pb.PROC_GRAYSCALE
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    completed_at: float = 0.0


class ImageJobManager:
    """
    Manajer antrean dan state pekerjaan pengolahan gambar yang thread-safe.
    Menggunakan:
    1. threading.Lock() (self._lock) untuk mencegah race condition saat banyak
       thread gRPC membaca/mengubah dictionary jobs dan tabel deduplikasi.
    2. queue.Queue() (self._queue) untuk mendistribusikan tugas konversi
       secara asinkron ke pool worker thread di latar belakang.
    """

    def __init__(self, input_dir: str, output_dir: str, num_workers: int = 4, simulated_delay: float = 0.15):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.num_workers = num_workers
        self.simulated_delay = simulated_delay

        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        self._lock = threading.Lock()
        self._jobs: Dict[str, JobRecord] = {}
        self._request_to_job: Dict[str, str] = {}  # request_id -> job_id (Deduplikasi At-Most-Once)
        self._queue: "queue.Queue[Optional[str]]" = queue.Queue()
        self._workers: list[threading.Thread] = []
        self._running = False
        self._total_processed = 0
        self._duplicate_hits = 0

    def start_workers(self) -> None:
        """Menjalankan background worker threads untuk memproses antrean gambar."""
        with self._lock:
            if self._running:
                return
            self._running = True
            for i in range(self.num_workers):
                t = threading.Thread(
                    target=self._worker_loop,
                    name=f"ImageWorker-{i+1}",
                    daemon=True,
                )
                t.start()
                self._workers.append(t)

    def stop_workers(self) -> None:
        """Menghentikan seluruh worker thread dengan aman (graceful shutdown)."""
        with self._lock:
            if not self._running:
                return
            self._running = False
        for _ in self._workers:
            self._queue.put(None)
        for t in self._workers:
            t.join(timeout=2.0)
        self._workers.clear()

    def register_or_get_job(
        self,
        request_id: str,
        job_id: str,
        filename: str,
        file_format: str,
        staged_input_path: str,
        input_size: int,
        processing_type: int = pb.PROC_GRAYSCALE,
    ) -> tuple[JobRecord, bool]:
        """
        Mendaftarkan pekerjaan baru secara atomik dengan deduplikasi request_id.
        Mengembalikan tuple (JobRecord, is_duplicate).
        Jika request_id sudah pernah terdaftar (akibat retry client), fungsi ini
        langsung mengembalikan JobRecord lama tanpa membuat antrean ganda.
        """
        duplicate_record = None
        with self._lock:
            if request_id and request_id in self._request_to_job:
                existing_job_id = self._request_to_job[request_id]
                self._duplicate_hits += 1
                duplicate_record = JobRecord(**self._jobs[existing_job_id].__dict__)
            else:
                safe_name = os.path.basename(filename) or "image.jpg"
                filter_name = PROCESSING_TYPE_NAMES.get(processing_type, "grayscale").lower().replace(" ", "_")
                input_path = os.path.join(self.input_dir, f"{job_id}_{safe_name}")
                output_path = os.path.join(self.output_dir, f"{job_id}_{filter_name}_{safe_name}")

                # File sementara berada di direktori yang sama agar os.replace atomik.
                os.replace(staged_input_path, input_path)

                record = JobRecord(
                    job_id=job_id,
                    request_id=request_id,
                    filename=safe_name,
                    file_format=file_format or "JPEG",
                    input_path=input_path,
                    output_path=output_path,
                    status=pb.STATUS_QUEUED,
                    progress_percent=10,
                    processing_type=processing_type,
                    message=f"Gambar masuk antrean pemrosesan ({PROCESSING_TYPE_NAMES.get(processing_type, 'Grayscale')})",
                    input_size=input_size,
                )
                self._jobs[job_id] = record
                if request_id:
                    self._request_to_job[request_id] = job_id
                queued_record = JobRecord(**record.__dict__)

        if duplicate_record is not None:
            try:
                os.remove(staged_input_path)
            except FileNotFoundError:
                pass
            return duplicate_record, True

        # Masukkan job_id ke dalam antrean asinkron
        self._queue.put(job_id)
        return queued_record, False

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        """Mengambil salinan informasi JobRecord secara thread-safe."""
        with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None:
                return None
            # Kembalikan salinan ringan agar pembacaan di luar lock tetap aman
            return JobRecord(**rec.__dict__)

    def list_jobs(self, filter_status: int = 0, active_only: bool = False) -> list[JobRecord]:
        """Mengambil daftar seluruh pekerjaan secara thread-safe dengan filter opsional."""
        with self._lock:
            result = []
            for rec in self._jobs.values():
                if active_only and rec.status not in (pb.STATUS_QUEUED, pb.STATUS_PROCESSING):
                    continue
                if filter_status and filter_status != rec.status:
                    continue
                result.append(JobRecord(**rec.__dict__))
            # Urutkan berdasarkan waktu pembuatan terbaru
            result.sort(key=lambda r: r.created_at, reverse=True)
            return result

    def cancel_job(self, job_id: str) -> tuple[bool, str]:
        """
        Membatalkan pekerjaan yang masih dalam antrean (QUEUED).
        Hanya pekerjaan dengan status QUEUED yang bisa dibatalkan.
        """
        with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None:
                return False, f"Job ID '{job_id}' tidak ditemukan."
            if rec.status == pb.STATUS_CANCELLED:
                return True, f"Job '{job_id}' sudah dibatalkan sebelumnya."
            if rec.status not in (pb.STATUS_QUEUED, pb.STATUS_PROCESSING):
                return False, (
                    f"Job '{job_id}' tidak bisa dibatalkan karena sudah berstatus "
                    f"{pb.JobStatus.Name(rec.status)}."
                )
            rec.status = pb.STATUS_CANCELLED
            rec.progress_percent = 0
            rec.message = "Pekerjaan dibatalkan oleh client."
            rec.updated_at = time.time()
            return True, f"Job '{job_id}' berhasil dibatalkan."

    def get_stats(self) -> dict:
        """Mengambil statistik antrean dan worker secara thread-safe."""
        with self._lock:
            return {
                "total_jobs": len(self._jobs),
                "total_processed": self._total_processed,
                "duplicate_hits": self._duplicate_hits,
                "queue_size": self._queue.qsize(),
            }

    def _update_job(self, job_id: str, **kwargs) -> None:
        """Memperbarui atribut JobRecord di dalam proteksi mutex lock."""
        with self._lock:
            rec = self._jobs.get(job_id)
            if rec is not None:
                for k, v in kwargs.items():
                    setattr(rec, k, v)
                rec.updated_at = time.time()

    def _apply_processing(self, img: Image.Image, processing_type: int) -> Image.Image:
        """
        Menerapkan filter pemrosesan citra sesuai ProcessingType yang diminta.
        Mendukung 6 jenis pemrosesan: Grayscale, Blur, Sepia, Invert, Edge Detect, Sharpen.
        """
        if processing_type == pb.PROC_GRAYSCALE:
            return img.convert("L")

        elif processing_type == pb.PROC_BLUR:
            return img.filter(ImageFilter.GaussianBlur(radius=3))

        elif processing_type == pb.PROC_SEPIA:
            # Konversi ke numpy array untuk manipulasi matriks sepia
            img_rgb = img.convert("RGB")
            arr = np.array(img_rgb, dtype=np.float64)
            # Matriks transformasi Sepia standar
            sepia_matrix = np.array([
                [0.393, 0.769, 0.189],
                [0.349, 0.686, 0.168],
                [0.272, 0.534, 0.131],
            ])
            sepia_arr = arr @ sepia_matrix.T
            sepia_arr = np.clip(sepia_arr, 0, 255).astype(np.uint8)
            return Image.fromarray(sepia_arr)

        elif processing_type == pb.PROC_INVERT:
            from PIL import ImageOps
            return ImageOps.invert(img.convert("RGB"))

        elif processing_type == pb.PROC_EDGE_DETECT:
            return img.convert("L").filter(ImageFilter.FIND_EDGES)

        elif processing_type == pb.PROC_SHARPEN:
            return img.filter(ImageFilter.SHARPEN)

        else:
            # Fallback ke grayscale jika tipe tidak dikenali
            return img.convert("L")

    def _worker_loop(self) -> None:
        """Loop utama worker thread yang mengambil tugas dari antrean dan memproses citra."""
        worker_name = threading.current_thread().name
        while True:
            job_id = self._queue.get()
            if job_id is None:
                self._queue.task_done()
                break

            rec = self.get_job(job_id)
            if rec is None:
                self._queue.task_done()
                continue

            # Periksa apakah job sudah dibatalkan sebelum mulai memproses
            if rec.status == pb.STATUS_CANCELLED:
                self._queue.task_done()
                continue

            filter_name = PROCESSING_TYPE_NAMES.get(rec.processing_type, "Grayscale")

            try:
                # Tahap 1: Mulai pemrosesan
                self._update_job(
                    job_id,
                    status=pb.STATUS_PROCESSING,
                    progress_percent=35,
                    message=f"[{worker_name}] Membaca citra asli ({rec.input_size} bytes) untuk filter {filter_name}...",
                )
                if self.simulated_delay > 0:
                    time.sleep(self.simulated_delay / 2.0)

                # Tahap 2: Periksa kembali status (mungkin dibatalkan saat antrean)
                current = self.get_job(job_id)
                if current and current.status == pb.STATUS_CANCELLED:
                    self._queue.task_done()
                    continue

                # Tahap 3: Terapkan filter pemrosesan
                with Image.open(rec.input_path) as img:
                    self._update_job(
                        job_id,
                        status=pb.STATUS_PROCESSING,
                        progress_percent=70,
                        message=f"[{worker_name}] Menerapkan filter {filter_name} pada citra {img.size[0]}x{img.size[1]}...",
                    )
                    result_img = self._apply_processing(img, rec.processing_type)
                    if self.simulated_delay > 0:
                        time.sleep(self.simulated_delay / 2.0)

                    # Tentukan format simpan yang sesuai
                    save_format = rec.file_format.upper()
                    if save_format not in ("JPEG", "PNG", "BMP", "TIFF", "WEBP"):
                        save_format = "JPEG"
                    # Mode 'L' atau 'RGB' bisa disimpan ke JPEG; 'RGBA' tidak bisa
                    if save_format == "JPEG" and result_img.mode == "RGBA":
                        result_img = result_img.convert("RGB")
                    result_img.save(rec.output_path, format=save_format)

                out_size = os.path.getsize(rec.output_path)
                now = time.time()
                with self._lock:
                    self._total_processed += 1
                    job_ref = self._jobs[job_id]
                    # Cek lagi apakah dibatalkan saat proses
                    if job_ref.status == pb.STATUS_CANCELLED:
                        pass  # Jangan timpa status cancelled
                    else:
                        job_ref.status = pb.STATUS_COMPLETED
                        job_ref.progress_percent = 100
                        job_ref.output_size = out_size
                        job_ref.completed_at = now
                        job_ref.updated_at = now
                        dur_ms = (now - job_ref.created_at) * 1000.0
                        job_ref.message = (
                            f"[{worker_name}] Filter {filter_name} selesai dalam {dur_ms:.1f} ms "
                            f"(Output: {out_size} bytes)"
                        )
            except Exception as exc:
                self._update_job(
                    job_id,
                    status=pb.STATUS_FAILED,
                    progress_percent=0,
                    message=f"[{worker_name}] Gagal memproses gambar: {exc}",
                )
            finally:
                self._queue.task_done()
