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
from PIL import Image

from protos import image_service_pb2 as pb


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
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    completed_at: float = 0.0


class ImageJobManager:
    """
    Manajer antrean dan state pekerjaan pengolahan gambar yang thread-safe.
    Menggunakan:
    1. threading.Lock() (self._lock) untuk mencegah race condition saat banyak
       thread gRPC membaca/mengubah dictionary jobs dan tabel deduplikasi.
    2. queue.Queue() (self._queue) untuk mendistribusikan tugas konversi grayscale
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
                input_path = os.path.join(self.input_dir, f"{job_id}_{safe_name}")
                output_path = os.path.join(self.output_dir, f"{job_id}_grayscale_{safe_name}")

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
                    message="Gambar berhasil diunggah dan masuk antrean pemrosesan",
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

    def _worker_loop(self) -> None:
        """Loop utama worker thread yang mengambil tugas dari antrean dan mengubah citra menjadi grayscale."""
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

            try:
                # Tahap 1: Mulai pemrosesan
                self._update_job(
                    job_id,
                    status=pb.STATUS_PROCESSING,
                    progress_percent=35,
                    message=f"[{worker_name}] Membaca citra asli ({rec.input_size} bytes)...",
                )
                if self.simulated_delay > 0:
                    time.sleep(self.simulated_delay / 2.0)

                # Tahap 2: Konversi RGB ke Grayscale (Mode 'L' pada Pillow)
                with Image.open(rec.input_path) as img:
                    self._update_job(
                        job_id,
                        status=pb.STATUS_PROCESSING,
                        progress_percent=70,
                        message=f"[{worker_name}] Mengonversi citra {img.size[0]}x{img.size[1]} ke Grayscale...",
                    )
                    gray_img = img.convert("L")
                    if self.simulated_delay > 0:
                        time.sleep(self.simulated_delay / 2.0)
                    gray_img.save(rec.output_path)

                out_size = os.path.getsize(rec.output_path)
                now = time.time()
                with self._lock:
                    self._total_processed += 1
                    job_ref = self._jobs[job_id]
                    job_ref.status = pb.STATUS_COMPLETED
                    job_ref.progress_percent = 100
                    job_ref.output_size = out_size
                    job_ref.completed_at = now
                    job_ref.updated_at = now
                    dur_ms = (now - job_ref.created_at) * 1000.0
                    job_ref.message = (
                        f"[{worker_name}] Konversi grayscale selesai dalam {dur_ms:.1f} ms "
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
