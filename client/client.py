"""
==============================================================
MATA KULIAH : KOMPUTASI PARALEL & TERDISTRIBUSI
PROYEK MINI : SISTEM ANTRIAN PENGOLAHAN CITRA BERBASIS gRPC
FILE        : client/client.py (gRPC Client, Streaming, Retry & Async)
TIM         : Zaki, Rafa, Khansa
==============================================================

Client CLI Interaktif untuk Sistem Antrian Pengolahan Citra gRPC.

Fitur utama:
  1. Upload gambar dengan chunking (64 KB per chunk)
  2. Pilihan 6 jenis filter pemrosesan (Grayscale, Blur, Sepia, Invert, Edge, Sharpen)
  3. Retry otomatis dengan Exponential Backoff + UUID request_id (At-Most-Once)
  4. Timeout per panggilan RPC
  5. Download hasil dengan chunking dan progress bar
  6. Pemantauan progres real-time (Server Streaming)
  7. Batch upload multi-file
  8. List semua pekerjaan di server (ListJobs)
  9. Pembatalan pekerjaan (BatalkanJob)
  10. gRPC Client Interceptor untuk logging dan timing (AOP Pattern)
  11. Tampilan interaktif menggunakan Rich (tabel, progress bar, panel)
  12. Validasi file gambar sebelum upload (format, ukuran, integritas file)
"""

import argparse
import os
import sys
import time
import uuid
from typing import Iterator, Optional
import grpc

# Konfigurasi encoding UTF-8 aman pada console Windows
if sys.platform == "win32":
    try:
        if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
            sys.stdout.reconfigure(encoding="utf-8")
        if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.prompt import Prompt, Confirm
from rich import box

from protos import image_service_pb2 as pb
from protos import image_service_pb2_grpc as pb_grpc
from client.interceptor import LoggingClientInterceptor, get_global_stats

CHUNK_SIZE = 64 * 1024  # 64 KB per chunk
console = Console(safe_box=True)

# Peta nama filter yang mudah dibaca
FILTER_MAP = {
    "1": (pb.PROC_GRAYSCALE, "Grayscale", "Konversi RGB ke skala abu-abu (Mode 'L')"),
    "2": (pb.PROC_BLUR, "Gaussian Blur", "Efek blur halus dengan radius=3"),
    "3": (pb.PROC_SEPIA, "Sepia", "Filter vintage berwarna cokelat hangat"),
    "4": (pb.PROC_INVERT, "Invert (Negatif)", "Inversi seluruh warna (negatif film)"),
    "5": (pb.PROC_EDGE_DETECT, "Edge Detect", "Deteksi tepi citra (kontur objek)"),
    "6": (pb.PROC_SHARPEN, "Sharpen", "Penajaman detail citra"),
}

PROCESSING_TYPE_NAMES = {
    pb.PROC_GRAYSCALE: "Grayscale",
    pb.PROC_BLUR: "Blur",
    pb.PROC_SEPIA: "Sepia",
    pb.PROC_INVERT: "Invert",
    pb.PROC_EDGE_DETECT: "Edge Detect",
    pb.PROC_SHARPEN: "Sharpen",
}


# ============================================================
# VALIDASI FILE
# ============================================================

ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".webp"}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB


def validate_image_file(file_path: str) -> tuple[bool, str]:
    """
    Validasi file gambar sebelum upload:
    - Memastikan file ada dan bisa dibaca
    - Memastikan ekstensi sesuai format gambar yang didukung
    - Memastikan ukuran file tidak melebihi batas maksimum (50 MB)
    - Memastikan integritas header citra valid (PIL verify check)
    """
    if not os.path.exists(file_path):
        return False, f"File tidak ditemukan: {file_path}"

    if not os.path.isfile(file_path):
        return False, f"Path bukan file reguler: {file_path}"

    ext = os.path.splitext(file_path)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        return False, (
            f"Ekstensi '{ext}' tidak didukung. "
            f"Format yang diizinkan: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
        )

    file_size = os.path.getsize(file_path)
    if file_size == 0:
        return False, "File kosong (0 bytes)."
    if file_size > MAX_FILE_SIZE:
        return False, f"File terlalu besar ({file_size:,} bytes). Maksimum: {MAX_FILE_SIZE:,} bytes."

    # Verifikasi header file gambar
    try:
        from PIL import Image
        with Image.open(file_path) as img:
            img.verify()
    except Exception as e:
        return False, f"File bukan gambar yang valid: {e}"

    return True, f"Valid - {os.path.basename(file_path)} ({file_size:,} bytes, format: {ext.upper().lstrip('.')})"


# ============================================================
# FUNGSI CORE RPC
# ============================================================

def _generate_upload_chunks(
    file_path: str,
    request_id: str,
    processing_type: int = pb.PROC_GRAYSCALE,
    chunk_size: int = CHUNK_SIZE,
) -> Iterator[pb.UploadChunk]:
    """
    Generator untuk Client Streaming RPC (UploadGambar):
    - Pesan pertama mengirimkan ImageMetadata (termasuk request_id unik & processing_type).
    - Pesan kedua dan seterusnya mengirimkan potongan byte file sebesar 64 KB.
    """
    filename = os.path.basename(file_path)
    ext = os.path.splitext(filename)[1].lower().lstrip(".") or "jpeg"
    file_size = os.path.getsize(file_path)

    # 1. Kirim metadata terlebih dahulu
    yield pb.UploadChunk(
        metadata=pb.ImageMetadata(
            request_id=request_id,
            filename=filename,
            file_format=ext.upper(),
            file_size=file_size,
            processing_type=processing_type,
        )
    )

    # 2. Kirim isi file per chunk (64 KB)
    with open(file_path, "rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            yield pb.UploadChunk(chunk_data=data)


def upload_gambar_dengan_retry(
    stub: pb_grpc.ImageServiceStub,
    file_path: str,
    request_id: Optional[str] = None,
    processing_type: int = pb.PROC_GRAYSCALE,
    timeout: float = 3.0,
    max_retries: int = 3,
    verbose: bool = True,
) -> tuple[pb.UploadResponse, int]:
    """
    Mengunggah gambar menggunakan Client Streaming RPC yang dilengkapi:
    - ID Permintaan unik (UUIDv4) yang tetap sama di setiap percobaan ulang
      untuk menjamin semantik At-Most-Once (mencegah antrean ganda).
    - Timeout per panggilan RPC.
    - Retry otomatis berbasis Exponential Backoff (0.5s, 1.0s, 2.0s) saat
      terjadi DEADLINE_EXCEEDED atau UNAVAILABLE.
    Mengembalikan tuple (UploadResponse, jumlah_percobaan).
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File gambar tidak ditemukan: {file_path}")

    rid = request_id or str(uuid.uuid4())

    for attempt in range(1, max_retries + 1):
        try:
            chunk_iter = _generate_upload_chunks(
                file_path, request_id=rid, processing_type=processing_type,
            )
            response = stub.UploadGambar(chunk_iter, timeout=timeout)
            if verbose:
                console.print(
                    f"  [bold green][OK][/bold green] [UPLOAD SUKSES] Percobaan #{attempt} | "
                    f"RequestID={rid[:8]}... | JobID={response.job_id}"
                )
            return response, attempt
        except grpc.RpcError as exc:
            retryable = exc.code() in (
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.UNAVAILABLE,
            )
            if not retryable or attempt == max_retries:
                if verbose:
                    console.print(
                        f"  [bold red][FAIL][/bold red] [UPLOAD GAGAL] Percobaan #{attempt}: "
                        f"{exc.code().name} - {exc.details()}"
                    )
                raise
            backoff = 0.5 * (2 ** (attempt - 1))
            if verbose:
                console.print(
                    f"  [bold yellow][RETRY][/bold yellow] Percobaan #{attempt} gagal ({exc.code().name}), "
                    f"retry dengan RequestID tetap ({rid[:8]}...) dalam {backoff:.1f}s..."
                )
            time.sleep(backoff)

    raise RuntimeError("Melebihi batas maksimum percobaan upload.")


def cek_status_pekerjaan(
    stub: pb_grpc.ImageServiceStub,
    job_id: str,
    use_async_future: bool = True,
    timeout: float = 3.0,
    verbose: bool = True,
) -> pb.StatusResponse:
    """
    Mengecek status pekerjaan menggunakan Unary RPC.
    Mendukung pemanggilan RPC Asinkron (non-blocking) menggunakan stub.CekStatus.future().
    """
    req = pb.StatusRequest(job_id=job_id)
    if use_async_future:
        future_call = stub.CekStatus.future(req, timeout=timeout)
        res = future_call.result()
    else:
        res = stub.CekStatus(req, timeout=timeout)

    if verbose:
        mode_str = "Async Future" if use_async_future else "Sync Unary"
        filter_name = PROCESSING_TYPE_NAMES.get(res.processing_type, "?")
        console.print(
            f"  [bold cyan][STATUS - {mode_str}][/bold cyan] {res.job_id} | "
            f"{pb.JobStatus.Name(res.status)} ({res.progress_percent}%) | "
            f"Filter: {filter_name} | {res.message}"
        )
    return res


def pantau_status_stream(
    stub: pb_grpc.ImageServiceStub,
    job_id: str,
    timeout: float = 15.0,
    verbose: bool = True,
) -> Optional[pb.StatusResponse]:
    """
    Memantau progres pekerjaan secara real-time melalui Server Streaming RPC (PantauStatus).
    Menampilkan progress bar animasi jika verbose aktif.
    """
    req = pb.StatusRequest(job_id=job_id)
    last_response = None

    if verbose:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(f"[cyan]Memproses {job_id}...", total=100)
            for status_res in stub.PantauStatus(req, timeout=timeout):
                last_response = status_res
                progress.update(
                    task,
                    completed=status_res.progress_percent,
                    description=f"[cyan]{pb.JobStatus.Name(status_res.status)}: {status_res.message[:60]}",
                )
    else:
        for status_res in stub.PantauStatus(req, timeout=timeout):
            last_response = status_res

    return last_response


def unduh_gambar(
    stub: pb_grpc.ImageServiceStub,
    job_id: str,
    output_path: str,
    timeout: float = 10.0,
    verbose: bool = True,
) -> int:
    """
    Mengunduh citra hasil pemrosesan melalui Server Streaming RPC (UnduhGambar).
    Menampilkan progress bar download dengan informasi byte dan chunk.
    """
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    req = pb.DownloadRequest(job_id=job_id)

    received_bytes = bytearray()
    chunk_count = 0
    total_expected = 0

    if verbose:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            TextColumn("{task.completed:,}/{task.total:,} bytes"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[green]Mengunduh...", total=0)
            for chunk in stub.UnduhGambar(req, timeout=timeout):
                chunk_count += 1
                received_bytes.extend(chunk.chunk_data)
                if chunk.total_bytes and total_expected == 0:
                    total_expected = chunk.total_bytes
                    progress.update(task, total=total_expected)
                progress.update(task, completed=len(received_bytes))
    else:
        for chunk in stub.UnduhGambar(req, timeout=timeout):
            chunk_count += 1
            received_bytes.extend(chunk.chunk_data)

    with open(output_path, "wb") as f:
        f.write(received_bytes)

    if verbose:
        console.print(
            f"  [bold green][OK][/bold green] [UNDUH SUKSES] {len(received_bytes):,} bytes "
            f"dalam {chunk_count} chunk -> {output_path}"
        )
    return len(received_bytes)


# Alias untuk kompatibilitas ke belakang (misal dipanggil dari skrip benchmark lama)
unduh_gambar_grayscale = unduh_gambar


def list_jobs_server(
    stub: pb_grpc.ImageServiceStub,
    active_only: bool = False,
    timeout: float = 3.0,
) -> pb.ListJobsResponse:
    """
    Mengambil daftar semua pekerjaan dari server melalui Unary RPC (ListJobs).
    """
    req = pb.ListJobsRequest(filter_active_only=active_only)
    return stub.ListJobs(req, timeout=timeout)


def batalkan_job(
    stub: pb_grpc.ImageServiceStub,
    job_id: str,
    timeout: float = 3.0,
) -> pb.CancelResponse:
    """
    Membatalkan pekerjaan di server melalui Unary RPC (BatalkanJob).
    """
    req = pb.CancelRequest(job_id=job_id)
    return stub.BatalkanJob(req, timeout=timeout)


def jalankan_pipeline_lengkap(
    target: str,
    file_path: str,
    output_path: str,
    processing_type: int = pb.PROC_GRAYSCALE,
    verbose: bool = True,
) -> dict:
    """
    Menjalankan alur kerja end-to-end 4 RPC untuk satu citra:
    1. UploadGambar (Client Streaming + UUID Request ID + Retry)
    2. CekStatus    (Unary RPC via Async Future)
    3. PantauStatus (Server Streaming Real-Time Progress)
    4. UnduhGambar  (Server Streaming Chunk Download)
    """
    t_start = time.perf_counter()

    interceptor = LoggingClientInterceptor(verbose=False)
    channel = grpc.insecure_channel(target)
    channel = grpc.intercept_channel(channel, interceptor)

    try:
        stub = pb_grpc.ImageServiceStub(channel)

        # 1. Upload Gambar (Client Streaming)
        up_res, attempts = upload_gambar_dengan_retry(
            stub=stub,
            file_path=file_path,
            processing_type=processing_type,
            verbose=verbose,
        )
        job_id = up_res.job_id

        # 2. Cek Status via Async Future (Unary RPC Asinkron)
        cek_status_pekerjaan(stub, job_id=job_id, use_async_future=True, verbose=verbose)

        # 3. Pantau Status Real-Time hingga selesai (Server Streaming)
        final_status = pantau_status_stream(stub, job_id=job_id, verbose=verbose)

        if final_status is None or final_status.status != pb.STATUS_COMPLETED:
            raise RuntimeError(f"Pemrosesan gambar gagal untuk Job ID {job_id}")

        # 4. Unduh Gambar Hasil (Server Streaming)
        out_bytes = unduh_gambar(
            stub=stub,
            job_id=job_id,
            output_path=output_path,
            verbose=verbose,
        )
    finally:
        channel.close()

    elapsed_ms = (time.perf_counter() - t_start) * 1000.0
    if verbose:
        console.print(f"  [bold green][OK] PIPELINE SELESAI[/bold green] Total waktu: {elapsed_ms:.2f} ms\n")
    return {
        "job_id": job_id,
        "attempts": attempts,
        "output_bytes": out_bytes,
        "latency_ms": elapsed_ms,
    }


# ============================================================
# CLI INTERAKTIF
# ============================================================

def _pilih_filter() -> int:
    """Menampilkan menu pemilihan filter dan mengembalikan ProcessingType."""
    table = Table(title="Filter Pemrosesan Citra", box=box.ROUNDED)
    table.add_column("No", style="bold cyan", justify="center", width=4)
    table.add_column("Filter", style="bold white", width=20)
    table.add_column("Deskripsi", style="dim", width=50)

    for key, (_, name, desc) in FILTER_MAP.items():
        table.add_row(key, name, desc)

    console.print(table)
    choice = Prompt.ask(
        "[cyan]Pilih nomor filter[/cyan]",
        choices=list(FILTER_MAP.keys()),
        default="1",
    )
    proc_type, name, _ = FILTER_MAP[choice]
    console.print(f"  Filter dipilih: [bold]{name}[/bold]")
    return proc_type


def _pilih_file() -> str:
    """Meminta path file dari user dengan validasi."""
    while True:
        path = Prompt.ask(
            "[cyan]Path file gambar[/cyan]",
            default="client/samples/sample.jpg",
        )
        valid, msg = validate_image_file(path)
        if valid:
            console.print(f"  [bold green][OK][/bold green] {msg}")
            return path
        else:
            console.print(f"  [bold red][FAIL][/bold red] {msg}")
            if not Confirm.ask("Coba lagi?", default=True):
                return ""


def _tampilkan_daftar_jobs(stub: pb_grpc.ImageServiceStub):
    """Menampilkan tabel daftar pekerjaan dari server."""
    try:
        res = list_jobs_server(stub)
        if res.total_count == 0:
            console.print("[yellow]Belum ada pekerjaan di server.[/yellow]")
            return

        table = Table(
            title=f"Daftar Pekerjaan di Server ({res.total_count} total)",
            box=box.ROUNDED,
        )
        table.add_column("Job ID", style="bold cyan", width=16)
        table.add_column("File", width=20)
        table.add_column("Filter", width=12)
        table.add_column("Status", width=16)
        table.add_column("Progress", justify="center", width=10)
        table.add_column("Input (B)", justify="right", width=11)
        table.add_column("Output (B)", justify="right", width=11)
        table.add_column("Pesan", width=38)

        STATUS_COLORS = {
            pb.STATUS_QUEUED: "yellow",
            pb.STATUS_PROCESSING: "blue",
            pb.STATUS_COMPLETED: "green",
            pb.STATUS_FAILED: "red",
            pb.STATUS_CANCELLED: "dim",
        }

        for job in res.jobs:
            status_name = pb.JobStatus.Name(job.status)
            color = STATUS_COLORS.get(job.status, "white")
            filter_name = PROCESSING_TYPE_NAMES.get(job.processing_type, "?")
            table.add_row(
                job.job_id,
                job.filename[:20],
                filter_name,
                f"[{color}]{status_name}[/{color}]",
                f"{job.progress_percent}%",
                f"{job.input_size:,}",
                f"{job.output_size:,}" if job.output_size else "-",
                job.message[:38] + "..." if len(job.message) > 38 else job.message,
            )

        console.print(table)
    except grpc.RpcError as exc:
        console.print(f"[red]Error: {exc.code().name} - {exc.details()}[/red]")


def _tampilkan_stats_interceptor():
    """Menampilkan statistik panggilan RPC yang tercatat oleh interceptor."""
    stats = get_global_stats().get_summary()
    if stats["total_calls"] == 0:
        console.print("[yellow]Belum ada panggilan RPC yang tercatat pada sesi ini.[/yellow]")
        return

    # Panel ringkasan
    summary = (
        f"Total Panggilan RPC : [bold]{stats['total_calls']}[/bold]\n"
        f"Total Error         : [bold red]{stats['total_errors']}[/bold red]\n"
        f"Error Rate          : [bold]{stats['error_rate_pct']:.1f}%[/bold]\n"
        f"Rata-rata Latensi   : [bold]{stats['avg_latency_ms']:.2f} ms[/bold]"
    )
    console.print(Panel(summary, title="[bold]Statistik RPC Interceptor[/bold]", border_style="cyan"))

    # Tabel per metode
    table = Table(title="Detail per Metode RPC", box=box.ROUNDED)
    table.add_column("Metode RPC", style="bold", width=20)
    table.add_column("Jumlah", justify="center", width=10)
    table.add_column("Avg (ms)", justify="right", width=12)
    table.add_column("Min (ms)", justify="right", width=12)
    table.add_column("Max (ms)", justify="right", width=12)
    table.add_column("Errors", justify="center", width=8)

    for method, info in stats["methods"].items():
        table.add_row(
            method,
            str(info["count"]),
            f"{info['avg_ms']:.2f}",
            f"{info['min_ms']:.2f}",
            f"{info['max_ms']:.2f}",
            str(info["errors"]),
        )

    console.print(table)


def _batch_upload(stub: pb_grpc.ImageServiceStub, target: str):
    """Upload beberapa file sekaligus dengan filter yang sama atau berbeda."""
    console.print("\n[bold]=== BATCH UPLOAD ===[/bold]")
    console.print("Masukkan path file (satu per baris). Ketik [bold]'selesai'[/bold] untuk mulai upload.\n")

    files = []
    while True:
        path = Prompt.ask(f"  File #{len(files)+1} (atau 'selesai')", default="selesai")
        if path.lower() == "selesai":
            break
        valid, msg = validate_image_file(path)
        if valid:
            console.print(f"    [bold green][OK][/bold green] {msg}")
            files.append(path)
        else:
            console.print(f"    [bold red][FAIL][/bold red] {msg}")

    if not files:
        console.print("[yellow]Tidak ada file untuk diupload.[/yellow]")
        return

    console.print(f"\n[cyan]{len(files)} file siap diupload.[/cyan]")
    same_filter = Confirm.ask("Gunakan filter yang sama untuk semua file?", default=True)

    if same_filter:
        proc_type = _pilih_filter()
        filters = [proc_type] * len(files)
    else:
        filters = []
        for i, f in enumerate(files):
            console.print(f"\n[cyan]Filter untuk: {os.path.basename(f)}[/cyan]")
            filters.append(_pilih_filter())

    console.print()
    results = []
    for i, (file_path, proc_type) in enumerate(zip(files, filters)):
        fname = os.path.basename(file_path)
        filter_name = PROCESSING_TYPE_NAMES.get(proc_type, "?")
        console.print(f"[bold]-- [{i+1}/{len(files)}] {fname} ({filter_name}) --[/bold]")

        out_name = f"batch_{i+1}_{filter_name.lower().replace(' ', '_')}_{fname}"
        out_path = os.path.join("client", "downloads", out_name)

        try:
            info = jalankan_pipeline_lengkap(
                target=target,
                file_path=file_path,
                output_path=out_path,
                processing_type=proc_type,
                verbose=True,
            )
            results.append({"file": fname, "status": "SUKSES", **info})
        except Exception as exc:
            console.print(f"  [bold red][FAIL] GAGAL: {exc}[/bold red]")
            results.append({"file": fname, "status": "GAGAL", "error": str(exc)})

    # Ringkasan batch
    console.print()
    table = Table(title="Ringkasan Batch Upload", box=box.ROUNDED)
    table.add_column("No", justify="center", width=4)
    table.add_column("File", width=25)
    table.add_column("Status", width=10)
    table.add_column("Job ID", width=16)
    table.add_column("Latensi (ms)", justify="right", width=14)

    for i, r in enumerate(results):
        color = "green" if r["status"] == "SUKSES" else "red"
        table.add_row(
            str(i + 1),
            r["file"],
            f"[{color}]{r['status']}[/{color}]",
            r.get("job_id", "-"),
            f"{r.get('latency_ms', 0):.2f}" if r["status"] == "SUKSES" else "-",
        )

    console.print(table)


def interactive_mode(target: str):
    """
    Mode CLI Interaktif: Menampilkan menu utama dan menangani
    input user dalam loop sampai user memilih keluar.
    """
    BANNER = """
[bold cyan]+==================================================================+
|       SISTEM ANTRIAN PENGOLAHAN CITRA TERDISTRIBUSI (gRPC)       |
|         Mata Kuliah: Komputasi Paralel & Terdistribusi           |
|              Tim: Zaki, Rafa, Khansa (247006111132)              |
+==================================================================+[/bold cyan]"""

    console.print(BANNER)
    console.print(f"  Server target : [bold]{target}[/bold]")
    console.print(f"  Chunk size    : [bold]{CHUNK_SIZE // 1024} KB[/bold]")
    console.print()

    # Buat channel dengan interceptor
    interceptor = LoggingClientInterceptor(verbose=False)
    channel = grpc.insecure_channel(target)
    channel = grpc.intercept_channel(channel, interceptor)
    stub = pb_grpc.ImageServiceStub(channel)

    try:
        while True:
            menu = Table(title="Menu Utama", box=box.ROUNDED, show_header=False, padding=(0, 2))
            menu.add_column("No", style="bold cyan", width=4, justify="center")
            menu.add_column("Aksi", width=55)
            menu.add_row("1", "[UPLOAD]   Upload & Proses Gambar (Pipeline Lengkap)")
            menu.add_row("2", "[BATCH]    Batch Upload (Multi-File)")
            menu.add_row("3", "[STATUS]   Cek Status Pekerjaan")
            menu.add_row("4", "[LIST]     Lihat Daftar Semua Pekerjaan (ListJobs)")
            menu.add_row("5", "[DOWNLOAD] Unduh Hasil Pekerjaan")
            menu.add_row("6", "[CANCEL]   Batalkan Pekerjaan")
            menu.add_row("7", "[STATS]    Statistik RPC Interceptor")
            menu.add_row("0", "[EXIT]     Keluar")
            console.print(menu)

            choice = Prompt.ask("[cyan]Pilih menu[/cyan]", choices=["0", "1", "2", "3", "4", "5", "6", "7"])

            if choice == "0":
                console.print("[dim]Terima kasih. Sampai jumpa![/dim]")
                break

            elif choice == "1":
                # -- Upload & Proses --
                console.print("\n[bold]=== UPLOAD & PROSES GAMBAR ===[/bold]")
                file_path = _pilih_file()
                if not file_path:
                    continue
                proc_type = _pilih_filter()

                fname = os.path.basename(file_path)
                filter_name = PROCESSING_TYPE_NAMES.get(proc_type, "grayscale")
                out_name = f"{filter_name.lower().replace(' ', '_')}_{fname}"
                out_path = os.path.join("client", "downloads", out_name)

                console.print()
                try:
                    jalankan_pipeline_lengkap(
                        target=target,
                        file_path=file_path,
                        output_path=out_path,
                        processing_type=proc_type,
                        verbose=True,
                    )
                except Exception as exc:
                    console.print(f"[bold red]Error: {exc}[/bold red]")

            elif choice == "2":
                # -- Batch Upload --
                _batch_upload(stub, target)

            elif choice == "3":
                # -- Cek Status --
                console.print("\n[bold]=== CEK STATUS PEKERJAAN ===[/bold]")
                job_id = Prompt.ask("[cyan]Masukkan Job ID[/cyan]")
                try:
                    cek_status_pekerjaan(stub, job_id, use_async_future=True, verbose=True)
                except grpc.RpcError as exc:
                    console.print(f"[bold red]Error: {exc.code().name} - {exc.details()}[/bold red]")

            elif choice == "4":
                # -- List Jobs --
                console.print("\n[bold]=== DAFTAR PEKERJAAN DI SERVER ===[/bold]")
                _tampilkan_daftar_jobs(stub)

            elif choice == "5":
                # -- Download --
                console.print("\n[bold]=== UNDUH HASIL PEKERJAAN ===[/bold]")
                job_id = Prompt.ask("[cyan]Masukkan Job ID[/cyan]")
                out_path = Prompt.ask(
                    "[cyan]Path output[/cyan]",
                    default=f"client/downloads/{job_id}_hasil.jpg",
                )
                try:
                    unduh_gambar(stub, job_id, out_path, verbose=True)
                except grpc.RpcError as exc:
                    console.print(f"[bold red]Error: {exc.code().name} - {exc.details()}[/bold red]")

            elif choice == "6":
                # -- Batalkan --
                console.print("\n[bold]=== BATALKAN PEKERJAAN ===[/bold]")
                job_id = Prompt.ask("[cyan]Masukkan Job ID yang ingin dibatalkan[/cyan]")
                try:
                    res = batalkan_job(stub, job_id)
                    if res.success:
                        console.print(f"[bold green][OK] {res.message}[/bold green]")
                    else:
                        console.print(f"[bold yellow][WARN] {res.message}[/bold yellow]")
                except grpc.RpcError as exc:
                    console.print(f"[bold red]Error: {exc.code().name} - {exc.details()}[/bold red]")

            elif choice == "7":
                # -- Stats --
                console.print("\n[bold]=== STATISTIK RPC INTERCEPTOR ===[/bold]")
                _tampilkan_stats_interceptor()

            console.print()

    finally:
        channel.close()


# ============================================================
# ENTRYPOINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Client gRPC Antrian Pengolahan Citra - Mode Interaktif & CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Mode penggunaan:
  python -m client.client                           # Mode interaktif (default)
  python -m client.client --action full-pipeline    # Pipeline langsung
  python -m client.client --action upload --file foto.jpg --filter sepia
  python -m client.client --action status --job-id JOB-XXXXXXXX
  python -m client.client --action list-jobs
        """,
    )
    parser.add_argument("--target", default="127.0.0.1:50051", help="Alamat server gRPC")
    parser.add_argument(
        "--action",
        choices=["interactive", "upload", "status", "monitor", "download", "full-pipeline", "list-jobs", "cancel"],
        default="interactive",
        help="Aksi RPC yang ingin dijalankan (default: interactive)",
    )
    parser.add_argument("--file", default="client/samples/sample.jpg", help="Path file citra input")
    parser.add_argument("--job-id", default="", help="Job ID untuk aksi status/monitor/download/cancel")
    parser.add_argument("--output", default="client/downloads/hasil.jpg", help="Path file unduhan")
    parser.add_argument(
        "--filter",
        choices=["grayscale", "blur", "sepia", "invert", "edge", "sharpen"],
        default="grayscale",
        help="Jenis filter pemrosesan (default: grayscale)",
    )
    args = parser.parse_args()

    # Mapping nama filter CLI ke enum
    filter_cli_map = {
        "grayscale": pb.PROC_GRAYSCALE,
        "blur": pb.PROC_BLUR,
        "sepia": pb.PROC_SEPIA,
        "invert": pb.PROC_INVERT,
        "edge": pb.PROC_EDGE_DETECT,
        "sharpen": pb.PROC_SHARPEN,
    }
    proc_type = filter_cli_map.get(args.filter, pb.PROC_GRAYSCALE)

    if args.action == "interactive":
        interactive_mode(args.target)
    elif args.action == "full-pipeline":
        jalankan_pipeline_lengkap(
            target=args.target,
            file_path=args.file,
            output_path=args.output,
            processing_type=proc_type,
            verbose=True,
        )
    else:
        interceptor = LoggingClientInterceptor(verbose=False)
        channel = grpc.insecure_channel(args.target)
        channel = grpc.intercept_channel(channel, interceptor)
        try:
            stub = pb_grpc.ImageServiceStub(channel)
            if args.action == "upload":
                upload_gambar_dengan_retry(stub, args.file, processing_type=proc_type, verbose=True)
            elif args.action == "status":
                cek_status_pekerjaan(stub, args.job_id, use_async_future=True, verbose=True)
            elif args.action == "monitor":
                pantau_status_stream(stub, args.job_id, verbose=True)
            elif args.action == "download":
                unduh_gambar(stub, args.job_id, args.output, verbose=True)
            elif args.action == "list-jobs":
                _tampilkan_daftar_jobs(stub)
            elif args.action == "cancel":
                res = batalkan_job(stub, args.job_id)
                if res.success:
                    console.print(f"[bold green][OK] {res.message}[/bold green]")
                else:
                    console.print(f"[bold yellow][WARN] {res.message}[/bold yellow]")
        finally:
            channel.close()


if __name__ == "__main__":
    main()
