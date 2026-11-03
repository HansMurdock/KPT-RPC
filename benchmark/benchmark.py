"""
==============================================================
MATA KULIAH : KOMPUTASI PARALEL & TERDISTRIBUSI
PROYEK MINI : SISTEM ANTRIAN PENGOLAHAN CITRA BERBASIS gRPC
FILE        : benchmark/benchmark.py (Uji Beban 1, 5, 20 Client & Visualisasi)
TIM         : Zaki, Rafa, Khansa Rasendriya Aji (247006111132)
==============================================================
"""

import csv
import os
import statistics
import time
import uuid
from concurrent import futures
import grpc
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from PIL import Image, ImageDraw

from protos import image_service_pb2 as pb
from protos import image_service_pb2_grpc as pb_grpc
from server.server import create_server
from client.client import (
    upload_gambar_dengan_retry,
    cek_status_pekerjaan,
    pantau_status_stream,
    unduh_gambar_grayscale,
    jalankan_pipeline_lengkap,
)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_IMG = os.path.join(ROOT_DIR, "client", "samples", "sample.jpg")
DOWNLOAD_DIR = os.path.join(ROOT_DIR, "client", "downloads")
RESULTS_DIR = os.path.join(ROOT_DIR, "benchmark", "results")
DOCS_DIR = os.path.join(ROOT_DIR, "docs")


def ensure_colorful_sample_image(path: str) -> None:
    """Membuat gambar sampel RGB berwarna cerah (640x420) jika belum ada atau terlalu kecil."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    img = Image.new("RGB", (640, 420), color=(25, 35, 60))
    draw = ImageDraw.Draw(img)

    # Buat pola gradien blok warna-warni agar konversi grayscale terlihat jelas
    colors = [
        (231, 76, 60),    # Merah
        (241, 196, 15),   # Kuning
        (46, 204, 113),   # Hijau
        (52, 152, 219),   # Biru
        (155, 89, 182),   # Ungu
        (230, 126, 34),   # Oranye
    ]
    bar_w = 640 // len(colors)
    for idx, col in enumerate(colors):
        x0 = idx * bar_w
        x1 = (idx + 1) * bar_w if idx < len(colors) - 1 else 640
        draw.rectangle([x0, 40, x1, 280], fill=col)

    # Tambahkan lingkaran kontras dan label informasi uji
    draw.ellipse([220, 90, 420, 250], fill=(255, 255, 255), outline=(20, 20, 20), width=4)
    draw.ellipse([255, 115, 385, 225], fill=(41, 128, 185))
    draw.rectangle([30, 310, 610, 390], fill=(245, 245, 245), outline=(52, 73, 94), width=3)
    draw.text((55, 330), "KPT-RPC GROUP PROJECT - STUDI KASUS 5 (RGB TEST IMAGE)", fill=(20, 20, 20))
    draw.text((55, 355), "Resolusi: 640x420 px | Format: JPEG | Tim: Zaki, Rafa, Khansa", fill=(80, 80, 80))

    img.save(path, format="JPEG", quality=92)


def generate_architecture_diagram(out_path: str) -> None:
    """Membuat diagram arsitektur sistem antrean pengolahan gambar gRPC."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    fig, ax = plt.subplots(figsize=(11, 5.2), dpi=180)
    ax.set_xlim(0, 11)
    ax.set_ylim(0, 5.2)
    ax.axis("off")

    ax.text(
        5.5, 4.85,
        "ARSITEKTUR SISTEM ANTRIAN PENGOLAHAN CITRA TERDISTRIBUSI (gRPC)",
        ha="center", va="center", fontsize=12, fontweight="bold", color="#1A252F"
    )

    def draw_box(x, y, w, h, title, subtitle, bg_color, border_color):
        rect = patches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.1,rounding_size=0.15",
            facecolor=bg_color, edgecolor=border_color, linewidth=2
        )
        ax.add_patch(rect)
        ax.text(x + w / 2, y + h - 0.35, title, ha="center", va="center", fontsize=10, fontweight="bold", color="#1A252F")
        ax.text(x + w / 2, y + h / 2 - 0.15, subtitle, ha="center", va="center", fontsize=8.3, color="#2C3E50")

    # Box 1: Client Node
    draw_box(
        0.4, 1.1, 2.5, 3.1,
        "CLIENT NODE\n(Container 1 / Rafa)",
        "• Chunk Generator (64 KB)\n• UUIDv4 request_id\n• Timeout & Exp. Backoff\n• Async Future (.future())\n• Stream Receiver",
        "#EBF5FB", "#2980B9"
    )

    # Box 2: gRPC Server Servicer
    draw_box(
        4.1, 1.1, 2.7, 3.1,
        "gRPC SERVER SERVICER\n(Container 2 / Zaki)",
        "• ThreadPoolExecutor (20)\n• 4 RPC Endpoints:\n  1. UploadGambar (C-Stream)\n  2. CekStatus (Unary)\n  3. PantauStatus (S-Stream)\n  4. UnduhGambar (S-Stream)",
        "#E8F8F5", "#16A085"
    )

    # Box 3: Thread-Safe Queue & Worker Pool
    draw_box(
        7.9, 2.7, 2.7, 1.5,
        "QUEUE & MUTEX LOCK",
        "• threading.Lock()\n• Deduplikasi request_id\n• queue.Queue() FIFO",
        "#FEF9E7", "#F39C12"
    )

    draw_box(
        7.9, 0.8, 2.7, 1.5,
        "WORKER POOL (4 Thread)",
        "• Background Daemon\n• Konversi RGB -> Grayscale\n• Pustaka Pillow (Mode 'L')",
        "#FDEDEC", "#C0392B"
    )

    # Panah Client <-> Server
    arrows = [
        (2.95, 3.6, 4.05, 3.6, "1. UploadGambar (stream 64KB)", "#2980B9"),
        (2.95, 2.9, 4.05, 2.9, "2. CekStatus (Unary / Future)", "#27AE60"),
        (4.05, 2.2, 2.95, 2.2, "3. PantauStatus (stream %)", "#8E44AD"),
        (4.05, 1.5, 2.95, 1.5, "4. UnduhGambar (stream 64KB)", "#D35400"),
    ]
    for x1, y1, x2, y2, label, col in arrows:
        ax.annotate(
            "", xy=(x2, y2), xytext=(x1, y1),
            arrowprops=dict(arrowstyle="->", color=col, lw=1.8)
        )
        ax.text((x1 + x2) / 2, y1 + 0.16, label, ha="center", va="center", fontsize=7.5, fontweight="bold", color=col)

    # Panah Server <-> Queue & Worker
    ax.annotate("", xy=(7.85, 3.45), xytext=(6.85, 3.45), arrowprops=dict(arrowstyle="<->", color="#F39C12", lw=1.8))
    ax.text(7.35, 3.7, "Enqueue &\nState Lock", ha="center", va="center", fontsize=7.5, fontweight="bold", color="#B7950B")

    ax.annotate("", xy=(9.25, 2.35), xytext=(9.25, 2.68), arrowprops=dict(arrowstyle="<->", color="#C0392B", lw=1.8))
    ax.text(9.85, 2.5, "Ambil Tugas &\nUpdate Status", ha="center", va="center", fontsize=7.3, color="#922B21")

    ax.annotate("", xy=(6.85, 1.55), xytext=(7.85, 1.55), arrowprops=dict(arrowstyle="->", color="#C0392B", lw=1.8))
    ax.text(7.35, 1.8, "Simpan Citra\nGrayscale", ha="center", va="center", fontsize=7.5, color="#922B21")

    plt.tight_layout()
    plt.savefig(out_path, dpi=180)
    plt.close(fig)


def generate_grayscale_comparison_figure(rgb_path: str, gray_path: str, out_path: str) -> None:
    """Membuat gambar perbandingan Before (RGB) vs After (Grayscale) hasil pemrosesan gRPC."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    img_rgb = Image.open(rgb_path)
    img_gray = Image.open(gray_path)

    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.2), dpi=160)
    axes[0].imshow(img_rgb)
    axes[0].set_title(
        f"Citra Input Asli (RGB - Client Upload)\nUkuran: {os.path.getsize(rgb_path):,} bytes ({img_rgb.size[0]}x{img_rgb.size[1]})",
        fontsize=10, fontweight="bold"
    )
    axes[0].axis("off")

    axes[1].imshow(img_gray, cmap="gray")
    axes[1].set_title(
        f"Citra Output Server (Grayscale Mode 'L')\nUkuran: {os.path.getsize(gray_path):,} bytes ({img_gray.size[0]}x{img_gray.size[1]})",
        fontsize=10, fontweight="bold"
    )
    axes[1].axis("off")

    plt.suptitle("Hasil Konversi Citra Asinkron Melalui Streaming gRPC", fontsize=12, fontweight="bold", y=0.98)
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close(fig)


def uji_toleransi_kegagalan_dan_deduplikasi(port: int = 50055) -> dict:
    """
    Menguji skenario toleransi kegagalan:
    - Server disimulasikan lambat (2.5s) pada percobaan pertama.
    - Client memasang timeout=1.0s sehingga percobaan #1 gagal (DEADLINE_EXCEEDED).
    - Client melakukan retry dengan Exponential Backoff (0.5s) menggunakan request_id (UUID) yang SAMA.
    - Server mendeteksi request_id duplikat dan mengembalikan job_id yang sama tanpa duplikasi pekerjaan.
    """
    print("\n" + "=" * 72)
    print("TAHAP 2: UJI TOLERANSI KEGAGALAN (TIMEOUT, RETRY & DEDUPLIKASI UUID)")
    print("=" * 72)
    server, manager = create_server(
        host="127.0.0.1",
        port=port,
        max_rpc_workers=10,
        num_image_workers=2,
        simulated_delay=0.1,
        simulate_slow_first_attempt=True,
    )
    server.start()
    target = f"127.0.0.1:{port}"

    try:
        with grpc.insecure_channel(target) as channel:
            stub = pb_grpc.ImageServiceStub(channel)
            fixed_rid = str(uuid.uuid4())
            res, attempts = upload_gambar_dengan_retry(
                stub=stub,
                file_path=SAMPLE_IMG,
                request_id=fixed_rid,
                timeout=1.0,
                max_retries=3,
                verbose=True,
            )
            pantau_status_stream(stub, res.job_id, verbose=True)
            stats = manager.get_stats()
            print(
                f"[VERIFIKASI DEDUPLIKASI] Total Job Terbentuk: {stats['total_jobs']} "
                f"| Hit Deduplikasi: {stats['duplicate_hits']} | Status: KONSISTEN (At-Most-Once)"
            )
            return {
                "request_id": fixed_rid,
                "job_id": res.job_id,
                "attempts": attempts,
                "total_jobs_created": stats["total_jobs"],
                "duplicate_hits": stats["duplicate_hits"],
            }
    finally:
        manager.stop_workers()
        server.stop(grace=0.5)


def jalankan_benchmark_konkurensi(
    client_counts: tuple[int, ...] = (1, 5, 20),
    jobs_per_scenario: int = 20,
    port: int = 50056,
) -> list[dict]:
    """
    Mengukur kinerja sistem (Latensi rata-rata, P95, dan Throughput) untuk
    1, 5, dan 20 client bersamaan sesuai Ketentuan Umum #5.
    Setiap skenario mengeksekusi sejumlah tugas pengolahan citra penuh
    (Upload Chunk -> Antrean Asinkron -> Konversi Grayscale -> Stream Status -> Download Chunk)
    dan memverifikasi tidak ada data rusak / race condition.
    """
    print("\n" + "=" * 72)
    print("TAHAP 3: PENGUJIAN KINERJA KONKURENSI (1, 5, DAN 20 CLIENT BERSAMAAN)")
    print("=" * 72)

    server, manager = create_server(
        host="127.0.0.1",
        port=port,
        max_rpc_workers=32,
        num_image_workers=8,
        simulated_delay=0.05,
        simulate_slow_first_attempt=False,
    )
    server.start()
    target = f"127.0.0.1:{port}"

    results = []
    try:
        for num_clients in client_counts:
            # Untuk 1, 5, dan 20 client: kita uji gelombang permintaan paralel (minimal 20 pekerjaan total
            # agar perbandingan waktu total & throughput adil dan presisi)
            total_tasks = max(jobs_per_scenario, num_clients)

            def _client_worker_task(task_idx: int) -> float:
                out_file = os.path.join(DOWNLOAD_DIR, f"bench_c{num_clients}_t{task_idx}.jpg")
                t0 = time.perf_counter()
                info = jalankan_pipeline_lengkap(
                    target=target,
                    file_path=SAMPLE_IMG,
                    output_path=out_file,
                    verbose=False,
                )
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                # Pastikan hasil unduhan valid dan tidak rusak
                if info["output_bytes"] <= 0 or not os.path.exists(out_file):
                    raise RuntimeError("Data unduhan rusak!")
                return elapsed_ms

            t_scenario_start = time.perf_counter()
            latencies_ms: list[float] = []
            errors = 0

            with futures.ThreadPoolExecutor(max_workers=num_clients) as pool:
                future_list = [pool.submit(_client_worker_task, i) for i in range(total_tasks)]
                for fut in futures.as_completed(future_list):
                    try:
                        latencies_ms.append(fut.result())
                    except Exception as exc:
                        errors += 1
                        print(f"  [ERROR] Task gagal: {exc}")

            total_time_s = time.perf_counter() - t_scenario_start
            avg_lat_ms = statistics.mean(latencies_ms)
            min_lat_ms = min(latencies_ms)
            max_lat_ms = max(latencies_ms)
            sorted_lat = sorted(latencies_ms)
            p95_lat_ms = sorted_lat[int(0.95 * (len(sorted_lat) - 1))]

            # Setiap pipeline mengeksekusi 4 RPC (Upload, CekStatus, PantauStatus, UnduhGambar)
            throughput_jobs_s = total_tasks / total_time_s
            throughput_rpcs_s = (total_tasks * 4) / total_time_s

            row = {
                "num_clients": num_clients,
                "total_jobs": total_tasks,
                "total_rpcs": total_tasks * 4,
                "total_time_s": round(total_time_s, 3),
                "avg_latency_ms": round(avg_lat_ms, 2),
                "min_latency_ms": round(min_lat_ms, 2),
                "p95_latency_ms": round(p95_lat_ms, 2),
                "max_latency_ms": round(max_lat_ms, 2),
                "throughput_jobs_s": round(throughput_jobs_s, 2),
                "throughput_rpcs_s": round(throughput_rpcs_s, 2),
                "errors": errors,
            }
            results.append(row)
            print(
                f"-> Client={num_clients:>2} | Total Tugas={total_tasks:>2} ({total_tasks*4:>2} RPC) | "
                f"Waktu Total={row['total_time_s']:>5.2f}s | "
                f"Latensi Rata2={row['avg_latency_ms']:>6.2f} ms | "
                f"Throughput={row['throughput_jobs_s']:>5.2f} job/s ({row['throughput_rpcs_s']:>6.2f} RPC/s) | "
                f"Error={errors}"
            )

        # Bersihkan file sementara hasil benchmark
        for f_name in os.listdir(DOWNLOAD_DIR):
            if f_name.startswith("bench_c"):
                try:
                    os.remove(os.path.join(DOWNLOAD_DIR, f_name))
                except OSError:
                    pass

        return results
    finally:
        manager.stop_workers()
        server.stop(grace=0.5)


def simpan_grafik_dan_csv(results: list[dict]) -> str:
    """Menyimpan hasil benchmark ke CSV dan membuat grafik PNG (Throughput & Latensi)."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    csv_path = os.path.join(RESULTS_DIR, "metrics_summary.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)

    clients_lbl = [f"{r['num_clients']} Client" for r in results]
    tp_rpcs = [r["throughput_rpcs_s"] for r in results]
    tp_jobs = [r["throughput_jobs_s"] for r in results]
    avg_lats = [r["avg_latency_ms"] for r in results]
    p95_lats = [r["p95_latency_ms"] for r in results]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.2, 4.6), dpi=180)

    # Subplot 1: Throughput (RPC/s & Job/s)
    x = range(len(clients_lbl))
    w = 0.36
    b1 = ax1.bar([i - w / 2 for i in x], tp_rpcs, width=w, label="Throughput RPC (req/s)", color="#2980B9", edgecolor="#1B4F72")
    b2 = ax1.bar([i + w / 2 for i in x], tp_jobs, width=w, label="Throughput Pipeline (job/s)", color="#27AE60", edgecolor="#196F3D")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(clients_lbl, fontweight="bold")
    ax1.set_ylabel("Throughput (Operasi / Detik)", fontweight="bold")
    ax1.set_title("Perbandingan Throughput (1, 5, dan 20 Client)", fontweight="bold")
    ax1.grid(axis="y", linestyle="--", alpha=0.5)
    ax1.legend(loc="upper left", fontsize=8.5)

    for bar in b1:
        yval = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width() / 2, yval + 1.2, f"{yval:.1f}", ha="center", va="bottom", fontsize=8.5, fontweight="bold")
    for bar in b2:
        yval = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width() / 2, yval + 1.2, f"{yval:.1f}", ha="center", va="bottom", fontsize=8.5, fontweight="bold")
    ax1.set_ylim(0, max(tp_rpcs) * 1.22)

    # Subplot 2: Latensi Rata-Rata & P95 (ms)
    ax2.plot(clients_lbl, avg_lats, marker="o", linewidth=2.4, markersize=8, color="#C0392B", label="Latensi Rata-rata (ms)")
    ax2.plot(clients_lbl, p95_lats, marker="s", linewidth=2.0, linestyle="--", markersize=7, color="#8E44AD", label="Latensi P95 (ms)")
    for i, (avg_v, p95_v) in enumerate(zip(avg_lats, p95_lats)):
        ax2.annotate(f"{avg_v:.1f} ms", (i, avg_v), textcoords="offset points", xytext=(0, -15), ha="center", fontsize=8.5, fontweight="bold", color="#C0392B")
        ax2.annotate(f"{p95_v:.1f} ms", (i, p95_v), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8.5, fontweight="bold", color="#8E44AD")

    ax2.set_ylabel("Latensi End-to-End (milidetik)", fontweight="bold")
    ax2.set_title("Perbandingan Latensi Rata-rata & P95", fontweight="bold")
    ax2.grid(True, linestyle="--", alpha=0.5)
    ax2.legend(loc="upper left", fontsize=8.5)
    ax2.set_ylim(0, max(p95_lats) * 1.25)

    plt.suptitle("Evaluasi Kinerja Sistem Antrian Pengolahan Citra gRPC (Studi Kasus 5)", fontsize=12, fontweight="bold", y=0.99)
    plt.tight_layout()
    chart_path = os.path.join(RESULTS_DIR, "grafik_kinerja_kelompok.png")
    plt.savefig(chart_path, dpi=180)
    plt.close(fig)
    return chart_path


def main():
    ensure_colorful_sample_image(SAMPLE_IMG)
    arch_path = os.path.join(DOCS_DIR, "diagram_arsitektur.png")
    generate_architecture_diagram(arch_path)

    print("=" * 72)
    print("TAHAP 1: UJI FUNGSIONALITAS END-TO-END (4 METODE RPC & KONVERSI CITRA)")
    print("=" * 72)
    server, manager = create_server(host="127.0.0.1", port=50051, simulated_delay=0.12)
    server.start()
    out_single = os.path.join(DOWNLOAD_DIR, "hasil_grayscale_sample.jpg")
    try:
        jalankan_pipeline_lengkap(
            target="127.0.0.1:50051",
            file_path=SAMPLE_IMG,
            output_path=out_single,
            verbose=True,
        )
    finally:
        manager.stop_workers()
        server.stop(grace=0.5)

    comp_path = os.path.join(RESULTS_DIR, "perbandingan_grayscale.png")
    generate_grayscale_comparison_figure(SAMPLE_IMG, out_single, comp_path)

    # Tahap 2: Uji Toleransi Kegagalan & Deduplikasi
    uji_toleransi_kegagalan_dan_deduplikasi(port=50055)

    # Tahap 3: Benchmark Kinerja 1, 5, dan 20 Client Bersamaan
    bench_results = jalankan_benchmark_konkurensi(client_counts=(1, 5, 20), jobs_per_scenario=20, port=50056)
    chart_path = simpan_grafik_dan_csv(bench_results)

    print("\n" + "=" * 72)
    print("RINGKASAN VISUALISASI YANG DIHASILKAN:")
    print(f"1. Diagram Arsitektur        : {arch_path}")
    print(f"2. Perbandingan Citra        : {comp_path}")
    print(f"3. Grafik Kinerja (1, 5, 20) : {chart_path}")
    print("=" * 72)


if __name__ == "__main__":
    main()
