"""
==============================================================
MATA KULIAH : KOMPUTASI PARALEL & TERDISTRIBUSI
PROYEK MINI : SISTEM ANTRIAN PENGOLAHAN CITRA BERBASIS gRPC
FILE        : client/client.py (gRPC Client, Streaming, Retry & Async)
TIM         : Zaki, Rafa, Khansa Rasendriya Aji (247006111132)
==============================================================
"""

import argparse
import os
import time
import uuid
from typing import Iterator, Optional
import grpc

from protos import image_service_pb2 as pb
from protos import image_service_pb2_grpc as pb_grpc

CHUNK_SIZE = 64 * 1024  # 64 KB per chunk


def _generate_upload_chunks(
    file_path: str,
    request_id: str,
    chunk_size: int = CHUNK_SIZE,
) -> Iterator[pb.UploadChunk]:
    """
    Generator untuk Client Streaming RPC (UploadGambar):
    - Pesan pertama mengirimkan ImageMetadata (termasuk request_id unik).
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
            chunk_iter = _generate_upload_chunks(file_path, request_id=rid)
            response = stub.UploadGambar(chunk_iter, timeout=timeout)
            if verbose:
                print(
                    f"[UPLOAD SUKSES] Percobaan #{attempt} | RequestID={rid[:8]}... | "
                    f"JobID={response.job_id} | Pesan: {response.message}"
                )
            return response, attempt
        except grpc.RpcError as exc:
            retryable = exc.code() in (
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.UNAVAILABLE,
            )
            if not retryable or attempt == max_retries:
                if verbose:
                    print(f"[UPLOAD GAGAL] Percobaan #{attempt} gagal permanen: {exc.code().name} - {exc.details()}")
                raise
            backoff = 0.5 * (2 ** (attempt - 1))
            if verbose:
                print(
                    f"[RETRY BACKOFF] Percobaan #{attempt} gagal ({exc.code().name}), "
                    f"mengulang dengan RequestID tetap ({rid[:8]}...) dalam {backoff:.1f} detik..."
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
        print(
            f"[CEK STATUS - {mode_str}] JobID={res.job_id} | "
            f"Status={pb.JobStatus.Name(res.status)} ({res.progress_percent}%) | {res.message}"
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
    """
    req = pb.StatusRequest(job_id=job_id)
    last_response = None
    for status_res in stub.PantauStatus(req, timeout=timeout):
        last_response = status_res
        if verbose:
            print(
                f"  -> [STREAM STATUS] {status_res.job_id} | "
                f"{pb.JobStatus.Name(status_res.status):<17} | "
                f"Progres: {status_res.progress_percent:>3}% | {status_res.message}"
            )
    return last_response


def unduh_gambar_grayscale(
    stub: pb_grpc.ImageServiceStub,
    job_id: str,
    output_path: str,
    timeout: float = 10.0,
    verbose: bool = True,
) -> int:
    """
    Mengunduh citra hasil konversi grayscale melalui Server Streaming RPC (UnduhGambar).
    Mengembalikan total byte yang berhasil disimpan.
    """
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    req = pb.DownloadRequest(job_id=job_id)

    received_bytes = bytearray()
    chunk_count = 0
    for chunk in stub.UnduhGambar(req, timeout=timeout):
        chunk_count += 1
        received_bytes.extend(chunk.chunk_data)

    with open(output_path, "wb") as f:
        f.write(received_bytes)

    if verbose:
        print(
            f"[UNDUH SUKSES] JobID={job_id} | Diterima {len(received_bytes)} bytes "
            f"dalam {chunk_count} chunk -> Disimpan ke: {output_path}"
        )
    return len(received_bytes)


def jalankan_pipeline_lengkap(
    target: str,
    file_path: str,
    output_path: str,
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
    with grpc.insecure_channel(target) as channel:
        stub = pb_grpc.ImageServiceStub(channel)

        # 1. Upload Gambar (Client Streaming)
        up_res, attempts = upload_gambar_dengan_retry(
            stub=stub,
            file_path=file_path,
            verbose=verbose,
        )
        job_id = up_res.job_id

        # 2. Cek Status via Async Future (Unary RPC Asinkron)
        cek_status_pekerjaan(stub, job_id=job_id, use_async_future=True, verbose=verbose)

        # 3. Pantau Status Real-Time hingga selesai (Server Streaming)
        final_status = pantau_status_stream(stub, job_id=job_id, verbose=verbose)

        if final_status is None or final_status.status != pb.STATUS_COMPLETED:
            raise RuntimeError(f"Pemrosesan gambar gagal untuk Job ID {job_id}")

        # 4. Unduh Gambar Hasil Grayscale (Server Streaming)
        out_bytes = unduh_gambar_grayscale(
            stub=stub,
            job_id=job_id,
            output_path=output_path,
            verbose=verbose,
        )

    elapsed_ms = (time.perf_counter() - t_start) * 1000.0
    if verbose:
        print(f"[PIPELINE SELESAI] Total waktu end-to-end: {elapsed_ms:.2f} ms\n")
    return {
        "job_id": job_id,
        "attempts": attempts,
        "output_bytes": out_bytes,
        "latency_ms": elapsed_ms,
    }


def main():
    parser = argparse.ArgumentParser(description="Client gRPC Antrian Pengolahan Citra")
    parser.add_argument("--target", default="127.0.0.1:50051", help="Alamat server gRPC (default: 127.0.0.1:50051)")
    parser.add_argument(
        "--action",
        choices=["upload", "status", "monitor", "download", "full-pipeline"],
        default="full-pipeline",
        help="Aksi RPC yang ingin dijalankan",
    )
    parser.add_argument("--file", default="client/samples/sample.jpg", help="Path file citra input")
    parser.add_argument("--job-id", default="", help="Job ID untuk aksi status/monitor/download")
    parser.add_argument("--output", default="client/downloads/hasil_grayscale.jpg", help="Path file unduhan")
    args = parser.parse_args()

    if args.action == "full-pipeline":
        jalankan_pipeline_lengkap(
            target=args.target,
            file_path=args.file,
            output_path=args.output,
            verbose=True,
        )
    else:
        with grpc.insecure_channel(args.target) as channel:
            stub = pb_grpc.ImageServiceStub(channel)
            if args.action == "upload":
                upload_gambar_dengan_retry(stub, args.file, verbose=True)
            elif args.action == "status":
                cek_status_pekerjaan(stub, args.job_id, use_async_future=True, verbose=True)
            elif args.action == "monitor":
                pantau_status_stream(stub, args.job_id, verbose=True)
            elif args.action == "download":
                unduh_gambar_grayscale(stub, args.job_id, args.output, verbose=True)


if __name__ == "__main__":
    main()
