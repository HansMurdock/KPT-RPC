"""
==============================================================
MATA KULIAH : KOMPUTASI PARALEL & TERDISTRIBUSI
PROYEK MINI : SISTEM ANTRIAN PENGOLAHAN CITRA BERBASIS gRPC
FILE        : server/server.py (gRPC Servicer & Server Entrypoint)
TIM         : Zaki, Rafa, Khansa Rasendriya Aji (247006111132)
==============================================================
"""

import argparse
import os
import tempfile
import time
import uuid
from concurrent import futures
import grpc

from protos import image_service_pb2 as pb
from protos import image_service_pb2_grpc as pb_grpc
from server.worker import ImageJobManager

CHUNK_SIZE = 64 * 1024  # 64 KB per chunk


class ImageServiceServicer(pb_grpc.ImageServiceServicer):
    """
    Implementasi layanan gRPC ImageService dengan 4 metode RPC:
    1. UploadGambar (Client Streaming RPC)
    2. CekStatus    (Unary RPC)
    3. PantauStatus (Server Streaming RPC)
    4. UnduhGambar  (Server Streaming RPC)
    """

    def __init__(self, job_manager: ImageJobManager, simulate_slow_first_attempt: bool = False):
        self.job_manager = job_manager
        self.simulate_slow_first_attempt = simulate_slow_first_attempt
        self._seen_slow_requests: set[str] = set()

    def UploadGambar(self, request_iterator, context):
        """
        1. Client Streaming RPC:
        Menerima aliran UploadChunk dan menulis data langsung ke file sementara,
        sehingga seluruh gambar tidak perlu ditahan di RAM. Setelah upload valid,
        file dipindahkan ke storage job dan pekerjaan dimasukkan ke antrean.
        """
        metadata = None
        request_id = ""
        total_bytes = 0
        staged_input_path = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix="upload-",
                suffix=".part",
                dir=self.job_manager.input_dir,
                delete=False,
            ) as staged_file:
                staged_input_path = staged_file.name

                for chunk_index, chunk in enumerate(request_iterator):
                    payload_type = chunk.WhichOneof("payload")

                    if chunk_index == 0:
                        if payload_type != "metadata":
                            context.abort(
                                grpc.StatusCode.INVALID_ARGUMENT,
                                "Pesan pertama upload harus berisi metadata gambar.",
                            )
                        metadata = chunk.metadata
                        request_id = metadata.request_id.strip()
                        if not request_id:
                            context.abort(
                                grpc.StatusCode.INVALID_ARGUMENT,
                                "request_id wajib diisi agar retry dapat dideduplikasi.",
                            )
                        continue

                    if payload_type == "metadata":
                        context.abort(
                            grpc.StatusCode.INVALID_ARGUMENT,
                            "Metadata gambar hanya boleh dikirim sekali, pada pesan pertama.",
                        )
                    if payload_type != "chunk_data":
                        context.abort(
                            grpc.StatusCode.INVALID_ARGUMENT,
                            "Setiap pesan setelah metadata harus berisi chunk_data.",
                        )

                    if chunk.chunk_data:
                        staged_file.write(chunk.chunk_data)
                        total_bytes += len(chunk.chunk_data)

            if metadata is None:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Upload harus berisi metadata gambar dan data citra.",
                )
            if total_bytes == 0:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Data byte gambar kosong (0 bytes).",
                )
            if metadata.file_size and metadata.file_size != total_bytes:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"Ukuran data upload ({total_bytes} bytes) tidak cocok dengan metadata "
                    f"({metadata.file_size} bytes).",
                )

            new_job_id = f"JOB-{uuid.uuid4().hex[:8].upper()}"
            record, is_dup = self.job_manager.register_or_get_job(
                request_id=request_id,
                job_id=new_job_id,
                filename=metadata.filename,
                file_format=metadata.file_format,
                staged_input_path=staged_input_path,
                input_size=total_bytes,
            )
            # ImageJobManager telah memindahkan file atau menghapus file duplikat.
            staged_input_path = None
        finally:
            # Hapus upload parsial jika client putus atau validasi gagal.
            if staged_input_path and os.path.exists(staged_input_path):
                os.remove(staged_input_path)

        # Jika mode simulasi timeout aktif untuk pengujian retry & deduplikasi:
        # Permintaan pertama ditunda melewati batas timeout client setelah job terdaftar
        if (
            self.simulate_slow_first_attempt
            and not is_dup
            and request_id not in self._seen_slow_requests
        ):
            self._seen_slow_requests.add(request_id)
            time.sleep(2.5)

        if is_dup:
            msg = (
                f"[DEDUPLIKASI AT-MOST-ONCE] Request ID {request_id[:8]}... sudah pernah "
                f"diproses -> Mengembalikan Job ID lama ({record.job_id}) tanpa duplikasi antrean."
            )
        else:
            msg = (
                f"Gambar '{record.filename}' ({record.input_size} bytes) berhasil diterima "
                f"dan masuk antrean dengan ID {record.job_id}."
            )

        return pb.UploadResponse(
            success=True,
            job_id=record.job_id,
            message=msg,
            status=record.status,
        )

    def CekStatus(self, request, context):
        """
        2. Unary RPC:
        Mengecek status terkini dari pekerjaan berdasarkan job_id.
        Operasi bersifat idempoten dan aman dipanggil secara sinkron maupun asinkron (Future).
        """
        rec = self.job_manager.get_job(request.job_id)
        if rec is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Job ID '{request.job_id}' tidak ditemukan pada server.",
            )
        return pb.StatusResponse(
            job_id=rec.job_id,
            status=rec.status,
            progress_percent=rec.progress_percent,
            message=rec.message,
            filename=rec.filename,
            timestamp=int(rec.updated_at * 1000),
        )

    def PantauStatus(self, request, context):
        """
        3. Server Streaming RPC:
        Mengirimkan aliran (stream) pembaruan StatusResponse secara real-time kepada client
        hingga pekerjaan mencapai STATUS_COMPLETED, STATUS_FAILED, atau STATUS_CANCELLED.
        """
        rec = self.job_manager.get_job(request.job_id)
        if rec is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Job ID '{request.job_id}' tidak ditemukan pada server.",
            )

        last_state = None
        while context.is_active():
            rec = self.job_manager.get_job(request.job_id)
            if rec is None:
                break

            current_state = (rec.status, rec.progress_percent, rec.message)
            if current_state != last_state:
                last_state = current_state
                yield pb.StatusResponse(
                    job_id=rec.job_id,
                    status=rec.status,
                    progress_percent=rec.progress_percent,
                    message=rec.message,
                    filename=rec.filename,
                    timestamp=int(rec.updated_at * 1000),
                )

            if rec.status in (pb.STATUS_COMPLETED, pb.STATUS_FAILED, pb.STATUS_CANCELLED):
                break

            time.sleep(0.04)

    def UnduhGambar(self, request, context):
        """
        4. Server Streaming RPC:
        Mengirimkan file citra hasil konversi grayscale kembali ke client
        secara bertahap per potongan chunk (64 KB).
        """
        rec = self.job_manager.get_job(request.job_id)
        if rec is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Job ID '{request.job_id}' tidak ditemukan pada server.",
            )

        if rec.status != pb.STATUS_COMPLETED:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"Pekerjaan '{request.job_id}' belum selesai (status saat ini: {pb.JobStatus.Name(rec.status)}).",
            )

        if not os.path.exists(rec.output_path):
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"File hasil konversi untuk '{request.job_id}' tidak ditemukan di penyimpanan server.",
            )

        total_bytes = os.path.getsize(rec.output_path)
        out_filename = os.path.basename(rec.output_path)

        with open(rec.output_path, "rb") as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                # Cek apakah ini potongan terakhir
                pos = f.tell()
                is_last = pos >= total_bytes
                yield pb.DownloadChunk(
                    filename=out_filename,
                    chunk_data=data,
                    is_last_chunk=is_last,
                    total_bytes=total_bytes,
                )


def create_server(
    host: str = "0.0.0.0",
    port: int = 50051,
    max_rpc_workers: int = 20,
    num_image_workers: int = 4,
    simulated_delay: float = 0.12,
    simulate_slow_first_attempt: bool = False,
) -> tuple[grpc.Server, ImageJobManager]:
    """Membangun dan mengonfigurasi instance gRPC Server beserta ImageJobManager."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(base_dir, "storage", "input")
    output_dir = os.path.join(base_dir, "storage", "output")

    job_manager = ImageJobManager(
        input_dir=input_dir,
        output_dir=output_dir,
        num_workers=num_image_workers,
        simulated_delay=simulated_delay,
    )
    job_manager.start_workers()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_rpc_workers),
        options=[
            ("grpc.max_send_message_length", 50 * 1024 * 1024),
            ("grpc.max_receive_message_length", 50 * 1024 * 1024),
        ],
    )
    servicer = ImageServiceServicer(
        job_manager=job_manager,
        simulate_slow_first_attempt=simulate_slow_first_attempt,
    )
    pb_grpc.add_ImageServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"{host}:{port}")
    return server, job_manager


def main():
    parser = argparse.ArgumentParser(description="Server gRPC Antrian Pengolahan Gambar")
    parser.add_argument("--host", default="0.0.0.0", help="Alamat bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=50051, help="Port TCP gRPC (default: 50051)")
    parser.add_argument("--rpc-workers", type=int, default=20, help="Jumlah thread gRPC server")
    parser.add_argument("--img-workers", type=int, default=4, help="Jumlah background worker pemroses gambar")
    parser.add_argument("--delay", type=float, default=0.12, help="Simulasi waktu pemrosesan per gambar (detik)")
    args = parser.parse_args()

    server, job_manager = create_server(
        host=args.host,
        port=args.port,
        max_rpc_workers=args.rpc_workers,
        num_image_workers=args.img_workers,
        simulated_delay=args.delay,
    )
    server.start()
    print("=" * 65)
    print(f"[SERVER AKTIF] gRPC ImageService berjalan pada {args.host}:{args.port}")
    print(f"[KONFIGURASI]  ThreadPool gRPC: {args.rpc_workers} | Image Worker Pool: {args.img_workers}")
    print("=" * 65)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Menghentikan worker dan server gRPC...")
    finally:
        # Hentikan penerimaan RPC dahulu agar tidak ada job baru masuk saat
        # worker sedang menerima sentinel untuk shutdown.
        server.stop(grace=2.0).wait()
        job_manager.stop_workers()


if __name__ == "__main__":
    main()
