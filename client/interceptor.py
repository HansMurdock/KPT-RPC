"""
==============================================================
MATA KULIAH : KOMPUTASI PARALEL & TERDISTRIBUSI
PROYEK MINI : SISTEM ANTRIAN PENGOLAHAN CITRA BERBASIS gRPC
FILE        : client/interceptor.py (gRPC Client Interceptor untuk Logging & Timing)
ANGGOTA     : Rafa (Anggota 2 — Contract .proto & Client Application)
==============================================================

Modul ini mengimplementasikan gRPC Client Interceptor yang menangkap
setiap panggilan RPC keluar dan mencatat:
  - Nama metode RPC yang dipanggil
  - Durasi eksekusi (latensi round-trip) dalam milidetik
  - Status keberhasilan atau kode error gRPC
  - Statistik akumulatif (total panggilan, rata-rata latensi, error rate)

Interceptor ini menerapkan pola AOP (Aspect-Oriented Programming) pada
sisi client sehingga logic logging & monitoring terpisah dari logic bisnis.
"""

import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

import grpc


@dataclass
class RPCCallRecord:
    """Catatan satu panggilan RPC yang diinterceptor."""
    method: str
    start_time: float
    end_time: float = 0.0
    latency_ms: float = 0.0
    status_code: Optional[str] = None
    error: Optional[str] = None
    success: bool = True


class InterceptorStats:
    """
    Akumulator statistik thread-safe untuk seluruh panggilan RPC
    yang melewati interceptor. Digunakan untuk menampilkan ringkasan
    di akhir sesi atau pada perintah 'stats' di CLI interaktif.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._calls: list[RPCCallRecord] = []
        self._method_counts: dict[str, int] = defaultdict(int)
        self._method_latencies: dict[str, list[float]] = defaultdict(list)
        self._method_errors: dict[str, int] = defaultdict(int)

    def record(self, call: RPCCallRecord) -> None:
        with self._lock:
            self._calls.append(call)
            short_method = call.method.rsplit("/", 1)[-1] if "/" in call.method else call.method
            self._method_counts[short_method] += 1
            self._method_latencies[short_method].append(call.latency_ms)
            if not call.success:
                self._method_errors[short_method] += 1

    def get_summary(self) -> dict:
        """Mengembalikan ringkasan statistik seluruh RPC yang tercatat."""
        with self._lock:
            if not self._calls:
                return {"total_calls": 0, "methods": {}}

            total = len(self._calls)
            total_errors = sum(1 for c in self._calls if not c.success)
            all_latencies = [c.latency_ms for c in self._calls]

            methods = {}
            for method, count in self._method_counts.items():
                lats = self._method_latencies[method]
                methods[method] = {
                    "count": count,
                    "avg_ms": sum(lats) / len(lats) if lats else 0.0,
                    "min_ms": min(lats) if lats else 0.0,
                    "max_ms": max(lats) if lats else 0.0,
                    "errors": self._method_errors.get(method, 0),
                }

            return {
                "total_calls": total,
                "total_errors": total_errors,
                "error_rate_pct": (total_errors / total * 100) if total else 0.0,
                "avg_latency_ms": sum(all_latencies) / len(all_latencies) if all_latencies else 0.0,
                "methods": methods,
            }

    def reset(self) -> None:
        with self._lock:
            self._calls.clear()
            self._method_counts.clear()
            self._method_latencies.clear()
            self._method_errors.clear()


# Singleton global statistik yang bisa diakses dari CLI
_global_stats = InterceptorStats()


def get_global_stats() -> InterceptorStats:
    return _global_stats


class _WrappedIterator:
    """
    Wrapper untuk stream response yang menandai selesai saat iterasi berakhir.
    Meneruskan semua pemanggilan atribut (seperti .cancel(), .code(), .details())
    ke iterator asli gRPC Call agar kompatibel penuh dengan protokol streaming.
    """

    def __init__(self, iterator: Any, record: RPCCallRecord, stats: InterceptorStats, verbose: bool):
        self._iterator = iterator
        self._record = record
        self._stats = stats
        self._verbose = verbose
        self._finished = False

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._iterator)
        except StopIteration:
            self._finish(success=True)
            raise
        except grpc.RpcError as exc:
            self._finish(success=False, error=exc)
            raise
        except Exception as exc:
            self._finish(success=False, error=exc)
            raise

    def cancel(self):
        """Membatalkan stream jika didukung oleh objek gRPC Call."""
        self._finish(success=False, status_code="CANCELLED")
        if hasattr(self._iterator, "cancel"):
            return self._iterator.cancel()

    def __getattr__(self, name: str) -> Any:
        """Mendelegasikan akses atribut gRPC Call (misal: is_active, code, details)."""
        return getattr(self._iterator, name)

    def __del__(self):
        """Pastikan record tercatat jika stream ditutup lebih awal sebelum StopIteration."""
        if not self._finished:
            self._finish(success=True)

    def _finish(self, success: bool, error: Any = None, status_code: Optional[str] = None) -> None:
        if self._finished:
            return
        self._finished = True
        self._record.end_time = time.perf_counter()
        self._record.latency_ms = (self._record.end_time - self._record.start_time) * 1000
        self._record.success = success

        if status_code:
            self._record.status_code = status_code
        elif error:
            code = error.code() if hasattr(error, "code") else None
            if code and hasattr(code, "name"):
                self._record.status_code = code.name
            else:
                self._record.status_code = str(code or "ERROR")
            self._record.error = str(error.details()) if hasattr(error, "details") else str(error)
        else:
            self._record.status_code = "OK"

        self._stats.record(self._record)
        if self._verbose:
            short = self._record.method.rsplit("/", 1)[-1] if "/" in self._record.method else self._record.method
            status_text = "OK" if success else f"ERROR {self._record.status_code}"
            print(f"    [INTERCEPTOR] {short} -> {status_text} ({self._record.latency_ms:.1f} ms)")


class LoggingClientInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """
    gRPC Client Interceptor yang mencatat waktu, status, dan latensi
    setiap panggilan RPC (Unary, Client Streaming, Server Streaming).
    Mendukung panggilan Sinkron maupun Asinkron (Future) tanpa memblokir.
    """

    def __init__(self, verbose: bool = False, stats: Optional[InterceptorStats] = None):
        self._verbose = verbose
        self._stats = stats or _global_stats

    def _get_method_name(self, client_call_details) -> str:
        method = getattr(client_call_details, "method", "")
        if isinstance(method, bytes):
            method = method.decode("utf-8")
        return method or "unknown"

    def _extract_error_details(self, exc: Any) -> tuple[str, str]:
        code = exc.code() if hasattr(exc, "code") else None
        code_str = code.name if (code and hasattr(code, "name")) else str(code or "ERROR")
        err_str = str(exc.details()) if hasattr(exc, "details") else str(exc)
        return code_str, err_str

    # --- Unary-Unary (CekStatus, ListJobs, BatalkanJob) ---
    def intercept_unary_unary(self, continuation, client_call_details, request):
        method = self._get_method_name(client_call_details)
        record = RPCCallRecord(method=method, start_time=time.perf_counter())

        try:
            response = continuation(client_call_details, request)
        except grpc.RpcError as exc:
            record.end_time = time.perf_counter()
            record.latency_ms = (record.end_time - record.start_time) * 1000
            record.success = False
            code_str, err_str = self._extract_error_details(exc)
            record.status_code = code_str
            record.error = err_str
            self._stats.record(record)
            if self._verbose:
                short = method.rsplit("/", 1)[-1]
                print(f"    [INTERCEPTOR] {short} -> ERROR {code_str} ({record.latency_ms:.1f} ms)")
            raise

        # Kasus 1: Panggilan Asinkron (stub.Method.future())
        if hasattr(response, "add_done_callback") and hasattr(response, "done") and not response.done():
            def _on_future_done(fut):
                record.end_time = time.perf_counter()
                record.latency_ms = (record.end_time - record.start_time) * 1000
                try:
                    fut.result()
                    record.status_code = "OK"
                    record.success = True
                except grpc.RpcError as fut_exc:
                    record.success = False
                    code_str, err_str = self._extract_error_details(fut_exc)
                    record.status_code = code_str
                    record.error = err_str
                except Exception as fut_exc:
                    record.success = False
                    record.status_code = "INTERNAL"
                    record.error = str(fut_exc)
                self._stats.record(record)
                if self._verbose:
                    short = method.rsplit("/", 1)[-1]
                    status_text = "OK" if record.success else f"ERROR {record.status_code}"
                    print(f"    [INTERCEPTOR] {short} (Async) -> {status_text} ({record.latency_ms:.1f} ms)")

            response.add_done_callback(_on_future_done)
            return response

        # Kasus 2: Response mengembalikan RpcError secara langsung (tanpa raise)
        if isinstance(response, grpc.RpcError):
            record.end_time = time.perf_counter()
            record.latency_ms = (record.end_time - record.start_time) * 1000
            record.success = False
            code_str, err_str = self._extract_error_details(response)
            record.status_code = code_str
            record.error = err_str
            self._stats.record(record)
            if self._verbose:
                short = method.rsplit("/", 1)[-1]
                print(f"    [INTERCEPTOR] {short} -> ERROR {code_str} ({record.latency_ms:.1f} ms)")
            return response

        # Kasus 3: Panggilan Sinkron Berhasil
        record.end_time = time.perf_counter()
        record.latency_ms = (record.end_time - record.start_time) * 1000
        record.status_code = "OK"
        record.success = True
        self._stats.record(record)
        if self._verbose:
            short = method.rsplit("/", 1)[-1]
            print(f"    [INTERCEPTOR] {short} -> OK ({record.latency_ms:.1f} ms)")
        return response

    # --- Unary-Stream (PantauStatus, UnduhGambar) ---
    def intercept_unary_stream(self, continuation, client_call_details, request):
        method = self._get_method_name(client_call_details)
        record = RPCCallRecord(method=method, start_time=time.perf_counter())

        try:
            response_iterator = continuation(client_call_details, request)
            return _WrappedIterator(response_iterator, record, self._stats, self._verbose)
        except grpc.RpcError as exc:
            record.end_time = time.perf_counter()
            record.latency_ms = (record.end_time - record.start_time) * 1000
            record.success = False
            code_str, err_str = self._extract_error_details(exc)
            record.status_code = code_str
            record.error = err_str
            self._stats.record(record)
            if self._verbose:
                short = method.rsplit("/", 1)[-1]
                print(f"    [INTERCEPTOR] {short} -> ERROR {code_str} ({record.latency_ms:.1f} ms)")
            raise

    # --- Stream-Unary (UploadGambar) ---
    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        method = self._get_method_name(client_call_details)
        record = RPCCallRecord(method=method, start_time=time.perf_counter())

        try:
            response = continuation(client_call_details, request_iterator)
        except grpc.RpcError as exc:
            record.end_time = time.perf_counter()
            record.latency_ms = (record.end_time - record.start_time) * 1000
            record.success = False
            code_str, err_str = self._extract_error_details(exc)
            record.status_code = code_str
            record.error = err_str
            self._stats.record(record)
            if self._verbose:
                short = method.rsplit("/", 1)[-1]
                print(f"    [INTERCEPTOR] {short} -> ERROR {code_str} ({record.latency_ms:.1f} ms)")
            raise

        # Kasus Asinkron
        if hasattr(response, "add_done_callback") and hasattr(response, "done") and not response.done():
            def _on_future_done(fut):
                record.end_time = time.perf_counter()
                record.latency_ms = (record.end_time - record.start_time) * 1000
                try:
                    fut.result()
                    record.status_code = "OK"
                    record.success = True
                except grpc.RpcError as fut_exc:
                    record.success = False
                    code_str, err_str = self._extract_error_details(fut_exc)
                    record.status_code = code_str
                    record.error = err_str
                except Exception as fut_exc:
                    record.success = False
                    record.status_code = "INTERNAL"
                    record.error = str(fut_exc)
                self._stats.record(record)

            response.add_done_callback(_on_future_done)
            return response

        if isinstance(response, grpc.RpcError):
            record.end_time = time.perf_counter()
            record.latency_ms = (record.end_time - record.start_time) * 1000
            record.success = False
            code_str, err_str = self._extract_error_details(response)
            record.status_code = code_str
            record.error = err_str
            self._stats.record(record)
            return response

        record.end_time = time.perf_counter()
        record.latency_ms = (record.end_time - record.start_time) * 1000
        record.status_code = "OK"
        record.success = True
        self._stats.record(record)
        if self._verbose:
            short = method.rsplit("/", 1)[-1]
            print(f"    [INTERCEPTOR] {short} -> OK ({record.latency_ms:.1f} ms)")
        return response

    # --- Stream-Stream (Ekspansi di masa depan) ---
    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return continuation(client_call_details, request_iterator)


if __name__ == "__main__":
    print("=" * 65)
    print("gRPC Client Interceptor - Modul Logging, Observabilitas & Timing")
    print("=" * 65)
    print("Modul ini digunakan oleh client/client.py untuk memonitor")
    print("latensi RTT (Round Trip Time), error rate, dan pemanggilan RPC.")
    print("Status modul: SIAP DIGUNAKAN (OK)")
    print("=" * 65)
