"""TCP block aggregator for SystMach/CSC4770MATRIXPROJECT."""
import argparse
import json
import math
import socket
import socketserver
import threading
import time
from pathlib import Path
from protocol import matrix, multiply, validate, receive_message, send_message


class Aggregator:
    def __init__(self, info):
        self.info = dict(info)
        for key in ("out_rows", "out_cols", "padded_rows", "padded_cols",
                    "block_rows", "block_cols", "num_jobs"):
            if type(info.get(key)) is not int or info[key] <= 0:
                raise ValueError("invalid scheduler info: " + key)
        br, bc = info["block_rows"], info["block_cols"]
        pr, pc = info["padded_rows"], info["padded_cols"]
        if (pr != ((info["out_rows"]+br-1)//br)*br or
                pc != ((info["out_cols"]+bc-1)//bc)*bc):
            raise ValueError("inconsistent padded dimensions")
        self.jobs = {}
        for row in range(0, pr, br):
            for col in range(0, pc, bc):
                self.jobs[len(self.jobs)+1] = (row, col)
        if len(self.jobs) != info["num_jobs"]:
            raise ValueError("job count mismatch")
        self.received = {}
        self.rows = [[0]*pc for _ in range(pr)]
        self.lock = threading.Lock()
        self.done = threading.Event()

    def add_result(self, msg):
        if not isinstance(msg, dict) or msg.get("type") != "result":
            raise ValueError("expected result")
        job = msg.get("job_id")
        if type(job) is not int or job not in self.jobs:
            raise ValueError("unknown job_id")
        row, col = self.jobs[job]
        if (type(msg.get("start_row")) is not int or
                type(msg.get("start_col")) is not int or
                (msg["start_row"], msg["start_col"]) != (row, col)):
            raise ValueError("coordinates do not match job_id")
        if not isinstance(msg.get("worker_id"), str) or not msg["worker_id"]:
            raise ValueError("worker_id required")
        elapsed = msg.get("compute_time")
        if type(elapsed) not in (int, float) or elapsed < 0 or not math.isfinite(elapsed):
            raise ValueError("invalid compute_time")
        block = matrix(msg.get("result_rows"))
        if len(block) != self.info["block_rows"] or len(block[0]) != self.info["block_cols"]:
            raise ValueError("incorrect block dimensions")
        snapshot = [r[:] for r in block]
        with self.lock:
            if job in self.received:
                if self.received[job] != snapshot:
                    raise ValueError("conflicting duplicate")
                return False
            for i, values in enumerate(snapshot):
                self.rows[row+i][col:col+len(values)] = values
            self.received[job] = snapshot
            if len(self.received) == len(self.jobs):
                self.done.set()
            return True

    def final_matrix(self):
        with self.lock:
            if not self.done.is_set():
                raise ValueError("missing result blocks")
            return [r[:self.info["out_cols"]] for r in self.rows[:self.info["out_rows"]]]


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.request.settimeout(self.server.client_timeout)
        while True:
            try:
                msg = receive_message(self.rfile)
                self.server.aggregator.add_result(msg)
                send_message(self.wfile, {"type": "ack", "job_id": msg["job_id"]})
            except EOFError:
                return
            except (ValueError, UnicodeError, TypeError, KeyError) as error:
                try:
                    send_message(self.wfile, {"type": "error", "message": str(error)})
                except OSError:
                    pass
                return
            except OSError:
                return


class AggregatorServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr, aggregator, client_timeout=120):
        self.aggregator = aggregator
        self.client_timeout = client_timeout
        super().__init__(addr, Handler)


def get_info(host, port, timeout=30):
    deadline = time.monotonic()+timeout
    while True:
        try:
            with socket.create_connection((host, port), timeout=min(3, timeout)) as sock:
                with sock.makefile("rwb") as stream:
                    send_message(stream, {"type": "get_info"})
                    info = receive_message(stream)
                    if info.get("type") != "info":
                        raise ValueError("scheduler did not return info")
                    return info
        except OSError:
            if time.monotonic() >= deadline:
                raise TimeoutError("scheduler readiness timeout")
            time.sleep(0.1)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=5001)
    p.add_argument("--scheduler-host", default="scheduler")
    p.add_argument("--scheduler-port", type=int, default=5000)
    p.add_argument("--timeout", type=float, default=300)
    p.add_argument("--output", default="result.json")
    p.add_argument("--matrices", help="optional scheduler saved matrices for verification")
    args = p.parse_args()
    agg = Aggregator(get_info(args.scheduler_host, args.scheduler_port))
    with AggregatorServer((args.host, args.port), agg) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            if not agg.done.wait(args.timeout):
                raise TimeoutError("result collection timeout")
            result = agg.final_matrix()
            if args.matrices:
                data = json.loads(Path(args.matrices).read_text())
                validate(result, multiply(data["A"], data["B"]))
            Path(args.output).write_text(json.dumps({"C": result}, allow_nan=False))
            # Keep connections alive long enough to deliver final acknowledgments.
            time.sleep(2)
        finally:
            server.shutdown()


if __name__ == "__main__":
    main()
