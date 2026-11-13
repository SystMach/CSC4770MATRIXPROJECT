"""Run actual teammate subprocesses; no fake workers or results."""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from aggregator import Aggregator, AggregatorServer, get_info

ROOT = Path(__file__).resolve().parent


def run(*, a, b, workers, block_size, timeout=120):
    if type(workers) is not int or workers < 1:
        raise ValueError("positive worker count required")
    # Small port-selection race remains; scheduler readiness/exit checks detect failure.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    processes = []
    server = None
    thread = None
    deadline = time.monotonic()+timeout
    with tempfile.TemporaryDirectory(dir=ROOT) as temp:
        temp = Path(temp)
        (temp/"input.json").write_text(json.dumps({"A": a, "B": b}, allow_nan=False))
        with (temp/"process.log").open("w+") as log:
            try:
                scheduler = subprocess.Popen(
                    [sys.executable, str(ROOT/"team/Person1/scheduler.py"),
                     "--host", "127.0.0.1", "--port", str(port),
                     "--input", str(temp/"input.json"), "--block-size", str(block_size),
                     "--grace", "1", "--output", str(temp/"summary.json")],
                    stdout=log, stderr=log)
                processes.append(scheduler)
                agg = Aggregator(get_info("127.0.0.1", port, timeout=min(15, timeout)))
                server = AggregatorServer(("127.0.0.1", 0), agg)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                for i in range(workers):
                    processes.append(subprocess.Popen(
                        [sys.executable, str(ROOT/"team/Person2/worker.py"),
                         "--host", "127.0.0.1", "--port", str(port),
                         "--aggregator-host", "127.0.0.1",
                         "--aggregator-port", str(server.server_address[1]),
                         "--worker-id", f"worker-{i}"], stdout=log, stderr=log))
                for process in processes:
                    code = process.wait(timeout=max(0.01, deadline-time.monotonic()))
                    if code:
                        raise RuntimeError(f"team process failed with exit {code}")
                return agg.final_matrix()
            except Exception as error:
                log.flush()
                log.seek(0)
                raise RuntimeError(str(error)+"\n"+log.read()) from error
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
                for process in processes:
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                if server:
                    if thread:
                        server.shutdown()
                        thread.join(timeout=3)
                    server.server_close()
