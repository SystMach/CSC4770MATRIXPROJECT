"""
scheduler.py - Person 1: Scheduler + Matrix Division
 
Central TCP server that:
  1. Validates Matrix A and Matrix B
  2. Splits A into row blocks (one job per block)
  3. Hands jobs to workers on request from a dynamic queue
  4. Tracks job status (pending / in_progress / completed)
  5. Places result rows into the final matrix by start_row
  6. Tells workers to stop when every job is done
 
Protocol: newline-delimited JSON over TCP (one JSON object per line).
 
  Worker -> Scheduler
    {"type": "register",    "worker_id": "worker-1"}                  (optional)
    {"type": "request_job", "worker_id": "worker-1"}
    {"type": "result", "worker_id": "worker-1", "job_id": 3,
     "start_row": 20, "result_rows": [[...]], "compute_time": 0.0012}
 
  Scheduler -> Worker
    {"type": "registered",  "worker_id": "worker-1"}
    {"type": "job", "job_id": 3, "start_row": 20, "rows_a": [[...]], "matrix_b": [[...]]}
    {"type": "ack", "job_id": 3}
    {"type": "wait", "retry_after": 0.2}     queue empty, but jobs still running
    {"type": "no_more_jobs"}                 everything finished - disconnect
    {"type": "error", "message": "..."}
 
Every option can be set with a command-line flag or an environment variable
(handy for Docker), e.g.  SCHEDULER_PORT=5000 BLOCK_SIZE=10 python scheduler.py
"""
 
import argparse
import json
import logging
import os
import socket
import socketserver
import threading
import time
from collections import deque
 
import matrix_utils as mu
 
log = logging.getLogger("scheduler")
 
 
# --------------------------------------------------------------------------
# Job bookkeeping (no networking here - easy to unit-test)
# --------------------------------------------------------------------------
class JobManager:
    """Owns the job queue, the status table, and the result matrix.
 
    All public methods are thread-safe: every worker connection runs in its
    own thread, and they all share one lock.
    """
 
    def __init__(self, a, b, block_size, job_timeout=None):
        self.rows_a, self.cols_a, self.cols_b = mu.validate_pair(a, b)
        self.a = a
        self.b = b
        self.block_size = block_size
        self.job_timeout = job_timeout  # seconds before an in-progress job is re-queued
 
        self.jobs = {j["job_id"]: j for j in mu.make_jobs(a, block_size)}
        self.queue = deque(self.jobs.keys())
        self.result = [None] * self.rows_a
        self.workers = {}  # worker_id -> stats
 
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.start_time = None   # first job handed out
        self.end_time = None     # last result received
 
    # ---- workers -----------------------------------------------------
    def register(self, worker_id):
        with self.lock:
            if worker_id not in self.workers:
                self.workers[worker_id] = {
                    "jobs_completed": 0,
                    "rows_completed": 0,
                    "compute_time": 0.0,
                    "registered_time": time.time(),
                }
                log.info("registered %s (%d workers total)", worker_id, len(self.workers))
 
    # ---- handing out work -------------------------------------------
    def next_job(self, worker_id):
        """Return a job message, or a wait / no_more_jobs message."""
        with self.lock:
            if self.done.is_set():
                return {"type": "no_more_jobs"}
 
            self._requeue_timed_out()
 
            while self.queue:
                job = self.jobs[self.queue.popleft()]
                if job["status"] == "completed":   # stale entry from a requeue
                    continue
                now = time.time()
                job["status"] = "in_progress"
                job["assigned_worker"] = worker_id
                job["assigned_time"] = now
                job["attempts"] += 1
                if self.start_time is None:
                    self.start_time = now
                s, e = job["start_row"], job["end_row"]
                log.info("job %d (rows %d-%d) -> %s", job["job_id"], s, e - 1, worker_id)
                return {
                    "type": "job",
                    "job_id": job["job_id"],
                    "start_row": s,
                    "rows_a": self.a[s:e],
                    "matrix_b": self.b,
                }
 
            # Queue empty, but some jobs are still in progress elsewhere.
            # Keep the worker around in case one of those jobs fails.
            return {"type": "wait", "retry_after": 0.2}
 
    # ---- receiving results ------------------------------------------
    def submit_result(self, worker_id, job_id, start_row, result_rows, compute_time=0.0):
        """Store a worker's result block. Returns (ok, message)."""
        with self.lock:
            job = self.jobs.get(job_id)
            if job is None:
                return False, f"unknown job_id {job_id}"
            if start_row != job["start_row"]:
                return False, f"job {job_id} starts at row {job['start_row']}, not {start_row}"
            if job["status"] == "completed":
                # Duplicate (e.g. a slow worker finishing a re-queued job) - ignore it.
                return True, "duplicate result ignored"
 
            n = job["end_row"] - job["start_row"]
            try:
                mu.validate_result_rows(result_rows, n, self.cols_b)
            except mu.MatrixError as exc:
                self._requeue(job)
                return False, f"bad result for job {job_id}: {exc} (job re-queued)"
 
            self.result[start_row:start_row + n] = result_rows
            now = time.time()
            job["status"] = "completed"
            job["completed_time"] = now
            job["assigned_worker"] = worker_id
 
            w = self.workers.setdefault(worker_id, {
                "jobs_completed": 0, "rows_completed": 0,
                "compute_time": 0.0, "registered_time": now})
            w["jobs_completed"] += 1
            w["rows_completed"] += n
            w["compute_time"] += float(compute_time or 0.0)
 
            log.info("job %d completed by %s (%d/%d done)",
                     job_id, worker_id, self.completed_count(), len(self.jobs))
 
            if all(j["status"] == "completed" for j in self.jobs.values()):
                self.end_time = now
                self.done.set()
                log.info("all %d jobs completed", len(self.jobs))
            return True, "ok"
 
    # ---- fault tolerance --------------------------------------------
    def worker_lost(self, worker_id):
        """Put any in-progress jobs of a disconnected worker back in the queue."""
        with self.lock:
            for job in self.jobs.values():
                if job["status"] == "in_progress" and job["assigned_worker"] == worker_id:
                    log.warning("%s disconnected; re-queuing job %d", worker_id, job["job_id"])
                    self._requeue(job)
 
    def _requeue(self, job):
        # caller holds the lock
        job["status"] = "pending"
        job["assigned_worker"] = None
        job["assigned_time"] = None
        self.queue.appendleft(job["job_id"])
 
    def _requeue_timed_out(self):
        # caller holds the lock
        if not self.job_timeout:
            return
        now = time.time()
        for job in self.jobs.values():
            if (job["status"] == "in_progress"
                    and now - job["assigned_time"] > self.job_timeout):
                log.warning("job %d timed out on %s; re-queuing",
                            job["job_id"], job["assigned_worker"])
                self._requeue(job)
 
    # ---- reporting ---------------------------------------------------
    def completed_count(self):
        return sum(1 for j in self.jobs.values() if j["status"] == "completed")
 
    def status_counts(self):
        with self.lock:
            counts = {"pending": 0, "in_progress": 0, "completed": 0}
            for j in self.jobs.values():
                counts[j["status"]] += 1
            return counts
 
    def summary(self):
        with self.lock:
            total = (self.end_time - self.start_time) if self.end_time and self.start_time else None
            return {
                "matrix_a_shape": [self.rows_a, self.cols_a],
                "matrix_b_shape": [self.cols_a, self.cols_b],
                "block_size": self.block_size,
                "num_jobs": len(self.jobs),
                "num_workers": len(self.workers),
                "total_time_sec": total,
                "workers": self.workers,
                "jobs": list(self.jobs.values()),
            }
 
 
# --------------------------------------------------------------------------
# Networking
# --------------------------------------------------------------------------
def send_msg(wfile, msg):
    wfile.write((json.dumps(msg) + "\n").encode("utf-8"))
    wfile.flush()
 
 
class WorkerHandler(socketserver.StreamRequestHandler):
    """One instance (and one thread) per connected worker."""
 
    def handle(self):
        mgr: JobManager = self.server.manager
        worker_id = None
        peer = "%s:%s" % self.client_address
        log.info("connection from %s", peer)
        try:
            for raw in self.rfile:  # one JSON message per line
                line = raw.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    send_msg(self.wfile, {"type": "error", "message": "invalid JSON"})
                    continue
 
                mtype = msg.get("type")
                wid = msg.get("worker_id")
                if wid and worker_id is None:
                    worker_id = str(wid)
                    mgr.register(worker_id)
 
                if mtype == "register":
                    send_msg(self.wfile, {"type": "registered", "worker_id": worker_id})
 
                elif mtype == "request_job":
                    if worker_id is None:
                        send_msg(self.wfile, {"type": "error", "message": "worker_id required"})
                        continue
                    reply = mgr.next_job(worker_id)
                    send_msg(self.wfile, reply)
                    if reply["type"] == "no_more_jobs":
                        break
 
                elif mtype == "result":
                    ok, info = mgr.submit_result(
                        worker_id or "unknown",
                        msg.get("job_id"),
                        msg.get("start_row"),
                        msg.get("result_rows"),
                        msg.get("compute_time", 0.0),
                    )
                    if ok:
                        send_msg(self.wfile, {"type": "ack", "job_id": msg.get("job_id")})
                    else:
                        log.warning("%s: %s", worker_id, info)
                        send_msg(self.wfile, {"type": "error", "message": info})
 
                else:
                    send_msg(self.wfile, {"type": "error", "message": f"unknown type {mtype!r}"})
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            if worker_id:
                mgr.worker_lost(worker_id)
            log.info("connection closed: %s (%s)", worker_id or "?", peer)
 
 
class SchedulerServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
 
    def __init__(self, addr, manager):
        super().__init__(addr, WorkerHandler)
        self.manager = manager
 
 
def run(manager, host="0.0.0.0", port=5000, grace=2.0, output=None, verify=False):
    """Serve until every job is complete, then return the result matrix."""
    server = SchedulerServer((host, port), manager)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("listening on %s:%d - %d jobs of up to %d rows",
             host, server.server_address[1], len(manager.jobs), manager.block_size)
 
    try:
        while not manager.done.wait(timeout=5):
            log.info("progress: %s", manager.status_counts())
        # Give waiting workers a moment to receive no_more_jobs.
        time.sleep(grace)
    except KeyboardInterrupt:
        log.warning("interrupted")
    finally:
        server.shutdown()
        server.server_close()
 
    summary = manager.summary()
    if summary["total_time_sec"] is not None:
        log.info("finished in %.4f s using %d workers",
                 summary["total_time_sec"], summary["num_workers"])
 
    if verify and manager.done.is_set():
        correct = manager.result == mu.multiply(manager.a, manager.b)
        summary["verified"] = correct
        log.info("verification against single-process multiply: %s",
                 "PASS" if correct else "FAIL")
 
    if output:
        with open(output, "w") as f:
            json.dump({"summary": summary, "result": manager.result}, f)
        log.info("wrote %s", output)
 
    return manager.result, summary
 
 
# --------------------------------------------------------------------------
# Entry point (also the Docker container entry point)
# --------------------------------------------------------------------------
def env(name, default):
    return os.environ.get(name, default)
 
 
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Distributed matrix multiplication scheduler")
    p.add_argument("--host", default=env("SCHEDULER_HOST", "0.0.0.0"))
    p.add_argument("--port", type=int, default=int(env("SCHEDULER_PORT", 5000)))
    p.add_argument("--size", type=int, default=int(env("MATRIX_SIZE", 100)),
                   help="square size N for random NxN A and B (ignored with --input)")
    p.add_argument("--block-size", type=int, default=int(env("BLOCK_SIZE", 10)),
                   help="rows of A per job")
    p.add_argument("--input", default=env("MATRIX_FILE", None),
                   help='JSON file with {"A": [[...]], "B": [[...]]}')
    p.add_argument("--seed", type=int, default=int(env("SEED", 42)))
    p.add_argument("--output", default=env("OUTPUT_FILE", "result.json"))
    p.add_argument("--job-timeout", type=float, default=float(env("JOB_TIMEOUT", 60)),
                   help="seconds before an unfinished job is re-queued (0 = never)")
    p.add_argument("--grace", type=float, default=float(env("GRACE_PERIOD", 2)))
    p.add_argument("--verify", action="store_true",
                   default=env("VERIFY", "0").lower() in ("1", "true", "yes"))
    return p.parse_args(argv)
 
 
def main(argv=None):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [scheduler] %(message)s",
                        datefmt="%H:%M:%S")
    args = parse_args(argv)
 
    if args.input:
        a, b = mu.load_matrices(args.input)
    else:
        a = mu.random_matrix(args.size, args.size, seed=args.seed)
        b = mu.random_matrix(args.size, args.size, seed=args.seed + 1)
 
    try:
        manager = JobManager(a, b, args.block_size, job_timeout=args.job_timeout or None)
    except mu.MatrixError as exc:
        log.error("invalid input: %s", exc)
        raise SystemExit(1)
 
    run(manager, args.host, args.port, args.grace, args.output, args.verify)
 
 
if __name__ == "__main__":
    main()
 