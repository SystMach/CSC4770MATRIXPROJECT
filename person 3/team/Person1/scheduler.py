"""
scheduler.py - Person 1: Scheduler + Matrix Division
 
Central TCP server that:
  1. Validates Matrix A and Matrix B
  2. Splits A into row groups and B into column groups (zero-padded so every
     job is the same size) and pairs them: one job per (row group, column group)
  3. Hands jobs to workers on request from a dynamic queue
  4. Tracks job status (pending / in_progress / completed)
  5. Marks jobs completed when workers report job_done
  6. Tells workers to stop only when every job is done
 
Workers send their result blocks to the aggregator (Person 3), NOT to the
scheduler. The scheduler never sees the answer - it only coordinates the work.
 
Protocol: newline-delimited JSON over TCP (one JSON object per line).
 
  Worker -> Scheduler
    {"type": "request_job", "worker_id": "worker-1"}
    {"type": "job_done",    "worker_id": "worker-1", "job_id": 3}
 
  Scheduler -> Worker
    {"type": "job", "job_id": 3, "start_row": 0, "start_col": 20,
     "block_a": [[...]], "block_b": [[...]]}
    {"type": "ack", "job_id": 3}
    {"type": "wait", "retry_after": 0.2}     queue empty, but jobs still running
    {"type": "no_more_jobs"}                 every job finished - disconnect
    {"type": "error", "message": "..."}
 
  Aggregator (or anyone) -> Scheduler
    {"type": "get_info"}
  Scheduler -> Aggregator
    {"type": "info", "out_rows": 25, "out_cols": 25, "padded_rows": 30,
     "padded_cols": 30, "inner_dim": 25, "block_rows": 10,
     "block_cols": 10, "num_jobs": 9}
 
Every option can be set with a command-line flag or an environment variable
(handy for Docker), e.g.  SCHEDULER_PORT=5000 BLOCK_SIZE=10 python scheduler.py
"""
 
import argparse
import json
import logging
import os
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
    """Owns the job queue and the status table.
 
    All public methods are thread-safe: every connection runs in its own
    thread, and they all share one lock.
    """
 
    def __init__(self, a, b, block_size):
        self.out_rows, self.inner_dim, self.out_cols = mu.validate_pair(a, b)
        if block_size <= 0:
            raise mu.MatrixError("block size must be positive")
 
        # A block bigger than the matrix would only add padding - shrink it.
        self.block_rows = min(block_size, self.out_rows)
        self.block_cols = min(block_size, self.out_cols)
 
        # Zero-pad so every job is exactly block_rows x block_cols.
        self.padded_rows = mu.padded_length(self.out_rows, self.block_rows)
        self.padded_cols = mu.padded_length(self.out_cols, self.block_cols)
        self.a_orig, self.b_orig = a, b
        self.a = mu.pad_rows(a, self.padded_rows)
        self.b = mu.pad_cols(b, self.padded_cols)
 
        self.jobs = {j["job_id"]: j for j in mu.make_jobs(
            self.out_rows, self.out_cols, self.block_rows, self.block_cols)}
        self.queue = deque(self.jobs.keys())
        self._col_cache = {}   # start_col -> column block of B (reused across jobs)
        self.workers = {}      # worker_id -> stats
 
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.start_time = None   # first job handed out
        self.end_time = None     # last job_done received
 
    # ---- info for the aggregator ------------------------------------
    def info(self):
        """Sizes Person 3 needs to assemble and trim the final matrix."""
        return {
            "type": "info",
            "out_rows": self.out_rows,        # real result size (trim to this)
            "out_cols": self.out_cols,
            "padded_rows": self.padded_rows,  # size including zero padding
            "padded_cols": self.padded_cols,
            "inner_dim": self.inner_dim,
            "block_rows": self.block_rows,    # usually both equal BLOCK_SIZE
            "block_cols": self.block_cols,
            "num_jobs": len(self.jobs),
        }
 
    # ---- workers -----------------------------------------------------
    def register(self, worker_id):
        with self.lock:
            if worker_id not in self.workers:
                self.workers[worker_id] = {
                    "jobs_completed": 0,
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
 
            while self.queue:
                job = self.jobs[self.queue.popleft()]
                if job["status"] != "pending":   # stale entry from a requeue
                    continue
                now = time.time()
                job["status"] = "in_progress"
                job["assigned_worker"] = worker_id
                job["assigned_time"] = now
                job["attempts"] += 1
                if self.start_time is None:
                    self.start_time = now
                log.info("job %d (rows %d-%d, cols %d-%d) -> %s", job["job_id"],
                         job["start_row"], job["end_row"] - 1,
                         job["start_col"], job["end_col"] - 1, worker_id)
                return self._job_message(job)
 
            # Queue empty, but some jobs are still in progress elsewhere.
            # Keep the worker around in case one of those jobs fails.
            return {"type": "wait", "retry_after": 0.2}
 
    def _job_message(self, job):
        # caller holds the lock
        sr, er = job["start_row"], job["end_row"]
        sc, ec = job["start_col"], job["end_col"]
        if sc not in self._col_cache:
            self._col_cache[sc] = mu.column_block(self.b, sc, ec)
        return {
            "type": "job",
            "job_id": job["job_id"],
            "start_row": sr,
            "start_col": sc,
            "block_a": self.a[sr:er],          # block_rows x inner_dim
            "block_b": self._col_cache[sc],    # inner_dim x block_cols
        }
 
    # ---- completion --------------------------------------------------
    def job_done(self, worker_id, job_id, compute_time=None):
        """Mark a job completed. Returns (ok, message)."""
        with self.lock:
            job = self.jobs.get(job_id)
            if job is None:
                return False, f"unknown job_id {job_id!r}"
            if job["status"] == "completed":
                # e.g. a worker that reconnected and reported the same job twice.
                return True, "already completed"
            if job["status"] == "pending" and job["attempts"] == 0:
                return False, f"job {job_id} was never handed out"
 
            now = time.time()
            job["status"] = "completed"
            job["completed_time"] = now
            job["assigned_worker"] = worker_id
 
            w = self.workers.setdefault(worker_id, {
                "jobs_completed": 0, "compute_time": 0.0, "registered_time": now})
            w["jobs_completed"] += 1
            if isinstance(compute_time, (int, float)) and not isinstance(compute_time, bool):
                w["compute_time"] += compute_time
 
            done = sum(1 for j in self.jobs.values() if j["status"] == "completed")
            log.info("job %d done by %s (%d/%d)", job_id, worker_id, done, len(self.jobs))
 
            if done == len(self.jobs):
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
 
    # ---- reporting ---------------------------------------------------
    def status_counts(self):
        with self.lock:
            counts = {"pending": 0, "in_progress": 0, "completed": 0}
            for j in self.jobs.values():
                counts[j["status"]] += 1
            return counts
 
    def summary(self):
        with self.lock:
            total = (self.end_time - self.start_time) if self.end_time and self.start_time else None
            s = self.info()
            del s["type"]
            s.update({
                "num_workers": len(self.workers),
                "total_time_sec": total,
                "workers": self.workers,
                "jobs": list(self.jobs.values()),
            })
            return s
 
 
# --------------------------------------------------------------------------
# Networking
# --------------------------------------------------------------------------
def send_msg(wfile, msg):
    wfile.write((json.dumps(msg) + "\n").encode("utf-8"))
    wfile.flush()
 
 
class ConnectionHandler(socketserver.StreamRequestHandler):
    """One instance (and one thread) per connection (worker or aggregator)."""
 
    def handle(self):
        mgr: JobManager = self.server.manager
        worker_id = None
        peer = "%s:%s" % self.client_address
        log.debug("connection from %s", peer)
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
                if not isinstance(msg, dict):
                    send_msg(self.wfile, {"type": "error", "message": "message must be a JSON object"})
                    continue
 
                mtype = msg.get("type")
                wid = msg.get("worker_id")
                if wid and worker_id is None:
                    worker_id = str(wid)
                    mgr.register(worker_id)
 
                if mtype == "get_info":
                    send_msg(self.wfile, mgr.info())
 
                elif mtype == "request_job":
                    if worker_id is None:
                        send_msg(self.wfile, {"type": "error", "message": "worker_id required"})
                        continue
                    reply = mgr.next_job(worker_id)
                    send_msg(self.wfile, reply)
                    if reply["type"] == "no_more_jobs":
                        break
 
                elif mtype == "job_done":
                    ok, info = mgr.job_done(worker_id or "unknown", msg.get("job_id"),
                                            msg.get("compute_time"))
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
                log.info("connection closed: %s", worker_id)
 
 
class SchedulerServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
 
    def __init__(self, addr, manager):
        super().__init__(addr, ConnectionHandler)
        self.manager = manager
 
 
def run(manager, host="0.0.0.0", port=5000, grace=2.0, output=None):
    """Serve until every job is complete, then return the run summary."""
    server = SchedulerServer((host, port), manager)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    i = manager.info()
    log.info("listening on %s:%d", host, server.server_address[1])
    log.info("result %dx%d (padded to %dx%d), %d jobs of %dx%d",
             i["out_rows"], i["out_cols"], i["padded_rows"], i["padded_cols"],
             i["num_jobs"], manager.block_rows, manager.block_cols)
 
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
    if output:
        with open(output, "w") as f:
            json.dump(summary, f, indent=2)
        log.info("wrote %s", output)
    return summary
 
 
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
                   help="size N for random NxN A and B (ignored with --input)")
    p.add_argument("--block-size", type=int, default=int(env("BLOCK_SIZE", 10)),
                   help="rows of A and columns of B per job")
    p.add_argument("--input", default=env("MATRIX_FILE", None),
                   help='JSON file with {"A": [[...]], "B": [[...]]}')
    p.add_argument("--save-matrices", default=env("SAVE_MATRICES", None),
                   help="write the A and B used to this JSON file (for verification)")
    p.add_argument("--seed", type=int, default=int(env("SEED", 42)))
    p.add_argument("--output", default=env("OUTPUT_FILE", "run_summary.json"))
    p.add_argument("--grace", type=float, default=float(env("GRACE_PERIOD", 2)))
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
        manager = JobManager(a, b, args.block_size)
    except mu.MatrixError as exc:
        log.error("invalid input: %s", exc)
        raise SystemExit(1)
 
    if args.save_matrices:
        mu.save_matrices(args.save_matrices, a, b)
        log.info("saved A and B to %s", args.save_matrices)
 
    run(manager, args.host, args.port, args.grace, args.output)
 
 
if __name__ == "__main__":
    main()
 