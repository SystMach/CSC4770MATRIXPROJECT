"""
test_scheduler.py - Person 1
Tests for validation, splitting, queue behavior, and result assembly.

Run:  python -m unittest -v test_scheduler.py
"""

import json
import socket
import threading
import time
import unittest

import matrix_utils as mu
from scheduler import JobManager, SchedulerServer


# ---------------------------------------------------------------------------
class TestValidation(unittest.TestCase):
    def test_valid_pair(self):
        self.assertEqual(mu.validate_pair([[1, 2, 3], [4, 5, 6]],
                                          [[7, 8], [9, 10], [11, 12]]), (2, 3, 2))

    def test_empty(self):
        with self.assertRaises(mu.MatrixError):
            mu.validate_matrix([])
        with self.assertRaises(mu.MatrixError):
            mu.validate_matrix([[]])

    def test_ragged_rows(self):
        with self.assertRaises(mu.MatrixError):
            mu.validate_matrix([[1, 2], [3]])

    def test_non_numeric(self):
        with self.assertRaises(mu.MatrixError):
            mu.validate_matrix([[1, "x"]])
        with self.assertRaises(mu.MatrixError):
            mu.validate_matrix([[1, True]])

    def test_dimension_mismatch(self):
        with self.assertRaises(mu.MatrixError):
            mu.validate_pair([[1, 2]], [[1, 2]])  # 1x2 * 1x2


# ---------------------------------------------------------------------------
class TestSplitting(unittest.TestCase):
    def test_even_split(self):
        self.assertEqual(mu.split_rows(40, 10),
                         [(0, 10), (10, 20), (20, 30), (30, 40)])

    def test_uneven_last_block(self):
        self.assertEqual(mu.split_rows(25, 10), [(0, 10), (10, 20), (20, 25)])

    def test_block_bigger_than_matrix(self):
        self.assertEqual(mu.split_rows(5, 10), [(0, 5)])

    def test_bad_block_size(self):
        with self.assertRaises(mu.MatrixError):
            mu.split_rows(10, 0)

    def test_jobs_cover_every_row_once(self):
        a = mu.random_matrix(37, 4, seed=1)
        rows = []
        for j in mu.make_jobs(a, 8):
            rows.extend(range(j["start_row"], j["end_row"]))
        self.assertEqual(rows, list(range(37)))

    def test_job_metadata(self):
        jobs = mu.make_jobs([[1]] * 20, 10)
        self.assertEqual([j["job_id"] for j in jobs], [1, 2])
        for key in ("job_id", "start_row", "end_row", "status",
                    "assigned_worker", "assigned_time", "completed_time"):
            self.assertIn(key, jobs[0])
        self.assertTrue(all(j["status"] == "pending" for j in jobs))


# ---------------------------------------------------------------------------
def compute(job):
    """What a worker does - used to fake worker results."""
    return mu.multiply(job["rows_a"], job["matrix_b"])


class TestQueueAndAssembly(unittest.TestCase):
    def setUp(self):
        self.a = mu.random_matrix(25, 6, seed=3)
        self.b = mu.random_matrix(6, 4, seed=4)
        self.mgr = JobManager(self.a, self.b, block_size=10)

    def test_jobs_handed_out_in_order(self):
        ids = [self.mgr.next_job("w1")["job_id"] for _ in range(3)]
        self.assertEqual(ids, [1, 2, 3])
        self.assertEqual(self.mgr.status_counts()["in_progress"], 3)

    def test_job_message_format(self):
        msg = self.mgr.next_job("w1")
        self.assertEqual(set(msg), {"type", "job_id", "start_row", "rows_a", "matrix_b"})
        self.assertEqual(msg["type"], "job")
        self.assertEqual(msg["rows_a"], self.a[0:10])
        self.assertEqual(msg["matrix_b"], self.b)

    def test_wait_when_queue_empty_but_jobs_running(self):
        for _ in range(3):
            self.mgr.next_job("w1")
        self.assertEqual(self.mgr.next_job("w2")["type"], "wait")

    def test_out_of_order_results_assemble_correctly(self):
        jobs = [self.mgr.next_job(f"w{i}") for i in range(3)]
        for job in reversed(jobs):  # job 3 finishes first
            ok, _ = self.mgr.submit_result("wX", job["job_id"], job["start_row"], compute(job))
            self.assertTrue(ok)
        self.assertTrue(self.mgr.done.is_set())
        self.assertEqual(self.mgr.result, mu.multiply(self.a, self.b))
        self.assertEqual(self.mgr.next_job("w1"), {"type": "no_more_jobs"})

    def test_bad_result_is_rejected_and_requeued(self):
        job = self.mgr.next_job("w1")
        ok, _ = self.mgr.submit_result("w1", job["job_id"], job["start_row"], [[1, 2]])
        self.assertFalse(ok)
        self.assertEqual(self.mgr.next_job("w2")["job_id"], job["job_id"])

    def test_wrong_start_row_rejected(self):
        job = self.mgr.next_job("w1")
        ok, _ = self.mgr.submit_result("w1", job["job_id"], 99, compute(job))
        self.assertFalse(ok)

    def test_duplicate_result_ignored(self):
        job = self.mgr.next_job("w1")
        self.mgr.submit_result("w1", job["job_id"], job["start_row"], compute(job))
        ok, info = self.mgr.submit_result("w2", job["job_id"], job["start_row"], compute(job))
        self.assertTrue(ok)
        self.assertIn("duplicate", info)
        self.assertEqual(self.mgr.workers["w1"]["jobs_completed"], 1)

    def test_lost_worker_jobs_requeued(self):
        job = self.mgr.next_job("w1")
        self.mgr.worker_lost("w1")
        self.assertEqual(self.mgr.next_job("w2")["job_id"], job["job_id"])

    def test_timeout_requeues(self):
        mgr = JobManager(self.a, self.b, block_size=25, job_timeout=0.05)
        first = mgr.next_job("slow")
        self.assertEqual(mgr.next_job("fast")["type"], "wait")
        time.sleep(0.1)
        self.assertEqual(mgr.next_job("fast")["job_id"], first["job_id"])

    def test_thread_safety_no_job_given_twice(self):
        mgr = JobManager(mu.random_matrix(500, 2), mu.random_matrix(2, 2), block_size=1)
        seen, lock = [], threading.Lock()

        def grab(wid):
            while True:
                m = mgr.next_job(wid)
                if m["type"] != "job":
                    return
                with lock:
                    seen.append(m["job_id"])

        threads = [threading.Thread(target=grab, args=(f"w{i}",)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(seen), list(range(1, 501)))


# ---------------------------------------------------------------------------
class FakeWorker(threading.Thread):
    """Minimal stand-in for Person 2's worker.py, speaking the agreed protocol."""

    def __init__(self, port, worker_id, crash_after=None):
        super().__init__(daemon=True)
        self.port, self.worker_id, self.crash_after = port, worker_id, crash_after
        self.jobs_done = 0

    def run(self):
        with socket.create_connection(("127.0.0.1", self.port)) as s:
            f = s.makefile("rwb")

            def send(m):
                f.write((json.dumps(m) + "\n").encode())
                f.flush()

            def recv():
                return json.loads(f.readline())

            while True:
                send({"type": "request_job", "worker_id": self.worker_id})
                msg = recv()
                if msg["type"] == "no_more_jobs":
                    return
                if msg["type"] == "wait":
                    time.sleep(msg["retry_after"])
                    continue
                if self.crash_after is not None and self.jobs_done >= self.crash_after:
                    return  # disconnect holding a job, never send result
                t0 = time.perf_counter()
                rows = compute(msg)
                send({"type": "result", "worker_id": self.worker_id,
                      "job_id": msg["job_id"], "start_row": msg["start_row"],
                      "result_rows": rows, "compute_time": time.perf_counter() - t0})
                assert recv()["type"] == "ack"
                self.jobs_done += 1


class TestEndToEnd(unittest.TestCase):
    def _run(self, workers_spec, n=60, block=7):
        a, b = mu.random_matrix(n, n, seed=10), mu.random_matrix(n, n, seed=11)
        mgr = JobManager(a, b, block)
        server = SchedulerServer(("127.0.0.1", 0), mgr)
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            workers = [FakeWorker(port, wid, crash) for wid, crash in workers_spec]
            for w in workers:
                w.start()
                if w.crash_after is not None:
                    w.join(timeout=5)  # let it grab a job and die before others start
            self.assertTrue(mgr.done.wait(timeout=20), "scheduler never finished")
            for w in workers:
                w.join(timeout=5)
        finally:
            server.shutdown()
            server.server_close()
        self.assertEqual(mgr.result, mu.multiply(a, b))
        return mgr

    def test_four_workers(self):
        mgr = self._run([(f"worker-{i}", None) for i in range(1, 5)])
        self.assertEqual(sum(w["jobs_completed"] for w in mgr.workers.values()),
                         len(mgr.jobs))

    def test_worker_crash_recovered(self):
        mgr = self._run([("crashy", 1), ("worker-2", None), ("worker-3", None)])
        self.assertTrue(any(j["attempts"] > 1 for j in mgr.jobs.values()))


if __name__ == "__main__":
    unittest.main(verbosity=2)