"""
test_scheduler.py - Person 1
Tests for validation, padding, row x column splitting, queue behavior, job_done
handling, re-queuing on disconnect, and a full run with fake workers and a fake aggregator.
 
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
class TestSplittingAndPadding(unittest.TestCase):
    def test_even_split(self):
        self.assertEqual(mu.split_ranges(40, 10),
                         [(0, 10), (10, 20), (20, 30), (30, 40)])
 
    def test_uneven_split_is_padded_to_equal_blocks(self):
        self.assertEqual(mu.split_ranges(25, 10), [(0, 10), (10, 20), (20, 30)])
        self.assertEqual(mu.padded_length(25, 10), 30)
 
    def test_bad_block_size(self):
        with self.assertRaises(mu.MatrixError):
            mu.split_ranges(10, 0)
 
    def test_pad_rows_and_cols(self):
        m = [[1, 2], [3, 4]]
        self.assertEqual(mu.pad_rows(m, 3), [[1, 2], [3, 4], [0, 0]])
        self.assertEqual(mu.pad_cols(m, 3), [[1, 2, 0], [3, 4, 0]])
        self.assertEqual(m, [[1, 2], [3, 4]])  # original unchanged
 
    def test_column_block(self):
        self.assertEqual(mu.column_block([[1, 2, 3], [4, 5, 6]], 1, 3), [[2, 3], [5, 6]])
 
    def test_jobs_cover_every_cell_once(self):
        jobs = mu.make_jobs(23, 17, 5, 4)
        cells = []
        for j in jobs:
            for r in range(j["start_row"], j["end_row"]):
                for c in range(j["start_col"], j["end_col"]):
                    cells.append((r, c))
        self.assertEqual(len(cells), len(set(cells)))           # no overlaps
        self.assertEqual(len(cells), 25 * 20)                   # padded size
        self.assertTrue({(r, c) for r in range(23) for c in range(17)} <= set(cells))
 
    def test_job_count_and_metadata(self):
        jobs = mu.make_jobs(25, 25, 10, 10)
        self.assertEqual(len(jobs), 9)                          # 3 row groups x 3 col groups
        self.assertEqual([j["job_id"] for j in jobs], list(range(1, 10)))
        for key in ("job_id", "start_row", "end_row", "start_col", "end_col", "status",
                    "assigned_worker", "assigned_time", "completed_time"):
            self.assertIn(key, jobs[0])
        self.assertTrue(all(j["status"] == "pending" for j in jobs))
 
 
# ---------------------------------------------------------------------------
class TestJobManager(unittest.TestCase):
    def setUp(self):
        self.a = mu.random_matrix(25, 6, seed=3)    # 25 x 6
        self.b = mu.random_matrix(6, 13, seed=4)    # 6 x 13  -> result 25 x 13
        self.mgr = JobManager(self.a, self.b, block_size=5)
 
    def test_info(self):
        info = self.mgr.info()
        self.assertEqual(info, {
            "type": "info", "out_rows": 25, "out_cols": 13,
            "padded_rows": 25, "padded_cols": 15, "inner_dim": 6,
            "block_rows": 5, "block_cols": 5, "num_jobs": 15,
        })
 
    def test_job_message_format(self):
        msg = self.mgr.next_job("w1")
        self.assertEqual(set(msg), {"type", "job_id", "start_row", "start_col",
                                    "block_a", "block_b"})
        self.assertEqual(msg["type"], "job")
        self.assertEqual(msg["block_a"], self.a[0:5])
        self.assertEqual(msg["block_b"], mu.column_block(self.b, 0, 5))
 
    def test_every_job_is_equal_size_and_keeps_full_inner_length(self):
        while True:
            msg = self.mgr.next_job("w1")
            if msg["type"] != "job":
                break
            self.assertEqual(len(msg["block_a"]), 5)
            self.assertTrue(all(len(r) == 6 for r in msg["block_a"]))     # full row of A
            self.assertEqual(len(msg["block_b"]), 6)                      # full column of B
            self.assertTrue(all(len(r) == 5 for r in msg["block_b"]))
 
    def test_padding_is_zeros(self):
        mgr = JobManager(self.a, self.b, block_size=10)     # 25x13 -> padded 30x20
        for _ in range(6):
            msg = mgr.next_job("w1")
            if msg["start_row"] == 20:
                self.assertEqual(msg["block_a"][5:], [[0] * 6] * 5)       # rows 25-29
            if msg["start_col"] == 10:
                self.assertTrue(all(r[3:] == [0] * 7 for r in msg["block_b"]))  # cols 13-19
 
    def test_block_larger_than_matrix_is_shrunk(self):
        mgr = JobManager([[1, 2]] * 4, [[1], [2]], block_size=100)
        self.assertEqual(mgr.info()["block_rows"], 4)
        self.assertEqual(mgr.info()["block_cols"], 1)
        self.assertEqual(mgr.info()["num_jobs"], 1)
 
    def test_jobs_handed_out_in_order(self):
        ids = [self.mgr.next_job("w1")["job_id"] for _ in range(3)]
        self.assertEqual(ids, [1, 2, 3])
        self.assertEqual(self.mgr.status_counts()["in_progress"], 3)
 
    def test_wait_when_queue_empty_but_jobs_running(self):
        for _ in range(15):
            self.mgr.next_job("w1")
        self.assertEqual(self.mgr.next_job("w2")["type"], "wait")
 
    def test_no_more_jobs_only_after_every_job_done(self):
        ids = [self.mgr.next_job("w1")["job_id"] for _ in range(15)]
        for jid in ids[:-1]:
            self.mgr.job_done("w1", jid)
            self.assertEqual(self.mgr.next_job("w2")["type"], "wait")
        self.mgr.job_done("w1", ids[-1])
        self.assertTrue(self.mgr.done.is_set())
        self.assertEqual(self.mgr.next_job("w2"), {"type": "no_more_jobs"})
 
    def test_job_done_unknown_or_never_assigned(self):
        self.assertFalse(self.mgr.job_done("w1", 999)[0])
        self.assertFalse(self.mgr.job_done("w1", 5)[0])      # never handed out
 
    def test_duplicate_job_done_ignored(self):
        jid = self.mgr.next_job("w1")["job_id"]
        self.assertTrue(self.mgr.job_done("w1", jid)[0])
        ok, info = self.mgr.job_done("w2", jid)
        self.assertTrue(ok)
        self.assertIn("already", info)
        self.assertEqual(self.mgr.workers["w1"]["jobs_completed"], 1)
 
    def test_lost_worker_jobs_requeued(self):
        jid = self.mgr.next_job("w1")["job_id"]
        self.mgr.worker_lost("w1")
        self.assertEqual(self.mgr.next_job("w2")["job_id"], jid)
 
    def test_finished_jobs_not_requeued_on_disconnect(self):
        jid = self.mgr.next_job("w1")["job_id"]
        self.mgr.job_done("w1", jid)
        self.mgr.worker_lost("w1")
        self.assertNotEqual(self.mgr.next_job("w2")["job_id"], jid)
 
    def test_thread_safety_no_job_given_twice(self):
        mgr = JobManager(mu.random_matrix(50, 2), mu.random_matrix(2, 50),
                         block_size=1)                    # 50 x 50 = 2500 jobs
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
        self.assertEqual(sorted(seen), list(range(1, 2501)))
 
 
# ---------------------------------------------------------------------------
class FakeAggregator:
    """Stand-in for Person 3: collects blocks, then assembles and trims."""
 
    def __init__(self):
        self.blocks = {}
        self.lock = threading.Lock()
 
    def receive(self, job_id, start_row, start_col, block):
        with self.lock:
            self.blocks[job_id] = (start_row, start_col, block)  # duplicates overwrite
 
    def assemble(self, info):
        c = [[0] * info["padded_cols"] for _ in range(info["padded_rows"])]
        for sr, sc, block in self.blocks.values():
            for i, row in enumerate(block):
                c[sr + i][sc:sc + len(row)] = row
        return [row[:info["out_cols"]] for row in c[:info["out_rows"]]]
 
 
class FakeWorker(threading.Thread):
    """Stand-in for Person 2's worker.py, speaking the agreed protocol."""
 
    def __init__(self, port, worker_id, aggregator, crash_after=None):
        super().__init__(daemon=True)
        self.port, self.worker_id, self.agg = port, worker_id, aggregator
        self.crash_after = crash_after
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
                    return  # disconnect holding a job, never report it
                block = mu.multiply(msg["block_a"], msg["block_b"])
                # 1) result to the aggregator first, 2) then tell the scheduler
                self.agg.receive(msg["job_id"], msg["start_row"], msg["start_col"], block)
                send({"type": "job_done", "worker_id": self.worker_id,
                      "job_id": msg["job_id"]})
                assert recv() == {"type": "ack", "job_id": msg["job_id"]}
                self.jobs_done += 1
 
 
def ask(port, msg):
    with socket.create_connection(("127.0.0.1", port)) as s:
        f = s.makefile("rwb")
        f.write((json.dumps(msg) + "\n").encode())
        f.flush()
        return json.loads(f.readline())
 
 
class TestEndToEnd(unittest.TestCase):
    def _run(self, workers_spec, rows=37, inner=11, cols=29, block_size=7):
        a = mu.random_matrix(rows, inner, seed=10)
        b = mu.random_matrix(inner, cols, seed=11)
        mgr = JobManager(a, b, block_size)
        server = SchedulerServer(("127.0.0.1", 0), mgr)
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        agg = FakeAggregator()
        try:
            info = ask(port, {"type": "get_info"})   # what Person 3 would do first
            workers = [FakeWorker(port, wid, agg, crash) for wid, crash in workers_spec]
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
        self.assertEqual(agg.assemble(info), mu.multiply(a, b))
        return mgr
 
    def test_get_info_over_tcp(self):
        mgr = JobManager(mu.random_matrix(5, 3), mu.random_matrix(3, 7), 2)
        server = SchedulerServer(("127.0.0.1", 0), mgr)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            self.assertEqual(ask(server.server_address[1], {"type": "get_info"}), mgr.info())
        finally:
            server.shutdown()
            server.server_close()
 
    def test_four_workers(self):
        mgr = self._run([(f"worker-{i}", None) for i in range(1, 5)])
        self.assertEqual(sum(w["jobs_completed"] for w in mgr.workers.values()),
                         len(mgr.jobs))
 
    def test_worker_crash_recovered(self):
        mgr = self._run([("crashy", 1), ("worker-2", None), ("worker-3", None)])
        self.assertTrue(any(j["attempts"] > 1 for j in mgr.jobs.values()))
 
    def test_teammate_example(self):
        # The exact example from Person 2's notes: A = [[1, 2]], B = [[3], [4]]
        mgr = JobManager([[1, 2]], [[3], [4]], 1)
        self.assertEqual(mgr.next_job("w1"), {
            "type": "job", "job_id": 1, "start_row": 0, "start_col": 0,
            "block_a": [[1, 2]], "block_b": [[3], [4]]})
 
 
if __name__ == "__main__":
    unittest.main(verbosity=2)
 