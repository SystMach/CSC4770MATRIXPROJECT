import io
import tempfile
import unittest
from pathlib import Path
from aggregator import Aggregator, multiply, validate, receive_message
from benchmark import benchmark
from team_adapter import run


def info():
    return dict(out_rows=3, out_cols=3, padded_rows=4, padded_cols=4,
                block_rows=2, block_cols=2, num_jobs=4)


def msg(job, row, col, values):
    return dict(type="result", job_id=job, start_row=row, start_col=col,
                worker_id="test", compute_time=0.01, result_rows=values)


class Tests(unittest.TestCase):
    def test_out_of_order_trim_retry_and_conflict(self):
        agg = Aggregator(info())
        agg.add_result(msg(4, 2, 2, [[9, 0], [0, 0]]))
        with self.assertRaises(ValueError):
            agg.final_matrix()
        agg.add_result(msg(2, 0, 2, [[3, 0], [6, 0]]))
        agg.add_result(msg(3, 2, 0, [[7, 8], [0, 0]]))
        first = msg(1, 0, 0, [[1, 2], [4, 5]])
        agg.add_result(first)
        self.assertFalse(agg.add_result(first))
        first["result_rows"][0][0] = 99
        with self.assertRaises(ValueError):
            agg.add_result(first)
        self.assertEqual(agg.final_matrix(), [[1, 2, 3], [4, 5, 6], [7, 8, 9]])

    def test_reject_bad_results(self):
        agg = Aggregator(info())
        for result in (msg(1, 0, 2, [[1, 2], [3, 4]]),
                       msg(1, 0, 0, [[1]]),
                       msg(1, 0, 0, [[float("nan"), 1], [1, 1]]),
                       msg(True, 0, 0, [[1, 2], [3, 4]])):
            with self.assertRaises(ValueError):
                agg.add_result(result)
        self.assertFalse(agg.done.is_set())

    def test_framing(self):
        stream = io.BytesIO(b'{"type":"ack","job_id":1}\n{"type":"no_more_jobs"}\n')
        self.assertEqual(receive_message(stream)["job_id"], 1)
        self.assertEqual(receive_message(stream)["type"], "no_more_jobs")
        for frame in (b'{}', b'[]\n', b'{"x":NaN}\n'):
            with self.assertRaises(ValueError):
                receive_message(io.BytesIO(frame))

    def test_real_system_rectangular_and_padding(self):
        a = [[1, 2, 3], [4, 5, 6], [-1, 0, 1]]
        b = [[7, 8, 1, 0, 2], [9, 10, 2, 1, 3], [11, 12, 3, 2, 4]]
        for workers, block in ((1, 2), (2, 2), (4, 10)):
            with self.subTest(workers=workers, block=block):
                validate(run(a=a, b=b, workers=workers, block_size=block, timeout=30),
                         multiply(a, b))

    def test_benchmark_real_runs(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as folder:
            records = benchmark(run, [3], [1, 2], [2], 1, Path(folder)/"test.csv")
            self.assertEqual(records[0]["speedup"], 1)
            self.assertEqual(len(records), 2)


if __name__ == "__main__":
    unittest.main()
