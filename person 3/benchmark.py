"""Benchmark real team runs through a configurable module:function adapter."""
import argparse
import csv
import importlib
import random
import statistics
import time
from aggregator import multiply, validate


def benchmark(runner, sizes, workers, blocks, repeats, output):
    if not sizes or not blocks or 1 not in workers or repeats < 1 or any(x < 1 for x in sizes+workers+blocks):
        raise ValueError("positive settings and one-worker baseline required")
    records = []
    for size in sizes:
        rng = random.Random(size)
        a = [[rng.randint(-5, 5) for _ in range(size)] for _ in range(size)]
        b = [[rng.randint(-5, 5) for _ in range(size)] for _ in range(size)]
        expected = multiply(a, b)
        for block in blocks:
            timings = {}
            for count in sorted(set(workers)):
                samples = []
                for _ in range(repeats):
                    aa, bb = [r[:] for r in a], [r[:] for r in b]
                    start = time.perf_counter()
                    result = runner(a=aa, b=bb, workers=count, block_size=block)
                    elapsed = time.perf_counter()-start
                    validate(result, expected)
                    samples.append(elapsed)
                timings[count] = statistics.median(samples)
            for count, elapsed in timings.items():
                speedup = timings[1]/elapsed
                records.append(dict(matrix_size=size, block_size=block, workers=count,
                                    repeats=repeats, median_seconds=elapsed,
                                    speedup=speedup, efficiency=speedup/count))
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    return records


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--runner", required=True, help="team adapter module:function")
    p.add_argument("--sizes", nargs="+", type=int, default=[100])
    p.add_argument("--workers", nargs="+", type=int, default=[1, 2, 4])
    p.add_argument("--blocks", nargs="+", type=int, default=[10])
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--output", default="benchmark_results.csv")
    args = p.parse_args()
    module, name = args.runner.split(":")
    benchmark(getattr(importlib.import_module(module), name), args.sizes, args.workers,
              args.blocks, args.repeats, args.output)
