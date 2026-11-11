"""
matrix_utils.py - Person 1
Matrix validation, generation, and row-block splitting for the scheduler.

Matrices are plain Python lists of lists so they serialize straight to JSON.
"""

import json
import random


class MatrixError(ValueError):
    """Raised when a matrix (or pair of matrices) is invalid."""


def validate_matrix(m, name="matrix"):
    """Check one matrix is non-empty, rectangular, and numeric.

    Returns (rows, cols).
    """
    if not isinstance(m, list) or len(m) == 0:
        raise MatrixError(f"{name} must be a non-empty list of rows")
    if not all(isinstance(row, list) for row in m):
        raise MatrixError(f"{name}: every row must be a list")

    cols = len(m[0])
    if cols == 0:
        raise MatrixError(f"{name}: rows must not be empty")

    for i, row in enumerate(m):
        if len(row) != cols:
            raise MatrixError(
                f"{name}: row {i} has {len(row)} values, expected {cols}"
            )
        for j, v in enumerate(row):
            # bool is a subclass of int; reject it explicitly
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise MatrixError(f"{name}[{i}][{j}] is not a number: {v!r}")
    return len(m), cols


def validate_pair(a, b):
    """Validate A and B and check they can be multiplied (cols A == rows B).

    Returns (rows_a, cols_a, cols_b) - the result is rows_a x cols_b.
    """
    rows_a, cols_a = validate_matrix(a, "Matrix A")
    rows_b, cols_b = validate_matrix(b, "Matrix B")
    if cols_a != rows_b:
        raise MatrixError(
            f"Cannot multiply: A is {rows_a}x{cols_a}, B is {rows_b}x{cols_b} "
            f"(columns of A must equal rows of B)"
        )
    return rows_a, cols_a, cols_b


def split_rows(num_rows, block_size):
    """Split row indices 0..num_rows-1 into blocks of `block_size`.

    Returns a list of (start_row, end_row) with end_row EXCLUSIVE.
    The last block may be smaller.

    >>> split_rows(25, 10)
    [(0, 10), (10, 20), (20, 25)]
    """
    if num_rows <= 0:
        raise MatrixError("num_rows must be positive")
    if block_size <= 0:
        raise MatrixError("block_size must be positive")
    return [
        (start, min(start + block_size, num_rows))
        for start in range(0, num_rows, block_size)
    ]


def make_jobs(a, block_size):
    """Turn Matrix A into a list of job metadata dicts (one per row block)."""
    jobs = []
    for job_id, (start, end) in enumerate(split_rows(len(a), block_size), start=1):
        jobs.append({
            "job_id": job_id,
            "start_row": start,
            "end_row": end,            # exclusive
            "status": "pending",       # pending -> in_progress -> completed
            "assigned_worker": None,
            "assigned_time": None,
            "completed_time": None,
            "attempts": 0,
        })
    return jobs


def validate_result_rows(result_rows, expected_rows, expected_cols):
    """Check a worker's result block has the right shape and numeric values."""
    if not isinstance(result_rows, list) or len(result_rows) != expected_rows:
        got = len(result_rows) if isinstance(result_rows, list) else type(result_rows).__name__
        raise MatrixError(f"expected {expected_rows} result rows, got {got}")
    for i, row in enumerate(result_rows):
        if not isinstance(row, list) or len(row) != expected_cols:
            raise MatrixError(f"result row {i} should have {expected_cols} values")
        for v in row:
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise MatrixError(f"result row {i} contains non-number {v!r}")


def random_matrix(rows, cols, low=0, high=9, seed=None):
    """Generate a rows x cols matrix of random integers (for testing/benchmarks)."""
    rng = random.Random(seed)
    return [[rng.randint(low, high) for _ in range(cols)] for _ in range(rows)]


def multiply(a, b):
    """Plain single-process multiplication - used only to verify results."""
    bt = list(zip(*b))
    return [[sum(x * y for x, y in zip(row, col)) for col in bt] for row in a]


def load_matrices(path):
    """Load {"A": [[...]], "B": [[...]]} from a JSON file."""
    with open(path) as f:
        data = json.load(f)
    if "A" not in data or "B" not in data:
        raise MatrixError(f"{path} must contain keys 'A' and 'B'")
    return data["A"], data["B"]