"""
matrix_utils.py - Person 1
Matrix validation, zero-padding, and row x column block splitting for the scheduler.

Matrices are plain Python lists of lists so they serialize straight to JSON.

How the work is divided
-----------------------
A (m x n) is split into groups of rows, and B (n x p) into groups of columns.
Each (row group, column group) pair is one job:

    block_a = rows  start_row..end_row  of A   (all n columns)
    block_b = cols  start_col..end_col  of B   (all n rows)

block_a x block_b is a complete block of the result C = A x B, placed at
(start_row, start_col). Keeping the full inner length n in every pair means each
worker produces a finished piece of C with no partial sums to add up later.

So that every job is the same size, A gets extra zero rows and B extra zero
columns when m or p don't divide evenly. The padded parts of C are all zeros and
are trimmed back to m x p by the aggregator.
"""

import json
import math
import random


class MatrixError(ValueError):
    """Raised when a matrix (or pair of matrices) is invalid."""


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Splitting and padding
# ---------------------------------------------------------------------------
def padded_length(n, block):
    """Smallest multiple of `block` that is >= n.

    >>> padded_length(25, 10)
    30
    """
    if n <= 0:
        raise MatrixError("length must be positive")
    if block <= 0:
        raise MatrixError("block size must be positive")
    return math.ceil(n / block) * block


def split_ranges(n, block):
    """Split 0..n-1 into equal blocks of `block`, padding n up if needed.

    Returns a list of (start, end) with end EXCLUSIVE. Every block has the same
    size; the last one may extend past n into padding.

    >>> split_ranges(25, 10)
    [(0, 10), (10, 20), (20, 30)]
    """
    total = padded_length(n, block)
    return [(s, s + block) for s in range(0, total, block)]


def pad_rows(m, total_rows):
    """Return a copy of m with zero rows added at the bottom up to total_rows."""
    cols = len(m[0])
    return [list(r) for r in m] + [[0] * cols for _ in range(total_rows - len(m))]


def pad_cols(m, total_cols):
    """Return a copy of m with zero columns added on the right up to total_cols."""
    extra = total_cols - len(m[0])
    return [list(r) + [0] * extra for r in m]


def column_block(m, start, end):
    """Columns start..end-1 of m, keeping every row."""
    return [row[start:end] for row in m]


def make_jobs(rows_a, cols_b, block_rows, block_cols):
    """Create one job per (row group of A, column group of B) pair.

    Jobs are ordered row by row: all column groups of rows 0..k, then the next
    row group, and so on. Coordinates refer to the padded matrices; because
    padding is only added at the bottom/right, start_row and start_col are also
    the correct positions in the real result.
    """
    jobs = []
    job_id = 1
    for start_row, end_row in split_ranges(rows_a, block_rows):
        for start_col, end_col in split_ranges(cols_b, block_cols):
            jobs.append({
                "job_id": job_id,
                "start_row": start_row,
                "end_row": end_row,        # exclusive
                "start_col": start_col,
                "end_col": end_col,        # exclusive
                "status": "pending",       # pending -> in_progress -> completed
                "assigned_worker": None,
                "assigned_time": None,
                "completed_time": None,
                "attempts": 0,
            })
            job_id += 1
    return jobs


# ---------------------------------------------------------------------------
# Helpers for testing / verification
# ---------------------------------------------------------------------------
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


def save_matrices(path, a, b):
    """Write {"A": [[...]], "B": [[...]]} so others can verify the result."""
    with open(path, "w") as f:
        json.dump({"A": a, "B": b}, f)