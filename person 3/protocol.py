"""Person 3: result assembly imported by the scheduler."""
import json
import math
import threading


def matrix(a):
    if not isinstance(a, list) or not a or not isinstance(a[0], list) or not a[0]:
        raise ValueError("non-empty matrix required")
    for row in a:
        if not isinstance(row, list) or len(row) != len(a[0]):
            raise ValueError("ragged matrix")
        for x in row:
            if type(x) not in (int, float) or (type(x) is float and not math.isfinite(x)):
                raise ValueError("finite numbers required")
    return a


def multiply(a, b):
    matrix(a)
    matrix(b)
    if len(a[0]) != len(b):
        raise ValueError("incompatible dimensions")
    return [[sum(x*y for x, y in zip(row, col)) for col in zip(*b)] for row in a]


def validate(actual, expected):
    matrix(actual)
    matrix(expected)
    if len(actual) != len(expected) or len(actual[0]) != len(expected[0]):
        raise ValueError("shape mismatch")
    for i, row in enumerate(actual):
        for j, x in enumerate(row):
            if not math.isclose(x, expected[i][j], rel_tol=1e-9, abs_tol=1e-9):
                raise ValueError(f"incorrect result at ({i}, {j})")


def send_message(stream, message):
    stream.write((json.dumps(message, allow_nan=False)+"\n").encode("utf-8"))
    stream.flush()


def receive_message(stream, max_bytes=64*1024*1024):
    """Binary socket file; caller sets socket timeout."""
    line = stream.readline(max_bytes+1)
    if not line:
        raise EOFError("connection closed")
    if len(line) > max_bytes or not line.endswith(b"\n"):
        raise ValueError("oversized or unterminated frame")
    def reject(value):
        raise ValueError("non-finite JSON constant: "+value)
    result = json.loads(line.decode("utf-8"), parse_constant=reject)
    if not isinstance(result, dict):
        raise ValueError("JSON object required")
    return result
