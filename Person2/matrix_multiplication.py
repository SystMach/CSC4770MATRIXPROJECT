"""Validate and multiply two matrix blocks."""

import math


def validate_matrices(a, b):
    # It checks A and B one at a time
    for name, matrix in (("A", a), ("B", b)):

        # A matrix needs at least one row, and each row needs numbers
        if not isinstance(matrix, list) or not matrix:
            raise ValueError(f"Block {name} must be a non-empty list")

        if not isinstance(matrix[0], list) or not matrix[0]:
            raise ValueError(f"Block {name} must have non-empty rows")

        columns = len(matrix[0])

        for row in matrix:

            # All rows in the matrix must have the same length
            if not isinstance(row, list) or len(row) != columns:
                raise ValueError(f"Block {name} must have equal-length rows")

            for value in row:

                # Accept just numbers
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError(f"Block {name} must contain only numbers")

                # Can't use Infinity and NaN for matrix values
                if isinstance(value, float) and not math.isfinite(value):
                    raise ValueError(f"Block {name} cannot contain infinity or NaN")

    # A's column count has to match B's row count
    if len(a[0]) != len(b):
        raise ValueError("Columns of block A must equal rows of block B")


def multiply_matrices(a, b):
    # The worker checks the inputs before calling this function
    result = []

    # Choose one row from A
    for row in a:
        result_row = []

        # Choose one column from B
        for column in range(len(b[0])):
            total = 0

            # Multiply matching numbers, then add them to the total
            # k moves across the A row and down the B column
            for k in range(len(b)):
                total += row[k] * b[k][column]

            result_row.append(total)

        result.append(result_row)

    # The answer is a list of rows, ready to send as JSON
    return result
