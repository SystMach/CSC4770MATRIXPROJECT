"""Compute matrix blocks; send answers to the aggregator over TCP.

Scheduler jobs: type, job_id, start_row, start_col, block_a, block_b.
Aggregator results: type, worker_id, job_id, start_row, start_col,
result_rows, compute_time. Every message is JSON followed by a newline.
"""

import argparse
import json
import os
import socket
import time
import uuid

from matrix_multiplication import validate_matrices, multiply_matrices


def connect(host, port):
    # Give the server a few chances to start before giving up
    for attempt in range(10):
        try:
            connection = socket.create_connection((host, port), timeout=5)

            # After connecting, let the worker wait for the full message
            connection.settimeout(None)
            return connection

        except OSError:
            if attempt == 9:
                raise

            time.sleep(1)


def exchange(stream, message):
    # Turn the message into text and add a newline at the end
    text = json.dumps(message, allow_nan=False) + "\n"

    # Send the message as bytes and flush the buffer
    stream.write(text.encode("utf-8"))
    stream.flush()

    # Wait for a full reply from the server
    line = stream.readline()

    if not line:
        raise ConnectionError("A server disconnected before replying")

    reply = json.loads(line)

    if not isinstance(reply, dict):
        raise ValueError("Expected a JSON object from the server")

    if reply.get("type") == "error":
        raise ValueError(f"Server error: {reply.get('message', 'unspecified error')}")

    return reply


def check_ack(reply, job_id):
    # The server has to confirm the same job we sent
    if reply.get("type") != "ack":
        raise ValueError("Expected an acknowledgment from the server")

    if reply.get("job_id") != job_id:
        raise ValueError(f"Expected acknowledgement for job {job_id}")


def execute_job(job, worker_id):
    # Check that the job has the information we need
    for field in ("job_id", "start_row", "start_col", "block_a", "block_b"):
        if field not in job:
            raise ValueError(f"Job is missing {field}")

    for field in ("job_id", "start_row", "start_col"):
        if type(job[field]) is not int or job[field] < 0:
            raise ValueError(f"{field} must be a non-negative integer")

    # Get the two blocks and check their shapes and values
    a = job["block_a"]
    b = job["block_b"]

    validate_matrices(a, b)

    # Calculate the answer and check how long it takes
    start = time.perf_counter()
    result = multiply_matrices(a, b)
    elapsed = time.perf_counter() - start

    # Get the result message ready to send to the aggregator
    return {
        "type": "result",
        "worker_id": worker_id,
        "job_id": job["job_id"],
        "start_row": job["start_row"],
        "start_col": job["start_col"],
        "result_rows": result,
        "compute_time": elapsed,
    }


def run_worker(host, port, aggregator_host, aggregator_port, worker_id):
    # Connect to the scheduler and aggregator
    # The with statement closes the connections when the worker stops
    with (
        connect(host, port) as scheduler_socket,
        connect(aggregator_host, aggregator_port) as aggregator_socket,
    ):
        # Read and write the messages one line at a time
        with (
            scheduler_socket.makefile("rwb") as scheduler,
            aggregator_socket.makefile("rwb") as aggregator,
        ):
            while True:

                # Ask the scheduler for a job
                request = {
                    "type": "request_job",
                    "worker_id": worker_id,
                }

                job = exchange(scheduler, request)

                # Stop the worker when all the jobs are done
                if job.get("type") == "no_more_jobs":
                    print(f"{worker_id}: finished", flush=True)
                    return

                # Wait a little, then ask for a job again
                if job.get("type") == "wait":
                    time.sleep(job["retry_after"])
                    continue

                if job.get("type") != "job":
                    raise ValueError("Expected a job, wait, or no_more_jobs message")

                # Check the blocks and multiply them
                result_message = execute_job(job, worker_id)

                # Send the answer to the aggregator and wait for its reply
                reply = exchange(aggregator, result_message)
                check_ack(reply, job["job_id"])

                # Tell the scheduler the job is done after the aggregator confirms it
                completion = {
                    "type": "job_done",
                    "worker_id": worker_id,
                    "job_id": job["job_id"],
                }

                reply = exchange(scheduler, completion)
                check_ack(reply, job["job_id"])

                print(f"{worker_id}: completed job {job['job_id']}", flush=True)


def main():
    # Get the settings from the command line or environment variables
    # Environment variables let Docker pass settings to the worker
    parser = argparse.ArgumentParser(description="Matrix block worker")

    parser.add_argument("--host", default=os.getenv("SCHEDULER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=os.getenv("SCHEDULER_PORT", "5000"))

    parser.add_argument(
        "--aggregator-host", default=os.getenv("AGGREGATOR_HOST", "127.0.0.1")
    )
    parser.add_argument(
        "--aggregator-port", type=int, default=os.getenv("AGGREGATOR_PORT", "5001")
    )

    # Give the worker its own name if one was not provided
    parser.add_argument(
        "--worker-id", default=os.getenv("WORKER_ID") or f"worker-{uuid.uuid4()}"
    )

    args = parser.parse_args()

    try:
        run_worker(
            args.host,
            args.port,
            args.aggregator_host,
            args.aggregator_port,
            args.worker_id,
        )

    except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
        # Show why the worker stopped
        parser.exit(1, f"Worker stopped: {error}\n")


if __name__ == "__main__":
    # Start the worker when this file is run directly
    main()
