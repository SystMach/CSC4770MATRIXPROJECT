# Person 1: Scheduler + Matrix Division

The scheduler is the central program of the system. It validates the input matrices, splits Matrix A into row blocks (one job per block), hands jobs to workers over TCP as they ask for them, and places the returned rows into the final result matrix.

## Files

| File | Purpose |
|---|---|
| `scheduler.py` | TCP server, worker registration, job queue, result collection. Also the Docker entry point. |
| `matrix_utils.py` | Matrix validation, row-block splitting, random matrix generation, and a plain multiply used for verification. |
| `test_scheduler.py` | Unit and end-to-end tests (validation, splitting, queue behavior, result assembly, worker crashes). |

Uses only the Python standard library, so nothing needs to be installed. Requires Python 3.8+.

## Quick start

From inside this folder:

```bash
# Start the scheduler: a random 200x200 multiplication split into 10-row jobs
python scheduler.py --size 200 --block-size 10 --verify

# In other terminals, start one or more workers (Person 2's worker.py)
python ../Person2/worker.py
```

When every job is done, the scheduler logs the total time, checks the answer if `--verify` is on, writes `result.json`, and exits.

```
18:40:01 [scheduler] listening on 0.0.0.0:5000 - 20 jobs of up to 10 rows
18:40:03 [scheduler] finished in 0.3442 s using 4 workers
18:40:03 [scheduler] verification against single-process multiply: PASS
18:40:03 [scheduler] wrote result.json
```

## Configuration

Every setting can be passed as a command-line flag or as an environment variable (environment variables are how Docker Compose sets them).

| Flag | Environment variable | Default | Meaning |
|---|---|---|---|
| `--host` | `SCHEDULER_HOST` | `0.0.0.0` | Address to listen on |
| `--port` | `SCHEDULER_PORT` | `5000` | TCP port workers connect to |
| `--size` | `MATRIX_SIZE` | `100` | Size N of the random N×N matrices A and B |
| `--block-size` | `BLOCK_SIZE` | `10` | Rows of A per job |
| `--input` | `MATRIX_FILE` | *(none)* | JSON file with `{"A": [[...]], "B": [[...]]}`; overrides `--size` |
| `--seed` | `SEED` | `42` | Random seed, so runs are repeatable |
| `--output` | `OUTPUT_FILE` | `result.json` | Where to write the result and run summary |
| `--job-timeout` | `JOB_TIMEOUT` | `60` | Seconds before an unfinished job is given to another worker (0 = never) |
| `--grace` | `GRACE_PERIOD` | `2` | Seconds to wait after the last job so workers receive `no_more_jobs` |
| `--verify` | `VERIFY` | off | Compare the result with a normal single-process multiply (`1`/`true` to enable) |

## How it works

### 1. Validation

Before any job is created, `matrix_utils.validate_pair` checks that:

- both matrices are non-empty
- every row of a matrix has the same length
- every value is a number
- the number of columns of A equals the number of rows of B

Invalid input stops the scheduler with an error message.

### 2. Row-block division

Matrix A is split into blocks of `BLOCK_SIZE` rows. Each block becomes one job. The last block may be smaller if the rows don't divide evenly.

```
A has 25 rows, BLOCK_SIZE = 10

Job 1: rows 0-9
Job 2: rows 10-19
Job 3: rows 20-24
```

Each output row depends only on one row of A and all of B, so jobs are independent and can run in parallel in any order.

Each job's metadata is stored as:

| Field | Meaning |
|---|---|
| `job_id` | Unique ID, starting at 1 |
| `start_row`, `end_row` | Rows of A in this job (`end_row` is exclusive) |
| `status` | `pending`, `in_progress`, or `completed` |
| `assigned_worker` | Worker currently holding (or that completed) the job |
| `assigned_time`, `completed_time` | Timestamps |
| `attempts` | How many times the job has been handed out |

### 3. Dynamic work queue

Jobs are not assigned to workers in advance. They wait in a shared queue, and each worker takes the next one when it is ready, so faster workers end up doing more jobs and no worker sits idle while others are busy.

Each worker connection runs in its own thread. The queue, the status table, and the result matrix are shared and protected by one lock, so two workers can never receive the same job.

### 4. Result assembly

When a worker returns a result, the scheduler checks that the job exists, the `start_row` matches, and the rows have the right shape. It then copies the rows into the final matrix starting at `start_row`. Results can arrive in any order.

### 5. Fault tolerance

| Situation | What the scheduler does |
|---|---|
| A worker disconnects while holding a job | The job goes back to the front of the queue |
| A worker takes longer than `JOB_TIMEOUT` | The job goes back to the queue for another worker |
| A result has the wrong shape or `start_row` | Rejected with an `error` reply; the job is re-queued |
| Two workers return the same job (after a re-queue) | The first result is kept; the second is ignored |

## Message protocol

Messages are JSON over TCP, one JSON object per line (each message ends with `\n`). The worker always sends a message and then reads exactly one reply.

### Worker → Scheduler

| Message | Example |
|---|---|
| Register *(optional)* | `{"type": "register", "worker_id": "worker-1"}` |
| Request a job | `{"type": "request_job", "worker_id": "worker-1"}` |
| Return a result | `{"type": "result", "worker_id": "worker-1", "job_id": 3, "start_row": 20, "result_rows": [[...]], "compute_time": 0.0012}` |

A worker is registered automatically the first time it sends its `worker_id`, so the `register` message isn't required.

### Scheduler → Worker

| Message | When | What the worker should do |
|---|---|---|
| `{"type": "registered", "worker_id": "worker-1"}` | Reply to `register` | Send `request_job` |
| `{"type": "job", "job_id": 3, "start_row": 20, "rows_a": [[...]], "matrix_b": [[...]]}` | A job is available | Multiply `rows_a` by `matrix_b` and send a `result` |
| `{"type": "ack", "job_id": 3}` | Result accepted | Request the next job |
| `{"type": "wait", "retry_after": 0.2}` | Queue empty, but other workers still have jobs | Sleep `retry_after` seconds, then request again. Don't exit. |
| `{"type": "no_more_jobs"}` | Every job is complete | Disconnect and exit |
| `{"type": "error", "message": "..."}` | Bad message or bad result | Log the message and continue |

### Example conversation

```
Worker → {"type": "request_job", "worker_id": "worker-1"}
Sched  → {"type": "job", "job_id": 3, "start_row": 20, "rows_a": [...], "matrix_b": [...]}
Worker → {"type": "result", "worker_id": "worker-1", "job_id": 3, "start_row": 20, "result_rows": [...], "compute_time": 0.0012}
Sched  → {"type": "ack", "job_id": 3}
Worker → {"type": "request_job", "worker_id": "worker-1"}
Sched  → {"type": "wait", "retry_after": 0.2}
Worker → {"type": "request_job", "worker_id": "worker-1"}
Sched  → {"type": "no_more_jobs"}
```

### Why `wait` exists

Near the end of a run, the queue can be empty while another worker is still working on the last job. If the scheduler sent `no_more_jobs` at that point and the busy worker then crashed, its job would go back on the queue with no worker left to take it. `wait` keeps idle workers connected until every job is actually complete.

After all jobs finish, the scheduler shuts down. A worker that connects after that gets "connection refused" and should treat it as the end of the run.

## Output file

`result.json` contains the final matrix and a summary of the run, which Person 3 can use for benchmarking:

```json
{
  "summary": {
    "matrix_a_shape": [200, 200],
    "matrix_b_shape": [200, 200],
    "block_size": 10,
    "num_jobs": 20,
    "num_workers": 4,
    "total_time_sec": 0.3442,
    "verified": true,
    "workers": {
      "worker-1": {"jobs_completed": 7, "rows_completed": 70, "compute_time": 0.081, "registered_time": 1731285601.2}
    },
    "jobs": [
      {"job_id": 1, "start_row": 0, "end_row": 10, "status": "completed", "assigned_worker": "worker-1", "assigned_time": 1731285601.5, "completed_time": 1731285601.6, "attempts": 1}
    ]
  },
  "result": [[...], [...]]
}
```

`total_time_sec` is measured from the moment the first job is handed out to the moment the last result arrives, so time spent waiting for workers to start isn't counted. `verified` only appears when `--verify` is on.

## Tests

From inside this folder:

```bash
python -m unittest -v test_scheduler.py
```

Or from the repo root:

```bash
python -m unittest discover -s Person1
```

The 23 tests cover:

- **Validation:** empty matrices, uneven rows, non-numbers, mismatched dimensions
- **Splitting:** even and uneven blocks, blocks larger than the matrix, every row covered exactly once
- **Queue:** jobs handed out in order, `wait` when the queue is empty, no job handed out twice across 8 threads
- **Results:** out-of-order assembly, bad results rejected and re-queued, wrong `start_row`, duplicate results ignored
- **Fault tolerance:** disconnected workers and timed-out jobs are re-queued
- **End to end:** a real scheduler server with 4 fake workers over TCP, and a run where one worker crashes while holding a job

## Using this folder from Docker or other folders

- **Docker:** build from the repo root and copy this folder in:
  ```dockerfile
  FROM python:3.12-slim
  WORKDIR /app
  COPY Person1/ /app/
  CMD ["python", "scheduler.py"]
  ```
  Set options with environment variables in `docker-compose.yml`. To keep `result.json`, mount a volume and set `OUTPUT_FILE` to a path inside it (for example `/output/result.json`).
- **Importing from another folder** (for example, Person 3 using `matrix_utils.multiply`): run with `PYTHONPATH=Person1`, or add `sys.path.insert(0, "Person1")` before the import.