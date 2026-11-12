# Person 1: Scheduler + Matrix Division
 
The scheduler coordinates the work. It validates the input matrices, splits A into groups of rows and B into groups of columns, pairs them into equal-sized jobs, and hands those jobs to workers over TCP as they ask for them. Workers send their answers to the aggregator (Person 3) and tell the scheduler when each job is done. The scheduler never sees the answers.
 
## Files
 
| File | Purpose |
|---|---|
| `scheduler.py` | TCP server, worker registration, job queue, job status tracking. Also the Docker entry point. |
| `matrix_utils.py` | Matrix validation, zero-padding, row and column splitting, random matrix generation, and a plain multiply used for verification. |
| `test_scheduler.py` | Unit and end-to-end tests (validation, padding, splitting, queue behavior, `job_done`, worker crashes). |
 
Uses only the Python standard library, so nothing needs to be installed. Requires Python 3.8+.
 
## Quick start
 
From inside this folder:
 
```bash
# Random 205x205 multiplication, split into 20x20 jobs
python scheduler.py --size 205 --block-size 20
```
 
Then start the aggregator (Person 3) and one or more workers (Person 2). When every job is done, the scheduler logs the total time, writes `run_summary.json`, and exits.
 
```
20:32:37 [scheduler] listening on 0.0.0.0:5000
20:32:37 [scheduler] result 205x205 (padded to 220x220), 121 jobs of 20x20
20:32:39 [scheduler] all 121 jobs completed
20:32:40 [scheduler] finished in 0.5858 s using 4 workers
20:32:40 [scheduler] wrote run_summary.json
```
 
## Configuration
 
Every setting can be passed as a command-line flag or as an environment variable (environment variables are how Docker Compose sets them).
 
| Flag | Environment variable | Default | Meaning |
|---|---|---|---|
| `--host` | `SCHEDULER_HOST` | `0.0.0.0` | Address to listen on |
| `--port` | `SCHEDULER_PORT` | `5000` | TCP port workers and the aggregator connect to |
| `--size` | `MATRIX_SIZE` | `100` | Size N of the random N×N matrices A and B |
| `--block-size` | `BLOCK_SIZE` | `10` | Rows of A **and** columns of B per job |
| `--input` | `MATRIX_FILE` | *(none)* | JSON file with `{"A": [[...]], "B": [[...]]}`; overrides `--size` |
| `--save-matrices` | `SAVE_MATRICES` | *(none)* | Write the A and B used to this JSON file, so the aggregator can verify the answer |
| `--seed` | `SEED` | `42` | Random seed, so runs are repeatable |
| `--output` | `OUTPUT_FILE` | `run_summary.json` | Where to write the timing and job summary |
| `--grace` | `GRACE_PERIOD` | `2` | Seconds to wait after the last job so workers receive `no_more_jobs` |
 
## How it works
 
### 1. Validation
 
Before any job is created, `matrix_utils.validate_pair` checks that:
 
- both matrices are non-empty
- every row of a matrix has the same length
- every value is a number
- the number of columns of A equals the number of rows of B
Invalid input stops the scheduler with an error message.
 
### 2. Row × column block division
 
A (m × n) is split into groups of rows, and B (n × p) is split into groups of columns. Every row group is paired with every column group, and each pair is one job:
 
```
block_a = a group of rows of A,    with all n columns
block_b = a group of columns of B, with all n rows
```
 
`block_a × block_b` is a **complete** block of the result, which belongs at (`start_row`, `start_col`). Because each pair keeps the full inner length n, workers never produce partial sums that would need adding together later.
 
```
A is 25x6, B is 6x25, block size 10   →   result is 25x25
 
          B cols 0-9   B cols 10-19   B cols 20-29
A rows 0-9      Job 1        Job 2          Job 3
A rows 10-19    Job 4        Job 5          Job 6
A rows 20-29    Job 7        Job 8          Job 9
```
 
### 3. Equal-sized jobs (zero padding)
 
So that every job is the same size, the scheduler adds zero rows to the bottom of A and zero columns to the right of B when the sizes don't divide evenly. In the example above, A gets 5 extra zero rows (25 → 30) and B gets 5 extra zero columns (25 → 30).
 
The extra rows and columns of the result are all zeros. **The aggregator must trim the assembled matrix back to `out_rows` × `out_cols`**, which it gets from `get_info` (see below). Padding only goes at the bottom and right, so `start_row` and `start_col` are already the correct positions in the real result.
 
If the block size is larger than the matrix in either direction, it's reduced to the matrix size in that direction so jobs aren't mostly padding. That's why `get_info` reports `block_rows` and `block_cols` separately, even though both normally equal `BLOCK_SIZE`.
 
Each job's metadata is stored as:
 
| Field | Meaning |
|---|---|
| `job_id` | Unique ID, starting at 1, ordered row group by row group |
| `start_row`, `end_row` | Rows of the result this job covers (`end_row` is exclusive) |
| `start_col`, `end_col` | Columns of the result this job covers (`end_col` is exclusive) |
| `status` | `pending`, `in_progress`, or `completed` |
| `assigned_worker` | Worker currently holding (or that completed) the job |
| `assigned_time`, `completed_time` | Timestamps |
| `attempts` | How many times the job has been handed out |
 
### 4. Dynamic work queue
 
Jobs are not assigned to workers in advance. They wait in a shared queue, and each worker takes the next one when it's ready, so faster workers end up doing more jobs and no worker sits idle while others are busy.
 
Each connection runs in its own thread. The queue and status table are shared and protected by one lock, so two workers can never receive the same job.
 
### 5. Completion
 
When a worker finishes a job, it sends the result block to the aggregator and then sends `job_done` to the scheduler. The scheduler marks the job completed and replies with `ack`. Once every job is completed, any worker that asks for more work gets `no_more_jobs`.
 
### 6. Fault tolerance
 
| Situation | What the scheduler does |
|---|---|
| A worker disconnects while holding a job | The job goes back to the front of the queue for another worker |
| A worker disconnects after finishing its jobs | Nothing; finished jobs stay finished |
| The same job is reported done twice | The first counts; the second still gets `ack` |
| `job_done` for an unknown job, or one never handed out | `error` reply |
 
## Message protocol
 
Messages are JSON over TCP, one JSON object per line (each message ends with `\n`). Every message sent to the scheduler gets exactly one reply.
 
### Worker → Scheduler
 
| Message | Example |
|---|---|
| Request a job | `{"type": "request_job", "worker_id": "worker-1"}` |
| Report a job finished | `{"type": "job_done", "worker_id": "worker-1", "job_id": 3}` |
 
A worker is registered automatically the first time it sends its `worker_id`. `job_done` may also include `"compute_time": 0.0012` (seconds); if present, it's added to that worker's stats in `run_summary.json`.
 
### Scheduler → Worker
 
| Message | When | What the worker should do |
|---|---|---|
| `{"type": "job", "job_id": 3, "start_row": 0, "start_col": 20, "block_a": [[...]], "block_b": [[...]]}` | A job is available | Multiply `block_a` by `block_b`, send the result to the aggregator, then send `job_done` |
| `{"type": "ack", "job_id": 3}` | `job_done` accepted | Request the next job |
| `{"type": "wait", "retry_after": 0.2}` | Queue empty, but other workers still have jobs | Sleep `retry_after` seconds, then request again. Don't exit. |
| `{"type": "no_more_jobs"}` | Every job is complete | Disconnect and exit |
| `{"type": "error", "message": "..."}` | Bad message | Log the message and continue |
 
### Aggregator → Scheduler
 
| Message | Reply |
|---|---|
| `{"type": "get_info"}` | `{"type": "info", "out_rows": 25, "out_cols": 25, "padded_rows": 30, "padded_cols": 30, "inner_dim": 6, "block_rows": 10, "block_cols": 10, "num_jobs": 9}` |
 
The aggregator should ask for this when it starts, before the run finishes, because the scheduler exits after the last job. It tells the aggregator:
 
- `out_rows` × `out_cols`: the real result size to trim to
- `padded_rows` × `padded_cols`: the size including zero padding
- `num_jobs`: how many blocks to expect before the matrix is complete
### Example conversation
 
```
Worker → {"type": "request_job", "worker_id": "worker-1"}
Sched  → {"type": "job", "job_id": 1, "start_row": 0, "start_col": 0, "block_a": [[1, 2]], "block_b": [[3], [4]]}
          (worker computes [[11]] and sends it to the aggregator)
Worker → {"type": "job_done", "worker_id": "worker-1", "job_id": 1}
Sched  → {"type": "ack", "job_id": 1}
Worker → {"type": "request_job", "worker_id": "worker-1"}
Sched  → {"type": "wait", "retry_after": 0.2}
Worker → {"type": "request_job", "worker_id": "worker-1"}
Sched  → {"type": "no_more_jobs"}
```
 
### Order matters: aggregator first, then `job_done`
 
A worker should send its result to the aggregator **before** sending `job_done`. If it reported `job_done` first and then crashed, the scheduler would think the job was finished, but the aggregator would never get that block.
 
### Why `wait` exists
 
Near the end of a run, the queue can be empty while another worker is still working on the last job. If the scheduler sent `no_more_jobs` at that point and the busy worker then crashed, its job would go back on the queue with no worker left to take it. `wait` keeps idle workers connected until every job is actually complete.
 
After all jobs finish, the scheduler shuts down. A worker that connects after that gets "connection refused" and should treat it as the end of the run.
 
## Output file
 
`run_summary.json` contains the sizes and timing of the run, which Person 3 can use for benchmarking:
 
```json
{
  "out_rows": 205, "out_cols": 205,
  "padded_rows": 220, "padded_cols": 220,
  "inner_dim": 205, "block_rows": 20, "block_cols": 20,
  "num_jobs": 121,
  "num_workers": 4,
  "total_time_sec": 0.5858,
  "workers": {
    "worker-1": {"jobs_completed": 32, "compute_time": 0.41, "registered_time": 1731378757.2}
  },
  "jobs": [
    {"job_id": 1, "start_row": 0, "end_row": 20, "start_col": 0, "end_col": 20,
     "status": "completed", "assigned_worker": "worker-1",
     "assigned_time": 1731378757.5, "completed_time": 1731378757.6, "attempts": 1}
  ]
}
```
 
`total_time_sec` is measured from the moment the first job is handed out to the moment the last `job_done` arrives, so time spent waiting for workers to start isn't counted.
 
The result matrix itself is **not** in this file. The aggregator builds it. To check the answer, run the scheduler with `--save-matrices ab.json` and have the aggregator compare its result with a normal multiplication of the A and B in that file.
 
## Tests
 
From inside this folder:
 
```bash
python -m unittest -v test_scheduler.py
```
 
Or from the repo root:
 
```bash
python -m unittest discover -s Person1
```
 
The 29 tests cover:
 
- **Validation:** empty matrices, uneven rows, non-numbers, mismatched dimensions
- **Splitting and padding:** equal-sized blocks, zero padding, column blocks, every cell of the result covered exactly once
- **Jobs:** message format, full inner length in every block, padding is zeros, oversized blocks shrunk, `get_info` sizes
- **Queue:** jobs handed out in order, `wait` while jobs are running, `no_more_jobs` only after every job is done, no job handed out twice across 8 threads
- **Completion:** unknown or never-assigned jobs rejected, duplicate `job_done` ignored
- **Fault tolerance:** a disconnected worker's unfinished jobs are re-queued, finished jobs are not
- **End to end:** a real scheduler server with 4 fake workers and a fake aggregator over TCP (the assembled, trimmed result matches a normal multiply), a run where one worker crashes while holding a job, and Person 2's example job
## Using this folder from Docker or other folders
 
- **Docker:** build from the repo root and copy this folder in:
```dockerfile
  FROM python:3.12-slim
  WORKDIR /app
  COPY Person1/ /app/
  CMD ["python", "scheduler.py"]
```
  Set options with environment variables in `docker-compose.yml`. To keep `run_summary.json` or the saved matrices, mount a volume and point `OUTPUT_FILE` and `SAVE_MATRICES` at paths inside it (for example `/output/run_summary.json`). The aggregator can mount the same volume to read `ab.json`.
- **Importing from another folder** (for example, Person 3 using `matrix_utils.multiply` to verify): run with `PYTHONPATH=Person1`, or add `sys.path.insert(0, "Person1")` before the import.
 
