# Person 3 — CSC4770 Matrix Project
Self-contained integration package adapted to https://github.com/SystMach/CSC4770MATRIXPROJECT.
Fetched 2026-11-13; source tree SHA 393f3391ad2d76e6701c92ea75b30702e29e0d1f.
team/ contains the fetched Person1 and Person2 files, unchanged. Replace these snapshots
when the team changes its interfaces. All Person 3 work is inside this folder.

## What changed
The current project splits A into row groups and B into column groups, not just A into rows.
Every job includes the entire inner multiplication dimension, so returned tiles are placed,
not summed. Zero padding on bottom/right is removed from the assembled answer.
Workers now send answers to a separate TCP aggregator. The scheduler tracks completion only.

Files:
- aggregator.py: TCP service, validated tile placement, idempotent retries, trimming, result export.
- protocol.py: bounded JSON framing, independent reference multiplication and comparison.
- team_adapter.py: launches the actual teammate programs with fresh inputs, bounded execution,
  captured logs and process cleanup; returns the assembled matrix.
- Dockerfile.scheduler, Dockerfile.worker, Dockerfile.aggregator: three service images.
- docker-compose.yml: service names, ports and shared result files.
- test_integration.py: block checks plus actual TCP system tests using 1/2/4 workers.
- benchmark.py: deterministic matrices, correctness checks, median timing, speedup and efficiency.
- REVIEW.md: flaws, fixes and remaining team work.

## Actual newline-delimited TCP JSON protocol
UTF-8 JSON, one object followed by LF per message.
Worker → scheduler: {"type":"request_job","worker_id":"w1"}
Scheduler → worker: {"type":"job","job_id":1,"start_row":0,"start_col":0,
"block_a":[[1,2]],"block_b":[[3],[4]]}
Worker → aggregator: {"type":"result","worker_id":"w1","job_id":1,
"start_row":0,"start_col":0,"result_rows":[[11]],"compute_time":0.001}
Aggregator → worker: {"type":"ack","job_id":1}
Worker → scheduler: {"type":"job_done","worker_id":"w1","job_id":1}
Scheduler → worker: {"type":"ack","job_id":1}
The worker repeats. Scheduler can return {"type":"wait","retry_after":0.2}
until all work is done, then {"type":"no_more_jobs"}.
Errors use {"type":"error","message":"description"}.

Aggregator → scheduler: {"type":"get_info"}
Scheduler replies with type=info, out_rows, out_cols, padded_rows, padded_cols,
inner_dim, block_rows, block_cols, num_jobs.
Job IDs start at 1 in row-major tile order, as implemented by Person1/matrix_utils.make_jobs.
Aggregator reconstructs this mapping and checks result coordinates against it.
Changes to that numbering require an aggregator change or an explicit manifest protocol.

## Run locally
Python 3.12 recommended; standard library only. Open a terminal in this folder:
~~~powershell
python -m unittest -v test_integration
python -m unittest discover -s team/Person1 -p "test_*.py" -v
python benchmark.py --runner team_adapter:run --sizes 25 50 --workers 1 2 4 --blocks 5 10 --repeats 3 --output benchmark_results.csv
~~~
The adapter runs all three actual components and cleans up afterward.
Benchmarks measure adapter wall time, including process startup, scheduler grace period and cleanup.
Reference multiplication and correctness checking happen outside that timed call.
Speedup=T1/Tp; efficiency=speedup/p, within the same size/block configuration.
Small examples are correctness checks, not evidence of scalable speedup.
Record hardware, Python/Docker versions, CPU limits and repeat variability for the final report.

## Run Docker
Start Docker Desktop first. From this folder:
~~~powershell
docker compose config --quiet
docker compose up --build --scale worker=4
docker compose down
~~~
Defaults: scheduler:5000, aggregator:5001 on an internal Compose network, no host ports published.
Each worker generates its own UUID. Worker connection retries handle startup order.
Scheduler saves matrices.json before serving; aggregator uses it to validate the final result.
Outputs in results/: matrices.json, run_summary.json and result.json (key C).
Run_summary is scheduler bookkeeping, not the final matrix.
Aggregator exits after all blocks arrive, validation/export and a two-second acknowledgment grace.
Scheduler exits after all job_done messages and its grace period.
Use a fresh compose run; no restart policy is configured. Remove old result files before interpreting
a failed run so stale outputs cannot be mistaken for new ones.
For other inputs, configure the scheduler's --input JSON {"A":...,"B":...} and mount that file.
Use --timeout on aggregator for larger workloads; default collection deadline is 300 seconds.

## Verification — 2026-11-13
- All 5 Person 3 tests passed (11.74 seconds), including actual scheduler/worker TCP runs,
  rectangular 3x3 by 3x5 inputs, padding, and worker counts 1/2/4.
- All 29 upstream Person 1 tests passed.
- docker compose config --quiet passed.
- Docker container execution was not tested: Docker Desktop Linux engine was not running.
- The benchmark test verifies real-run CSV generation; its tiny inputs are not a speedup study.
