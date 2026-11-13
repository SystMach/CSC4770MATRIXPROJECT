# Review — flaws and required changes
Source: SystMach/CSC4770MATRIXPROJECT, fetched 2026-11-13.
The following describes inspected code, not hypothetical test failures.

## Fixed in the revised Person 3 package
1. Original aggregator handled full-width row blocks only. Current scheduler creates row × column
   tiles. Added start_col placement and final trimming of bottom/right zero padding.
2. Original design imported aggregation into scheduler. Actual worker opens its own aggregator
   TCP connection and requires an ack before job_done. Added a threaded aggregator service.
3. Original Docker setup had only scheduler/worker. Added aggregator image, DNS settings,
   result mounts, actual source paths and the worker's AGGREGATOR_HOST/PORT.
4. Original benchmark required an unimplemented adapter. Added team_adapter.run using actual
   scheduler CLI flags, input files, get_info, real worker subprocesses and bounded cleanup.
5. Original tests covered old row-only protocol. Replaced with tile tests and real system tests.
6. Added exact expected block shape/ID/coordinate checks, finite-value checks, missing-block
   rejection, thread protection and identical-retry handling before sending ack.

## Team changes still recommended
1. Scheduler JobManager.job_done accepts completion from any worker for a previously handed-out
   job; it does not enforce assigned_worker or current attempt. ConnectionHandler also accepts
   job_done without prior worker registration. Verify identity/assignment and use an attempt token.
2. There is no trusted confirmation from aggregator to scheduler. A worker can send job_done
   without depositing a result, letting scheduler finish while aggregator waits. A stronger protocol
   needs a shared completion receipt or scheduler/aggregator coordination. Person 3 does not
   silently invent that protocol.
3. Scheduler matrix_utils.validate_matrix rejects booleans but accepts NaN/Infinity. Person 2
   rejects them later. Reject non-finite values before splitting or opening the server.
4. Worker connect() disables socket timeouts after connection; exchange() has an unbounded
   readline. A stalled peer can hold a worker forever and large frames can exhaust memory.
   Add configurable timeouts and bounded frame reads to Person1 and Person2 too.
5. Scheduler requeues on disconnect but has no task lease/deadline. A connected, stuck worker
   can retain a job indefinitely. Add deadlines and reassignment, with attempt IDs.
6. Worker does not reconnect/retry after an established connection fails. It exits; scheduler
   may requeue, but no replacement worker is launched automatically.
7. Worker completion omits compute_time, although scheduler job_done accepts it; scheduler's
   per-worker compute_time statistics stay zero. Send that field or obtain it from aggregator.
8. No authentication or run IDs exist on either channel. Keep deployment on a trusted private
   network and create fresh processes per run. A shared production service needs a revised protocol.
9. Aggregator derives job-ID mapping from row-major numbering because get_info supplies sizes,
   not a job manifest. This matches the inspected source; expose a manifest before changing ordering.
10. Benchmark wall time includes scheduler grace/startup/teardown. This is documented end-to-end
    timing; add a separately reported compute/distribution interval for performance analysis.

## Current Person 3 limitations
- Result acknowledgments confirm in-memory receipt, not durable storage per job. An aggregator
  crash after ack can lose accepted blocks; persistence/recovery needs coordinated team changes.
- Aggregator validates structure and final correctness when saved inputs are supplied, but cannot
  verify scheduler worker assignment because the current info protocol does not expose it.
- Local adapter selects a free port before scheduler startup; another process could claim it.
  Failures are bounded/reported; a future scheduler readiness channel can eliminate this race.
- Full output and reference multiplication are held in memory; very large matrices need streaming.
- Docker startup uses existing worker retries (10 attempts), not health checks. Slow image/service
  initialization may exceed that window; raise retry settings or introduce health checks as a team.
- No team source files were edited, and nothing has been pushed to GitHub by this update.
