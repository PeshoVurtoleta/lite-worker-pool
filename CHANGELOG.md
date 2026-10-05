# Changelog

All notable changes to `@zakkster/lite-worker-pool` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-10-05

A service mode for the @zakkster/lite-pick capstone (research/worker-set.md, decisions E1-E11). `map()` is unchanged.

### Added

- **`createWorkerSet(workerFn, opts?)` and `WORKER_STATE`.** The caller chooses the worker per job:
  `submit(i, value, { signal }?) -> Promise<number>`, or the zero-allocation `post(i, value, tag)` with an
  `onSettle(i, tag, ok, value, code)` callback (0.00 B/op in this package's code over the loopback). Per worker:
  `slots` jobs in flight (default 2; measured 207k -> 397k jobs/s on 4 threads with an empty transform, 34k -> 39k
  at ~0.1 ms of work) and a capped FIFO `queue` (default 32; full -> `LWP_QUEUE_FULL` at once). Jobs to one worker
  settle in submission order; a reply out of order takes the worker DOWN. A transform throw fails one job
  (`LWP_TRANSFORM`); a worker death or `kill(i)` fails only that worker's jobs (`LWP_WORKER_DOWN`); `respawn(i)`
  restarts one worker in the same slot. READY handshake (STARTING accepts nothing). 0-alloc probes `isReady`,
  `load`, `busySince`; `control(i, Float64Array)` hands per-worker values to the transform as its second argument
  (re-applied after respawn). Abort: a queued job never runs; an executing one rejects `LWP_ABORTED` and its late
  result is discarded. Every error carries a `code`. Types: `WorkerSet`, `WorkerSetOptions`, `SetWorker`,
  `SetSpawnSpec`, `WorkerSetErrorCode`, `WorkerState`.
- **`test/WorkerSet.test.js`** (13 cases over a step-driven loopback) and **`npm run torture:set`**
  (`test/torture-set.mjs`, real `worker_threads`): 40000-job conservation with a mid-stream kill + respawn,
  transform throws, a thread crash + 50 respawns, hung-job detection, control values, retention (1000 respawns,
  1024 create/dispose cycles, residual <= 16), and controls that must fail (`TORTURE_BREAK=set-drop|set-reorder|set-leak`).

### Changed

- The real-thread test entry (`test/torture/thread-entry.mjs`) also dispatches typed `{ t, d }` messages to
  `ctx.on(type, fn)` handlers (set mode's control values); the raw buffer path is unchanged.
- README discloses what Node's `MessagePort` allocates per job on the main thread (~1.06 KB on Node 26.8, ~1.44 KB
  on Node 22.23; transient, for `map()` too) -- outside this package's gated code path.

## [1.0.1] - 2026-08-31

Test-only maintenance release. Library source (`WorkerPool.js`) is unchanged.

### Changed

- **T6 retention gate** converted from a vacuous track-then-immediate-untrack
  `tracker.size() === 0` tautology to the finalization-authority pattern: the real
  pool is tracked with no untrack, references are hard-settled, and the residual
  live count is asserted `<= RES` (RES = 16).

### Added

- **`TORTURE_BREAK=leak`** control that pins pools to force the retention gate RED
  (residual ~2048), proving the T6 gate can fail.

## [1.0.0] - 2026-08-10

Initial release. A zero-GC data-parallel worker pool over `@zakkster/lite-worker`:
bind one worker body once, `map` an array across every core, get results back in
input order, with nothing allocating in the per-item dispatch loop.

### Added

- **`createWorkerPool(workerFn, opts?) -> Pool`** -- binds the per-item transform
  `(item) => result` ONCE, serialized into all N workers. No per-`map` worker
  function. `opts`: `{ size?, scratchBytes?, name?, spawn? }`.
- **`pool.map(items) -> Promise<results[]>`** -- a saturating job queue feeds every
  worker (each pulls the next unassigned index as it finishes); results are written
  into a preallocated array BY INDEX so output order === input order regardless of
  completion order. One batch at a time.
- **Per-worker transferable scratch ArrayBuffer** -- one per worker, allocated once
  at construction, ping-ponged main<->worker via ownership transfer with a single
  reused transfer list, so the per-item dispatch path allocates nothing. `map()`
  allocates only the results array + one Promise per batch (cold).
- **`pool.stats() -> { size, queued, active, done }`** and **`pool.size`** -- cold
  progress snapshots.
- **`pool.dispose()`** -- idempotent full teardown; terminates all workers, rejects
  an in-flight batch, and a second call is a no-op that never throws.
- **`opts.spawn`** -- an injectable worker factory returning
  `{ send, onRaw, onError, terminate }`, so the pool logic is transport-agnostic and
  can be proven over a real `node:worker_threads` bridge without a browser (mirrors
  lite-worker's `frameChannel(transport, ...)`). Default factory builds workers via
  `defineWorker`.
- **`VERSION`** export, three-place synced with package.json and this file.
- **Fail-closed sizing** -- a missing/NaN/negative/zero `size` floors to
  `navigator.hardwareConcurrency`, then to 1; never 0.
- **Fail-closed worker errors** -- an uncaught transform throw (reported on the
  reserved `lwp:err` channel) or a worker crash (via the handle's `onError`)
  rejects the in-flight `map()` with the underlying error instead of hanging it,
  and poisons the pool until `dispose()` (a thrown transform is deterministic, so
  the item is not reassigned).
- **Fail-closed configuration** -- an unknown option key throws with a
  did-you-mean hint (never a silent default); `map()` rejects uniformly for
  null/undefined/non-array-like input rather than throwing a raw `TypeError`.
- **Torture suite** (`test/torture.mjs`, `npm run torture`) -- T0 conservation
  (10k items over real threads, every item processed exactly once, filed by index),
  T1 ordering (out-of-order completion, output order preserved), T2 saturation (no
  starved worker, queue drains, `active` returns to 0), T3 worker-error (a throwing
  transform rejects `map()` bounded over the real bridge, no hang, no lost item),
  T6 retention (`< 8 B/op` over 10k dispatch hops via @zakkster/lite-gc-profiler,
  scratch byteLength stable, @zakkster/lite-leak `tracker.size() === 0` after
  create/dispose cycles), and T9 controls that prove the conservation, ordering,
  retention, and worker-error gates each fail on a break.
- **Release gate** (`bench/gc-gate.mjs`, `npm run gate`) -- the fast per-item
  retention check.

[1.0.1]: https://github.com/PeshoVurtoleta/lite-worker-pool/releases/tag/v1.0.1
[1.0.0]: https://github.com/PeshoVurtoleta/lite-worker-pool/releases/tag/v1.0.0
