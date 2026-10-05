# Research: a service-shaped mode for lite-worker-pool -- `createWorkerSet`

**Status:** DECIDED 2026-10-05 (all recommendations accepted, E1-E11) and IMPLEMENTED 2026-10-05 in 1.1.0:
`createWorkerSet` in `WorkerPool.js`, `test/WorkerSet.test.js`, `test/torture-set.mjs` (`npm run torture:set`).
**Why now:** the lite-pick capstone (LitePick `research/capstone-pickEcosystem.md`, decided 2026-10-05, C3) needs a
worker layer where a load balancer CHOOSES the worker. lite-pick's ADR 0001 said this is the one place lite-pick
adds value next to this package: keyed, push and heterogeneous dispatch. It is P0 of the capstone.

**Question:** what does lite-worker-pool need so that a caller (lite-pick's `/pool`, or anyone) can run a job on
worker i, queue a few behind it, survive one worker dying, restart that worker alone, and see each worker's load
and readiness -- without touching `map()` and without allocating in our own code per job?

**Short answer:** a second export, `createWorkerSet(workerFn, opts)`, in the same file. Per worker: two jobs in
flight (measured: +71-92% throughput for short jobs over one), a capped FIFO queue behind them, its own state
(STARTING -> READY -> DOWN), and `respawn(i)`. A transform throw fails ONE job; a dead worker fails ITS jobs only.
`map()` and its batch semantics stay exactly as they are -- the two modes never share a queue (Akka makes the
same split: a shared mailbox and keyed routing cannot be combined).

---

## 1. What `map()` does today, and why the service case needs something else

`createWorkerPool(fn).map(items)` is a batch: each worker pulls the next unassigned item (work stealing), results
land by index, one batch at a time, and any worker error rejects the batch and POISONS the pool (dispose and
recreate). All of that is right for a batch. For a service:

| Need | `map()` today | WorkerSet |
|---|---|---|
| Run a job on a CHOSEN worker | no -- workers pull | `submit(i, value)` |
| More than one job per worker | one scratch buffer -> one in flight | k slots + a capped queue |
| One worker dies | the whole pool is poisoned | that worker goes DOWN, its jobs fail, the rest serve |
| Restart one worker | no | `respawn(i)` |
| Is worker i up? how loaded? | `stats()` for the whole pool | `isReady(i)`, `load(i)`, `state(i)` -- 0-alloc reads for health probes |
| Make a worker genuinely slow / flaky (the demo's fault injection) | no | per-worker control values the transform reads |

## 2. Measured on this machine (Node 26.8, 12 cores, `worker_threads`, 32-byte transferred buffer)

**Jobs in flight per worker** (4 workers, jobs/s):

| work per job | 1 in flight | 2 | 4 |
|---|---|---|---|
| none | 207k | 397k | 510k |
| ~0.01 ms | 168k | 287k | 296k |
| ~0.1 ms | 33.6k | 38.9k | 39.2k |

With one job in flight the worker idles for a whole main-thread round trip between jobs; a second slot hides it.
Beyond two the gain on real work is small. -> default 2 (E3).

**Start-up:** a probe that waited a fixed 200-1000 ms before sending failed intermittently under load; with a
READY message from the worker it never failed. -> a READY handshake, and STARTING is not eligible (E6).

**What a job costs the main thread** (`--trace-gc`, pinned 1 MB semi-space, 400k jobs, 2 in flight):

| | Node 26.8 | Node 22.23 |
|---|---|---|
| Node's MessagePort receive alone (we touch nothing) | ~1.06 KB/job | ~1.44 KB/job |
| + one `Float64Array` view over the returned buffer | ~1.19 KB/job | ~1.56 KB/job |

So per-job allocation is dominated by Node's own messaging (the message event + deserialization), not by any
pool code. The existing `bench/gc-gate.mjs` measures `map()`'s own dispatch over an in-process loopback (< 8 B/op)
and never sees this. We keep that discipline for WorkerSet's own code AND disclose the transport cost (E10).

## 3. Prior art (summarised; full table in the capstone note, section 7)

- **Akka / Pekko:** keyed routing (`ConsistentHashingPool`) and a shared work-stealing mailbox (`BalancingPool`)
  are separate routers and cannot be combined -> `map()` and WorkerSet stay separate.
- **Piscina:** a pluggable `loadBalancer(task, workers)`, idle-first by default, `concurrentTasksPerWorker`,
  `maxQueue` (`'auto'` = threads squared). **poolifier:** per-worker queues opt-in, default size = pool size
  squared, load = executing + queued. **workerpool / threads.js:** one queue, unbounded by default -- do not copy.
- **BookKeeper `OrderedExecutor` / Kafka:** same key -> same thread -> FIFO per key. Per-worker FIFO is what
  makes strict affinity (capstone C8) give per-key ordering.
- **HAProxy `hash-preserve-affinity maxqueue`:** a full queue is a reason to leave the hash target -> a full
  WorkerSet queue rejects immediately with a code, so `/pool` can fail over (or a strict caller can wait).

## 4. Proposed API (one more export from `WorkerPool.js`)

```js
import { createWorkerSet } from '@zakkster/lite-worker-pool';

const set = createWorkerSet((item, ctl) => work(item, ctl), {
  size: 8,            // workers (as createWorkerPool)
  slots: 2,           // jobs in flight per worker (E3)
  queue: 32,          // queued jobs per worker beyond the slots; full -> reject (E4)
  spawn, name,        // as createWorkerPool (injectable transport, used by the tests)
  onSettle,           // optional zero-alloc completion callback for post() (E2)
});

await set.ready();                        // every worker has said READY (or rejects on a start failure)
const r = await set.submit(i, value, { signal });   // Promise<number>; rejects with a coded Error
set.post(i, value, tag);                  // -> boolean accepted; completion via onSettle(i, tag, ok, value)
set.state(i);                             // STARTING | READY | DOWN  (frozen enum)
set.isReady(i); set.load(i);              // 0-alloc reads: health probes, a balancer's inflight
set.kill(i);                              // terminate worker i now (DOWN; its jobs fail LWP_WORKER_DOWN)
await set.respawn(i);                     // fresh worker in slot i; READY after its handshake
set.control(i, ctlValues);                // Float64Array copied to worker i; the transform sees it as `ctl`
set.dispose();
```

Composition with lite-pick (the capstone): `pool.run((i, signal) => set.submit(i, job, { signal }), { tries: 2 })`
-- lite-pick chooses, WorkerSet executes; a rejection (queue full, worker down) is a failed attempt, so `/pool`
fails over to a different worker. `/pool`'s own `inflight[i]` then counts executing + queued for free. WorkerSet
imports nothing from lite-pick (and lite-pick nothing from it).

## 5. Decisions needed

1. **E1 Shape:** a second export `createWorkerSet` in `WorkerPool.js` (single main file law), `map()` untouched
   (recommended). Name alternatives: `createWorkerService`, `createWorkerFleet`.
2. **E2 Two completion paths:** `submit()` -> Promise (what `/pool` needs; one Promise per job, like any async
   API) AND `post()` + `onSettle` callback, which allocates nothing in our code per job (the zero-GC path for
   callers that do not need a Promise). `submit` is built on the same internals. Recommended: both.
3. **E3 Slots:** default 2 jobs in flight per worker (measured above); configurable 1-8.
4. **E4 Queue:** a fixed-capacity FIFO ring per worker (default 32, never unbounded); a full queue REJECTS at
   once with `LWP_QUEUE_FULL` (no waiting inside the set -- the caller decides: fail over, or retry later).
5. **E5 Failure isolation:** a transform throw rejects only that job (`LWP_TRANSFORM`, with the message); the
   worker stays READY and its buffer comes back (the set-mode worker body returns it flagged -- scratch grows to
   24 bytes). A worker death or `kill(i)`: worker i goes DOWN, its in-flight and queued jobs reject
   `LWP_WORKER_DOWN`, every other worker keeps serving, nothing is poisoned. `respawn(i)` makes a fresh worker
   in the same slot (stable index, so a keyed balancer's mapping stays valid).
6. **E6 Readiness and load for health probes:** STARTING until the worker's READY message (never eligible
   before), `isReady(i)` and `load(i)` are 0-alloc reads, `busySince(i)` gives the age of the oldest executing
   job so a health probe can call a worker hung and the supervisor can `kill` + `respawn` it.
7. **E7 Per-worker control values:** `control(i, Float64Array)` copies a few numbers to worker i; the transform
   receives them as its second argument. This is how the capstone makes a worker GENUINELY slow or flaky (the
   cost is paid inside the real thread), rather than faking it on the main thread. `map()`'s transform keeps
   its one-argument contract.
8. **E8 Ordering guarantee:** jobs submitted to the same worker complete in submission order (per-worker FIFO,
   tested). This is what makes strict keyed routing give per-key order (capstone C8).
9. **E9 Abort:** a `signal` aborts a QUEUED job (removed, rejects with the signal's reason, never sent); an
   EXECUTING job cannot be interrupted inside the thread -- it rejects at once with `LWP_ABORTED`, its late
   result is discarded, and its slot frees when the reply arrives. A hung job is handled by `kill(i)`.
10. **E10 Transport cost, disclosed:** our own per-job code is gated < 8 B/op over the loopback (as `map()` is);
    the README states Node's MessagePort cost measured above (~1.1 KB/job Node 26, ~1.4 KB/job Node 22). A
    SharedArrayBuffer ring transport would remove it (Node always; browsers only when cross-origin isolated --
    on GitHub Pages only through a service-worker header shim) -- a separate research item, NOT in this release.
11. **E11 Release:** lite-worker-pool 1.1.0 (additive), built on top of your pending, uncommitted 1.0.1
    (test-only) -- which should be committed first.

## 6. Gates (the package's existing discipline, extended)

- `node:test` over the synchronous loopback: submit/post, queue cap, abort (queued and executing), codes,
  state transitions, control values, per-worker FIFO.
- Torture over REAL `worker_threads` (the existing harness): conservation (every submitted job settles exactly
  once -- resolved or rejected, never lost), per-worker FIFO, isolation (kill one of 8 mid-stream: only its jobs
  fail, the other 7 keep their throughput), respawn (READY again, serves), transform throw (one job fails, worker
  stays READY), a hung job detected through `busySince`.
- Retention: 1000 kill/respawn cycles and 1000 set create/dispose cycles, finalization residual (the T6 pattern).
- GC gate: the set's own dispatch + settle path < 8 B/op over the loopback (post/onSettle path).
- Break controls (`TORTURE_BREAK=...`) for each new gate: a queue that grows past its cap, a death that poisons
  the whole set, a reorder within a worker, a lost job.

## What we would NOT do

- Merge the modes (keyed jobs in `map()`'s shared queue) -- Akka shows why they exclude each other.
- Unbounded queues (workerpool / threads.js defaults).
- Work stealing between workers' queues: it would break keyed placement and per-key order; idle workers in a
  set just receive what the chooser sends them.
- A chooser inside the set: selection is lite-pick's job (or the caller's); the set only executes.
- Fake slowness on the main thread for the demo: `control()` puts it inside the real worker.
