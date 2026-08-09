# @zakkster/lite-worker-pool

> Zero-GC data-parallel worker pool over @zakkster/lite-worker. Bind one worker body ONCE, `map` an array across every core, and get results back in input order -- with nothing allocating in the per-item dispatch loop. A saturating job queue keeps every worker fed; a per-worker transferable scratch buffer ping-pongs in and out so steady-state dispatch is allocation-free. Fails closed: a worker throw or death rejects the batch instead of hanging it.

[![npm version](https://img.shields.io/npm/v/@zakkster/lite-worker-pool.svg?style=for-the-badge&color=latest)](https://www.npmjs.com/package/@zakkster/lite-worker-pool)
[![sponsor](https://img.shields.io/badge/sponsor-PeshoVurtoleta-ea4aaa.svg?logo=github)](https://github.com/sponsors/PeshoVurtoleta)
![Zero-GC](https://img.shields.io/badge/Zero--GC-Engine-00C853?style=for-the-badge&logo=leaf&logoColor=white)
[![npm bundle size](https://img.shields.io/bundlephobia/minzip/@zakkster/lite-worker-pool?style=for-the-badge)](https://bundlephobia.com/result?p=@zakkster/lite-worker-pool)
[![npm downloads](https://img.shields.io/npm/dm/@zakkster/lite-worker-pool?style=for-the-badge&color=blue)](https://www.npmjs.com/package/@zakkster/lite-worker-pool)
[![npm total downloads](https://img.shields.io/npm/dt/@zakkster/lite-worker-pool?style=for-the-badge&color=blue)](https://www.npmjs.com/package/@zakkster/lite-worker-pool)
![Tree-Shakeable](https://img.shields.io/badge/tree--shakeable-yes-brightgreen)
![TypeScript](https://img.shields.io/badge/TypeScript-Types-informational)
![Dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)
[![license](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](./LICENSE.txt)

## The N-core pool `lite-worker` deliberately isn't

`@zakkster/lite-worker` gives you ONE off-thread core with a clean main thread: define a worker inline, ping-pong transferable buffers, keep 60fps rendering unblocked. That is exactly one extra core -- by design, so the core stays single-file and dependency-free. What it never does is fan a batch of work out across all the cores the machine actually has.

`lite-worker-pool` is that piece. It builds N workers over `defineWorker`, binds one transform into all of them, and exposes a single `map(items)` that saturates every worker with a job queue and files results back in input order. The pool depends on the core; the core never depends on the pool.

```bash
npm install @zakkster/lite-worker-pool
```

Peer dependency (not bundled, install it alongside):

```bash
npm install @zakkster/lite-worker
```

```js
import { createWorkerPool } from '@zakkster/lite-worker-pool';

// The transform is serialized into EVERY worker, so it must be self-contained --
// it cannot close over anything (it crosses a thread boundary). Items in and
// results out are numbers (see "Numeric items" below).
const pool = createWorkerPool((n) => {
  // an expensive per-item job -- runs off the main thread, one per core
  let x = n;
  for (let i = 0; i < 2000; i++) x = (x * 1.0000001 + 1) % 4294967296;
  return x;
});

const items = Uint32Array.from({ length: 100000 }, (_, i) => i);

const results = await pool.map(items);   // saturates all cores; results in input order
console.log(results.length, results[0]); // 100000, results[i] === transform(items[i])

pool.dispose();                          // terminate every worker; map() throws after
```

One transform, one `map`, N cores, zero allocation on the per-item dispatch path. Worker count defaults to `navigator.hardwareConcurrency` (floored to 1, never 0). In Node -- or any host without a Blob-URL Worker -- inject `opts.spawn` to run the identical pool logic over a `node:worker_threads` bridge (this is how the torture suite proves it without a browser).

---

## Table of contents

- [Why this exists](#why-this-exists)
- [What you get](#what-you-get)
- [How map, the queue, and the scratch fit together](#how-map-the-queue-and-the-scratch-fit-together)
- [API reference](#api-reference)
  - [The factory](#the-factory)
  - [The pool](#the-pool)
  - [Injecting a transport](#injecting-a-transport)
  - [Contract values](#contract-values)
- [Fail-closed behavior](#fail-closed-behavior)
- [Composability with the ecosystem](#composability-with-the-ecosystem)
- [Zero-GC design notes](#zero-gc-design-notes)
- [Throughput](#throughput)
- [Design decisions worth knowing](#design-decisions-worth-knowing)
- [Testing](#testing)
- [What this is not](#what-this-is-not)
- [Ecosystem](#ecosystem)

---

## Why this exists

Fanning batch work across cores in the browser (or Node) has three problems that no small library solves at once:

1. **Saturating every worker, not round-robining them.** A naive pool assigns item `k` to worker `k % N`. If job durations vary -- and for real work they always do -- fast workers finish and idle while one slow worker holds the tail. The pool has to be a job QUEUE: each worker pulls the next unassigned item the instant it finishes, so no worker sits idle while work remains. lite-worker-pool does exactly this, over a saturating index queue.

2. **Order without allocation.** Results must come back in input order even though workers finish out of order -- but you cannot afford to allocate a wrapper object, a closure, or a promise per item to remember where each result belongs. The pool writes each result into a preallocated array BY INDEX (the index round-trips inside the transferable scratch), so ordering is free and the per-item path allocates nothing.

3. **Failing closed instead of hanging.** If a worker's transform throws, or a worker dies, a pool that only settles on success just... never settles. The `map()` promise hangs forever with the item silently lost. lite-worker-pool subscribes to a worker error channel and rejects the batch with the underlying error the moment a worker fails -- it never drops an item and never hangs.

Existing options: hand-rolled `new Worker()` fan-out (round-robin, no queue, no ordering guarantee, no error path), a Comlink-based pool (Proxy-per-call allocation, structured clone per job), or a heavyweight thread-pool framework (not embeddable, not zero-GC). lite-worker-pool is the API for this specific job -- embarrassingly parallel numeric batch work -- and nothing else.

---

## What you get

- **`createWorkerPool(workerFn, opts?)`** -- one factory that binds the per-item transform `(item) => result` ONCE, serialized into all N workers (there is NO per-`map` worker function), spawns the workers, and returns a `Pool`. Build it once; reuse it across many `map` calls.
- **`pool.map(items)`** -- dispatch every item across the pool through a saturating job queue and resolve with an array the length of `items`, filled BY INDEX so output order === input order regardless of completion order. One batch at a time.
- **`pool.stats()`** -- a cold `{ size, queued, active, done }` progress snapshot.
- **`pool.size`** -- the resolved worker count.
- **`pool.dispose()`** -- idempotent full teardown: terminate every worker, drop handlers, reject an in-flight batch. A second call is a no-op that never throws.
- **`opts.spawn`** -- an injectable worker factory, so the pool logic is transport-agnostic and can run over a real `node:worker_threads` bridge (or any mock) without a browser -- mirroring lite-worker's `frameChannel(transport, ...)`.
- **`VERSION`** -- the package version constant, three-place synced with `package.json` and `CHANGELOG.md`.

Full types ship in [`WorkerPool.d.ts`](./WorkerPool.d.ts). Every export is documented.

---

## How map, the queue, and the scratch fit together

<details>
<summary>The dispatch loop, the ownership ping-pong, and why ordering is free.</summary>

**One transform, bound once.** `createWorkerPool(workerFn)` serializes `workerFn` (via `.toString()`, inside `defineWorker`) into every worker at construction. Because the body crosses a thread boundary, it cannot close over anything -- it must be self-contained. There is no way to hand a different transform to `map`; a pool IS its transform. Need two transforms? Build two pools.

**The saturating queue.** `map(items)` seeds each worker with its first item, then every time a worker returns a result the pool hands it the next unassigned index -- until the queue drains. A worker is never idle while work remains, and a slow item never blocks a fast worker from taking more. `stats().queued` is the count not yet assigned; `stats().active` is the count in flight; both return to 0 when the batch finishes.

**The ownership ping-pong.** Each worker owns exactly ONE transferable scratch `ArrayBuffer`, allocated once at pool construction. To dispatch, the main side writes `[index, value]` into the scratch's two `Float64` slots and TRANSFERS the buffer to the worker (transfer moves memory, it does not copy). The worker applies the transform to the value slot and transfers the same buffer back. The main side reads the result, files it under `index`, and immediately reuses that buffer for the next queued item. The buffer count is conserved at exactly `size` -- a buffer always comes to rest back at its worker when the queue drains.

**Why ordering is free.** The job index rides in slot 0 of the scratch and round-trips untouched, so when a result comes back the pool knows its output slot without holding any per-item bookkeeping. Results are written into a preallocated `Array(items.length)` by that index -- so out-of-order completion produces in-order output with zero extra allocation.

**Why the hot path is allocation-free.** The dispatch loop reuses a single one-element transfer list across every `send`, and reuses each worker's scratch buffer for the life of the pool. The only per-hop cost is one transient `Float64Array` view header rebuilt over the returned buffer (a transfer mints a fresh `ArrayBuffer` identity each hop, so the view cannot be cached) -- transient, GC'd, never retained. `map()` itself allocates the results array plus one Promise per batch; that is cold, once per batch, not per item.

</details>

---

## API reference

### The factory

```ts
createWorkerPool(workerFn, opts?): Pool
```

- **`workerFn`** -- the per-item transform `(item: number) => number`. Serialized ONCE into every worker; MUST be self-contained (no closure over module scope, no bare `import`). There is no per-`map` transform.
- **`opts.size`** -- worker count. Fail-closed: a missing / NaN / negative / zero value floors to `navigator.hardwareConcurrency`, then to 1. Never 0.
- **`opts.scratchBytes`** -- per-worker scratch byte length. Floors to 16 (two `Float64` slots: index + value).
- **`opts.name`** -- pool name, used for worker naming. Default `"lite-worker-pool"`.
- **`opts.spawn`** -- injectable worker factory `(spec) => PoolWorker`. Defaults to a real lite-worker over a Blob URL. See [Injecting a transport](#injecting-a-transport).
- An **unknown option key throws** at construction with a did-you-mean hint -- misconfiguration fails loud, never silently defaults.

Returns a `Pool`. The factory validates the config and allocates every scratch buffer and worker up front.

### The pool

```ts
pool.map(items): Promise<number[]>     // dispatch all items; results in INPUT order
pool.stats(): PoolStats                // { size, queued, active, done }
pool.size: number                      // resolved worker count
pool.dispose(): void                   // terminate all workers; idempotent
```

- **`map(items)`** -- `items` is an `ArrayLike<number>` (a plain array or a typed array). Resolves with a `number[]` the length of `items`, `results[i]` === `workerFn(items[i])`. One batch at a time: a second concurrent `map` rejects. An empty array resolves to `[]`. See [Fail-closed behavior](#fail-closed-behavior) for every rejection path.
- **`stats()`** -- a cold snapshot (allocates one small object per call). `queued` is items not yet assigned (0 when idle), `active` is jobs in flight, `done` is replies filed in the current or last batch.
- **`dispose()`** -- terminates every worker, detaches every handler, and rejects an in-flight batch. Idempotent: a second call is a no-op and never throws.

### Injecting a transport

The pool never talks to a `Worker` directly -- it drives a minimal `PoolWorker` surface, so you can run the identical pool logic over any transport. This is what lets the torture suite prove the pool over real threads with no browser.

```ts
interface PoolWorker {
  send(buffer: ArrayBuffer, transfer?: Transferable[]): void;
  onRaw(handler: (buffer: ArrayBuffer) => void): () => void;   // returns off()
  onError(handler: (error: Error) => void): () => void;        // returns off()
  terminate(): void;
}

createWorkerPool(workerFn, {
  spawn(spec) {                 // spec: { workerFn, index, name, scratchBytes }
    // build a PoolWorker over node:worker_threads, a mock, or anything else
    return { send, onRaw, onError, terminate };
  },
});
```

`onError` is **required and load-bearing**: the pool subscribes to it so a worker throw or death fails the batch closed instead of hanging. An injected factory that omits `onError` reintroduces the hang -- forward the transport's uncaught-error / thread `error` events to it. The default factory (over `defineWorker`) wires all four for you.

### Contract values

| Value                      | Kind               | Meaning                                                                 |
| -------------------------- | ------------------ | ----------------------------------------------------------------------- |
| `VERSION`                  | exported constant  | Package version string, three-place synced with package.json + CHANGELOG. |
| default worker count       | behavior           | `navigator.hardwareConcurrency` when `size` is not a positive integer.  |
| minimum worker count       | behavior           | `1` -- floored, never 0 (fail closed).                                   |
| scratch floor              | behavior           | `16` bytes = two `Float64` slots (index + value); `scratchBytes` floors here. |
| batches in flight          | behavior           | `1` -- a second concurrent `map()` rejects.                              |

---

## Fail-closed behavior

The pool never drops an item and never hangs on an error. `map()` returns a promise that REJECTS -- with a clear `lite-worker-pool:` message -- in every one of these cases:

- **Disposed pool.** Calling `map()` after `dispose()` rejects immediately.
- **Bad input.** `map(null)`, `map(undefined)`, or anything not array-like rejects (uniform with the disposed/busy paths) rather than throwing a raw synchronous `TypeError`.
- **Concurrent batch.** A second `map()` while one is in flight rejects -- one batch at a time; await the first.
- **Worker throw or death.** If the transform throws on an item, or a worker crashes mid-batch, the batch rejects with the underlying error. A thrown transform is deterministic, so the item is NOT reassigned (it would just throw again). The error also **poisons** the pool: its scratch buffer went down with the failed worker, so reuse is unsafe -- every later `map()` rejects until you `dispose()` and recreate.

Sizing is fail-closed too: a missing / NaN / negative / zero `size` floors to `hardwareConcurrency`, then to 1 -- an absent count is one worker, never a silent no-op pool. `null` is not zero.

---

## Composability with the ecosystem

The core and the pool are two answers to two different shapes of off-thread work -- use each for what it is:

```js
import { defineWorker }     from '@zakkster/lite-worker';
import { createWorkerPool } from '@zakkster/lite-worker-pool';

// ONE persistent off-thread loop feeding a 60fps render -- use the CORE.
// A single worker, a frameChannel, latest-wins frames, zero per-frame alloc.
const sim = defineWorker((ctx) => { /* tick a simulation, post frames */ }).spawn();

// A BATCH fanned across every core, collected in order -- use the POOL.
// Embarrassingly parallel numeric work: one transform, N workers, index-ordered.
const pool = createWorkerPool((n) => {
  let x = n; for (let i = 0; i < 4000; i++) x = (x * 1.0000001 + 1) % 4294967296;
  return x;
});
const results = await pool.map(new Float64Array(200000));  // saturates all cores
pool.dispose();
```

For tests and Node, inject a `worker_threads`-backed `spawn` so the same pool logic runs cross-core without a browser -- the pool does not care what a `PoolWorker` is made of, only that it exposes `{ send, onRaw, onError, terminate }`. The rule of thumb: reach for **`lite-worker`** when one worker owns a long-lived loop, and for **`lite-worker-pool`** when a finite batch should be spread across all the cores and gathered back in order.

---

## Zero-GC design notes

<details>
<summary>What the hot path allocates (nothing), and how it stays that way.</summary>

One `createWorkerPool` allocates everything it will ever need at construction: N scratch `ArrayBuffer`s (one per worker), N workers, one reused single-element transfer list, and the per-batch state slots. Every dispatch afterward moves a buffer, does arithmetic on two `Float64` slots, and files a result by index.

| Operation                        | Steady-state allocations |
| -------------------------------- | ------------------------ |
| per-item dispatch (`_dispatch`)  | **0**                    |
| per-item result filing           | 1 transient `Float64` view header per hop (GC'd, never retained) |
| `map()` per batch                | results array + 1 Promise (cold, once per batch) |
| `stats()`                        | 1 small snapshot object per call (cold) |
| `createWorkerPool`               | once, at construction (all scratch + workers, then reused) |

The transient per-hop view header is the ring's only per-hop cost -- the same one lite-worker's frame pool pays, because a transfer mints a fresh `ArrayBuffer` identity each hop, so the view cannot be cached across the boundary. It is transient garbage, not retained.

The **hard, gated claim is RETENTION**: the torture harness (`@zakkster/lite-leak` + `@zakkster/lite-gc-profiler`) proves the per-item dispatch path holds **`< 8 B/op`** -- in practice `0.00 B/op` -- with **0 major GCs** over 10k dispatch hops, the scratch `byteLength` unchanged across the whole run, and `@zakkster/lite-leak`'s `tracker.size() === 0` after 2048 pool create/dispose cycles. No gate output is a FAIL; the retention gate runs on every change.

</details>

---

## Throughput

Wall-clock scales with core count for CPU-bound transforms: the queue keeps every worker busy until the batch drains, so a pool of N workers does the work in roughly `1/N` the time of a single worker, minus the transfer overhead per item. The exact speedup is **machine- and workload-dependent** -- it moves with core count, per-item cost, and system load -- so this package commits **no fixed throughput or speedup number**. Retention (above) is the reproducible, gated claim; measure throughput on your own hardware with your own transform. As a rule, the heavier each item's work relative to a single buffer transfer, the closer the pool gets to linear scaling.

---

## Design decisions worth knowing

- **One transform per pool, bound once.** `defineWorker` serializes a worker body ONCE into a Blob at construction; you cannot cheaply hand a fresh transform to every `map` call without re-serializing and re-spawning. So the pool binds the transform at creation and `map` only carries items. It is the honest shape for the underlying mechanism -- and it keeps `map`'s hot path free of any per-call serialization. Two transforms means two pools.
- **A job queue, not round-robin.** Assigning item `k` to worker `k % N` leaves fast workers idle behind one slow item. The pool pulls the next unassigned index whenever a worker frees up, so every worker stays saturated until the work is gone. T2 in the torture suite proves no worker starves and the queue drains.
- **Results by index, not by push order.** The output slot rides inside the scratch and round-trips, so ordering costs nothing -- no per-item wrapper, no completion-order sort. A control that files results by completion order is a deliberately-broken torture case that must fail.
- **Fail closed on a worker error, and poison the pool.** A thrown transform is deterministic; reassigning the item would just throw again, so the batch rejects with that error. And because the failed worker's scratch buffer is gone, the pool poisons itself -- later `map()` calls reject until `dispose()` -- rather than silently running short-handed. A pool that hangs on a lost item is a fail-OPEN bug; the T3 tier proves `map()` rejects within a bounded wait instead.
- **Numeric items, by contract.** The scratch protocol moves each item and result as a `Float64` in the two scratch slots -- that is what makes dispatch zero-copy and zero-alloc. Richer payloads mean extending the protocol yourself; the shipped surface is `ArrayLike<number> -> number[]`. This is a deliberate floor, not an oversight.
- **Transport-agnostic core.** The pool drives a minimal `{ send, onRaw, onError, terminate }` surface, injectable via `opts.spawn`, exactly like lite-worker's `frameChannel(transport, ...)`. That is what lets the torture suite prove conservation, ordering, and zero-retention over a REAL `node:worker_threads` bridge with no browser -- the tests exercise the shipping logic, not a stand-in.

---

## Testing

**13 deterministic `node:test` cases, all pass**, plus a torture gate that proves conservation, ordering, saturation, fail-closed errors, and zero retention over a real thread bridge.

```bash
npm test          # 13 node:test cases (contract + boundary + fail-closed surface)
npm run torture   # @zakkster/lite-leak + lite-gc-profiler: real-thread conservation + 0 B/op
npm run gate      # the fast per-item retention gate
npm run check     # node --check + tsc --strict on the .d.ts
npm run soak      # the torture suite (extended)
```

The torture suite (`test/torture.mjs`) runs every tier over a REAL `node:worker_threads` bridge injected through `opts.spawn`:

- **T0 conservation** -- 10000 items over 4 real threads: exactly 10000 results, `results[i] === transform(items[i])`, every item processed exactly once (no drops, no doubles), filed by index.
- **T1 ordering** -- staggered, out-of-order completion; output stays in input index order.
- **T2 saturation** -- every worker completes `> 0` jobs (no starvation), the queue drains to 0, and `active` returns to 0.
- **T3 worker-error** -- a transform that throws on one item makes `map()` REJECT within a bounded wait (not resolve, not hang) -- fail-closed, no lost item -- proven over the real bridge.
- **T6 retention** -- `< 8 B/op` over 10k dispatch hops via `@zakkster/lite-gc-profiler`, scratch `byteLength` stable, and `@zakkster/lite-leak` `tracker.size() === 0` after 2048 create/dispose cycles.
- **T9 controls** -- deliberately-broken variants proving each gate can fail: a misrouting dispatch (conservation), a completion-order collector (ordering), a per-job scratch realloc (retention), and an error-swallowing worker (worker-error). Each exits non-zero when armed; all are OFF in the normal run.

A normal `node --expose-gc test/torture.mjs` prints exactly `ok` and exits 0. No gate output is a FAIL.

---

## What this is not

- **Not a replacement for `@zakkster/lite-worker`.** The core is one persistent off-thread loop with a clean main thread (frameChannel, adoptCanvas, RPC). The pool is a batch fanned across cores. The pool depends on the core; the core never depends on the pool. Use the core for a long-lived single worker.
- **Not a general RPC or actor system.** There is no `call`, no message protocol, no per-worker state, no bidirectional streaming. One transform, applied to items, results back. For request/response to a single worker, use the core's `call`/`post`/`on`.
- **Not for non-numeric payloads out of the box.** The zero-copy scratch protocol moves `Float64` values. Items and results are numbers; extend the protocol yourself for structured payloads.
- **Not a SharedArrayBuffer pool.** It uses transfer semantics only -- no cross-origin isolation, no COOP/COEP required. (The core's frameChannel has an opt-in shared mode; the pool does not.)
- **Not a task scheduler.** No priorities, no cancellation of individual items, no retry policy, no backpressure knobs. One batch at a time, run to completion or reject.
- **Not a GUI or a benchmark harness.** Bring your own workload and your own measurement; the pool is the fan-out kernel underneath.

---

## Ecosystem

Part of the **@zakkster** zero-GC stack:

- [`lite-worker`](https://www.npmjs.com/package/@zakkster/lite-worker) -- zero-GC per-frame Web Worker channel with frameChannel + adoptCanvas (this package's peer dep)
- [`lite-gc-profiler`](https://www.npmjs.com/package/@zakkster/lite-gc-profiler) -- GC budget instrument (torture peer)
- [`lite-leak`](https://www.npmjs.com/package/@zakkster/lite-leak) -- retention / leak tracker (torture peer)
- **`lite-worker-pool`** -- this package

---

## License

MIT (c) Zahary Shinikchiev <shinikchiev@yahoo.com>
