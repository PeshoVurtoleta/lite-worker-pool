// test/WorkerPool.test.js -- node --test test/WorkerPool.test.js
//
// Minimal node:test coverage for @zakkster/lite-worker-pool over a synchronous
// in-process worker (no browser, no real threads needed for the unit surface).
// The exhaustive conservation/ordering/saturation/retention proof is
// test/torture.mjs; qa expands this file.

import { test } from "node:test";
import assert from "node:assert/strict";
import { createWorkerPool, VERSION } from "../WorkerPool.js";
import { syncLoopbackSpawn } from "./torture/harness.mjs";

const f = (x) => x * 3 + 1;

// A synchronous in-process worker factory: send() runs the transform and replies
// on a microtask, so map() resolves without a browser or a real thread. A
// throwing transform is caught and reported on onError, matching the real
// worker contract, so map() fails the batch closed instead of hanging.
function syncSpawn(workerFn) {
  return function () {
    const handlers = new Set();
    const errHandlers = new Set();
    return {
      send(buf) {
        queueMicrotask(() => {
          const view = new Float64Array(buf);
          let val;
          try {
            val = workerFn(view[1]);
          } catch (e) {
            errHandlers.forEach((fn) => fn(e instanceof Error ? e : new Error(String(e))));
            return;
          }
          view[1] = val;
          handlers.forEach((fn) => fn(buf));
        });
      },
      onRaw(fn) { handlers.add(fn); return () => handlers.delete(fn); },
      onError(fn) { errHandlers.add(fn); return () => errHandlers.delete(fn); },
      terminate() { handlers.clear(); errHandlers.clear(); },
    };
  };
}

test("VERSION is the three-place-synced string", () => {
  assert.equal(VERSION, "1.0.1");
});

test("map fills results by index for every item", async () => {
  const pool = createWorkerPool(f, { size: 3, spawn: syncSpawn(f) });
  const items = [];
  for (let i = 0; i < 500; i++) items.push(i);
  const results = await pool.map(items);
  assert.equal(results.length, 500);
  for (let i = 0; i < 500; i++) assert.equal(results[i], f(i));
  pool.dispose();
});

test("map on an empty array resolves to an empty array", async () => {
  const pool = createWorkerPool(f, { size: 2, spawn: syncSpawn(f) });
  const results = await pool.map([]);
  assert.deepEqual(results, []);
  pool.dispose();
});

test("size resolves to >=1 and never 0 for bad input", () => {
  for (const bad of [0, -4, NaN, undefined]) {
    const pool = createWorkerPool(f, { size: bad, spawn: syncSpawn(f) });
    assert.ok(pool.size >= 1);
    pool.dispose();
  }
});

test("stats reports queue drain and active back to zero", async () => {
  const pool = createWorkerPool(f, { size: 4, spawn: syncSpawn(f) });
  const items = [];
  for (let i = 0; i < 200; i++) items.push(i);
  await pool.map(items);
  const st = pool.stats();
  assert.equal(st.size, 4);
  assert.equal(st.active, 0);
  assert.equal(st.queued, 0);
  assert.equal(st.done, 200);
  pool.dispose();
});

test("dispose is idempotent and never throws", () => {
  const pool = createWorkerPool(f, { size: 2, spawn: syncSpawn(f) });
  pool.dispose();
  assert.doesNotThrow(() => pool.dispose());
});

test("map after dispose rejects", async () => {
  const pool = createWorkerPool(f, { size: 2, spawn: syncSpawn(f) });
  pool.dispose();
  await assert.rejects(() => pool.map([1, 2, 3]), /disposed/);
});

test("a second concurrent map rejects", async () => {
  const pool = createWorkerPool(f, { size: 2, spawn: syncSpawn(f) });
  const items = [];
  for (let i = 0; i < 100; i++) items.push(i);
  const first = pool.map(items);
  await assert.rejects(() => pool.map(items), /in progress/);
  await first;
  pool.dispose();
});

test("createWorkerPool requires a function", () => {
  assert.throws(() => createWorkerPool(42), TypeError);
});

test("an unknown option key throws with a did-you-mean hint", () => {
  assert.throws(
    () => createWorkerPool(f, { sise: 4, spawn: syncSpawn(f) }),
    /unknown option 'sise'.*did you mean 'size'/
  );
});

test("map rejects on null / undefined / non-array-like input", async () => {
  const pool = createWorkerPool(f, { size: 2, spawn: syncSpawn(f) });
  await assert.rejects(() => pool.map(null), /array-like/);
  await assert.rejects(() => pool.map(undefined), /array-like/);
  await assert.rejects(() => pool.map(42), /array-like/);
  pool.dispose();
});

test("a throwing transform rejects map() and does not hang", async () => {
  const throwing = (x) => { if (x === 3) throw new Error("boom at 3"); return x * 2; };
  const pool = createWorkerPool(throwing, { size: 2, spawn: syncSpawn(throwing) });
  await assert.rejects(() => pool.map([0, 1, 2, 3, 4, 5]), /boom at 3/);
  pool.dispose();
});

test("map is poisoned after a worker error until dispose", async () => {
  const throwing = (x) => { if (x === 1) throw new Error("boom"); return x; };
  const pool = createWorkerPool(throwing, { size: 1, spawn: syncSpawn(throwing) });
  await assert.rejects(() => pool.map([0, 1, 2]), /boom/);
  await assert.rejects(() => pool.map([9]), /poisoned/);
  pool.dispose();
});

// ---------------------------------------------------------------------------
// QA boundary expansion (13 -> 32 cases). Covers the 0/1/N-1/N/N+1 matrix
// against worker count, typed-array input, out-of-order completion,
// concurrent-map isolation, reentrant dispose/map, item-level -0/NaN, and one
// adversarial case (a negative-length array-like bypasses the "rejects, never
// throws" contract). Deterministic and hermetic: every stepped case drives
// syncLoopbackSpawn's manual step() -- no real timer races, no open handles.
// ---------------------------------------------------------------------------

test("map handles a single item (N=1)", async () => {
  const pool = createWorkerPool(f, { size: 3, spawn: syncSpawn(f) });
  const results = await pool.map([7]);
  assert.deepEqual(results, [f(7)]);
  assert.equal(pool.stats().active, 0);
  pool.dispose();
});

test("map handles fewer items than workers (2 items, size 8): no hang, extras idle", async () => {
  const pool = createWorkerPool(f, { size: 8, spawn: syncSpawn(f) });
  const results = await pool.map([10, 20]);
  assert.deepEqual(results, [f(10), f(20)]);
  const st = pool.stats();
  assert.equal(st.size, 8);
  assert.equal(st.active, 0);
  assert.equal(st.queued, 0);
  assert.equal(st.done, 2);
  pool.dispose();
});

test("map handles exactly N items for N workers (N === size)", async () => {
  const pool = createWorkerPool(f, { size: 4, spawn: syncSpawn(f) });
  const items = [1, 2, 3, 4];
  const results = await pool.map(items);
  assert.deepEqual(results, items.map(f));
  pool.dispose();
});

test("map handles N-1 items for N workers (one worker idle)", async () => {
  const pool = createWorkerPool(f, { size: 4, spawn: syncSpawn(f) });
  const items = [1, 2, 3];
  const results = await pool.map(items);
  assert.deepEqual(results, items.map(f));
  pool.dispose();
});

test("map handles N+1 items for N workers (one worker gets a second job)", async () => {
  const pool = createWorkerPool(f, { size: 3, spawn: syncSpawn(f) });
  const items = [1, 2, 3, 4];
  const results = await pool.map(items);
  assert.deepEqual(results, items.map(f));
  pool.dispose();
});

test("map accepts a Float64Array and preserves index order", async () => {
  const pool = createWorkerPool(f, { size: 3, spawn: syncSpawn(f) });
  const items = new Float64Array([5, 1, 9, 2, 7]);
  const results = await pool.map(items);
  for (let i = 0; i < items.length; i++) assert.equal(results[i], f(items[i]));
  pool.dispose();
});

test("map accepts a Uint32Array and preserves index order", async () => {
  const pool = createWorkerPool(f, { size: 3, spawn: syncSpawn(f) });
  const items = new Uint32Array([5, 1, 9, 2, 7]);
  const results = await pool.map(items);
  for (let i = 0; i < items.length; i++) assert.equal(results[i], f(items[i]));
  pool.dispose();
});

test("out-of-order worker completion still yields input order", async () => {
  // Reverse the pending-job queue before every step(): the most-recently
  // dispatched worker's job always replies first, so completion order is the
  // opposite of dispatch order. Results must still land by index.
  const drv = syncLoopbackSpawn(f, {});
  const pool = createWorkerPool(f, { size: 4, spawn: drv.factory });
  const items = [];
  for (let i = 0; i < 40; i++) items.push(i * 2);
  const p = pool.map(items);
  while (drv.queue.length) {
    drv.queue.reverse();
    drv.step();
  }
  const results = await p;
  for (let i = 0; i < items.length; i++) {
    assert.equal(results[i], f(items[i]), "results[" + i + "] out of order");
  }
  pool.dispose();
});

test("a concurrent map() rejects without corrupting the first batch's results", async () => {
  const pool = createWorkerPool(f, { size: 3, spawn: syncSpawn(f) });
  const items = [];
  for (let i = 0; i < 300; i++) items.push(i);
  const first = pool.map(items);
  const second = pool.map([9999]).then(() => "resolved", (e) => e);
  const [firstResults, secondOutcome] = await Promise.all([first, second]);
  assert.equal(firstResults.length, 300);
  for (let i = 0; i < 300; i++) assert.equal(firstResults[i], f(i));
  assert.ok(secondOutcome instanceof Error);
  assert.match(secondOutcome.message, /in progress/);
  pool.dispose();
});

test("a map() call issued re-entrantly from inside an in-flight batch rejects (re-entrant write)", async () => {
  let pool;
  let reentrant = null;
  const transform = (x) => {
    if (x === 5 && !reentrant) {
      reentrant = pool.map([1, 2]).then(() => "resolved", (e) => e);
    }
    return x * 3 + 1;
  };
  const drv = syncLoopbackSpawn(transform, {});
  pool = createWorkerPool(transform, { size: 2, spawn: drv.factory });
  const items = [];
  for (let i = 0; i < 20; i++) items.push(i);
  const p = pool.map(items);
  while (drv.step()) { /* drain */ }
  const results = await p;
  assert.equal(results.length, 20);
  for (let i = 0; i < 20; i++) assert.equal(results[i], i * 3 + 1);
  assert.ok(reentrant, "re-entrant map() call was never triggered");
  const outcome = await reentrant;
  assert.ok(outcome instanceof Error);
  assert.match(outcome.message, /in progress/);
  pool.dispose();
});

test("dispose() called re-entrantly from inside batch processing settles cleanly (dispose-during-iteration)", async () => {
  let pool;
  const disposer = (x) => {
    if (x === 5) pool.dispose();
    return x * 3 + 1;
  };
  const drv = syncLoopbackSpawn(disposer, {});
  pool = createWorkerPool(disposer, { size: 2, spawn: drv.factory });
  const items = [];
  for (let i = 0; i < 20; i++) items.push(i);
  const pending = pool.map(items);
  const outcome = pending.then(() => "resolved", (e) => e);
  while (drv.step()) { /* item 5 disposes the pool mid-drain; later steps must not throw */ }
  const result = await outcome;
  assert.ok(result instanceof Error);
  assert.match(result.message, /disposed mid-batch/);
  assert.doesNotThrow(() => pool.dispose()); // duplicate dispose after re-entrant dispose
  await assert.rejects(() => pool.map([1]), /disposed/);
});

test("dispose is idempotent after a poisoned pool (duplicate dispose, post-poison)", async () => {
  const throwing = (x) => { if (x === 1) throw new Error("boom"); return x; };
  const pool = createWorkerPool(throwing, { size: 1, spawn: syncSpawn(throwing) });
  await assert.rejects(() => pool.map([0, 1, 2]), /boom/);
  pool.dispose();
  assert.doesNotThrow(() => pool.dispose());
});

test("ADVERSARIAL: an invalid numeric length REJECTS uniformly, never throws synchronously", async () => {
  // Uniform surface: every bad-input path settles a rejected promise rather than
  // throwing on the caller's stack. A length that is not a non-negative safe
  // integer (negative, fractional, non-finite) is caught before `new Array(n)`,
  // so `map({ length: -1 })` rejects instead of crashing with a RangeError.
  const pool = createWorkerPool(f, { size: 2, spawn: syncSpawn(f) });
  await assert.rejects(() => pool.map({ length: -1 }), /lite-worker-pool: .*non-negative integer length/);
  await assert.rejects(() => pool.map({ length: 1.5 }), /lite-worker-pool: .*non-negative integer length/);
  await assert.rejects(() => pool.map({ length: NaN }), /lite-worker-pool: .*non-negative integer length/);
  await assert.rejects(() => pool.map({ length: Infinity }), /lite-worker-pool: .*non-negative integer length/);
  pool.dispose();
});

test("map(NaN) rejects (NaN.length is undefined, not array-like)", async () => {
  const pool = createWorkerPool(f, { size: 2, spawn: syncSpawn(f) });
  await assert.rejects(() => pool.map(NaN), /array-like/);
  pool.dispose();
});

test("size floors to >=1 for -0 and null (never a silent 0-worker pool)", () => {
  for (const bad of [-0, null]) {
    const pool = createWorkerPool(f, { size: bad, spawn: syncSpawn(f) });
    assert.ok(pool.size >= 1);
    pool.dispose();
  }
});

test("item values -0 and NaN round-trip through the Float64 scratch buffer", async () => {
  const identity = (x) => x;
  const pool = createWorkerPool(identity, { size: 2, spawn: syncSpawn(identity) });
  const results = await pool.map([-0, NaN, 0, 3]);
  assert.ok(Object.is(results[0], -0), "expected -0 to round-trip as -0, got " + results[0]);
  assert.ok(Number.isNaN(results[1]), "expected NaN to round-trip as NaN");
  assert.ok(Object.is(results[2], 0));
  assert.equal(results[3], 3);
  pool.dispose();
});

test("stats() fields are numbers and reflect an in-flight batch before draining", async () => {
  const drv = syncLoopbackSpawn(f, {});
  const pool = createWorkerPool(f, { size: 3, spawn: drv.factory });
  const items = [];
  for (let i = 0; i < 30; i++) items.push(i);
  const p = pool.map(items);
  let st = pool.stats();
  assert.equal(typeof st.size, "number");
  assert.equal(typeof st.queued, "number");
  assert.equal(typeof st.active, "number");
  assert.equal(typeof st.done, "number");
  assert.ok(st.active > 0, "expected in-flight jobs before draining");
  assert.ok(st.queued > 0, "expected undispatched items before draining");
  while (drv.step()) { /* drain */ }
  await p;
  st = pool.stats();
  assert.equal(st.done, 30);
  assert.equal(st.active, 0);
  assert.equal(st.queued, 0);
  pool.dispose();
});

test("scratch buffer byteLength is unchanged after a run (unit-level; torture T6 proves the gate)", async () => {
  const drv = syncLoopbackSpawn(f, {});
  const pool = createWorkerPool(f, { size: 1, spawn: drv.factory, scratchBytes: 32 });
  const p = pool.map([1, 2, 3]);
  while (drv.step()) { /* drain */ }
  await p;
  assert.equal(drv.lastBuf.byteLength, 32);
  pool.dispose();
});

test("a fully synchronous (re-entrant) transport still produces correct, non-duplicated results", async () => {
  // send() invokes onRaw() BEFORE returning -- no microtask boundary at all --
  // so _dispatch -> send -> onRaw -> _onResult -> _dispatch recurses on the JS
  // stack itself. The adversarial end of "re-entrant write": every write to
  // pool-internal state (_next/_done/_active/w.scratch) happens nested inside
  // the write that triggered it. Correctness (no dropped/duplicated index) must
  // still hold even though saturation across workers does not (worker 0 drains
  // the whole queue before the seed loop reaches worker 1 -- that is provable,
  // not asserted here since T2 already owns the saturation proof over async
  // transports).
  function reentrantSyncSpawn(workerFn) {
    return function () {
      const handlers = new Set();
      return {
        send(buf) {
          const view = new Float64Array(buf);
          view[1] = workerFn(view[1]);
          handlers.forEach((fn) => fn(buf));
        },
        onRaw(fn) { handlers.add(fn); return () => handlers.delete(fn); },
        onError() { return () => {}; },
        terminate() { handlers.clear(); },
      };
    };
  }
  const pool = createWorkerPool(f, { size: 4, spawn: reentrantSyncSpawn(f) });
  const items = [];
  for (let i = 0; i < 200; i++) items.push(i);
  const results = await pool.map(items);
  assert.equal(results.length, 200);
  for (let i = 0; i < 200; i++) assert.equal(results[i], f(i));
  const st = pool.stats();
  assert.equal(st.done, 200);
  assert.equal(st.active, 0);
  assert.equal(st.queued, 0);
  pool.dispose();
});
