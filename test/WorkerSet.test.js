// test/WorkerSet.test.js -- node --test test/WorkerSet.test.js
//
// createWorkerSet (1.1.0, research/worker-set.md E1-E11) over the deterministic
// step-driven loopback (test/torture/harness.mjs setLoopbackSpawn): a job runs
// only when the test pumps driver.step(), so every interleaving is exact. The
// real-thread proof is test/torture-set.mjs.
//
// Falsifiable assertions:
//   W1. READY handshake: workers start STARTING (submit -> LWP_NOT_READY), ready() resolves, all READY.
//   W2. submit resolves with workerFn(value); post + onSettle delivers (i, tag, ok, value, null).
//   W3. Per-worker FIFO: jobs to one worker settle in submission order (E8).
//   W4. Slots + capped queue: slots + queue accepted, the next rejects LWP_QUEUE_FULL; load(i) tracks
//       executing + queued; the queue drains into freed slots (E3, E4).
//   W5. A transform throw fails ONE job (LWP_TRANSFORM + the message); the worker stays READY (E5).
//   W6. A worker death fails ITS jobs (LWP_WORKER_DOWN), the others keep serving; respawn(i) brings the
//       same slot back READY; kill(i) is idempotent (E5).
//   W7. Abort: queued -> rejects with the signal's reason and never runs; executing -> LWP_ABORTED at
//       once, the late result is discarded, the slot frees on reply (E9).
//   W8. busySince(i): NaN idle, the send time of the oldest in-flight job (E6).
//   W9. control(i, values): the transform sees them as ctl; re-applied after respawn (E7).
//   W10. Validation fails closed with codes; a transport without onPost throws LWP_TRANSPORT and the
//        half-built set is torn down.
//   W11. A reply out of FIFO order takes the worker DOWN (LWP_PROTOCOL cause), never misfiles a result.
//   W12. dispose(): every pending job fails LWP_DISPOSED (post jobs via onSettle), ready() rejects
//        DISPOSED, idempotent; afterwards submit rejects and post returns false.
//   W13. A throwing onSettle cannot strand the other jobs (each settles; the error is re-raised).

import { test } from "node:test";
import assert from "node:assert/strict";
import { createWorkerSet, WORKER_STATE } from "../WorkerPool.js";
import { setLoopbackSpawn } from "./torture/harness.mjs";

const f = (x, ctl) => (ctl && ctl.length > 0 ? x * ctl[0] : x * 3 + 1);

function mkSet(opts, spawnOpts, fn) {
  const drv = setLoopbackSpawn(fn || f, spawnOpts);
  const settled = [];
  const set = createWorkerSet(fn || f, {
    size: 2, slots: 2, queue: 3, spawn: drv.factory,
    onSettle: (i, tag, ok, value, code) => settled.push({ i, tag, ok, value, code }),
    ...(opts || {}),
  });
  return { set, drv, settled };
}

async function codeOf(p) {
  try { await p; } catch (e) { return e.code; }
  return "resolved";
}

test("W1: STARTING until the READY handshake; ready() resolves", async () => {
  const { set } = mkSet();
  assert.equal(set.state(0), WORKER_STATE.STARTING);
  const early = set.submit(0, 1);
  assert.equal(set.post(0, 1, 7), false, "post is refused while STARTING");
  assert.equal(await codeOf(early), "LWP_NOT_READY");
  await set.ready();
  assert.equal(set.state(0), WORKER_STATE.READY);
  assert.equal(set.isReady(1), true);
  set.dispose();
});

test("W2: submit resolves with the result; post settles through onSettle", async () => {
  const { set, drv, settled } = mkSet();
  await set.ready();
  const p = set.submit(1, 5);
  assert.equal(set.post(0, 2, 42), true);
  drv.drain();
  assert.equal(await p, 16);
  assert.deepEqual(settled, [{ i: 0, tag: 42, ok: true, value: 7, code: null }]);
  set.dispose();
});

test("W3: per-worker FIFO -- jobs to one worker settle in submission order", async () => {
  const { set, drv, settled } = mkSet({ slots: 2, queue: 8 });
  await set.ready();
  for (let k = 0; k < 10; k++) assert.equal(set.post(0, k, k), true);
  drv.drain();
  assert.deepEqual(settled.map((s) => s.tag), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  set.dispose();
});

test("W4: slots + capped queue; load(i) = executing + queued; the queue drains", async () => {
  const { set, drv } = mkSet({ slots: 2, queue: 3 });
  await set.ready();
  const ps = [];
  for (let k = 0; k < 5; k++) ps.push(set.submit(0, k));
  assert.equal(set.load(0), 5);
  assert.equal(await codeOf(set.submit(0, 99)), "LWP_QUEUE_FULL");
  assert.equal(set.post(0, 99, 0), false);
  assert.equal(set.load(1), 0, "worker 1 untouched");
  assert.equal(drv.queue.length, 2, "only `slots` jobs are in flight; the rest wait in the set's queue");
  drv.drain();
  assert.deepEqual(await Promise.all(ps), [1, 4, 7, 10, 13]);
  assert.equal(set.load(0), 0);
  set.dispose();
});

test("W5: a transform throw fails one job; the worker stays READY", async () => {
  const boom = (x) => { if (x === 3) throw new Error("bad item 3"); return x + 100; };
  const { set, drv } = mkSet({}, {}, boom);
  await set.ready();
  const a = set.submit(0, 3);
  const b = set.submit(0, 4);
  drv.drain();
  await assert.rejects(a, (e) => e.code === "LWP_TRANSFORM" && /bad item 3/.test(e.message));
  assert.equal(await b, 104);
  assert.equal(set.state(0), WORKER_STATE.READY);
  set.dispose();
});

test("W6: a death fails only that worker's jobs; respawn brings the slot back", async () => {
  const { set, drv } = mkSet({ slots: 1, queue: 2 });
  await set.ready();
  const dead = [set.submit(1, 1), set.submit(1, 2), set.submit(1, 3)];
  const alive = set.submit(0, 10);
  drv.die(1);
  for (const p of dead) assert.equal(await codeOf(p), "LWP_WORKER_DOWN");
  assert.equal(set.state(1), WORKER_STATE.DOWN);
  assert.equal(set.load(1), 0);
  assert.equal(await codeOf(set.submit(1, 4)), "LWP_WORKER_DOWN");
  drv.drain();
  assert.equal(await alive, 31, "worker 0 kept serving");
  await set.respawn(1);
  assert.equal(set.state(1), WORKER_STATE.READY);
  const again = set.submit(1, 5);
  drv.drain();
  assert.equal(await again, 16);
  set.kill(1);
  set.kill(1);
  assert.equal(set.state(1), WORKER_STATE.DOWN);
  set.dispose();
});

test("W7: abort -- queued never runs; executing rejects at once and its late result is dropped", async () => {
  let runs = 0;
  const counted = (x) => { runs++; return x; };
  const { set, drv } = mkSet({ slots: 1, queue: 2 }, {}, counted);
  await set.ready();
  const acExec = new AbortController();
  const acQueued = new AbortController();
  const exec = set.submit(0, 1, { signal: acExec.signal });
  const queued = set.submit(0, 2, { signal: acQueued.signal });
  const last = set.submit(0, 3);
  assert.equal(set.load(0), 3);
  const why = new Error("caller gave up");
  acQueued.abort(why);
  assert.equal(await queued.catch((e) => e), why, "a queued job rejects with the signal's reason");
  assert.equal(set.load(0), 2);
  acExec.abort();
  await assert.rejects(exec, (e) => e.code === "LWP_ABORTED");
  assert.equal(set.load(0), 2, "the aborted job still occupies its slot until the worker replies");
  drv.drain();
  assert.equal(await last, 3);
  assert.equal(runs, 2, "the queued-aborted job never ran");
  assert.equal(set.load(0), 0);
  const pre = new AbortController();
  pre.abort();
  const preErr = await set.submit(0, 9, { signal: pre.signal }).catch((e) => e);
  assert.equal(preErr, pre.signal.reason, "pre-aborted: rejects with the signal's reason, nothing dispatched");
  assert.equal(set.load(0), 0);
  set.dispose();
});

test("W8: busySince is NaN when idle, else the send time of the oldest in-flight job", async () => {
  let t = 1000;
  const { set, drv } = mkSet({ now: () => t });
  await set.ready();
  assert.ok(Number.isNaN(set.busySince(0)));
  set.post(0, 1, 0);
  t = 2000;
  set.post(0, 2, 0);
  assert.equal(set.busySince(0), 1000);
  drv.step();
  assert.equal(set.busySince(0), 2000);
  drv.step();
  assert.ok(Number.isNaN(set.busySince(0)));
  assert.ok(Number.isNaN(set.busySince(7)), "bad index: NaN, never a throw");
  set.dispose();
});

test("W9: control values reach the transform and survive respawn", async () => {
  const { set, drv } = mkSet();
  await set.ready();
  set.control(0, new Float64Array([10]));
  const a = set.submit(0, 4);
  drv.drain();
  assert.equal(await a, 40);
  await set.respawn(0);
  const b = set.submit(0, 4);
  drv.drain();
  assert.equal(await b, 40, "re-applied after respawn");
  assert.throws(() => set.control(0, [1]), (e) => e.code === "LWP_ARGUMENT");
  set.dispose();
});

test("W10: validation fails closed with codes; a bad transport tears the set down", async () => {
  const { set } = mkSet();
  await set.ready();
  assert.throws(() => set.post(2, 1, 0), (e) => e instanceof RangeError && e.code === "LWP_INDEX");
  assert.throws(() => set.post(0, "1", 0), (e) => e.code === "LWP_ARGUMENT");
  assert.equal(await codeOf(set.submit(1.5, 1)), "LWP_INDEX");
  assert.equal(await codeOf(set.submit(0, 1, { sigal: null })), "LWP_OPTION");
  assert.equal(set.isReady(-1), false);
  assert.equal(set.load(99), 0);
  set.dispose();
  assert.throws(() => createWorkerSet(f, { slot: 2 }), (e) => e.code === "LWP_OPTION" && /did you mean 'slots'/.test(e.message));
  assert.throws(() => createWorkerSet(f, { slots: 9 }), (e) => e.code === "LWP_OPTION");
  assert.throws(() => createWorkerSet(f, { queue: -1 }), (e) => e.code === "LWP_OPTION");
  assert.throws(() => createWorkerSet(null), (e) => e.code === "LWP_ARGUMENT");
  const terminated = [];
  let n = 0;
  const half = () => {
    const k = n++;
    if (k === 2) return { send() {}, onRaw() { return () => {}; }, terminate() {} };   // no onPost
    return { send() {}, onRaw() { return () => {}; }, onPost() { return () => {}; }, terminate() { terminated.push(k); } };
  };
  assert.throws(() => createWorkerSet(f, { size: 3, spawn: half }), (e) => e.code === "LWP_TRANSPORT");
  assert.deepEqual(terminated.sort(), [0, 1], "the workers already started were terminated");
});

test("W11: a reply out of FIFO order takes the worker down, never misfiles", async () => {
  const { set, drv, settled } = mkSet({ slots: 2, queue: 0 }, { reorder: true });
  await set.ready();
  set.post(0, 1, 1);
  set.post(0, 2, 2);
  const other = set.submit(1, 5);
  drv.step();
  assert.equal(set.state(0), WORKER_STATE.DOWN);
  assert.deepEqual(settled.map((s) => [s.tag, s.ok, s.code]), [[1, false, "LWP_WORKER_DOWN"], [2, false, "LWP_WORKER_DOWN"]]);
  drv.drain();
  assert.equal(await other, 16, "worker 1 unaffected");
  set.dispose();
});

test("W12: dispose fails every pending job and is idempotent", async () => {
  const { set, settled } = mkSet({ slots: 1, queue: 1 });
  await set.ready();
  const p = set.submit(0, 1);
  set.post(0, 2, 9);
  const r = set.ready();
  set.dispose();
  set.dispose();
  assert.equal(await codeOf(p), "LWP_DISPOSED");
  assert.deepEqual(settled, [{ i: 0, tag: 9, ok: false, value: NaN, code: "LWP_DISPOSED" }]);
  await r;   // resolved before dispose: every worker was READY
  assert.equal(await codeOf(set.ready()), "LWP_DISPOSED");
  assert.equal(await codeOf(set.submit(0, 1)), "LWP_DISPOSED");
  assert.equal(set.post(0, 1, 0), false);
});

test("W13: a throwing onSettle cannot strand the other jobs", async () => {
  const raised = [];
  const prev = globalThis.reportError;
  globalThis.reportError = (e) => raised.push(e);
  try {
    const drv = setLoopbackSpawn(f);
    const seen = [];
    const set = createWorkerSet(f, {
      size: 1, slots: 2, queue: 2, spawn: drv.factory,
      onSettle: (i, tag) => { seen.push(tag); if (tag === 0) throw new Error("callback bug"); },
    });
    await set.ready();
    for (let k = 0; k < 4; k++) set.post(0, k, k);
    drv.die(0);
    assert.deepEqual(seen, [0, 1, 2, 3], "every job settled despite the first callback throwing");
    assert.equal(raised.length, 1);
    assert.match(raised[0].message, /callback bug/);
    set.dispose();
  } finally {
    globalThis.reportError = prev;
  }
});
