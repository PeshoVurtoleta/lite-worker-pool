// test/torture/harness.mjs
//
// Shared scaffolding for the lite-worker-pool torture suite: two injectable
// worker-factory shapes for createWorkerPool(workerFn, { spawn }) --
//
//   - realThreadSpawn: spins an ACTUAL node:worker_threads Worker per pool
//     worker, running the identical per-item body over real ArrayBuffer
//     transfer. This is what proves conservation/ordering/saturation without a
//     browser (the seqlock-of-this-package: transfer across a real OS thread).
//   - syncLoopbackSpawn: a synchronous, iterative in-process worker used by the
//     retention gate so measureOps can wrap one deterministic dispatch hop, and
//     by the T9 controls so a fault can be injected in plain JS.
//
// All scratch a hot loop touches is allocated by the CALLER once, outside the
// loop -- this file only builds factories and stateless helpers.

import { Worker as NodeWorker } from "node:worker_threads";
import { fileURLToPath } from "node:url";
import {
  createLeakTracker,
  createOwnerCascadeOrphanKernel,
  createAsyncRetentionKernel,
} from "@zakkster/lite-leak";

const THREAD_ENTRY = fileURLToPath(import.meta.resolve("./thread-entry.mjs"));

// Build the worker body the real thread evaluates. A transform throw is caught
// and reported on the reserved "lwp:err" channel (matching WorkerPool's own
// default body) so the pool can fail the batch closed. `fault` optionally
// corrupts the body the same way a broken pool would, so a control can prove a
// gate is not decorative:
//   "misroute" -- clobbers the job index in slot [0] to a fixed wrong slot,
//                 dropping one output (conservation break).
//   "clobber"  -- overwrites slot [0] with a per-worker completion counter, so
//                 the pool files results in completion order (ordering break).
// `swallow` makes a caught throw report NOTHING (no error, no reply) so the batch
// hangs -- the deliberately-broken worker-error case. `delayMod` (>0) staggers
// the reply by (index % delayMod) ms via setTimeout so workers finish out of order.
export function buildBody(workerFn, opts) {
  const o = opts || {};
  const src = workerFn.toString();
  const fault = o.fault || "";
  const delayMod = o.delayMod | 0;
  const swallow = !!o.swallow;

  let indexLine = "";           // mutate slot [0] before the reply
  if (fault === "misroute") {
    indexLine = "  if ((idx | 0) === " + (o.at | 0) + ") f[0] = " + (o.to | 0) + ";\n";
  } else if (fault === "clobber") {
    indexLine = "  f[0] = (__seq++);\n";
  }

  const onThrow = swallow
    ? "    return;\n"   // swallow: never report, never reply -> the batch hangs
    : "    ctx.post('lwp:err', { message: (e && e.message) || String(e), index: idx });\n    return;\n";

  let replyBody;
  if (delayMod > 0) {
    replyBody =
      "  var val;\n" +
      "  try { val = __fn(f[1]); } catch (e) {\n" + onThrow + "  }\n" +
      "  var d = (idx % " + delayMod + ");\n" +
      indexLine +
      "  setTimeout(function () { f[1] = val; ctx.send(buf); }, d);\n";
  } else {
    replyBody =
      "  try { f[1] = __fn(f[1]); } catch (e) {\n" + onThrow + "  }\n" +
      indexLine +
      "  ctx.send(buf);\n";
  }

  return (
    "var __fn = (" + src + ");\n" +
    "var __seq = 0;\n" +
    "ctx.onRaw(function (buf) {\n" +
    "  var f = new Float64Array(buf);\n" +
    "  var idx = f[0];\n" +
    replyBody +
    "});\n"
  );
}

// A spawn factory that runs each pool worker on a real OS thread. Returns a
// factory closing over the shared body options; the pool calls it per worker.
export function realThreadSpawn(workerFn, opts) {
  const body = buildBody(workerFn, opts);
  const workers = [];
  const factory = function () {
    const worker = new NodeWorker(THREAD_ENTRY, { workerData: { body } });
    workers.push(worker);
    const handlers = new Set();
    const errHandlers = new Set();
    let err = null;
    const emit = (e) => { err = e; errHandlers.forEach((fn) => fn(e)); };
    // A thread crash (uncaught error) and a message-deserialization error both
    // fail the batch closed; a caught transform throw arrives as an "lwp:err"
    // typed message on the same port.
    worker.on("error", (e) => emit(e instanceof Error ? e : new Error(String(e))));
    worker.on("messageerror", (e) => emit(new Error("lite-worker-pool: worker messageerror: " + String(e))));
    worker.on("message", (msg) => {
      if (msg instanceof ArrayBuffer || ArrayBuffer.isView(msg)) {
        handlers.forEach((fn) => fn(msg));
        return;
      }
      if (msg && msg.t === "lwp:err") {
        emit(new Error((msg.d && msg.d.message) || "lite-worker-pool: worker transform threw"));
      }
    });
    return {
      send(buf, transfer) {
        const ab = buf instanceof ArrayBuffer ? buf : (ArrayBuffer.isView(buf) ? buf.buffer : null);
        worker.postMessage(buf, transfer || (ab ? [ab] : []));
      },
      onRaw(fn) { handlers.add(fn); return () => handlers.delete(fn); },
      onError(fn) { errHandlers.add(fn); return () => errHandlers.delete(fn); },
      terminate() { worker.terminate(); },
      err() { return err; },
    };
  };
  factory.workers = workers;
  return factory;
}

// A synchronous, iterative loopback worker. send() stashes the job; a job is only
// executed when the caller pumps it via the returned driver's step(). Because the
// reply is delivered on a later step() rather than reentrantly inside send(), a
// whole map drains iteratively with no stack recursion -- and each step() is one
// deterministic dispatch hop measureOps can wrap. `alloc` makes each hop retain a
// fresh ArrayBuffer (a scratch-reallocation break -> retention gate). `misrouteAt`/
// `misrouteTo` drop an output (conservation break). A transform throw is reported
// on onError unless `swallow` is set, in which case the job neither replies nor
// reports -- the batch hangs (the worker-error control). `clobber` files results
// by a per-worker completion counter instead of by index (ordering break).
export function syncLoopbackSpawn(workerFn, opts) {
  const o = opts || {};
  const driver = { queue: [], lastBuf: null, sink: null };
  if (o.alloc) driver.sink = [];

  const factory = function () {
    const rawHandlers = new Set();
    const errHandlers = new Set();
    const state = { seq: 0 };
    return {
      send(buf) { driver.queue.push({ buf, rawHandlers, errHandlers, state }); },
      onRaw(fn) { rawHandlers.add(fn); return () => rawHandlers.delete(fn); },
      onError(fn) { errHandlers.add(fn); return () => errHandlers.delete(fn); },
      terminate() { rawHandlers.clear(); errHandlers.clear(); },
    };
  };

  // Execute exactly one stashed job -> reply (or error). Returns false if nothing
  // is pending. Reentrancy is impossible: replies fire here, not inside send().
  driver.step = function () {
    const job = driver.queue.shift();
    if (!job) return false;
    const buf = job.buf;
    const f = new Float64Array(buf);
    const idx = f[0];
    let val;
    try {
      val = workerFn(f[1]);
    } catch (e) {
      if (!o.swallow) {
        const err = e instanceof Error ? e : new Error(String(e));
        job.errHandlers.forEach((fn) => fn(err));
      }
      return true; // swallow: no reply, no error -> the batch hangs
    }
    f[1] = val;
    if (o.clobber) f[0] = job.state.seq++;
    if (o.misrouteAt !== undefined && (idx | 0) === (o.misrouteAt | 0)) f[0] = o.misrouteTo | 0;
    if (o.alloc) driver.sink.push(new ArrayBuffer(16)); // retained: reallocation break
    driver.lastBuf = buf;
    job.rawHandlers.forEach((fn) => fn(buf));
    return true;
  };
  driver.factory = factory;
  return driver;
}

// Pump the event loop until pred() is truthy or the deadline passes.
export function waitFor(pred, timeoutMs, label) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      if (pred()) return resolve();
      if (Date.now() - start > timeoutMs) return reject(new Error(label + ": timed out after " + timeoutMs + "ms"));
      setImmediate(tick);
    };
    tick();
  });
}

// GC entries arrive asynchronously; await a settle tick before reading a tracker
// size or heap figure, or the window is empty and the gate falsely passes.
export function settle(ms) {
  globalThis.gc?.();
  return new Promise((r) => setTimeout(r, ms || 50));
}

// lite-leak tracker. Owner-cascade + async-retention kernels only: the pool
// patches no global surface here, so patching timers would only add settle noise.
export function makeTracker() {
  const leaks = [];
  const warns = [];
  const tracker = createLeakTracker({
    name: "lite-worker-pool-torture",
    onLeak: (r) => leaks.push(r.kind + ":" + String(r.tag)),
    onWarning: (w) => warns.push(w.kind + ":" + w.reason),
  });
  tracker.registerKernel(createOwnerCascadeOrphanKernel());
  tracker.registerKernel(createAsyncRetentionKernel());
  return { tracker, leaks, warns };
}

// Held-value contract: the cleanup passed to tracker.track MUST NOT close over
// the tracked target, or finalization is defeated. A shared no-op captures nothing.
export const NOOP_CLEANUP = function () {};
