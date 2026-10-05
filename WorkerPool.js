/**
 * @zakkster/lite-worker-pool
 * Zero-GC data-parallel worker pool over @zakkster/lite-worker (v1.0.1).
 *
 * One worker body, bound ONCE, serialized into N workers. `pool.map(items)`
 * saturates every worker with a job queue: each worker pulls the next unassigned
 * index as it finishes, so no worker idles while work remains. Results land BY
 * INDEX -- output order === input order regardless of completion order.
 *
 * The per-item dispatch path is allocation-free: one transferable scratch
 * ArrayBuffer per worker, allocated once at construction, ping-ponged in/out
 * (like lite-worker's frame pool) and a single reused transfer list. `map()`
 * itself allocates the results array + one Promise per batch (cold); the loop
 * that feeds items to workers does not.
 *
 * The pool logic is transport-agnostic: `opts.spawn` injects a worker factory
 * returning `{ send, onRaw, onError, terminate }` (the raw surface of a lite-worker
 * WorkerHandle), which is what lets it be proven over a real node:worker_threads
 * bridge with no browser. This mirrors lite-worker's frameChannel(transport,...).
 *
 * @license MIT
 * @copyright Zahary Shinikchiev
 */

import { defineWorker } from "@zakkster/lite-worker";

/** Package version. Kept in three-place sync with package.json and CHANGELOG.md. */
export const VERSION = "1.0.1";

// Scratch layout: two Float64 slots per job -- [0] = job index (round-trips so a
// worker's reply is filed under the right output slot), [1] = the item value in,
// the result value out. 16 bytes is the floor; a larger scratch is allowed but
// the dispatch protocol only touches these two slots.
const SLOT_BYTES = 16;

// Resolve the worker count. Fail closed: a missing/NaN/negative/zero request
// floors to >=1, never 0. Prefers an explicit size, then the host's logical core
// count (navigator.hardwareConcurrency exists on modern Node and in browsers),
// then 1. null is not zero: an absent count is 1, never a silent no-op pool.
function resolveSize(opts) {
  const req = opts.size | 0;
  if (req > 0) return req;
  const nav = globalThis.navigator;
  const hc = (nav && nav.hardwareConcurrency) | 0;
  if (hc > 0) return hc;
  return 1;
}

// Build the worker module the default factory serializes. `workerFn` is the
// per-item transform (item) -> result; it is inlined by source so the serialized
// body closes over nothing (it cannot -- it crosses a thread boundary). The body
// applies the transform to slot [1] and transfers the scratch straight back,
// leaving slot [0] (the job index) untouched so ordering is preserved on the
// main side. Constructed via Function so a string body can be handed to
// defineWorker's .toString() serializer with zero drift.
function buildWorkerModule(workerFn) {
  const src = workerFn.toString();
  // Fail closed: a throwing transform must never swallow the item. The throw is
  // caught and reported on the reserved "lwp:err" typed channel (which cannot
  // collide with a valid Float64 result on the raw channel); the buffer is not
  // sent back, and the pool rejects the batch.
  const body =
    "var __fn = (" + src + ");\n" +
    "ctx.onRaw(function (buf) {\n" +
    "  var f = new Float64Array(buf);\n" +
    "  try {\n" +
    "    f[1] = __fn(f[1]);\n" +
    "  } catch (e) {\n" +
    "    ctx.post('lwp:err', { message: (e && e.message) || String(e), index: f[0] });\n" +
    "    return;\n" +
    "  }\n" +
    "  ctx.send(buf);\n" +
    "});\n";
  // eslint-disable-next-line no-new-func
  return new Function("ctx", body);
}

// Default worker factory: a real lite-worker over a Blob URL. Used in a browser
// or any host with Worker/Blob/URL. Torture and tests inject `opts.spawn` to run
// the identical pool logic over a real node:worker_threads bridge instead. The
// returned transport adds onError to the raw surface: a caught transform throw
// arrives on the "lwp:err" typed channel, a worker load/crash on the handle's
// onError; both are funneled to the pool so a failure fails the batch closed
// rather than hanging it.
function defaultSpawn(spec) {
  const mod = buildWorkerModule(spec.workerFn);
  const errHandlers = new Set();
  const emit = function (err) { errHandlers.forEach(function (fn) { fn(err); }); };
  const handle = defineWorker(mod, {
    name: spec.name + "#" + spec.index,
    onError: function (e) { emit(e instanceof Error ? e : new Error(String(e))); },
  });
  handle.on("lwp:err", function (d) {
    emit(new Error((d && d.message) || "lite-worker-pool: worker transform threw"));
  });
  handle.spawn();
  return {
    send: function (buf, transfer) { handle.send(buf, transfer); },
    onRaw: function (fn) { return handle.onRaw(fn); },
    onError: function (fn) { errHandlers.add(fn); return function () { errHandlers.delete(fn); }; },
    terminate: function () { handle.terminate(); },
  };
}

// Known option keys. An unknown key is a misconfiguration, not a silent default:
// fail closed with a did-you-mean hint (Source law).
const KNOWN_OPTS = ["size", "scratchBytes", "name", "spawn"];

function nearestOpt(key) {
  let best = "";
  let bestD = 1e9;
  for (let i = 0; i < KNOWN_OPTS.length; i++) {
    const cand = KNOWN_OPTS[i];
    const d = editDistance(key, cand);
    if (d < bestD) { bestD = d; best = cand; }
  }
  // Only suggest when it is plausibly a typo (<= a third of the key length + 1).
  return bestD <= Math.max(1, (key.length / 3) | 0) + 1 ? best : "";
}

function editDistance(a, b) {
  const m = a.length;
  const n = b.length;
  const prev = new Array(n + 1);
  const cur = new Array(n + 1);
  for (let j = 0; j <= n; j++) prev[j] = j;
  for (let i = 1; i <= m; i++) {
    cur[0] = i;
    for (let j = 1; j <= n; j++) {
      const cost = a.charCodeAt(i - 1) === b.charCodeAt(j - 1) ? 0 : 1;
      let v = prev[j] + 1;
      if (cur[j - 1] + 1 < v) v = cur[j - 1] + 1;
      if (prev[j - 1] + cost < v) v = prev[j - 1] + cost;
      cur[j] = v;
    }
    for (let j = 0; j <= n; j++) prev[j] = cur[j];
  }
  return prev[n];
}

class Pool {
  constructor(workerFn, opts) {
    if (typeof workerFn !== "function") {
      throw new TypeError("lite-worker-pool: createWorkerPool(workerFn) requires a function");
    }
    const o = opts || {};
    for (const k in o) {
      if (KNOWN_OPTS.indexOf(k) === -1) {
        const hint = nearestOpt(k);
        throw new Error(
          "lite-worker-pool: unknown option '" + k + "'" +
          (hint ? " -- did you mean '" + hint + "'?" : "; known: " + KNOWN_OPTS.join(", "))
        );
      }
    }
    this._size = Math.max(1, resolveSize(o));
    const bytes = o.scratchBytes | 0;
    this._scratchBytes = bytes > SLOT_BYTES ? bytes : SLOT_BYTES;
    this._name = typeof o.name === "string" && o.name ? o.name : "lite-worker-pool";
    const spawn = typeof o.spawn === "function" ? o.spawn : defaultSpawn;

    this._disposed = false;
    this._poison = null;   // set to the Error that killed a batch; poisons future map()s

    // Per-batch state. Reset by _reset(); cold, one allocation per map() call.
    this._items = null;
    this._results = null;
    this._total = 0;
    this._next = 0;      // next unassigned item index
    this._done = 0;      // replies filed
    this._active = 0;    // jobs currently in flight
    this._busy = false;
    this._resolve = null;
    this._reject = null;

    // Reused single-slot transfer list -- postMessage reads it synchronously, so
    // sharing one instance across every send() allocates nothing on the hot path.
    this._t1 = [null];

    // Build workers. Each owns one scratch buffer for the life of the pool; the
    // buffer ping-pongs main<->worker via transfer and always comes to rest back
    // at its worker, so the buffer count is conserved at exactly `size`.
    this._workers = [];
    for (let i = 0; i < this._size; i++) {
      const scratch = new ArrayBuffer(this._scratchBytes);
      const transport = spawn({
        workerFn: workerFn,
        index: i,
        name: this._name,
        scratchBytes: this._scratchBytes,
      });
      const w = {
        transport: transport,
        scratch: scratch,               // owned buffer when idle; null when in flight
        view: new Float64Array(scratch),
        jobs: 0,                         // jobs this worker has completed (saturation proof)
        off: null,
        offError: null,
      };
      const self = this;
      w.off = transport.onRaw(function (buf) { self._onResult(w, buf); });
      // onError is part of the PoolWorker contract; guard so an older injected
      // factory that omits it does not crash construction (it just cannot report
      // worker deaths, which is the injector's responsibility).
      if (typeof transport.onError === "function") {
        w.offError = transport.onError(function (err) { self._onWorkerError(err); });
      }
      this._workers.push(w);
    }
  }

  get size() { return this._size; }

  // Dispatch every item across the pool, filling results BY INDEX. Resolves with
  // an array the length of `items`, in input order, regardless of the order
  // workers finish. One batch at a time.
  map(items) {
    if (this._disposed) {
      return Promise.reject(new Error("lite-worker-pool: pool disposed"));
    }
    if (this._poison) {
      return Promise.reject(new Error("lite-worker-pool: pool poisoned by a worker error (" + this._poison.message + "); dispose and recreate"));
    }
    if (this._busy) {
      return Promise.reject(new Error("lite-worker-pool: map() already in progress; await it first"));
    }
    // Uniform surface: bad input rejects (like the disposed/busy paths) rather
    // than throwing a raw synchronous TypeError/RangeError. A length that is not a
    // non-negative safe integer (negative, fractional, NaN/Infinity) would blow up
    // `new Array(n)`, so it fails closed here first. Cold: runs once per map().
    const len = items == null ? undefined : items.length;
    if (typeof len !== "number" || !Number.isInteger(len) || len < 0) {
      return Promise.reject(new TypeError("lite-worker-pool: map(items) requires an array-like with a non-negative integer length"));
    }
    const n = len;
    const results = new Array(n);
    if (n === 0) return Promise.resolve(results);

    this._items = items;
    this._results = results;
    this._total = n;
    this._next = 0;
    this._done = 0;
    this._active = 0;
    this._busy = true;

    const self = this;
    return new Promise(function (resolve, reject) {
      self._resolve = resolve;
      self._reject = reject;
      // Seed: give each worker its first job. The pump keeps them fed thereafter.
      const workers = self._workers;
      for (let i = 0; i < workers.length && self._next < n; i++) {
        self._dispatch(workers[i]);
      }
    });
  }

  // --- hot path: per-item dispatch (allocation-free) -----------------------

  // Assign the next unassigned item to worker `w` using w's owned scratch buffer,
  // then transfer it. Writes the index + value into the two reused slots and
  // sends via the reused transfer list -- no allocation.
  _dispatch(w) {
    const idx = this._next++;
    const view = w.view;
    view[0] = idx;
    view[1] = this._items[idx];
    this._active++;
    const buf = w.scratch;
    w.scratch = null;                    // in flight; ownership transferred away
    const t1 = this._t1;
    t1[0] = buf;
    w.transport.send(buf, t1);
    t1[0] = null;
  }

  // A worker returned a filled scratch buffer. File the result under its own
  // index (order-independent), account the job, then either feed the worker the
  // next item or park the buffer with it as idle. A transfer mints a fresh
  // ArrayBuffer identity per hop, so the view is rebuilt here (transient, GC'd --
  // this is the ring's only per-hop cost, same as lite-worker's frame pool); the
  // buffer's byte length never changes.
  _onResult(w, buf) {
    if (this._disposed || !this._busy) return;
    const view = new Float64Array(buf);
    const idx = view[0] | 0;
    this._results[idx] = view[1];
    w.jobs++;
    this._active--;
    this._done++;
    w.scratch = buf;                     // re-owned
    w.view = view;
    if (this._next < this._total) {
      this._dispatch(w);
    }
    if (this._done === this._total) this._finish();
  }

  _finish() {
    const resolve = this._resolve;
    const results = this._results;
    this._busy = false;
    this._items = null;
    this._results = null;
    this._resolve = null;
    this._reject = null;
    resolve(results);
  }

  // A worker died or its transform threw. Fail closed: a thrown transform is
  // deterministic, so reassigning the item is pointless -- reject the whole batch
  // with the underlying error and poison the pool (its scratch buffer went down
  // with the worker; reuse is unsafe until dispose). Clears batch state like
  // _finish so dispose() can run cleanly. Errors outside a live batch are recorded
  // as poison but have no batch to reject.
  _onWorkerError(err) {
    if (this._disposed) return;
    const e = err instanceof Error ? err : new Error(String(err));
    if (!this._poison) this._poison = e;
    if (!this._busy) return;
    const reject = this._reject;
    this._busy = false;
    this._items = null;
    this._results = null;
    this._resolve = null;
    this._reject = null;
    reject(e);
  }

  // --- cold surface --------------------------------------------------------

  stats() {
    return {
      size: this._size,
      queued: this._busy ? this._total - this._next : 0,
      active: this._active,
      done: this._done,
    };
  }

  // Idempotent full teardown: terminate every worker, drop handlers, reject any
  // in-flight batch. A second call is a no-op and never throws.
  dispose() {
    if (this._disposed) return;
    this._disposed = true;
    const workers = this._workers;
    for (let i = 0; i < workers.length; i++) {
      const w = workers[i];
      if (w.off) { try { w.off(); } catch (e) { /* detach is best-effort */ } }
      if (w.offError) { try { w.offError(); } catch (e) { /* detach is best-effort */ } }
      try { w.transport.terminate(); } catch (e) { /* teardown never throws */ }
    }
    this._workers.length = 0;
    if (this._busy && this._reject) {
      const reject = this._reject;
      this._busy = false;
      this._items = null;
      this._results = null;
      this._resolve = null;
      this._reject = null;
      reject(new Error("lite-worker-pool: pool disposed mid-batch"));
    }
  }
}

/**
 * Create a data-parallel worker pool. `workerFn` is the per-item transform,
 * `(item) => result`; it is serialized ONCE into every worker (it cannot close
 * over anything -- it crosses a thread boundary). There is no per-map worker fn.
 *
 * @param {(item: number) => number} workerFn self-contained per-item transform
 * @param {{ size?: number, scratchBytes?: number, name?: string, spawn?: Function }} [opts]
 * @returns {Pool}
 */
export function createWorkerPool(workerFn, opts) {
  return new Pool(workerFn, opts || {});
}

export default createWorkerPool;
