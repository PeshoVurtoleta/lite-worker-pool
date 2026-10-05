/**
 * @zakkster/lite-worker-pool
 * Zero-GC data-parallel worker pool over @zakkster/lite-worker (v1.1.0).
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
export const VERSION = "1.1.0";

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

function nearestOpt(key, known) {
  const list = known || KNOWN_OPTS;
  let best = "";
  let bestD = 1e9;
  for (let i = 0; i < list.length; i++) {
    const cand = list[i];
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

// ===========================================================================
// WorkerSet (1.1.0) -- the service-shaped mode (research/worker-set.md, E1-E11)
// ===========================================================================
//
// map() above is a BATCH: workers pull, one failure poisons the pool. A WorkerSet
// is a SERVICE: the caller (a load balancer such as @zakkster/lite-pick, or anyone)
// chooses the worker; each worker runs up to `slots` jobs at once (measured: two
// hide the main-thread round trip) with a capped FIFO queue behind them; a
// transform throw fails ONE job; a dead worker fails ITS jobs and goes DOWN while
// the others keep serving; respawn(i) restarts one worker in the same slot. The two
// modes never share a queue (a shared work-stealing queue and targeted dispatch
// exclude each other -- the Akka BalancingPool / ConsistentHashingPool split).
//
// Allocation: every table is preallocated at construction (job ids index flat typed
// arrays; per-worker rings are slices of shared arrays). post() + onSettle allocate
// nothing in this code per job; submit() allocates its Promise (like any async API).
// Node's own MessagePort allocates ~1.1-1.4 KB per job on the main thread (measured,
// research/worker-set.md section 2) -- that is the transport's, disclosed, not ours.

/** Worker states. A worker is only eligible for work while READY. */
export const WORKER_STATE = Object.freeze({ STARTING: 0, READY: 1, DOWN: 2 });

const ST_STARTING = 0;
const ST_READY = 1;
const ST_DOWN = 2;

// Job kinds and job states (per job id).
const K_FREE = 0;
const K_POST = 1;
const K_SUBMIT = 2;
const J_NONE = 0;
const J_QUEUED = 1;
const J_EXEC = 2;
const J_DISCARD = 3;   // executing, but already settled (aborted): its late reply is dropped

// Set-mode scratch: [0] job id, [1] value in / result out, [2] transform-error flag.
const SET_SCRATCH_BYTES = 24;
const MAX_SLOTS = 8;
const MAX_QUEUE = 65536;
const SET_OPTS = ["size", "slots", "queue", "name", "spawn", "onSettle", "now"];

function lwpError(Ctor, code, msg, cause) {
  const e = new Ctor("lite-worker-pool: " + msg);
  e.code = code;
  if (cause !== undefined) e.cause = cause;
  return e;
}

// A user callback (onSettle) must never be able to strand other jobs mid-loop: it
// runs last, guarded, and a throw is re-raised asynchronously (loud, not swallowed).
function raiseLater(e) {
  if (typeof globalThis.reportError === "function") globalThis.reportError(e);
  else queueMicrotask(function () { throw e; });
}

// The set-mode worker body. Differs from map()'s in three ways: it says READY once
// loaded; a transform throw returns the buffer FLAGGED (the job fails, the worker
// and its buffer stay); and the transform receives the per-worker control values
// as its second argument (set via WorkerSet.control).
function buildSetModule(workerFn) {
  const src = workerFn.toString();
  const body =
    "var __fn = (" + src + ");\n" +
    "var __ctl = new Float64Array(0);\n" +
    "ctx.on('lwp:ctl', function (d) { __ctl = d instanceof Float64Array ? d : new Float64Array(0); });\n" +
    "ctx.onRaw(function (buf) {\n" +
    "  var f = new Float64Array(buf);\n" +
    "  try {\n" +
    "    f[1] = __fn(f[1], __ctl);\n" +
    "    f[2] = 0;\n" +
    "  } catch (e) {\n" +
    "    f[2] = 1;\n" +
    "    ctx.post('lwp:terr', { message: (e && e.message) || String(e) });\n" +
    "  }\n" +
    "  ctx.send(buf);\n" +
    "});\n" +
    "ctx.post('lwp:ready', null);\n";
  // eslint-disable-next-line no-new-func
  return new Function("ctx", body);
}

// Default set-mode transport: a real lite-worker over a Blob URL, adding the typed
// channel the set needs -- onPost (READY + transform-error messages in) and post
// (control values out). Injected transports (tests: node:worker_threads) implement
// the same six members.
function defaultSetSpawn(spec) {
  const mod = buildSetModule(spec.workerFn);
  const errHandlers = new Set();
  const postHandlers = new Set();
  const emitErr = function (err) { errHandlers.forEach(function (fn) { fn(err); }); };
  const emitPost = function (type, data) { postHandlers.forEach(function (fn) { fn(type, data); }); };
  const handle = defineWorker(mod, {
    name: spec.name + "#" + spec.index,
    onError: function (e) { emitErr(e instanceof Error ? e : new Error(String(e))); },
  });
  handle.on("lwp:ready", function () { emitPost("lwp:ready", null); });
  handle.on("lwp:terr", function (d) { emitPost("lwp:terr", d); });
  handle.spawn();
  return {
    send: function (buf, transfer) { handle.send(buf, transfer); },
    onRaw: function (fn) { return handle.onRaw(fn); },
    onError: function (fn) { errHandlers.add(fn); return function () { errHandlers.delete(fn); }; },
    onPost: function (fn) { postHandlers.add(fn); return function () { postHandlers.delete(fn); }; },
    post: function (type, data) { handle.post(type, data); },
    terminate: function () { handle.terminate(); },
  };
}

function defaultNow() {
  const p = globalThis.performance;
  return p && typeof p.now === "function" ? p.now() : Date.now();
}

class WorkerSet {
  constructor(workerFn, opts) {
    if (typeof workerFn !== "function") {
      throw lwpError(TypeError, "LWP_ARGUMENT", "createWorkerSet(workerFn) requires a function");
    }
    const o = opts || {};
    for (const k in o) {
      if (SET_OPTS.indexOf(k) === -1) {
        const hint = nearestOpt(k, SET_OPTS);
        throw lwpError(Error, "LWP_OPTION",
          "unknown option '" + k + "'" + (hint ? " -- did you mean '" + hint + "'?" : "; known: " + SET_OPTS.join(", ")));
      }
    }
    const slots = o.slots === undefined ? 2 : o.slots;
    if (typeof slots !== "number" || !Number.isInteger(slots) || slots < 1 || slots > MAX_SLOTS) {
      throw lwpError(RangeError, "LWP_OPTION", "slots must be an integer in [1, " + MAX_SLOTS + "], got " + String(slots));
    }
    const queue = o.queue === undefined ? 32 : o.queue;
    if (typeof queue !== "number" || !Number.isInteger(queue) || queue < 0 || queue > MAX_QUEUE) {
      throw lwpError(RangeError, "LWP_OPTION", "queue must be an integer in [0, " + MAX_QUEUE + "], got " + String(queue));
    }
    if (o.onSettle !== undefined && typeof o.onSettle !== "function") {
      throw lwpError(TypeError, "LWP_OPTION", "onSettle must be a function");
    }
    if (o.now !== undefined && typeof o.now !== "function") {
      throw lwpError(TypeError, "LWP_OPTION", "now must be a function");
    }
    if (o.spawn !== undefined && typeof o.spawn !== "function") {
      throw lwpError(TypeError, "LWP_OPTION", "spawn must be a function");
    }

    const n = Math.max(1, resolveSize(o));
    this._size = n;
    this._slots = slots;
    this._queue = queue;
    this._name = typeof o.name === "string" && o.name ? o.name : "lite-worker-set";
    this._spawn = o.spawn || defaultSetSpawn;
    this._workerFn = workerFn;
    this._onSettle = o.onSettle || null;
    this._now = o.now || defaultNow;
    this._disposed = false;
    this._t1 = [null];   // reused transfer list (postMessage reads it synchronously)

    // ---- job tables (job id = index) -------------------------------------
    const cap = n * (slots + queue);
    this._jobCap = cap;
    this._jKind = new Uint8Array(cap);
    this._jState = new Uint8Array(cap);
    this._jWorker = new Int32Array(cap);
    this._jSlot = new Int32Array(cap);
    this._jValue = new Float64Array(cap);
    this._jTag = new Float64Array(cap);
    this._jSent = new Float64Array(cap);
    this._jRes = new Array(cap).fill(null);
    this._jRej = new Array(cap).fill(null);
    this._jSig = new Array(cap).fill(null);
    this._jAbort = new Array(cap).fill(null);
    this._free = new Int32Array(cap);
    for (let j = 0; j < cap; j++) this._free[j] = cap - 1 - j;
    this._freeTop = cap;

    // ---- per-worker state ------------------------------------------------
    this._state = new Uint8Array(n);
    this._gen = new Uint32Array(n);
    this._exec = new Int32Array(n);            // jobs in flight (incl. discarded ones)
    this._execHead = new Int32Array(n);        // FIFO of in-flight job ids: ring slice [i*slots, +slots)
    this._execRing = new Int32Array(n * slots);
    this._qHead = new Int32Array(n);           // FIFO of queued job ids: ring slice [i*queue, +queue)
    this._qCount = new Int32Array(n);
    this._qRing = new Int32Array(n * queue);
    this._slotBuf = new Array(n * slots).fill(null);   // owned buffer per slot; null while in flight
    this._slotView = new Array(n * slots).fill(null);
    this._freeSlots = new Int32Array(n * slots);       // stack slice [i*slots, +freeCount)
    this._freeCount = new Int32Array(n);
    this._done = new Float64Array(n);
    this._failed = new Float64Array(n);
    this._t = new Array(n).fill(null);
    this._offRaw = new Array(n).fill(null);
    this._offErr = new Array(n).fill(null);
    this._offPost = new Array(n).fill(null);
    this._terr = new Array(n).fill(null);      // the transform message that precedes a flagged buffer
    this._ctl = new Array(n).fill(null);
    this._wRes = new Array(n).fill(null);      // respawn(i) waiter
    this._wRej = new Array(n).fill(null);
    this._readyWaiters = [];

    for (let i = 0; i < n; i++) this._state[i] = ST_DOWN;
    try {
      for (let i = 0; i < n; i++) this._start(i);
    } catch (e) {
      this.dispose();                                 // fail closed: no half-built set keeps threads alive
      throw e;
    }
  }

  get size() { return this._size; }
  get slots() { return this._slots; }
  get queue() { return this._queue; }

  // --- cold: worker lifecycle ---------------------------------------------

  _start(i) {
    const gen = ++this._gen[i];
    const slots = this._slots;
    const b = i * slots;
    for (let s = 0; s < slots; s++) {
      const buf = new ArrayBuffer(SET_SCRATCH_BYTES);
      this._slotBuf[b + s] = buf;
      this._slotView[b + s] = new Float64Array(buf);
      this._freeSlots[b + s] = s;
    }
    this._freeCount[i] = slots;
    this._exec[i] = 0;
    this._execHead[i] = 0;
    this._qHead[i] = 0;
    this._qCount[i] = 0;
    this._terr[i] = null;
    this._state[i] = ST_STARTING;

    const t = this._spawn({
      workerFn: this._workerFn, index: i, name: this._name,
      scratchBytes: SET_SCRATCH_BYTES, mode: "set",
    });
    if (!t || typeof t.send !== "function" || typeof t.onRaw !== "function" ||
        typeof t.onPost !== "function" || typeof t.terminate !== "function") {
      this._state[i] = ST_DOWN;
      throw lwpError(TypeError, "LWP_TRANSPORT",
        "a set-mode transport needs send(), onRaw(), onPost() and terminate() (onError() and post() recommended)");
    }
    this._t[i] = t;
    const self = this;
    this._offRaw[i] = t.onRaw(function (buf) { self._onReply(i, gen, buf); });
    this._offPost[i] = t.onPost(function (type, data) { self._onPost(i, gen, type, data); });
    this._offErr[i] = typeof t.onError === "function"
      ? t.onError(function (err) { self._onDeath(i, gen, err); })
      : null;
    if (this._ctl[i] !== null && typeof t.post === "function") t.post("lwp:ctl", this._ctl[i]);
  }

  _onPost(i, gen, type, data) {
    if (this._disposed || gen !== this._gen[i]) return;
    if (type === "lwp:ready") {
      if (this._state[i] !== ST_STARTING) return;
      this._state[i] = ST_READY;
      const res = this._wRes[i];
      this._wRes[i] = null;
      this._wRej[i] = null;
      if (res !== null) res();
      this._checkReady();
    } else if (type === "lwp:terr") {
      this._terr[i] = (data && data.message) || "worker transform threw";
    }
  }

  _onDeath(i, gen, err) {
    if (this._disposed || gen !== this._gen[i] || this._state[i] === ST_DOWN) return;
    this._down(i, "LWP_WORKER_DOWN", err instanceof Error ? err : new Error(String(err)));
  }

  // Take worker i DOWN: detach + terminate its transport, fail every job it holds,
  // reject a pending respawn. Bookkeeping completes BEFORE any user callback runs.
  _down(i, code, cause) {
    this._state[i] = ST_DOWN;
    this._gen[i]++;                                   // late messages from the old transport are ignored
    this._detach(i);
    const slots = this._slots;
    const queue = this._queue;
    const failed = [];                                // cold path: collect, then settle
    const eb = i * slots;
    for (let k = 0; k < this._exec[i]; k++) failed.push(this._execRing[eb + ((this._execHead[i] + k) % slots)]);
    const qb = i * queue;
    for (let k = 0; k < this._qCount[i]; k++) failed.push(this._qRing[qb + ((this._qHead[i] + k) % queue)]);
    this._exec[i] = 0;
    this._qCount[i] = 0;
    this._freeCount[i] = 0;                           // in-flight buffers died with the worker
    for (let s = 0; s < slots; s++) { this._slotBuf[eb + s] = null; this._slotView[eb + s] = null; }
    const rej = this._wRej[i];
    this._wRes[i] = null;
    this._wRej[i] = null;
    const msg = code === "LWP_DISPOSED" ? "worker set disposed" : "worker " + i + " is down";
    for (let k = 0; k < failed.length; k++) {
      const id = failed[k];
      if (this._jState[id] === J_DISCARD) { this._release(id); continue; }
      this._failJob(id, code, msg, cause);
    }
    if (rej !== null) rej(lwpError(Error, code, msg + " before it was ready", cause));
    this._checkReady();
  }

  _detach(i) {
    const offs = [this._offRaw, this._offPost, this._offErr];
    for (let k = 0; k < 3; k++) {
      const off = offs[k][i];
      offs[k][i] = null;
      if (off) { try { off(); } catch (e) { /* detach is best-effort */ } }
    }
    const t = this._t[i];
    this._t[i] = null;
    if (t) { try { t.terminate(); } catch (e) { /* teardown never throws */ } }
  }

  _checkReady() {
    const w = this._readyWaiters;
    if (w.length === 0) return;
    let allReady = true;
    let anyDown = false;
    for (let i = 0; i < this._size; i++) {
      if (this._state[i] !== ST_READY) allReady = false;
      if (this._state[i] === ST_DOWN) anyDown = true;
    }
    if (!allReady && !anyDown) return;
    this._readyWaiters = [];
    for (let k = 0; k < w.length; k++) {
      if (allReady) w[k].res();
      else w[k].rej(lwpError(Error, "LWP_WORKER_DOWN", "a worker went down before the set was ready"));
    }
  }

  // --- hot: admission, dispatch, reply --------------------------------------

  // 0 when accepted, else the rejection code. Validation of i / value is done by the caller.
  _admitCode(i) {
    const st = this._state[i];
    if (st !== ST_READY) return st === ST_STARTING ? "LWP_NOT_READY" : "LWP_WORKER_DOWN";
    if (this._exec[i] < this._slots || this._qCount[i] < this._queue) return 0;
    return "LWP_QUEUE_FULL";
  }

  _alloc(i, kind, value) {
    const id = this._free[--this._freeTop];
    this._jKind[id] = kind;
    this._jWorker[id] = i;
    this._jValue[id] = value;
    return id;
  }

  _admit(i, id) {
    if (this._exec[i] < this._slots) {
      this._send(i, id);
    } else {
      const q = this._queue;
      this._qRing[i * q + ((this._qHead[i] + this._qCount[i]) % q)] = id;
      this._qCount[i]++;
      this._jState[id] = J_QUEUED;
    }
  }

  _send(i, id) {
    const slots = this._slots;
    const b = i * slots;
    const s = this._freeSlots[b + (--this._freeCount[i])];
    const buf = this._slotBuf[b + s];
    const view = this._slotView[b + s];
    view[0] = id;
    view[1] = this._jValue[id];
    view[2] = 0;
    this._slotBuf[b + s] = null;                       // in flight
    this._jSlot[id] = s;
    this._jState[id] = J_EXEC;
    this._jSent[id] = this._now();
    this._execRing[b + ((this._execHead[i] + this._exec[i]) % slots)] = id;
    this._exec[i]++;
    const t1 = this._t1;
    t1[0] = buf;
    this._t[i].send(buf, t1);
    t1[0] = null;
  }

  _onReply(i, gen, buf) {
    if (this._disposed || gen !== this._gen[i]) return;
    const slots = this._slots;
    const b = i * slots;
    // Find the slot: over an in-process loopback the same buffer object comes back
    // (reuse its view, 0 B); over a real transfer a new ArrayBuffer object arrives
    // and needs a new view (part of the measured, disclosed transport cost).
    let s = -1;
    let view = null;
    for (let k = 0; k < slots; k++) {
      const v = this._slotView[b + k];
      if (this._slotBuf[b + k] === null && v !== null && v.buffer === buf) { s = k; view = v; break; }
    }
    if (view === null) view = new Float64Array(buf);
    const id = view[0];
    const head = this._execRing[b + this._execHead[i]];
    // Per-worker FIFO is the contract (E8): the reply must be for the OLDEST job in
    // flight on this worker. Anything else is a protocol violation -> fail closed.
    if (this._exec[i] === 0 || id !== head || (s >= 0 && this._jSlot[id] !== s)) {
      this._down(i, "LWP_WORKER_DOWN", lwpError(Error, "LWP_PROTOCOL", "worker " + i + " replied out of order"));
      return;
    }
    if (s < 0) { s = this._jSlot[id]; this._slotView[b + s] = view; }
    const result = view[1];
    const failed = view[2] !== 0;
    this._execHead[i] = (this._execHead[i] + 1) % slots;
    this._exec[i]--;
    this._slotBuf[b + s] = buf;                        // re-owned
    this._freeSlots[b + (this._freeCount[i]++)] = s;

    // Feed the worker from its queue before running any user callback.
    const q = this._queue;
    while (this._exec[i] < slots && this._qCount[i] > 0) {
      const next = this._qRing[i * q + this._qHead[i]];
      this._qHead[i] = (this._qHead[i] + 1) % q;
      this._qCount[i]--;
      this._send(i, next);
    }

    if (this._jState[id] === J_DISCARD) { this._release(id); return; }
    if (failed) {
      const msg = this._terr[i] || "worker transform threw";
      this._terr[i] = null;
      this._failJob(id, "LWP_TRANSFORM", msg, undefined);
    } else {
      this._done[i]++;
      this._okJob(id, result);
    }
  }

  _release(id) {
    this._jKind[id] = K_FREE;
    this._jState[id] = J_NONE;
    this._jRes[id] = null;
    this._jRej[id] = null;
    const sig = this._jSig[id];
    if (sig !== null) {
      sig.removeEventListener("abort", this._jAbort[id]);
      this._jSig[id] = null;
      this._jAbort[id] = null;
    }
    this._free[this._freeTop++] = id;
  }

  _okJob(id, value) {
    const i = this._jWorker[id];
    if (this._jKind[id] === K_SUBMIT) {
      const res = this._jRes[id];
      this._release(id);
      res(value);
    } else {
      const tag = this._jTag[id];
      this._release(id);
      this._callSettle(i, tag, true, value, null);
    }
  }

  _failJob(id, code, msg, cause) {
    const i = this._jWorker[id];
    this._failed[i]++;
    if (this._jKind[id] === K_SUBMIT) {
      const rej = this._jRej[id];
      this._release(id);
      rej(lwpError(Error, code, msg, cause));
    } else {
      const tag = this._jTag[id];
      this._release(id);
      this._callSettle(i, tag, false, NaN, code);
    }
  }

  _callSettle(i, tag, ok, value, code) {
    const fn = this._onSettle;
    if (fn === null) return;
    try { fn(i, tag, ok, value, code); } catch (e) { raiseLater(e); }
  }

  // signal fired for job `id`
  _abort(id) {
    const st = this._jState[id];
    const sig = this._jSig[id];
    const reason = sig !== null ? sig.reason : undefined;
    const i = this._jWorker[id];
    if (st === J_QUEUED) {
      // Remove it from worker i's queue (cold: shift the ring tail left by one).
      const q = this._queue;
      const qb = i * q;
      const n = this._qCount[i];
      let k = 0;
      while (k < n && this._qRing[qb + ((this._qHead[i] + k) % q)] !== id) k++;
      for (; k < n - 1; k++) {
        this._qRing[qb + ((this._qHead[i] + k) % q)] = this._qRing[qb + ((this._qHead[i] + k + 1) % q)];
      }
      this._qCount[i] = n - 1;
      const rej = this._jRej[id];
      this._release(id);
      rej(reason !== undefined ? reason : lwpError(Error, "LWP_ABORTED", "aborted while queued"));
    } else if (st === J_EXEC) {
      // Cannot interrupt the thread: settle now, drop the late result, free the slot on reply.
      const rej = this._jRej[id];
      this._jState[id] = J_DISCARD;
      this._jRes[id] = null;
      this._jRej[id] = null;
      sig.removeEventListener("abort", this._jAbort[id]);
      this._jSig[id] = null;
      this._jAbort[id] = null;
      rej(lwpError(Error, "LWP_ABORTED", "aborted while executing on worker " + i + " (its result will be discarded)", reason));
    }
  }

  _vIdx(i) {
    if (typeof i !== "number" || !Number.isInteger(i) || i < 0 || i >= this._size) {
      throw lwpError(RangeError, "LWP_INDEX", "worker index must be an integer in [0, " + this._size + "), got " + String(i));
    }
  }

  // --- public: dispatch ------------------------------------------------------

  /**
   * Run `value` on worker `i`. Resolves with the transform's result; rejects with a
   * coded Error: LWP_INDEX, LWP_ARGUMENT, LWP_NOT_READY, LWP_WORKER_DOWN, LWP_QUEUE_FULL,
   * LWP_TRANSFORM, LWP_ABORTED, LWP_DISPOSED (or the signal's reason while queued).
   */
  submit(i, value, opts) {
    if (this._disposed) return Promise.reject(lwpError(Error, "LWP_DISPOSED", "worker set disposed"));
    try {
      this._vIdx(i);
      if (typeof value !== "number") throw lwpError(TypeError, "LWP_ARGUMENT", "value must be a number");
      if (opts !== undefined && opts !== null) {
        for (const k in opts) {
          if (k !== "signal") throw lwpError(Error, "LWP_OPTION", "unknown submit option '" + k + "'; known: signal");
        }
      }
    } catch (e) {
      return Promise.reject(e);
    }
    const signal = opts ? opts.signal : undefined;
    if (signal !== undefined && signal !== null) {
      if (signal.aborted) {
        return Promise.reject(signal.reason !== undefined ? signal.reason : lwpError(Error, "LWP_ABORTED", "aborted before dispatch"));
      }
    }
    const code = this._admitCode(i);
    if (code !== 0) return Promise.reject(lwpError(Error, code, code === "LWP_QUEUE_FULL" ? "worker " + i + " queue is full" : "worker " + i + " is not ready"));
    const self = this;
    return new Promise(function (resolve, reject) {
      const id = self._alloc(i, K_SUBMIT, value);
      self._jRes[id] = resolve;
      self._jRej[id] = reject;
      if (signal !== undefined && signal !== null) {
        const onAbort = function () { self._abort(id); };
        self._jSig[id] = signal;
        self._jAbort[id] = onAbort;
        signal.addEventListener("abort", onAbort, { once: true });
      }
      self._admit(i, id);
    });
  }

  /**
   * Zero-allocation dispatch: run `value` on worker `i`, completion via the
   * `onSettle(i, tag, ok, value, code)` option. Returns false (nothing queued) when
   * the set is disposed, worker i is not READY, or its queue is full.
   */
  post(i, value, tag) {
    this._vIdx(i);
    if (typeof value !== "number") throw lwpError(TypeError, "LWP_ARGUMENT", "value must be a number");
    if (typeof tag !== "number") throw lwpError(TypeError, "LWP_ARGUMENT", "tag must be a number");
    if (this._disposed || this._admitCode(i) !== 0) return false;
    const id = this._alloc(i, K_POST, value);
    this._jTag[id] = tag;
    this._admit(i, id);
    return true;
  }

  // --- public: 0-alloc reads (health probes, a balancer's load view) ---------------

  /** WORKER_STATE of worker i. */
  state(i) { this._vIdx(i); return this._state[i]; }
  /** True iff worker i is READY. Never throws (false for a bad index). */
  isReady(i) { return (i >>> 0) === i && i < this._size && this._state[i] === ST_READY; }
  /** Jobs executing + queued on worker i. Never throws (0 for a bad index). */
  load(i) { return (i >>> 0) === i && i < this._size ? this._exec[i] + this._qCount[i] : 0; }
  /** `now()` at which worker i's oldest in-flight job was sent, or NaN when idle. Never throws. */
  busySince(i) {
    if ((i >>> 0) !== i || i >= this._size || this._exec[i] === 0) return NaN;
    return this._jSent[this._execRing[i * this._slots + this._execHead[i]]];
  }

  // --- public: cold lifecycle -----------------------------------------------------

  /** Resolves when every worker is READY; rejects if one goes DOWN first. */
  ready() {
    if (this._disposed) return Promise.reject(lwpError(Error, "LWP_DISPOSED", "worker set disposed"));
    const self = this;
    return new Promise(function (resolve, reject) {
      self._readyWaiters.push({ res: resolve, rej: reject });
      self._checkReady();
    });
  }

  /** Terminate worker i now: DOWN, its jobs fail LWP_WORKER_DOWN. Idempotent. */
  kill(i) {
    this._vIdx(i);
    if (this._disposed || this._state[i] === ST_DOWN) return;
    this._down(i, "LWP_WORKER_DOWN", lwpError(Error, "LWP_KILLED", "worker " + i + " was killed"));
  }

  /**
   * Replace worker i with a fresh one in the same slot (a live worker is killed
   * first). Resolves when it is READY; its last control values are re-applied.
   */
  respawn(i) {
    try { this._vIdx(i); } catch (e) { return Promise.reject(e); }
    if (this._disposed) return Promise.reject(lwpError(Error, "LWP_DISPOSED", "worker set disposed"));
    if (this._state[i] !== ST_DOWN) {
      this._down(i, "LWP_WORKER_DOWN", lwpError(Error, "LWP_KILLED", "worker " + i + " was respawned"));
    }
    const self = this;
    return new Promise(function (resolve, reject) {
      self._wRes[i] = resolve;
      self._wRej[i] = reject;
      try {
        self._start(i);
      } catch (e) {
        self._wRes[i] = null;
        self._wRej[i] = null;
        reject(e);
      }
    });
  }

  /**
   * Copy `values` to worker i; its transform receives them as `(item, ctl)`. Kept
   * per worker and re-applied after respawn(i).
   */
  control(i, values) {
    this._vIdx(i);
    if (!(values instanceof Float64Array)) throw lwpError(TypeError, "LWP_ARGUMENT", "control values must be a Float64Array");
    const copy = new Float64Array(values);
    this._ctl[i] = copy;
    const t = this._t[i];
    if (t !== null && this._state[i] !== ST_DOWN) {
      if (typeof t.post !== "function") throw lwpError(TypeError, "LWP_TRANSPORT", "this transport has no post(); control() needs it");
      t.post("lwp:ctl", copy);
    }
  }

  /** Cold snapshot: per-worker state, executing, queued, done, failed. Allocates. */
  stats() {
    const workers = [];
    for (let i = 0; i < this._size; i++) {
      workers.push({ state: this._state[i], executing: this._exec[i], queued: this._qCount[i], done: this._done[i], failed: this._failed[i] });
    }
    return { size: this._size, slots: this._slots, queue: this._queue, workers };
  }

  /** Idempotent: terminate every worker; every pending job fails LWP_DISPOSED. */
  dispose() {
    if (this._disposed) return;
    const w = this._readyWaiters;                     // taken first: they reject DISPOSED, not WORKER_DOWN
    this._readyWaiters = [];
    for (let i = 0; i < this._size; i++) {
      if (this._state[i] !== ST_DOWN || this._wRej[i] !== null) this._down(i, "LWP_DISPOSED", undefined);
    }
    this._disposed = true;
    for (let k = 0; k < w.length; k++) w[k].rej(lwpError(Error, "LWP_DISPOSED", "worker set disposed"));
  }
}

/**
 * Create a service-shaped worker set (1.1.0): the caller chooses the worker for each
 * job. `workerFn(item, ctl)` is serialized ONCE into every worker; `ctl` is that
 * worker's control values (WorkerSet.control), an empty Float64Array until set.
 *
 * @param {(item: number, ctl: Float64Array) => number} workerFn self-contained transform
 * @param {{ size?: number, slots?: number, queue?: number, name?: string, spawn?: Function,
 *           onSettle?: Function, now?: () => number }} [opts]
 * @returns {WorkerSet}
 */
export function createWorkerSet(workerFn, opts) {
  return new WorkerSet(workerFn, opts || {});
}

export default createWorkerPool;
