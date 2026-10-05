// test/torture.mjs -- node --expose-gc test/torture.mjs
//
// The lite-worker-pool torture suite. Runs its tiers in order; prints exactly
// "ok" to stdout on success (exit 0) and a diagnostic + exit 1 on any failure.
// Tier progress and the gate summary go to stderr so stdout stays clean.
//
// Tiers:
//   T0 conservation -- N=10000 over a REAL node:worker_threads bridge: exactly N
//      results, results[i] === f(items[i]), every item processed exactly once.
//   T1 ordering     -- workers finish OUT of order (staggered reply); results
//      still land in input index order over the real bridge.
//   T2 saturation   -- every worker makes progress (none starved), the queue
//      drains to 0, and stats().active returns to 0 at completion.
//   T3 worker-error -- a transform that throws on one item makes map() REJECT
//      (bounded, no hang) over the real bridge; the item is never silently lost.
//   T6 retention    -- measureOps over >=10k synchronous dispatch hops shows
//      < 8 B/op and 0 majors/kOp; scratch byteLength unchanged; and a
//      FINALIZATION residual (lite-leak tracker.size() read after a HARD settle,
//      the pools tracked but never untracked) stays <= RES over many pool
//      create/dispose cycles -- a disposed pool that leaked is not collected.
//   T9 controls     -- each gate above, deliberately broken, MUST be caught:
//      (a) a misrouting dispatch drops an output -> conservation fails;
//      (b) a completion-order collector driven through the real pool -> ordering fails;
//      (c) a scratch that reallocates per job -> retention fails;
//      (d) a pool that swallows a worker throw -> map() hangs (bounded wait trips).
//
// Proof-of-teardown: set TORTURE_BREAK=conservation|ordering|retention|worker-error|leak
// to inject the matching break into the REAL tier so it exits nonzero -- the
// failing-before half of a failing-before/passing-after control. TORTURE_BREAK=leak
// pins every disposed pool so it can never finalize -> the T6 residual gate trips.
// No output is a FAIL.

import { measureOps, checkOps } from "@zakkster/lite-gc-profiler";
import { createWorkerPool } from "../WorkerPool.js";
import {
  realThreadSpawn,
  syncLoopbackSpawn,
  waitFor,
  settle,
  makeResidualTracker,
  NOOP_CLEANUP,
} from "./torture/harness.mjs";

const log = (s) => process.stderr.write(s + "\n");
const BREAK = process.env.TORTURE_BREAK || "";
const CYCLES = 2048;

// Finalization residual ceiling for the T6 pool-retention gate. A cleanly
// disposed pool is collected (size--); a leaked one is not. Clean runs leave
// single digits; a real leak leaves ~CYCLES. Slack matches the di-* authority
// pattern (max(16, CYCLES/1000)).
const RES = Math.max(16, (CYCLES / 1000) | 0); // 16

// TORTURE_BREAK=leak pins every disposed pool in this sink so it can NEVER be
// finalized -> the T6 residual stays ~CYCLES -> the residual gate trips RED.
const LEAK_HOLD = BREAK === "leak";
const __leakSink = [];

// Hard settle: drive FinalizationRegistry callbacks to ground (>=10 gc()+tick
// passes) before reading tracker.size(), or the residual reads an empty window
// and the gate falsely passes.
async function settleHard() {
  for (let i = 0; i < 10; i++) {
    globalThis.gc?.();
    await new Promise((r) => setTimeout(r, 15));
  }
}

// The per-item transform. Self-contained (no closure) so it serializes into the
// real worker thread verbatim; exact in Float64 for every index used here.
const f = (x) => x * 3 + 1;

const metrics = {
  leakSize: 0, findings: 0, warnings: 0,
  gcMajor: 0, gcMinor: 0, gcMaxMs: 0,
  bytesPerOp: 0,
};

function fail(msg, op) {
  const e = new Error(msg);
  if (op !== undefined) e.op = op;
  return e;
}

function withTimeout(promise, ms, label) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(fail(label + ": timed out after " + ms + "ms")), ms);
    promise.then((v) => { clearTimeout(t); resolve(v); }, (e) => { clearTimeout(t); reject(e); });
  });
}

// ---------------------------------------------------------------------------
// T0 -- conservation over a REAL node:worker_threads bridge.
// ---------------------------------------------------------------------------
async function t0() {
  const N = 10000;
  const size = 4;
  const spawn = realThreadSpawn(
    f,
    BREAK === "conservation" ? { fault: "misroute", at: 4321, to: 0 } : {}
  );
  const pool = createWorkerPool(f, { size, spawn });
  try {
    const items = new Array(N);
    for (let i = 0; i < N; i++) items[i] = i;
    const results = await withTimeout(pool.map(items), 30000, "T0 map");
    if (results.length !== N) throw fail("T0: results.length " + results.length + " != " + N);
    for (let i = 0; i < N; i++) {
      if (results[i] !== f(i)) throw fail("T0: results[" + i + "]=" + results[i] + " != f(" + i + ")=" + f(i), i);
    }
    const st = pool.stats();
    if (st.done !== N) throw fail("T0: stats().done " + st.done + " != " + N + " -- an item was dropped or double-counted");
    if (st.active !== 0) throw fail("T0: stats().active " + st.active + " != 0 at completion");
    if (st.queued !== 0) throw fail("T0: stats().queued " + st.queued + " != 0 at completion");
    log("    T0: " + N + " items over " + size + " real threads, all filed by index");
  } finally {
    pool.dispose();
  }
}

// ---------------------------------------------------------------------------
// T1 -- ordering: workers finish out of order, results stay in input order.
// ---------------------------------------------------------------------------
async function t1() {
  const N = 2000;
  const size = 4;
  const opts = BREAK === "ordering"
    ? { fault: "clobber", delayMod: 5 }
    : { delayMod: 5 };
  const spawn = realThreadSpawn(f, opts);
  const pool = createWorkerPool(f, { size, spawn });
  try {
    const items = new Array(N);
    for (let i = 0; i < N; i++) items[i] = i * 2; // distinct from the index
    const results = await withTimeout(pool.map(items), 30000, "T1 map");
    for (let i = 0; i < N; i++) {
      if (results[i] !== f(i * 2)) throw fail("T1: results[" + i + "]=" + results[i] + " != f(items[" + i + "])=" + f(i * 2) + " -- out-of-order completion leaked into output order", i);
    }
    log("    T1: " + N + " staggered replies, output order preserved");
  } finally {
    pool.dispose();
  }
}

// ---------------------------------------------------------------------------
// T2 -- saturation/liveness: every worker did >0 jobs, queue drains, active -> 0.
// ---------------------------------------------------------------------------
async function t2() {
  const N = 5000;
  const size = 4;
  const base = realThreadSpawn(f, {});
  const counts = [];
  // Wrap the factory to count replies per worker -- proves none starved without
  // widening the public surface.
  const spawn = function (spec) {
    const t = base(spec);
    const mine = counts.length;
    counts.push(0);
    return {
      send: (buf, transfer) => t.send(buf, transfer),
      onRaw: (fn) => t.onRaw((buf) => { counts[mine]++; fn(buf); }),
      onError: (fn) => t.onError(fn),
      terminate: () => t.terminate(),
    };
  };
  const pool = createWorkerPool(f, { size, spawn });
  try {
    const items = new Array(N);
    for (let i = 0; i < N; i++) items[i] = i;
    const results = await withTimeout(pool.map(items), 30000, "T2 map");
    if (results.length !== N) throw fail("T2: results.length " + results.length + " != " + N);
    if (counts.length !== size) throw fail("T2: spawned " + counts.length + " workers, expected " + size);
    let total = 0;
    for (let i = 0; i < counts.length; i++) {
      if (counts[i] <= 0) throw fail("T2: worker " + i + " did 0 jobs -- starved");
      total += counts[i];
    }
    if (total !== N) throw fail("T2: workers did " + total + " jobs total != " + N);
    const st = pool.stats();
    if (st.active !== 0) throw fail("T2: stats().active " + st.active + " != 0 -- a job is still in flight");
    if (st.queued !== 0) throw fail("T2: stats().queued " + st.queued + " != 0 -- the queue did not drain");
    if (st.done !== N) throw fail("T2: stats().done " + st.done + " != " + N);
    log("    T2: " + size + " workers, jobs-per-worker [" + counts.join(", ") + "], queue drained");
  } finally {
    pool.dispose();
  }
}

// ---------------------------------------------------------------------------
// T3 -- worker-error: a throwing transform must FAIL the batch closed (reject),
// never silently drop the item and never hang. Proven over the real bridge, with
// a bounded wait so a regression that hangs FAILS this tier instead of the suite.
// ---------------------------------------------------------------------------
async function t3() {
  const N = 300;
  const size = 3;
  const THROW_AT = 137;
  // Self-contained: throws on one specific item value, transforms the rest.
  const throwing = (x) => { if (x === 137) throw new Error("lite-worker-pool-torture: boom at 137"); return x * 3 + 1; };
  // The break makes the worker SWALLOW the throw (no error, no reply) so map()
  // would hang -- the bounded wait below must then trip and fail the tier.
  const spawn = realThreadSpawn(throwing, BREAK === "worker-error" ? { swallow: true } : {});
  const pool = createWorkerPool(throwing, { size, spawn });
  const items = new Array(N);
  for (let i = 0; i < N; i++) items[i] = i; // includes THROW_AT
  let outcome = "pending";
  let errMsg = "";
  try {
    await withTimeout(pool.map(items), 8000, "T3 map");
    outcome = "resolved";
  } catch (e) {
    errMsg = e.message || String(e);
    outcome = errMsg.indexOf("timed out") >= 0 ? "hung" : "rejected";
  } finally {
    pool.dispose();
  }
  if (outcome !== "rejected") {
    throw fail("T3: throwing transform did not reject map() (outcome=" + outcome + (errMsg ? ", " + errMsg : "") + ") -- item " + THROW_AT + " was lost or the batch hung");
  }
  log("    T3: transform throw over the real bridge -> map rejected (bounded), no hang");
}

// ---------------------------------------------------------------------------
// T6 -- retention: the per-item dispatch loop is allocation-free.
// ---------------------------------------------------------------------------
async function t6() {
  // Drive the REAL pool dispatch synchronously, one hop per measureOps op, so the
  // gate measures WorkerPool.js's _dispatch/_onResult path -- not a per-map cost.
  // The results array is allocated once by map() BEFORE the measured window, so
  // only the per-hop transient (a view over the returned buffer) can show up, and
  // that is collected -- a retained per-op allocation would break the 8 B gate.
  const OPS = 10000;
  const WARM = 2000;
  const NN = OPS + WARM + 64;
  const drv = syncLoopbackSpawn(f, BREAK === "retention" ? { alloc: true } : {});
  const pool = createWorkerPool(f, { size: 1, spawn: drv.factory, scratchBytes: 16 });
  const items = new Array(NN);
  for (let i = 0; i < NN; i++) items[i] = i;
  pool.map(items).catch(() => {}); // disposed below before it can finish; swallow the reject
  const op = () => { drv.step(); };
  const r = measureOps(op, { ops: OPS, warmup: WARM, stabilize: true });
  const rep = checkOps(r, { maxBytesPerOp: 8, maxMajorsPerKOp: 0 });
  const scratchBytes = drv.lastBuf ? drv.lastBuf.byteLength : -1;
  pool.dispose();

  metrics.bytesPerOp = r.bytesPerOp === null ? 0 : r.bytesPerOp;
  metrics.gcMajor = r.summary.gc.major;
  metrics.gcMinor = r.summary.gc.minor;
  metrics.gcMaxMs = r.summary.gc.maxMs;

  if (rep.verdict !== "pass") {
    throw fail("T6: retention gate " + rep.verdict + " (bytesPerOp=" +
      (r.bytesPerOp === null ? "n/a" : r.bytesPerOp.toFixed(2)) + ")");
  }
  if (scratchBytes !== 16) throw fail("T6: scratch byteLength drifted to " + scratchBytes + " != 16");
  log("    T6: " + (r.bytesPerOp === null ? "n/a" : r.bytesPerOp.toFixed(2)) + " B/op over " + OPS + " dispatch hops, scratch " + scratchBytes + " B, major=" + r.summary.gc.major);

  // Retention across many pool build/tear-down cycles is proven by FINALIZATION,
  // not a counter trick. Each cycle tracks the REAL disposed pool with a shared
  // NOOP cleanup + a numeric tag (neither closes over the pool -- the held-value
  // contract) and does NOT untrack it: a pool that was truly released is collected
  // (size--), one that leaked is not. After the loop we settle HARD and assert the
  // residual tracker.size() <= RES.
  //
  // (The earlier track-then-immediate-untrack asserted size()===0 -- a VACUOUS
  // TAUTOLOGY: untrack decrements the live counter synchronously, netting to 0
  // every cycle even if the pool were retained forever. A retention gate must
  // FAIL on a retained object; TORTURE_BREAK=leak proves this one does.)
  const { tracker, warns } = makeResidualTracker();
  for (let i = 0; i < CYCLES; i++) {
    const d = syncLoopbackSpawn(f, {});
    const p = createWorkerPool(f, { size: 2, spawn: d.factory });
    p.dispose();
    tracker.track(p, NOOP_CLEANUP, i);
    if (LEAK_HOLD) __leakSink.push(p); // pin -> can NEVER finalize -> residual ~CYCLES
  }
  await settleHard();
  const live = tracker.size();
  const findings = tracker.audit();
  metrics.leakSize = live;
  metrics.findings = findings.length;
  metrics.warnings = warns.length;
  if (live > RES) throw fail("T6: finalization residual size()=" + live + " > " + RES + " -- a disposed pool outlived its dispose()");
  if (findings.length !== 0) throw fail("T6: " + findings.length + " leak findings");
  log("    T6: " + CYCLES + " create/dispose cycles, residual size=" + live + "/" + RES + ", findings=0");
}

// ---------------------------------------------------------------------------
// T9 -- controls. Each gate, deliberately broken, MUST be caught. A control that
// cannot fail is decorative.
// ---------------------------------------------------------------------------
async function t9() {
  // (a) conservation: a dispatch that misroutes one job's index drops that
  // output -- the conservation gate (all indices filed with their own value)
  // MUST catch the resulting hole.
  {
    const N = 64;
    const drv = syncLoopbackSpawn(f, { misrouteAt: 3, misrouteTo: 0 });
    const pool = createWorkerPool(f, { size: 2, spawn: drv.factory });
    const items = new Array(N);
    for (let i = 0; i < N; i++) items[i] = i;
    const p = pool.map(items);
    while (drv.step()) { /* drain iteratively */ }
    const results = await p;
    let hole = false;
    for (let i = 0; i < N; i++) { if (results[i] !== f(i)) { hole = true; break; } }
    pool.dispose();
    if (!hole) throw fail("T9(a): misrouting dispatch did not break conservation -- the gate is decorative");
  }

  // (b) ordering: a worker that files results by a per-worker COMPLETION counter
  // instead of by index (the clobber fault), driven through the ACTUAL pool, MUST
  // break the input-order guarantee T1 proves. Real break, real pool -- not a
  // local model.
  {
    const N = 256;
    const size = 4;
    const spawn = syncLoopbackSpawn(f, { clobber: true });
    const pool = createWorkerPool(f, { size, spawn: spawn.factory });
    const items = new Array(N);
    for (let i = 0; i < N; i++) items[i] = i * 2;
    const p = pool.map(items);
    while (spawn.step()) { /* drain iteratively */ }
    const results = await p;
    let broken = false;
    for (let i = 0; i < N; i++) { if (results[i] !== f(i * 2)) { broken = true; break; } }
    pool.dispose();
    if (!broken) throw fail("T9(b): completion-order collector preserved input order through the real pool -- ordering gate decorative");
  }

  // (c) retention: a dispatch that reallocates + retains a buffer per job MUST
  // fail the same maxBytesPerOp:8 gate T6 relies on.
  {
    const OPS = 2000;
    const WARM = 500;
    const NN = OPS + WARM + 64;
    const drv = syncLoopbackSpawn(f, { alloc: true });
    const pool = createWorkerPool(f, { size: 1, spawn: drv.factory });
    const items = new Array(NN);
    for (let i = 0; i < NN; i++) items[i] = i;
    pool.map(items).catch(() => {});
    const r = measureOps(() => { drv.step(); }, { ops: OPS, warmup: WARM, stabilize: true });
    const rep = checkOps(r, { maxBytesPerOp: 8, maxMajorsPerKOp: 0 });
    pool.dispose();
    if (rep.verdict === "pass") throw fail("T9(c): reallocating-scratch dispatch passed the retention gate -- decorative");
  }

  // (d) worker-error: a pool whose worker SWALLOWS a transform throw (no error,
  // no reply) leaves map() unsettled -- proving T3's bounded-reject check is not
  // decorative. Driven through the actual pool; the bounded wait must observe the
  // hang (pending), not a resolve or a reject.
  {
    const throwing = (x) => { if (x === 1) throw new Error("boom"); return x * 3 + 1; };
    const drv = syncLoopbackSpawn(throwing, { swallow: true });
    const pool = createWorkerPool(throwing, { size: 1, spawn: drv.factory });
    const p = pool.map([0, 1, 2]).then(() => "resolved", () => "rejected");
    while (drv.step()) { /* drain: item 1 is swallowed, so map never settles */ }
    const outcome = await Promise.race([
      p,
      new Promise((r) => setTimeout(() => r("pending"), 100)),
    ]);
    pool.dispose(); // now p rejects (disposed mid-batch); already handled, no unhandled rejection
    if (outcome !== "pending") throw fail("T9(d): swallowing pool settled with '" + outcome + "' -- the worker-error bounded-reject check is decorative");
  }

  log("    T9: conservation / ordering / retention / worker-error gates all fail on a break");
}

// ---------------------------------------------------------------------------
// Tier registry.
// ---------------------------------------------------------------------------
const TIERS = [
  { name: "T0", run: t0 },
  { name: "T1", run: t1 },
  { name: "T2", run: t2 },
  { name: "T3", run: t3 },
  { name: "T6", run: t6 },
  { name: "T9", run: t9 },
];

let failed = false;
for (const tier of TIERS) {
  try {
    await tier.run();
    log("  " + tier.name + " ok");
  } catch (e) {
    log("  " + tier.name + " FAIL: " + e.message + (e.op !== undefined ? " (op " + e.op + ")" : ""));
    failed = true;
    break;
  }
}

log(
  "GATE leak=size " + metrics.leakSize + "/" + RES + " findings=" + metrics.findings +
  " warnings=" + metrics.warnings +
  " | gc major=" + metrics.gcMajor + " minor=" + metrics.gcMinor +
  " maxMs=" + metrics.gcMaxMs.toFixed(2) +
  " | alloc=" + metrics.bytesPerOp.toFixed(2) + " B/op"
);

if (failed) {
  process.exitCode = 1;
  process.stderr.write("", () => process.exit(1));
} else {
  process.stdout.write("ok\n", () => process.exit(0));
}
