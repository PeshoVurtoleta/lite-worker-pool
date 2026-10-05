// test/torture-set.mjs -- node --expose-gc test/torture-set.mjs
//
// The createWorkerSet torture suite (1.1.0, research/worker-set.md section 6). Same
// contract as test/torture.mjs: prints exactly "ok" to stdout on success (exit 0), a
// diagnostic + exit 1 on any failure; progress and the GATE line go to stderr.
//
// Tiers:
//   TS0 conservation -- 40000 jobs over 8 REAL worker_threads (closed loop, <= 8 per worker), worker 3
//       killed mid-stream and respawned: every job settles EXACTLY once; a job on any other worker
//       resolves with f(x); a job on worker 3 resolves with f(x) or fails LWP_WORKER_DOWN, nothing
//       else; per-worker settle order == submission order (FIFO); the respawned worker serves again.
//   TS1 transform throw -- over real threads, a throwing item fails ONLY that job (LWP_TRANSFORM, the
//       worker's message); every other job resolves; every worker is still READY.
//   TS2 crash + respawn -- a thread that exits mid-stream (process.exit) goes DOWN through the exit
//       event: its jobs fail LWP_WORKER_DOWN, the other workers finish; then 50 kill/respawn cycles,
//       each READY and serving.
//   TS3 hung worker -- busySince(i) exposes a job stuck in the thread; kill(i) fails it, respawn(i)
//       serves again (the supervisor's recipe).
//   TS4 control values -- reach the real thread's transform as `ctl`, and survive respawn.
//   TS5 gc + retention -- the set's own post/onSettle hop allocates < 8 B/op over the loopback
//       (measureOps); 1000 respawn cycles and CYCLES create/dispose cycles leave a finalization
//       residual <= RES (the T6 pattern).
//   TS9 controls -- each gate, deliberately broken, MUST be caught: a dropped reply (TS0 times out),
//       replies out of order (TS0 sees failures on a healthy worker), a pinned set (TS5 residual).
//
// TORTURE_BREAK=set-drop|set-reorder|set-leak injects the matching break into the REAL tier.

import { measureOps, checkOps } from "@zakkster/lite-gc-profiler";
import { createWorkerSet, WORKER_STATE } from "../WorkerPool.js";
import {
  realThreadSetSpawn,
  setLoopbackSpawn,
  waitFor,
  makeResidualTracker,
  NOOP_CLEANUP,
} from "./torture/harness.mjs";

const log = (s) => process.stderr.write(s + "\n");
const BREAK = process.env.TORTURE_BREAK || "";
const CYCLES = 1024;
const RES = Math.max(16, (CYCLES / 1000) | 0);
const __leakSink = [];

// Serialized into real threads: self-contained, exact in Float64 for every id used here.
const fs = (x, ctl) => (ctl.length > 0 ? x * ctl[0] : x * 3 + 1);
const expect = (x) => x * 3 + 1;

const metrics = { bytesPerOp: 0, gcMajor: 0, leakSize: 0, findings: 0, jobs: 0 };

function fail(msg) { return new Error(msg); }

function withTimeout(promise, ms, label) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(fail(label + ": timed out after " + ms + "ms")), ms);
    promise.then((v) => { clearTimeout(t); resolve(v); }, (e) => { clearTimeout(t); reject(e); });
  });
}

async function settleHard() {
  for (let i = 0; i < 10; i++) {
    globalThis.gc?.();
    await new Promise((r) => setTimeout(r, 15));
  }
}

// ---------------------------------------------------------------------------
// TS0 -- conservation, isolation, FIFO and respawn over real threads.
// ---------------------------------------------------------------------------
async function stream(o) {
  const TOTAL = 40000, W = 8, TARGET = 8, KILL = 3, KILL_AT = 10000, RESPAWN_AT = 12000;
  const spawn = realThreadSetSpawn(fs, { fault: o.fault, at: o.at });
  const set = createWorkerSet(fs, { size: W, slots: 2, queue: 32, spawn });
  try {
    await withTimeout(set.ready(), 15000, "TS0 ready");
    const count = new Uint8Array(TOTAL);
    const worker = new Int8Array(TOTAL).fill(-1);
    const val = new Float64Array(TOTAL).fill(NaN);
    const code = new Array(TOTAL).fill(null);
    const last = new Int32Array(W).fill(-1);
    let fifoBreaks = 0, issued = 0, settled = 0, killed = false, respawning = false, servedAfter = 0;
    let respawnedFrom = -1;
    let resolveAll;
    const all = new Promise((r) => { resolveAll = r; });

    const feed = (i) => {
      while (issued < TOTAL && set.isReady(i) && set.load(i) < TARGET) {
        const id = issued++;
        worker[id] = i;
        set.submit(i, id).then(
          (v) => { val[id] = v; done(id, i); },
          (e) => { code[id] = e.code || String(e); done(id, i); },
        );
      }
    };
    const done = (id, i) => {
      count[id]++;
      if (id < last[i]) fifoBreaks++;
      last[i] = id;
      settled++;
      if (respawnedFrom >= 0 && i === KILL && id >= respawnedFrom && code[id] === null) servedAfter++;
      if (!killed && settled >= KILL_AT) { killed = true; set.kill(KILL); }
      if (killed && !respawning && settled >= RESPAWN_AT) {
        respawning = true;
        set.respawn(KILL).then(() => { respawnedFrom = issued; feed(KILL); });
      }
      feed(i);
      if (settled === TOTAL) resolveAll();
    };
    for (let i = 0; i < W; i++) feed(i);
    await withTimeout(all, o.timeoutMs || 60000, "TS0 stream");

    let wrong = 0, lost = 0, dupes = 0, unexpected = 0, killedFails = 0;
    for (let id = 0; id < TOTAL; id++) {
      if (count[id] === 0) lost++;
      if (count[id] > 1) dupes++;
      if (code[id] === null) { if (val[id] !== expect(id)) wrong++; continue; }
      if (worker[id] === KILL && code[id] === "LWP_WORKER_DOWN") { killedFails++; continue; }
      unexpected++;
    }
    return { lost, dupes, wrong, unexpected, killedFails, fifoBreaks, servedAfter, TOTAL };
  } finally {
    set.dispose();
  }
}

function checkStream(r) {
  if (r.lost) return "lost " + r.lost + " jobs";
  if (r.dupes) return r.dupes + " jobs settled twice";
  if (r.wrong) return r.wrong + " wrong results";
  if (r.unexpected) return r.unexpected + " failures outside the killed worker";
  if (r.fifoBreaks) return r.fifoBreaks + " per-worker FIFO breaks";
  if (r.killedFails === 0) return "the kill failed no job (the kill did not land mid-stream)";
  if (r.servedAfter === 0) return "the respawned worker served nothing";
  return null;
}

async function ts0() {
  const o = BREAK === "set-drop" ? { fault: "drop", at: 777 } : BREAK === "set-reorder" ? { fault: "reorder" } : {};
  const r = await stream(o);
  const bad = checkStream(r);
  if (bad) throw fail("TS0: " + bad);
  metrics.jobs = r.TOTAL;
  log("    TS0: " + r.TOTAL + " jobs over 8 threads, each settled once; worker 3 killed (" + r.killedFails +
    " of its jobs failed LWP_WORKER_DOWN, no other failures), respawned and served " + r.servedAfter + "; FIFO held");
}

// ---------------------------------------------------------------------------
// TS1 -- a transform throw fails one job, over real threads.
// ---------------------------------------------------------------------------
async function ts1() {
  const thrower = (x) => { if (x % 97 === 96) throw new Error("bad item " + x); return x * 3 + 1; };
  const set = createWorkerSet(thrower, { size: 4, slots: 2, queue: 64, spawn: realThreadSetSpawn(thrower) });
  try {
    await withTimeout(set.ready(), 15000, "TS1 ready");
    const N = 4000;
    const out = await withTimeout(Promise.all(Array.from({ length: N }, (_, id) =>
      new Promise((res) => {
        const go = () => set.submit(id % 4, id).then((v) => res(["ok", v]), (e) => (e.code === "LWP_QUEUE_FULL" ? setImmediate(go) : res([e.code, e.message])));
        go();
      }))), 30000, "TS1");
    for (let id = 0; id < N; id++) {
      const [c, v] = out[id];
      if (id % 97 === 96) {
        if (c !== "LWP_TRANSFORM" || !String(v).includes("bad item " + id)) throw fail("TS1: item " + id + " should fail LWP_TRANSFORM, got " + c);
      } else if (c !== "ok" || v !== expect(id)) throw fail("TS1: item " + id + " got " + c + " " + v);
    }
    for (let i = 0; i < 4; i++) if (set.state(i) !== WORKER_STATE.READY) throw fail("TS1: worker " + i + " not READY after transform throws");
    log("    TS1: " + N + " jobs, " + Math.floor(N / 97) + " transform throws failed only themselves; all workers READY");
  } finally {
    set.dispose();
  }
}

// ---------------------------------------------------------------------------
// TS2 -- a thread crash, then repeated kill/respawn.
// ---------------------------------------------------------------------------
async function ts2() {
  const POISON = 1501;
  const set = createWorkerSet(fs, { size: 4, slots: 2, queue: 64, spawn: realThreadSetSpawn(fs, { fault: "crash", at: POISON }) });
  try {
    await withTimeout(set.ready(), 15000, "TS2 ready");
    const N = 3000;
    const results = await withTimeout(Promise.all(Array.from({ length: N }, (_, id) =>
      new Promise((res) => {
        const i = id % 4;
        const go = () => set.submit(i, id).then((v) => res([i, "ok", v]), (e) => (e.code === "LWP_QUEUE_FULL" ? setImmediate(go) : res([i, e.code])));
        go();
      }))), 30000, "TS2 stream");
    const victim = POISON % 4;
    if (set.state(victim) !== WORKER_STATE.DOWN) throw fail("TS2: the crashed worker is not DOWN");
    for (let id = 0; id < N; id++) {
      const [i, c, v] = results[id];
      if (i === victim) { if (c !== "ok" && c !== "LWP_WORKER_DOWN") throw fail("TS2: victim job " + id + " -> " + c); continue; }
      if (c !== "ok" || v !== expect(id)) throw fail("TS2: healthy worker " + i + " job " + id + " -> " + c);
    }
    for (let k = 0; k < 50; k++) {
      await withTimeout(set.respawn(victim), 10000, "TS2 respawn " + k);
      const v = await set.submit(victim, k);
      if (v !== expect(k)) throw fail("TS2: respawned worker returned " + v);
      if (k % 2 === 0) set.kill(victim);
    }
    log("    TS2: a thread exit took only its worker DOWN (the other 3 finished " + N + " jobs); 50 kill/respawn cycles served");
  } finally {
    set.dispose();
  }
}

// ---------------------------------------------------------------------------
// TS3 -- a hung job is visible through busySince; kill + respawn recovers.
// ---------------------------------------------------------------------------
async function ts3() {
  const set = createWorkerSet(fs, { size: 1, slots: 1, queue: 4, spawn: realThreadSetSpawn(fs, { fault: "hang", at: 7, ms: 3000 }) });
  try {
    await withTimeout(set.ready(), 15000, "TS3 ready");
    const hung = set.submit(0, 7).then(() => "resolved", (e) => e.code);
    await waitFor(() => !Number.isNaN(set.busySince(0)) && performance.now() - set.busySince(0) > 150, 2000, "TS3 busySince");
    set.kill(0);
    if ((await hung) !== "LWP_WORKER_DOWN") throw fail("TS3: the hung job did not fail LWP_WORKER_DOWN");
    await withTimeout(set.respawn(0), 10000, "TS3 respawn");
    const v = await withTimeout(set.submit(0, 8), 5000, "TS3 after respawn");
    if (v !== expect(8)) throw fail("TS3: respawned worker returned " + v);
    log("    TS3: a 3 s hang was visible via busySince within 150 ms; kill failed it, respawn served");
  } finally {
    set.dispose();
  }
}

// ---------------------------------------------------------------------------
// TS4 -- control values over a real thread.
// ---------------------------------------------------------------------------
async function ts4() {
  const set = createWorkerSet(fs, { size: 2, spawn: realThreadSetSpawn(fs) });
  try {
    await withTimeout(set.ready(), 15000, "TS4 ready");
    set.control(1, new Float64Array([7]));
    const a = await set.submit(1, 6);
    const b = await set.submit(0, 6);
    if (a !== 42 || b !== expect(6)) throw fail("TS4: control not applied per worker (" + a + ", " + b + ")");
    await set.respawn(1);
    const c = await set.submit(1, 6);
    if (c !== 42) throw fail("TS4: control lost on respawn (" + c + ")");
    log("    TS4: control values reach only their worker's transform and survive respawn");
  } finally {
    set.dispose();
  }
}

// ---------------------------------------------------------------------------
// TS5 -- the set's own hop allocates nothing; nothing outlives dispose().
// ---------------------------------------------------------------------------
async function ts5() {
  const drv = setLoopbackSpawn(fs);
  let sink = 0;
  const set = createWorkerSet(fs, { size: 1, slots: 2, queue: 4, spawn: drv.factory, onSettle: (i, tag, ok, v) => { sink += v; } });
  await set.ready();
  const op = () => { set.post(0, 5, 1); drv.step(); };
  const r = measureOps(op, { ops: 10000, warmup: 2000, stabilize: true });
  const rep = checkOps(r, { maxBytesPerOp: 8, maxMajorsPerKOp: 0 });
  metrics.bytesPerOp = r.bytesPerOp === null ? 0 : r.bytesPerOp;
  metrics.gcMajor = r.summary.gc.major;
  if (rep.verdict !== "pass") throw fail("TS5: post/onSettle hop " + rep.verdict + " (" + metrics.bytesPerOp.toFixed(2) + " B/op)");
  if (sink !== 16 * 12000) throw fail("TS5: hop results wrong (" + sink + ")");
  for (let k = 0; k < 1000; k++) {
    await set.respawn(0);
    set.post(0, k, k);
    drv.drain();
  }
  set.dispose();

  const { tracker, warns } = makeResidualTracker();
  for (let k = 0; k < CYCLES; k++) {
    const d = setLoopbackSpawn(fs);
    const s = createWorkerSet(fs, { size: 2, spawn: d.factory });
    await s.ready();
    s.post(0, k, k);
    d.drain();
    s.dispose();
    tracker.track(s, NOOP_CLEANUP, k);
    if (BREAK === "set-leak") __leakSink.push(s);
  }
  await settleHard();
  const live = tracker.size();
  const findings = tracker.audit();
  metrics.leakSize = live;
  metrics.findings = findings.length;
  if (live > RES) throw fail("TS5: finalization residual size()=" + live + " > " + RES + " -- a disposed set outlived dispose()");
  if (findings.length) throw fail("TS5: " + findings.length + " leak findings (warnings " + warns.length + ")");
  log("    TS5: post/onSettle hop " + metrics.bytesPerOp.toFixed(2) + " B/op (major " + metrics.gcMajor + "); 1000 respawns; " +
    CYCLES + " create/dispose cycles, residual " + live + "/" + RES);
}

// ---------------------------------------------------------------------------
// TS9 -- the gates have teeth.
// ---------------------------------------------------------------------------
// A break is caught when the TS0 run either cannot finish (timeout) or finishes with a check failure.
// A dropped reply is NOT a silent hang: the next reply on that worker arrives out of FIFO order, the set
// takes the worker DOWN (fail closed) and its jobs fail -- failures on a worker nobody killed.
async function caughtBy(o) {
  try { return checkStream(await stream({ ...o, timeoutMs: 15000 })); } catch (e) { return e.message; }
}
async function ts9() {
  const drop = await caughtBy({ fault: "drop", at: 777 });
  const reorder = await caughtBy({ fault: "reorder" });
  if (drop === null || reorder === null) {
    throw fail("TS9: a TS0 break went unnoticed (drop: " + drop + "; reorder: " + reorder + ")");
  }
  log("    TS9: drop -> " + drop + "; reorder -> " + reorder + " (set-leak: TORTURE_BREAK=set-leak)");
}

const TIERS = [
  { name: "TS0", run: ts0 }, { name: "TS1", run: ts1 }, { name: "TS2", run: ts2 },
  { name: "TS3", run: ts3 }, { name: "TS4", run: ts4 }, { name: "TS5", run: ts5 }, { name: "TS9", run: ts9 },
];

let failed = false;
for (const tier of TIERS) {
  try {
    await tier.run();
    log("  " + tier.name + " ok");
  } catch (e) {
    log("  " + tier.name + " FAIL: " + e.message);
    failed = true;
    break;
  }
}

log("GATE set jobs=" + metrics.jobs + " | alloc=" + metrics.bytesPerOp.toFixed(2) + " B/op gc major=" + metrics.gcMajor +
  " | leak=size " + metrics.leakSize + "/" + RES + " findings=" + metrics.findings);

if (failed) {
  process.exitCode = 1;
  process.stderr.write("", () => process.exit(1));
} else {
  process.stdout.write("ok\n", () => process.exit(0));
}
