// bench/gc-gate.mjs -- node --expose-gc bench/gc-gate.mjs
//
// Release retention gate for @zakkster/lite-worker-pool. Measures the per-item
// dispatch loop with @zakkster/lite-gc-profiler over a synchronous loopback
// worker (one hop per op, so measureOps wraps the real WorkerPool.js dispatch
// path with no per-map cost) and asserts < 8 B/op and 0 majors/kOp. Prints a
// GATE line and exits nonzero on a violation. The exhaustive proof (conservation,
// ordering, saturation over real threads, plus the deliberately-broken controls)
// is test/torture.mjs; this is the fast release check.

import { measureOps, checkOps } from "@zakkster/lite-gc-profiler";
import { createWorkerPool } from "../WorkerPool.js";
import { syncLoopbackSpawn } from "../test/torture/harness.mjs";

const f = (x) => x * 3 + 1;
const OPS = 20000;
const WARM = 4000;
const NN = OPS + WARM + 64;

const drv = syncLoopbackSpawn(f, {});
const pool = createWorkerPool(f, { size: 1, spawn: drv.factory, scratchBytes: 16 });
const items = new Array(NN);
for (let i = 0; i < NN; i++) items[i] = i;
pool.map(items).catch(() => {});

const r = measureOps(() => { drv.step(); }, { ops: OPS, warmup: WARM, stabilize: true });
const rep = checkOps(r, { maxBytesPerOp: 8, maxMajorsPerKOp: 0 });
const scratchBytes = drv.lastBuf ? drv.lastBuf.byteLength : -1;
pool.dispose();

const bpo = r.bytesPerOp === null ? 0 : r.bytesPerOp;
process.stderr.write(
  "GATE alloc=" + bpo.toFixed(2) + " B/op | gc major=" + r.summary.gc.major +
  " minor=" + r.summary.gc.minor + " | scratch=" + scratchBytes + " B | verdict=" + rep.verdict + "\n"
);

let ok = rep.verdict === "pass" && scratchBytes === 16;
if (!ok) {
  for (const v of rep.violations || []) {
    process.stderr.write("  violation " + v.metric + " limit=" + v.limit + " actual=" + v.actual + "\n");
  }
  if (scratchBytes !== 16) process.stderr.write("  scratch byteLength drifted to " + scratchBytes + "\n");
}

if (ok) process.stdout.write("ok\n", () => process.exit(0));
else { process.exitCode = 1; process.stderr.write("", () => process.exit(1)); }
