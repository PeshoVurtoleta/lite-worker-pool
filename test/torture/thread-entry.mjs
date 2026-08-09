// test/torture/thread-entry.mjs -- real node:worker_threads entry for the pool.
//
// Receives a worker body source string (built by the torture from the per-item
// transform, exactly as WorkerPool's default factory builds it, optionally with
// an out-of-order delay or a fault injection) and evaluates it with a minimal
// `ctx` wired over parentPort. The ctx exposes the same raw surface the pool
// depends on -- onRaw(fn), send(buf, transfer), and post(type, data) for the
// reserved "lwp:err" error channel -- transferring the scratch ArrayBuffer each
// hop, so the ACTUAL per-item ping-pong runs across a second OS thread with real
// transfer semantics, not an in-process mock.

import { parentPort, workerData } from "node:worker_threads";

const handlers = new Set();

const ctx = {
  onRaw(fn) { handlers.add(fn); return () => handlers.delete(fn); },
  send(buf, transfer) {
    const ab = buf instanceof ArrayBuffer ? buf : (ArrayBuffer.isView(buf) ? buf.buffer : null);
    parentPort.postMessage(buf, transfer || (ab ? [ab] : []));
  },
  // Typed channel back to the main side. A caught transform throw rides
  // "lwp:err" here; the pool's transport routes it to onError so map() fails the
  // batch closed rather than hanging.
  post(type, data) { parentPort.postMessage({ t: type, d: data === undefined ? null : data }); },
};

parentPort.on("message", (buf) => { handlers.forEach((fn) => fn(buf)); });

// eslint-disable-next-line no-new-func
new Function("ctx", workerData.body)(ctx);
