/**
 * @zakkster/lite-worker-pool -- type declarations.
 * @copyright Zahary Shinikchiev
 */

/** Package version (1.0.1). Kept in three-place sync with package.json and CHANGELOG.md. */
export declare const VERSION: string;

/**
 * The minimal raw worker surface the pool drives -- the same shape a lite-worker
 * WorkerHandle exposes and a node:worker_threads adapter can implement. Injected
 * via {@link WorkerPoolOptions.spawn} so the pool can run over any transport.
 */
export interface PoolWorker {
  /**
   * Transfer a filled scratch buffer to the worker. When `transfer` is omitted
   * the buffer should be auto-transferred (detached on this side).
   */
  send(buffer: ArrayBuffer, transfer?: Transferable[]): void;
  /** Subscribe to buffers the worker transfers back. Returns an unsubscribe fn. */
  onRaw(handler: (buffer: ArrayBuffer) => void): () => void;
  /**
   * Subscribe to worker failures: an uncaught transform throw or a worker
   * crash/death. Returns an unsubscribe fn. On the first error during a batch the
   * pool fails closed -- it rejects `map()` with the error rather than hanging.
   * Required for fail-closed behavior; an injected factory that omits it cannot
   * surface a worker death and risks a hung `map()`.
   */
  onError(handler: (error: Error) => void): () => void;
  /** Stop the worker. Called once per worker by {@link Pool.dispose}. */
  terminate(): void;
}

/** Describes one worker the {@link WorkerPoolOptions.spawn} factory must build. */
export interface SpawnSpec {
  /** The per-item transform passed to {@link createWorkerPool}. */
  workerFn: (item: number) => number;
  /** Zero-based worker index within the pool. */
  index: number;
  /** The pool name (for devtools / worker naming). */
  name: string;
  /** Scratch buffer byte length each worker ping-pongs. */
  scratchBytes: number;
}

/** Options for {@link createWorkerPool}. */
export interface WorkerPoolOptions {
  /**
   * Worker count. Fail-closed: a missing/NaN/negative/zero value floors to the
   * host's logical core count (`navigator.hardwareConcurrency`), then to 1 --
   * never 0.
   */
  size?: number;
  /** Scratch buffer byte length per worker. Floors to 16 (two Float64 slots). */
  scratchBytes?: number;
  /** Pool name, used for worker naming. Default `"lite-worker-pool"`. */
  name?: string;
  /**
   * Injectable worker factory. Returns a {@link PoolWorker} for each of `size`
   * workers. Defaults to a real lite-worker over a Blob URL; inject this to run
   * the pool over a node:worker_threads bridge (how the torture suite proves it
   * without a browser).
   */
  spawn?: (spec: SpawnSpec) => PoolWorker;
}

/** Snapshot of pool progress. Cold -- allocates a small object per call. */
export interface PoolStats {
  /** Number of workers. */
  size: number;
  /** Items not yet assigned to a worker (0 when idle). */
  queued: number;
  /** Jobs currently in flight. */
  active: number;
  /** Replies filed in the current/last batch. */
  done: number;
}

/** A data-parallel worker pool. Create with {@link createWorkerPool}. */
export declare class Pool {
  private constructor();

  /** Number of workers. */
  readonly size: number;

  /**
   * Dispatch every item across the pool and resolve with an array the length of
   * `items`, filled BY INDEX so output order === input order regardless of the
   * order workers finish. A job queue keeps every worker fed until the work
   * drains. One batch at a time: a second concurrent call rejects.
   *
   * Fails closed rather than hanging or losing an item: rejects for
   * null/undefined/non-array-like input, for a disposed or poisoned pool, and --
   * if a worker's transform throws or a worker dies mid-batch -- with the
   * underlying error (a thrown transform is deterministic, so the item is not
   * reassigned; the batch is rejected and the pool is poisoned until dispose).
   *
   * `map()` allocates the results array + one Promise per batch (cold); the loop
   * that feeds items to workers allocates nothing.
   */
  map(items: ArrayLike<number>): Promise<number[]>;

  /** Progress snapshot: `{ size, queued, active, done }`. */
  stats(): PoolStats;

  /**
   * Idempotent full teardown: terminate every worker, drop handlers, and reject
   * an in-flight batch. A second call is a no-op and never throws.
   */
  dispose(): void;
}

/**
 * Create a data-parallel worker pool. `workerFn` is the per-item transform,
 * `(item) => result`, serialized ONCE into every worker -- it cannot close over
 * anything (it crosses a thread boundary). There is no per-map worker function.
 *
 * @param workerFn self-contained per-item transform
 * @param opts pool options
 */
export declare function createWorkerPool(
  workerFn: (item: number) => number,
  opts?: WorkerPoolOptions
): Pool;

export default createWorkerPool;
