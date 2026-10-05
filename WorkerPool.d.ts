/**
 * @zakkster/lite-worker-pool -- type declarations.
 * @copyright Zahary Shinikchiev
 */

/** Package version (1.1.0). Kept in three-place sync with package.json and CHANGELOG.md. */
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


// ---------------------------------------------------------------------------
// WorkerSet (1.1.0) -- the service-shaped mode (research/worker-set.md)
// ---------------------------------------------------------------------------

/** Worker states. A worker accepts work only while READY. */
export declare const WORKER_STATE: Readonly<{ STARTING: 0; READY: 1; DOWN: 2 }>;
export type WorkerState = 0 | 1 | 2;

/** The `code` carried by every WorkerSet error (semver API; messages are not). */
export type WorkerSetErrorCode =
  | "LWP_INDEX" | "LWP_ARGUMENT" | "LWP_OPTION" | "LWP_TRANSPORT"
  | "LWP_NOT_READY" | "LWP_WORKER_DOWN" | "LWP_QUEUE_FULL"
  | "LWP_TRANSFORM" | "LWP_ABORTED" | "LWP_DISPOSED";

/**
 * A set-mode transport: the map-mode surface plus a typed channel -- `onPost` delivers
 * the worker's READY and transform-error messages, `post` carries control values.
 */
export interface SetWorker extends PoolWorker {
  onPost(handler: (type: string, data: unknown) => void): () => void;
  /** Needed by {@link WorkerSet.control}. */
  post?(type: string, data: unknown): void;
}

/** Describes one set worker the {@link WorkerSetOptions.spawn} factory must build. */
export interface SetSpawnSpec {
  workerFn: (item: number, ctl: Float64Array) => number;
  index: number;
  name: string;
  /** 24: [0] job id, [1] value in / result out, [2] transform-error flag. */
  scratchBytes: number;
  mode: "set";
}

/** Options for {@link createWorkerSet}. An unknown key throws (did-you-mean). */
export interface WorkerSetOptions {
  /** Worker count; same resolution as {@link WorkerPoolOptions.size}. */
  size?: number;
  /** Jobs in flight per worker, integer in [1, 8]. Default 2. */
  slots?: number;
  /** Queued jobs per worker beyond the slots, integer in [0, 65536]. Default 32. Never unbounded. */
  queue?: number;
  /** Set name, used for worker naming. Default `"lite-worker-set"`. */
  name?: string;
  /** Injectable set-mode transport factory (default: a real lite-worker over a Blob URL). */
  spawn?: (spec: SetSpawnSpec) => SetWorker;
  /** Completion callback for {@link WorkerSet.post}. `code` is null on success. */
  onSettle?: (worker: number, tag: number, ok: boolean, value: number, code: WorkerSetErrorCode | null) => void;
  /** Clock for {@link WorkerSet.busySince}. Default `performance.now`. */
  now?: () => number;
}

/** Per-worker snapshot from {@link WorkerSet.stats}. */
export interface WorkerSetWorkerStats {
  state: WorkerState;
  executing: number;
  queued: number;
  done: number;
  failed: number;
}

/** A service-shaped worker set. Create with {@link createWorkerSet}. */
export declare class WorkerSet {
  private constructor();
  readonly size: number;
  readonly slots: number;
  readonly queue: number;

  /**
   * Run `value` on worker `i`. Resolves with the transform's result. Rejects with a coded Error
   * (LWP_INDEX, LWP_ARGUMENT, LWP_OPTION, LWP_NOT_READY, LWP_WORKER_DOWN, LWP_QUEUE_FULL,
   * LWP_TRANSFORM, LWP_ABORTED, LWP_DISPOSED) -- or with the signal's reason when aborted before it
   * ran. Jobs to the same worker settle in submission order.
   */
  submit(i: number, value: number, opts?: { signal?: AbortSignal } | null): Promise<number>;
  /**
   * Zero-allocation dispatch; completion arrives through `onSettle`. Returns false (nothing
   * queued) when disposed, when worker `i` is not READY, or when its queue is full.
   * Throws LWP_INDEX / LWP_ARGUMENT.
   */
  post(i: number, value: number, tag: number): boolean;

  /** The state of worker `i` (throws LWP_INDEX). */
  state(i: number): WorkerState;
  /** True iff worker `i` is READY; never throws. */
  isReady(i: number): boolean;
  /** Jobs executing + queued on worker `i`; never throws (0 for a bad index). */
  load(i: number): number;
  /** `now()` when worker `i`'s oldest in-flight job was sent, NaN when idle; never throws. */
  busySince(i: number): number;

  /** Resolves when every worker is READY; rejects if one goes DOWN first. */
  ready(): Promise<void>;
  /** Terminate worker `i` now: DOWN, its jobs fail LWP_WORKER_DOWN. Idempotent. */
  kill(i: number): void;
  /** Fresh worker in slot `i` (a live one is killed first); resolves when READY. */
  respawn(i: number): Promise<void>;
  /** Copy `values` to worker `i`; its transform receives them as `ctl`. Re-applied after respawn. */
  control(i: number, values: Float64Array): void;
  /** Cold snapshot. Allocates. */
  stats(): { size: number; slots: number; queue: number; workers: WorkerSetWorkerStats[] };
  /** Idempotent: terminate every worker; every pending job fails LWP_DISPOSED. */
  dispose(): void;
}

/**
 * Create a service-shaped worker set (1.1.0): the caller chooses the worker for each job (a load
 * balancer such as `@zakkster/lite-pick`, or any policy). `workerFn(item, ctl)` is serialized ONCE
 * into every worker; `ctl` is that worker's control values (an empty Float64Array until set).
 */
export declare function createWorkerSet(
  workerFn: (item: number, ctl: Float64Array) => number,
  opts?: WorkerSetOptions
): WorkerSet;

export default createWorkerPool;
