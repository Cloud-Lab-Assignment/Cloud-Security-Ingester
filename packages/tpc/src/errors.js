// A message arrived for a transaction in a state that does not allow it.
// `state` is the participant's current state (null when the txId is unknown),
// so a coordinator can tell "already committed" from "already aborted".
export class TpcStateError extends Error {
  constructor(txId, state, operation) {
    super(`tx ${txId}: ${operation} not allowed in state ${state ?? 'UNKNOWN'}`);
    this.name = 'TpcStateError';
    this.txId = txId;
    this.state = state ?? null;
    this.operation = operation;
  }
}

// Thrown by the coordinator's fault hook to simulate a crash after every
// participant acknowledged preCommit. The owning process is expected to exit.
export class CoordinatorCrash extends Error {
  constructor(txId) {
    super(`coordinator crashed after preCommit for tx ${txId} (injected fault)`);
    this.name = 'CoordinatorCrash';
    this.txId = txId;
  }
}

// A conditional store write found a different state than the caller read.
// Stores throw it from put(record, expected) when expected.state (null = "the
// record must not exist") does not match; the participant re-reads and
// re-evaluates. `actual` is the state found, when the store knows it.
export class StoreConflict extends Error {
  constructor(txId, expected, actual) {
    super(`tx ${txId}: store conflict, expected state ${expected ?? 'NONE'}${actual === undefined ? '' : `, found ${actual ?? 'NONE'}`}`);
    this.name = 'StoreConflict';
    this.txId = txId;
    this.expected = expected ?? null;
    if (actual !== undefined) this.actual = actual;
  }
}

// run() gave up waiting for the per-txId lock (R3-2): another operation on the
// same txId (usually recovery) still holds it after lockWaitMs. Nothing was
// written for this call. Callers should treat it as retryable (gRPC
// UNAVAILABLE): the transaction can be retried later.
export class CoordinatorBusy extends Error {
  constructor(txId, waitedMs) {
    super(`tx ${txId}: coordinator busy, gave up waiting ${waitedMs} ms for the transaction lock`);
    this.name = 'CoordinatorBusy';
    this.txId = txId;
    this.waitedMs = waitedMs;
  }
}
