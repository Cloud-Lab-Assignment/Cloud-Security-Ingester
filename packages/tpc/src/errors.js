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
