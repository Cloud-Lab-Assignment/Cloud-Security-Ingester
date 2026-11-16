import { CoordinatorCrash, TpcStateError } from './errors.js';

export const WAITING = 'WAITING';

const TIMEOUT = Symbol('timeout');

function withTimeout(promise, ms) {
  let timer;
  const timeout = new Promise((resolve) => {
    timer = setTimeout(() => resolve(TIMEOUT), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Three-phase commit coordinator. `log` is durable; `participants` are
// adapters ({name, canCommit, preCommit, doCommit, abort}); `payloads` maps
// participant name -> that participant's payload.
//
// Log contract: put(entry), listUnfinished() -> WAITING/PRECOMMITTED entries,
// get(txId) -> entry | null. Entries are {txId, state, payloads, updatedAt, reason?}.
export function createCoordinator({
  log,
  participants,
  timeoutMs,
  now = Date.now,
  fault,
  onCommitted,
  logger,
  retries = 5,
  retryDelayMs = 200,
}) {
  const write = (txId, state, payloads, extra = {}) => log.put({ txId, state, payloads, updatedAt: now(), ...extra });

  // Send abort/doCommit until acked, up to `retries` attempts. Returns the
  // names of participants that never acked. `ackStates` lists TpcStateError
  // states that already mean "done" (e.g. abort on an ABORTED record).
  async function sendWithRetry(txId, phase, ackStates = []) {
    const unacked = [];
    await Promise.all(
      participants.map(async (p) => {
        for (let attempt = 1; attempt <= retries; attempt += 1) {
          try {
            await p[phase](txId);
            return;
          } catch (err) {
            if (err instanceof TpcStateError && ackStates.includes(err.state)) return;
            if (err instanceof TpcStateError) {
              logger?.error({ txId, participant: p.name, phase, state: err.state }, '3pc participant in conflicting state');
              unacked.push(p.name);
              return;
            }
            logger?.warn({ txId, participant: p.name, phase, attempt, err: err.message }, '3pc send failed, retrying');
            if (attempt < retries) await sleep(retryDelayMs * attempt);
          }
        }
        unacked.push(p.name);
      }),
    );
    if (unacked.length) logger?.error({ txId, phase, unacked }, '3pc participants did not ack');
    return unacked;
  }

  async function abortAll(txId, payloads, reason) {
    await write(txId, 'ABORTED', payloads, { reason });
    await sendWithRetry(txId, 'abort');
    logger?.info({ txId, reason }, '3pc aborted');
    return { outcome: 'aborted', reason };
  }

  // The decision is commit. If any participant never acked doCommit, the log
  // entry stays PRECOMMITTED and onCommitted waits: recover() replays it later
  // (the participant's own PRECOMMITTED timeout also commits it meanwhile).
  async function commitAll(txId, payloads) {
    const pending = await sendWithRetry(txId, 'doCommit');
    if (pending.length) {
      logger?.warn({ txId, pending }, '3pc commit decided but not acknowledged; recovery will finish it');
      return { outcome: 'committed', pending };
    }
    await write(txId, 'COMMITTED', payloads);
    logger?.info({ txId }, '3pc committed');
    if (onCommitted) await onCommitted(txId, payloads);
    return { outcome: 'committed' };
  }

  // Per-txId mutex (R2-1): run() holds it for its whole duration and
  // recovery while it handles that txId, so recovery never acts on a log
  // entry that run() is changing underneath it.
  const locks = new Map();
  function withLock(txId, fn) {
    const prev = locks.get(txId) ?? Promise.resolve();
    const result = prev.then(fn, fn);
    const tail = result.catch(() => {});
    locks.set(txId, tail);
    tail.then(() => {
      if (locks.get(txId) === tail) locks.delete(txId);
    });
    return result;
  }

  // txIds a run() in this process is driving right now. recover() skips them
  // instead of queueing on their lock, so one slow transaction does not hold
  // up a recovery pass (the lock alone would also be correct, just slower).
  const inFlight = new Set();

  async function run(txId, payloads) {
    inFlight.add(txId);
    try {
      return await withLock(txId, () => drive(txId, payloads));
    } finally {
      inFlight.delete(txId);
    }
  }

  async function drive(txId, payloads) {
    await write(txId, WAITING, payloads);

    // Phase 1: canCommit, each vote raced against the timeout.
    const votes = await Promise.all(
      participants.map((p) =>
        withTimeout(
          Promise.resolve()
            .then(() => p.canCommit(txId, payloads[p.name]))
            .catch((err) => {
              logger?.warn({ txId, participant: p.name, err: err.message }, '3pc canCommit failed');
              return 'no';
            }),
          timeoutMs,
        ),
      ),
    );
    if (votes.includes(TIMEOUT)) return abortAll(txId, payloads, 'vote_timeout');
    if (votes.some((v) => v !== 'yes')) return abortAll(txId, payloads, 'vote_no');

    // Phase 2: preCommit.
    await write(txId, 'PRECOMMITTED', payloads);
    const acks = await Promise.all(
      participants.map((p) =>
        withTimeout(
          Promise.resolve()
            .then(() => p.preCommit(txId))
            .then(() => true)
            .catch((err) => {
              logger?.warn({ txId, participant: p.name, err: err.message }, '3pc preCommit failed');
              return false;
            }),
          timeoutMs,
        ),
      ),
    );
    if (acks.some((a) => a !== true)) return abortAll(txId, payloads, 'precommit_failed');

    if (fault === 'crash-after-precommit') throw new CoordinatorCrash(txId);

    // Phase 3: doCommit.
    return commitAll(txId, payloads);
  }

  // Finish every transaction the log left unfinished (after a crash/restart,
  // or a commit whose doCommit was never acked), except those run() is still
  // driving. Safe to call periodically: a call made while another recovery is
  // running does not start a second pass, it joins the running one and
  // resolves with the same list of finished txIds. (The per-txId lock and
  // re-read already make overlapping passes safe; joining just avoids
  // replaying the same transactions twice.)
  let recovering = null;
  function recover() {
    if (!recovering) {
      recovering = recoverOnce().finally(() => {
        recovering = null;
      });
    }
    return recovering;
  }

  async function recoverOnce() {
    const finished = [];
    // Only a list of candidates: each entry is re-read under its lock before
    // anything is done, because the snapshot can be stale by then (R2-1).
    const entries = await log.listUnfinished();
    for (const { txId } of entries) {
      if (inFlight.has(txId)) continue;
      if (await withLock(txId, () => recoverTx(txId))) finished.push(txId);
    }
    return finished;
  }

  // Recover one transaction from its current log entry. Returns true when it
  // reached a final state here.
  async function recoverTx(txId) {
    const entry = await log.get(txId);
    if (!entry) return false;
    const { state, payloads } = entry;
    // Finished meanwhile (e.g. by a run() that completed after the snapshot).
    if (state !== WAITING && state !== 'PRECOMMITTED') return false;
    if (state === WAITING) {
      // No commit decision was ever logged: abort is safe.
      await abortAll(txId, payloads, 'recovered_waiting');
      return true;
    }
    // PRECOMMITTED: replay preCommit. A participant that already committed
    // counts as an ack; one that already aborted (it never got preCommit and
    // timed out in UNCERTAIN) forces a global abort, the one case where the
    // log goes PRECOMMITTED -> ABORTED, as in run()'s precommit_failed.
    let selfAborted = false;
    let failed = false;
    for (const p of participants) {
      try {
        await withTimeout(Promise.resolve().then(() => p.preCommit(txId)), timeoutMs).then((r) => {
          if (r === TIMEOUT) throw new Error('preCommit timeout');
        });
      } catch (err) {
        if (err instanceof TpcStateError && err.state === 'COMMITTED') continue;
        if (err instanceof TpcStateError) selfAborted = true;
        else failed = true;
        logger?.warn({ txId, participant: p.name, err: err.message, state: err.state }, '3pc recovery preCommit failed');
      }
    }
    if (selfAborted) {
      await abortAll(txId, payloads, 'recovered_participant_aborted');
      return true;
    }
    if (failed) {
      // Leave the entry PRECOMMITTED: a later recover() retries, and the
      // participants' own PRECOMMITTED timeouts commit in the meantime.
      logger?.warn({ txId }, '3pc recovery deferred: participant unreachable');
      return false;
    }
    const res = await commitAll(txId, payloads);
    return !res.pending; // still unacked: stays PRECOMMITTED for the next recover()
  }

  return { run, recover, inFlight: () => [...inFlight] };
}
