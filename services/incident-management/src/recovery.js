// Re-run coordinator recovery on an interval (P1-2). A transaction recover()
// deferred because a participant was unreachable is finished on a later pass
// instead of waiting for the next restart. The coordinator itself skips
// transactions run() is still driving and never overlaps two recoveries.
export function startRecoveryLoop({ recover, intervalMs = 15000, logger }) {
  let running = null;
  const tick = async () => {
    try {
      const finished = await recover();
      if (finished.length) logger?.info({ recovered: finished }, '3PC periodic recovery finished transactions');
    } catch (err) {
      logger?.error({ err }, '3PC periodic recovery failed');
    }
  };
  const timer = setInterval(() => {
    if (running) return;
    running = tick().finally(() => {
      running = null;
    });
  }, intervalMs);
  return {
    async stop() {
      clearInterval(timer);
      if (running) await running;
    },
  };
}
