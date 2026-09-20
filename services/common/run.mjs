// Close everything cleanly on `docker stop` (SIGTERM) or Ctrl+C.
export function onShutdown(log, ...closers) {
  let done = false;
  const stop = async (signal) => {
    if (done) return;
    done = true;
    log.info('shutting down', { signal });
    for (const close of closers) {
      try {
        await close();
      } catch {
        /* ignore */
      }
    }
    process.exit(0);
  };
  process.on('SIGTERM', () => stop('SIGTERM'));
  process.on('SIGINT', () => stop('SIGINT'));
}
