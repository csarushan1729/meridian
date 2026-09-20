export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export function httpError(status, message, extra = {}) {
  const err = new Error(message);
  err.status = status;
  err.extra = extra;
  return err;
}
