// One JSON object per line. Easy to read, easy to grep by traceId:
//   docker compose logs | findstr 3f9a1c22
export function makeLogger(service) {
  const w = (level, msg, fields) =>
    console.log(JSON.stringify({ t: new Date().toISOString(), level, service, msg, ...fields }));
  return {
    info: (msg, f) => w('info', msg, f),
    warn: (msg, f) => w('warn', msg, f),
    error: (msg, f) => w('error', msg, f),
  };
}
