import http from 'node:http';

/**
 * Tiny JSON HTTP server (no framework).
 * routes: [[method, '/path/:param', async (ctx) => ({ status?, body, headers? })]]
 * ctx: { params, query (URLSearchParams), body, headers, ip }
 */
export function startHttp({ port, routes, log }) {
  const compiled = routes.map(([method, pattern, handler]) => {
    const names = [];
    const re = new RegExp(
      '^' + pattern.replace(/:([a-zA-Z_]+)/g, (_, n) => (names.push(n), '([^/]+)')) + '/?$',
    );
    return { method, re, names, handler };
  });

  const send = (res, status, body, headers = {}) => {
    res.writeHead(status, { 'content-type': 'application/json', 'access-control-allow-origin': '*', ...headers });
    res.end(JSON.stringify(body));
  };

  const server = http.createServer(async (req, res) => {
    try {
      if (req.method === 'OPTIONS') {
        return send(res, 204, {}, {
          'access-control-allow-methods': 'GET,POST,OPTIONS',
          'access-control-allow-headers': 'content-type,idempotency-key',
        });
      }
      const url = new URL(req.url, 'http://local');
      const route = compiled.find((r) => r.method === req.method && r.re.test(url.pathname));
      if (!route) return send(res, 404, { error: 'not_found' });
      const m = url.pathname.match(route.re);
      const params = Object.fromEntries(route.names.map((n, i) => [n, decodeURIComponent(m[i + 1])]));

      let body = {};
      if (req.method === 'POST') {
        const chunks = [];
        let size = 0;
        for await (const c of req) {
          size += c.length;
          if (size > 64 * 1024) return send(res, 413, { error: 'body_too_large' });
          chunks.push(c);
        }
        const text = Buffer.concat(chunks).toString();
        if (text.trim()) {
          try {
            body = JSON.parse(text);
          } catch {
            return send(res, 400, { error: 'invalid_json' });
          }
        }
      }
      const ip = (req.headers['x-forwarded-for'] ?? req.socket.remoteAddress ?? 'unknown').toString().split(',')[0].trim();
      const out = await route.handler({ params, query: url.searchParams, body, headers: req.headers, ip });
      send(res, out.status ?? 200, out.body ?? {}, out.headers);
    } catch (err) {
      const status = err.status ?? 500;
      if (status >= 500) log?.error('http error', { err: err.message, url: req.url });
      send(res, status, { error: err.message, ...(err.extra ?? {}) });
    }
  });
  server.listen(port, () => log?.info('http listening', { port }));
  return server;
}
