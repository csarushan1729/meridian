import assert from 'node:assert/strict';
import { test } from 'node:test';
import { CircuitBreaker } from '../common/breaker.mjs';

test('circuit breaker: closed -> open -> half_open -> closed', () => {
  let t = 0;
  const changes = [];
  const b = new CircuitBreaker({ threshold: 3, cooldownMs: 1000, now: () => t, onChange: (f, to) => changes.push(`${f}>${to}`) });

  assert.equal(b.allow(), true);
  b.fail();
  b.fail();
  assert.equal(b.state, 'closed'); // 2 failures, threshold is 3
  b.success(); // a success resets the count
  b.fail();
  b.fail();
  assert.equal(b.state, 'closed');
  b.fail();
  assert.equal(b.state, 'open');
  assert.equal(b.allow(), false); // fail fast

  t = 999;
  assert.equal(b.allow(), false); // still cooling down
  t = 1000;
  assert.equal(b.allow(), true); // first probe
  assert.equal(b.state, 'half_open');
  assert.equal(b.allow(), false); // only one probe at a time

  b.fail(); // probe failed -> open again
  assert.equal(b.state, 'open');
  t = 2000;
  assert.equal(b.allow(), true);
  b.success(); // probe worked -> closed
  assert.equal(b.state, 'closed');
  assert.deepEqual(changes, ['closed>open', 'open>half_open', 'half_open>open', 'open>half_open', 'half_open>closed']);
});
