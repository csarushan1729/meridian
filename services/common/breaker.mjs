/**
 * Circuit breaker.
 *   closed    -> normal. Too many failures in a row -> open.
 *   open      -> fail fast (do not even try). After cooldown -> half_open.
 *   half_open -> let ONE request through as a probe. Success -> closed, failure -> open.
 */
export class CircuitBreaker {
  constructor({ threshold = 3, cooldownMs = 10_000, now = () => Date.now(), onChange } = {}) {
    this.threshold = threshold;
    this.cooldownMs = cooldownMs;
    this.now = now;
    this.onChange = onChange;
    this.state = 'closed';
    this.failures = 0;
    this.openedAt = 0;
    this.probing = false;
  }

  #set(state) {
    if (state === this.state) return;
    const from = this.state;
    this.state = state;
    this.onChange?.(from, state);
  }

  allow() {
    if (this.state === 'closed') return true;
    if (this.state === 'open') {
      if (this.now() - this.openedAt < this.cooldownMs) return false;
      this.#set('half_open');
      this.probing = true;
      return true;
    }
    // half_open: only one probe at a time
    if (this.probing) return false;
    this.probing = true;
    return true;
  }

  success() {
    this.failures = 0;
    this.probing = false;
    this.#set('closed');
  }

  fail() {
    this.probing = false;
    if (this.state === 'half_open') {
      this.openedAt = this.now();
      this.#set('open');
      return;
    }
    this.failures += 1;
    if (this.state === 'closed' && this.failures >= this.threshold) {
      this.openedAt = this.now();
      this.#set('open');
    }
  }

  snapshot() {
    return { state: this.state, failures: this.failures, threshold: this.threshold, cooldownMs: this.cooldownMs };
  }
}
