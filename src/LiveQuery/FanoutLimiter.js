import logger from '../logger';

// Bound active fanout work without evicting queued events.
export default class FanoutLimiter {
  constructor(max = 256) {
    if (!Number.isInteger(max) || max < 1) { throw Error('Invalid fanout concurrency'); }
    this.max = max;
    this.active = 0;
    this.q = [];
    this.head = 0;
    this.maxSeen = 0;
    this.maxAgeSeen = 0;
  }
  get queued() {
    return this.q.length - this.head;
  }
  run(fn) {
    return new Promise(resolve => {
      const item = { fn, resolve, t: Date.now() };
      if (this.active < this.max) { this._start(item); }
      else {
        this.q.push(item);
        this.maxSeen = Math.max(this.maxSeen, this.queued);
      }
    });
  }
  _start(item) {
    this.active++;
    this.maxAgeSeen = Math.max(this.maxAgeSeen, Date.now() - item.t);
    // The existing Parse callbacks report their own errors. The final catch also
    // prevents an unexpected callback failure from stalling the entire queue.
    Promise.resolve()
      .then(item.fn)
      .catch(error => {
        logger.error('LiveQuery fanout callback failed', error);
      })
      .then(() => {
        this.active--;
        item.resolve();
        if (this.queued) {
          const next = this.q[this.head++];
          if (this.head > 1024 && this.head * 2 > this.q.length) {
            this.q = this.q.slice(this.head);
            this.head = 0;
          }
          this._start(next);
        } else {
          this.q = [];
          this.head = 0;
        }
      });
  }
  stats() {
    return {
      active: this.active,
      queued: this.queued,
      maxQueued: this.maxSeen,
      maxAgeMs: this.maxAgeSeen,
      dropped: 0,
      droppedStale: 0,
      max: this.max,
      maxQueue: 0,
      maxAgeLimitMs: 0,
    };
  }
}
