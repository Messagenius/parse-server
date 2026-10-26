import logger from '../logger';

// Callback starts and recipient enumeration spend the same budget. Only an I/O
// turn replenishes it: resolving Promises must never replenish a microtask loop.
export default class FanoutLimiter {
  constructor(max = 256, batchSize = 64) {
    if (!Number.isInteger(max) || max < 1) { throw Error('Invalid fanout concurrency'); }
    if (!Number.isInteger(batchSize) || batchSize < 1) { throw Error('Invalid fanout batch size'); }
    this.max = max;
    this.batchSize = batchSize;
    this.remaining = batchSize;
    this.active = 0;
    this.q = [];
    this.head = 0;
    this.producers = [];
    this.producerHead = 0;
    this.maxSeen = 0;
    this.maxEventsSeen = 0;
    this.maxAgeSeen = 0;
    this.yields = 0;
    this.callbackErrors = 0;
    this.producerErrors = 0;
    this.draining = false;
    this.immediate = null;
    this.idleWaiters = [];
  }
  get queued() {
    return this.q.length - this.head;
  }
  get queuedEvents() {
    return this.producers.length - this.producerHead;
  }
  run(fn) {
    return new Promise(resolve => {
      this.q.push({ fn, resolve, t: Date.now() });
      this._drain();
      this.maxSeen = Math.max(this.maxSeen, this.queued);
    });
  }
  // Each next() must do bounded work, including skipped subscriptions/clients.
  // Producers are consumed in event admission order, never round-robin: a newer
  // save/delete cannot overtake the remaining recipients of an earlier event.
  produce(iterator) {
    return new Promise(resolve => {
      this.producers.push({ iterator, resolve, t: Date.now() });
      this.maxEventsSeen = Math.max(this.maxEventsSeen, this.queuedEvents);
      this._drain();
    });
  }
  waitIdle() {
    if (!this.active && !this.queued && !this.queuedEvents) { return Promise.resolve(); }
    return new Promise(resolve => this.idleWaiters.push(resolve));
  }
  _yield() {
    if (this.immediate !== null) { return; }
    this.immediate = setImmediate(() => {
      this.immediate = null;
      this.remaining = this.batchSize;
      this.yields++;
      this._drain();
    });
  }
  _drain() {
    if (this.draining) { return; }
    this.draining = true;
    try {
      while (this.remaining > 0) {
        if (this.queued) {
          if (this.active >= this.max) { break; }
          this.remaining--;
          const item = this.q[this.head];
          this.q[this.head++] = null;
          this._compact('q', 'head');
          this._start(item);
        } else if (this.queuedEvents) {
          this.remaining--;
          const producer = this.producers[this.producerHead];
          this.maxAgeSeen = Math.max(this.maxAgeSeen, Date.now() - producer.t);
          let done;
          try {
            done = producer.iterator.next().done;
          } catch (error) {
            done = true;
            this.producerErrors++;
            this._report('LiveQuery fanout producer failed', error);
          }
          if (done) {
            this.producers[this.producerHead++] = null;
            this._compact('producers', 'producerHead');
            producer.resolve();
          }
        } else {
          break;
        }
      }
    } finally {
      this.draining = false;
    }
    if (!this.remaining && (this.queued || this.queuedEvents)) { this._yield(); }
    if (!this.active && !this.queued && !this.queuedEvents) {
      const waiters = this.idleWaiters;
      this.idleWaiters = [];
      for (const resolve of waiters) { resolve(); }
    }
  }
  _compact(queue, head) {
    if (this[head] === this[queue].length) {
      this[queue] = [];
      this[head] = 0;
    } else if (this[head] > 1024 && this[head] * 2 > this[queue].length) {
      this[queue] = this[queue].slice(this[head]);
      this[head] = 0;
    }
  }
  _report(message, error) {
    // Even a custom logger throwing must not strand active work or waiters.
    try { logger.error(message, error); } catch { /* Keep draining. */ }
  }
  _start(item) {
    this.active++;
    if (process.env.MSG_DEBUG_LQ_FANAUT === 'true') {
      this._diagStarted = (this._diagStarted || 0) + 1;
      const diagNow = Date.now();
      if (diagNow - (this._diagAt || 0) >= 10000) {
        console.log('[LQ-FANOUT]', JSON.stringify({
          time: new Date(diagNow).toISOString(),
          pid: process.pid,
          startedTotal: this._diagStarted,
          startedSinceLast: this._diagStarted - (this._diagPrevious || 0),
          intervalMs: this._diagAt ? diagNow - this._diagAt : null,
          active: this.active,
          queued: this.queued,
          oldestQueuedMs: this.queued ? diagNow - this.q[this.head].t : 0,
        }));
        this._diagAt = diagNow;
        this._diagPrevious = this._diagStarted;
      }
    }
    this.maxAgeSeen = Math.max(this.maxAgeSeen, Date.now() - item.t);
    const finish = () => {
      this.active--;
      item.resolve();
      this._drain();
    };
    Promise.resolve().then(item.fn).then(finish, error => {
      this.callbackErrors++;
      this._report('LiveQuery fanout callback failed', error);
      finish();
    });
  }
  stats() {
    const now = Date.now();
    const oldestCallback = this.queued ? now - this.q[this.head].t : 0;
    const oldestEvent = this.queuedEvents ? now - this.producers[this.producerHead].t : 0;
    return {
      active: this.active,
      queued: this.queued,
      queuedEvents: this.queuedEvents,
      maxQueuedEvents: this.maxEventsSeen,
      maxQueued: this.maxSeen,
      oldestAgeMs: Math.max(oldestCallback, oldestEvent),
      maxAgeMs: this.maxAgeSeen,
      yields: this.yields,
      callbackErrors: this.callbackErrors,
      producerErrors: this.producerErrors,
      dropped: 0,
      droppedStale: 0,
      max: this.max,
      maxQueue: 0,
      maxAgeLimitMs: 0,
    };
  }
}
