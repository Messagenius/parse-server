'use strict';
// Independent open-loop producer + HTTP client; real LiveQuery/Client code, no database.
// node --expose-gc http-fairness.cjs --root <built parse-server> --rate 10 --seconds 6 --clients 4000 [--profile]
const {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} = require('node:worker_threads');
const { performance } = require('node:perf_hooks');
const http = require('node:http');
const path = require('node:path');
const fs = require('node:fs');
const assert = require('node:assert/strict');
const percentile = (a, p) =>
  a.length
    ? a.slice().sort((a, b) => a - b)[
        Math.min(a.length - 1, Math.floor(a.length * p))
      ]
    : null;
const summarize = (a) => ({
  count: a.length,
  p50: percentile(a, 0.5),
  p95: percentile(a, 0.95),
  max: a.length ? Math.max(...a) : null,
});
const options = {};
for (let i = 2; i < process.argv.length; i++) {
  const key = process.argv[i].replace(/^--/, '');
  options[key] =
    process.argv[i + 1]?.startsWith('--') || i + 1 === process.argv.length
      ? true
      : process.argv[++i];
}
if (!isMainThread) {
  const { shared, port, rate, seconds, clients } = workerData;
  const counters = new Int32Array(shared);
  const agent = new http.Agent({ keepAlive: true, maxSockets: 1024 });
  const latency = [],
    scheduledLatency = [],
    backlog = [];
  let begin,
    sent = 0,
    probes = 0,
    pending = 0,
    errors = 0,
    loadEnd,
    ended = false,
    peak = 0;
  parentPort.on('message', (message) => {
    if (message === 'start') {
      begin = performance.now();
      const count = Math.floor(rate * seconds),
        duration = seconds * 1000;
      const tick = setInterval(() => {
        const now = performance.now(),
          elapsed = now - begin;
        while (sent < count && (sent * 1000) / rate <= elapsed) {
          Atomics.add(counters, 0, 1);
          parentPort.postMessage({ event: sent++ });
        }
        while (probes * 20 < Math.min(elapsed, duration)) {
          const due = begin + probes++ * 20;
          const launched = performance.now();
          pending++;
          const req = http.get(
            { host: '127.0.0.1', port, path: '/', agent },
            (res) => {
              res.resume();
              res.on('end', () => {
                latency.push(performance.now() - launched);
                scheduledLatency.push(performance.now() - due);
                pending--;
              });
            }
          );
          req.on('error', () => {
            errors++;
            pending--;
          });
        }
        const queued =
          Atomics.load(counters, 0) * clients - Atomics.load(counters, 1);
        peak = Math.max(peak, queued);
        backlog.push([elapsed, queued]);
        if (!ended && elapsed >= duration && sent === count) {
          ended = true;
          loadEnd = begin + duration;
          parentPort.postMessage({ sourceEnded: true });
        }
        if (ended && queued === 0 && pending === 0) {
          clearInterval(tick);
          const active = backlog.filter(
            ([t]) => t >= duration * 0.25 && t <= duration
          );
          const meanT = active.reduce((s, x) => s + x[0], 0) / active.length;
          const meanQ = active.reduce((s, x) => s + x[1], 0) / active.length;
          const slope =
            (active.reduce((s, x) => s + (x[0] - meanT) * (x[1] - meanQ), 0) /
              active.reduce((s, x) => s + (x[0] - meanT) ** 2, 0)) *
            1000;
          const nearest = (target) =>
            backlog.reduce((a, b) =>
              Math.abs(b[0] - target) < Math.abs(a[0] - target) ? b : a
            );
          parentPort.postMessage({
            done: true,
            sourceDurationMs: duration,
            elapsedMs: performance.now() - begin,
            drainMs: Math.max(0, performance.now() - loadEnd),
            sent,
            probes,
            httpErrors: errors,
            httpMs: summarize(latency),
            httpScheduledMs: summarize(scheduledLatency),
            totalBacklog: {
              peak,
              atHalf: nearest(duration / 2)[1],
              atSourceEnd: nearest(duration)[1],
              growthTasksPerSecond: slope,
              final: queued,
            },
          });
          agent.destroy();
          parentPort.close();
        }
      }, 5);
    }
  });
} else {
  (async () => {
    const root = path.resolve(options.root || path.join(__dirname, '../..'));
    const rate = Number(options.rate || 10),
      seconds = Number(options.seconds || 6),
      clients = Number(options.clients || 4000);
    const profile = !!options.profile;
    const Parse = require(require.resolve('parse/node', { paths: [root] }));
    Parse.initialize('lq-fairness-benchmark');
    Parse.Object.disableSingleInstance();
    global.Parse = Parse;
    const { ParseLiveQueryServer } = require(
      path.join(root, 'lib/LiveQuery/ParseLiveQueryServer')
    );
    const { Client } = require(path.join(root, 'lib/LiveQuery/Client'));
    const { Subscription } = require(
      path.join(root, 'lib/LiveQuery/Subscription')
    );
    const logger = require(path.join(root, 'lib/logger')).getLogger();
    let loggedErrors = 0,
      firstError;
    for (const method of [
      'error',
      'warn',
      'info',
      'verbose',
      'debug',
      'silly',
      'log',
    ])
      logger[method] =
        method === 'error'
          ? (...args) => {
              loggedErrors++;
              firstError ??= args.map(String).join(' ');
            }
          : () => {};
    const server = Object.create(ParseLiveQueryServer.prototype);
    server.config = {
      cacheTimeout: 5000,
      collectionPrefix: '',
      databaseAdapter: {},
    };
    server.clients = new Map();
    server.subscriptions = new Map();
    server.authCache = new Map();
    server._initSpikadCombined();
    for (const token of ['allowed', 'denied']) {
      const roles = token === 'allowed' ? ['role:readers'] : [];
      const promise = Promise.resolve({
        userId: token,
        auth: {
          user: { id: token },
          userRoles: roles,
          getUserRoles: async () => roles,
        },
      });
      server.authCache.set(token, promise);
      server._tokenUser.set(token, { userId: token, promise });
    }
    server.getAuthForSessionToken = (token) =>
      server.authCache.get(token) || Promise.resolve({});
    let frames = 0,
      bytes = 0,
      frameErrors = 0,
      forbiddenFrames = 0;
    const subscription = new Subscription(
      'Fairness',
      { score: { $gte: 0 } },
      'fairness'
    );
    for (let i = 0; i < clients; i++) {
      const token = i % 2 ? 'denied' : 'allowed';
      const client = new Client(
        String(i),
        {
          send: (raw) => {
            frames++;
            bytes += raw.length;
            if (token === 'denied') forbiddenFrames++;
            if (raw.includes('"op":"error"')) frameErrors++;
          },
        },
        false,
        token
      );
      client.addSubscriptionInfo(1, { sessionToken: token, watch: ['score'] });
      server.clients.set(String(i), client);
      subscription.addClientSubscription(String(i), 1);
    }
    server.subscriptions.set(
      'Fairness',
      new Map([[subscription.hash, subscription]])
    );
    const shared = new SharedArrayBuffer(8),
      counters = new Int32Array(shared);
    const run = server._fanout.run.bind(server._fanout);
    let limiterPeak = 0,
      scheduled = 0,
      outstanding = 0;
    server._fanout.run = (fn) => {
      scheduled++;
      outstanding++;
      limiterPeak = Math.max(limiterPeak, outstanding);
      return run(fn).then(() => {
        outstanding--;
        Atomics.add(counters, 1, 1);
      });
    };
    const base = {
      className: 'Fairness',
      objectId: 'object',
      score: 0,
      createdAt: '2026-01-01T00:00:00.000Z',
      updatedAt: '2026-01-01T00:00:00.000Z',
      ACL: { 'role:readers': { read: true } },
      content: 'message '.repeat(64),
      secret: 'private',
      tags: ['one', 'two'],
      nested: { city: 'Rome' },
    };
    const clp = {
      find: { '*': true },
      get: { '*': true },
      protectedFields: { '*': ['secret'] },
    };
    const publish = (i) => {
      const message = {
        originalParseObject: { ...base, score: i },
        currentParseObject: { ...base, score: i + 1 },
        classLevelPermissions: clp,
      };
      server._inflateParseObject(message);
      return server._onAfterSave(message);
    };
    // Warm real authorization, clones and serialization before all measurements.
    for (let i = 0; i < 3; i++) {
      await publish(i);
      while (outstanding) await new Promise((resolve) => setImmediate(resolve));
      await server._fanout.waitIdle?.();
    }
    frames = bytes = scheduled = limiterPeak = 0;
    Atomics.store(counters, 1, 0);
    global.gc?.();
    global.gc?.();
    const retainedBefore = process.memoryUsage().heapUsed;
    const httpServer = http.createServer((_req, res) => {
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ok');
    });
    await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
    const worker = new Worker(__filename, {
      workerData: {
        shared,
        port: httpServer.address().port,
        rate,
        seconds,
        clients,
      },
    });
    await new Promise((resolve, reject) => {
      worker.once('online', resolve);
      worker.once('error', reject);
    });
    let inspector, sampleStart, sampledBytes;
    const post = (name, params) =>
      new Promise((resolve, reject) =>
        inspector.post(name, params || {}, (e, result) =>
          e ? reject(e) : resolve(result)
        )
      );
    if (profile) {
      inspector = new (require('node:inspector').Session)();
      inspector.connect();
      await post('HeapProfiler.startSampling', {
        samplingInterval: 32768,
        includeObjectsCollectedByMajorGC: true,
        includeObjectsCollectedByMinorGC: true,
      });
      sampleStart = performance.now();
    }
    let timerLast = performance.now();
    const lag = [];
    const timer = setInterval(() => {
      const now = performance.now();
      lag.push(Math.max(0, now - timerLast - 10));
      timerLast = now;
    }, 10);
    const cpuBefore = process.cpuUsage(),
      started = performance.now();
    const data = await new Promise((resolve, reject) => {
      worker.on('message', (message) => {
        if (message.event !== undefined) publish(message.event).catch(reject);
        if (message.done) resolve(message);
      });
      worker.once('error', reject);
      worker.postMessage('start');
    });
    await server._fanout.waitIdle?.();
    const wallMs = performance.now() - started,
      cpu = process.cpuUsage(cpuBefore);
    clearInterval(timer);
    let sampleDurationMs;
    if (profile) {
      sampleDurationMs = performance.now() - sampleStart;
      const result = await post('HeapProfiler.stopSampling');
      sampledBytes = result.profile.samples.reduce(
        (sum, sample) => sum + sample.size,
        0
      );
      if (options['profile-out'])
        fs.writeFileSync(
          options['profile-out'],
          JSON.stringify(result.profile)
        );
      result.profile = null; // Do not count the materialized sampling profile as retained application heap.
      inspector.disconnect();
    }
    await worker.terminate();
    await new Promise((resolve) => httpServer.close(resolve));
    global.gc?.();
    global.gc?.();
    const retainedAfter = process.memoryUsage().heapUsed;
    assert.equal(data.httpErrors, 0);
    assert.equal(data.httpMs.count, data.probes);
    assert.equal(data.totalBacklog.final, 0);
    assert.equal(scheduled, data.sent * clients);
    assert.equal(Atomics.load(counters, 1), scheduled);
    assert.equal(frames, data.sent * Math.ceil(clients / 2));
    assert.equal(frameErrors + forbiddenFrames + loggedErrors, 0, firstError);
    assert.equal(server._prefilter.skipped, 0);
    const result = {
      root,
      node: process.version,
      label: options.label || '',
      parameters: {
        rate,
        seconds,
        clients,
        deniedFraction: 0.5,
        profile,
        concurrency: server._fanout.max,
      },
      ...data,
      timerLagMs: summarize(lag),
      completed: scheduled,
      frames,
      frameErrors,
      forbiddenFrames,
      bytes,
      limiterOutstandingPeak: limiterPeak,
      limiterStats: server._fanout.stats(),
      prefilter: server._prefilter,
      wallMs,
      throughputTasksPerSecond: scheduled / (wallMs / 1000),
      throughputEventsPerSecond: data.sent / (wallMs / 1000),
      cpuMs: (cpu.user + cpu.system) / 1000,
      cpuPercent: (cpu.user + cpu.system) / (wallMs * 10),
      heapRetainedBefore: retainedBefore,
      heapRetainedAfter: retainedAfter,
      heapRetainedDelta: retainedAfter - retainedBefore,
      sampling: profile
        ? {
            estimatedAllocatedBytes: sampledBytes,
            actualDurationMs: sampleDurationMs,
            estimatedBytesPerSecond: sampledBytes / (sampleDurationMs / 1000),
            intervalBytes: 32768,
            includesCollectedObjects: true,
          }
        : null,
    };
    delete result.done;
    console.log(JSON.stringify(result));
  })().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
