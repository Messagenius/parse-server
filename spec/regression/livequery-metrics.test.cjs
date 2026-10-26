'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const Parse = require('parse/node');
const { ParseLiveQueryServer } = require('../../lib/LiveQuery/ParseLiveQueryServer');
const Config = require('../../lib/Config');
const logger = require('../../lib/logger').getLogger();
const methods = ['error', 'warn', 'info', 'verbose', 'debug', 'silly', 'log'];
for (const name of methods) {
  logger[name] = () => {};
}
test.after(() => {
  for (const name of methods) {
    delete logger[name];
  }
});
Parse.initialize('metrics-test-app');
global.Parse = Parse;
const OPEN = { find: { '*': true }, get: { '*': true } };
const deferred = () => {
  let resolve;
  const promise = new Promise(done => {
    resolve = done;
  });
  return { promise, resolve };
};

function fixture(t) {
  t.mock.method(Config, 'get', () => ({
    requestComplexity: { queryDepth: 20 },
    database: {
      loadSchema: async () => ({ getClassLevelPermissions: () => OPEN }),
      addProtectedFields: () => [],
    },
  }));
  const server = Object.create(ParseLiveQueryServer.prototype);
  server.config = {
    appId: Parse.applicationId,
    cacheTimeout: 5000,
    collectionPrefix: '',
    databaseAdapter: {},
  };
  server.clients = new Map();
  server.subscriptions = new Map();
  server.authCache = new Map();
  server.keyPairs = new Map();
  server._initSpikadCombined();
  server.getAuthForSessionToken = async token => {
    if (!token) {
      return {};
    }
    return {
      userId: token,
      auth: { user: { id: token }, userRoles: [], getUserRoles: async () => [] },
    };
  };
  const connect = async token => {
    const ws = new EventEmitter();
    ws.frames = [];
    ws.send = raw => ws.frames.push(JSON.parse(raw));
    ws.ws = { close: () => ws.emit('disconnect') };
    server._onConnect(ws);
    await server._handleConnect(ws, {
      sessionToken: token,
      installationId: 'private-installation',
    });
    assert.ok(ws.clientId);
    return ws;
  };
  const subscribe = async (ws, requestId, className = 'MetricObjects', where = {}) => {
    await server._handleSubscribe(ws, {
      requestId,
      sessionToken: server.clients.get(ws.clientId).sessionToken,
      query: { className, where },
    });
    assert.equal(ws.frames.at(-1).op, 'subscribed');
  };
  const event = (changes = {}, clp = OPEN) => {
    const message = {
      currentParseObject: {
        className: 'MetricObjects',
        objectId: 'private-object-id',
        body: 'private-payload',
        ACL: { '*': { read: true } },
        ...changes,
      },
      classLevelPermissions: clp,
    };
    server._inflateParseObject(message);
    return message;
  };
  const deliver = async message => {
    await server._onAfterSave(message);
    await server._fanout.waitIdle();
  };
  return { server, connect, subscribe, event, deliver };
}

// Recompute from ownership maps, independently of the increment/decrement counters.
function expectCounts(server, clients, subscriptions, classes, queries) {
  const metrics = server.getMetrics();
  const actualSubscriptions = [...server.clients.values()].reduce(
    (sum, client) => sum + client.subscriptionInfos.size,
    0
  );
  const actualQueries = [...server.subscriptions.values()].reduce((sum, map) => sum + map.size, 0);
  assert.equal(metrics.clients, clients);
  assert.equal(metrics.clientSubscriptions, subscriptions);
  assert.equal(metrics.classes, classes);
  assert.equal(metrics.distinctQueries, queries);
  assert.equal(metrics.clientSubscriptions, actualSubscriptions);
  assert.equal(metrics.distinctQueries, actualQueries);
}

test('real lifecycle counts clients, subscriptions, classes and queries without replacement drift', async t => {
  const f = fixture(t);
  const a = await f.connect('private-token-a');
  const b = await f.connect('private-token-b');
  expectCounts(f.server, 2, 0, 0, 0);
  await f.subscribe(a, 1);
  await f.subscribe(a, 2);
  await f.subscribe(b, 1);
  expectCounts(f.server, 2, 3, 1, 1);
  for (let i = 0; i < 12; i++) {
    await f.subscribe(a, 1); // identical requestId/query replaces, never increments
    expectCounts(f.server, 2, 3, 1, 1);
    await f.subscribe(a, 1, 'SecondClass', { score: i });
    expectCounts(f.server, 2, 3, 2, 2);
    await f.subscribe(a, 1, 'SecondClass', { score: i + 1 });
    expectCounts(f.server, 2, 3, 2, 2);
    f.server._handleUnsubscribe(a, { requestId: 1 });
    expectCounts(f.server, 2, 2, 1, 1);
    f.server._handleUnsubscribe(a, { requestId: 1 }); // invalid repeat is not decremented
    expectCounts(f.server, 2, 2, 1, 1);
    await f.subscribe(a, 1);
  }
  a.emit('disconnect');
  expectCounts(f.server, 1, 1, 1, 1);
  a.emit('disconnect');
  expectCounts(f.server, 1, 1, 1, 1);
  b.emit('disconnect');
  expectCounts(f.server, 0, 0, 0, 0);
});

test(
  'disconnect while subscribe awaits schema cannot register a zombie subscription',
  { timeout: 5000 },
  async t => {
    const f = fixture(t);
    const a = await f.connect('private-token-a');
    const entered = deferred();
    const release = deferred();
    t.mock.method(Config, 'get', () => ({
      requestComplexity: { queryDepth: 20 },
      database: {
        loadSchema: async () => {
          entered.resolve();
          await release.promise;
          return { getClassLevelPermissions: () => OPEN };
        },
        addProtectedFields: () => [],
      },
    }));
    const frameCount = a.frames.length;
    const pending = f.server._handleSubscribe(a, {
      requestId: 1,
      sessionToken: 'private-token-a',
      query: { className: 'MetricObjects', where: {} },
    });
    await entered.promise;
    a.emit('disconnect');
    expectCounts(f.server, 0, 0, 0, 0);
    release.resolve();
    await pending;
    expectCounts(f.server, 0, 0, 0, 0);
    assert.equal(
      a.frames.length,
      frameCount,
      'no subscribe acknowledgement or stale error after disconnect'
    );
  }
);

test('event, candidate, ACL/CLP denial and notification counts describe the completed work', async t => {
  const f = fixture(t);
  const a = await f.connect('private-token-a');
  const b = await f.connect('private-token-b');
  await f.subscribe(a, 1);
  await f.subscribe(b, 1);
  await f.deliver(f.event());
  await f.deliver(f.event({ ACL: { 'private-token-a': { read: true } } }));
  await f.deliver(
    f.event(
      { owner: { __type: 'Pointer', className: '_User', objectId: 'private-token-a' } },
      { find: { pointerFields: ['owner'] }, get: { '*': true } }
    )
  );
  const metrics = f.server.getMetrics();
  assert.equal(metrics.eventsReceived, 3);
  assert.equal(metrics.candidates, 6);
  assert.equal(metrics.notifications, 4);
  assert.equal(metrics.deniedACL, 1);
  assert.equal(metrics.deniedCLP, 1);
  assert.equal(metrics.discarded, 2);
  assert.equal(metrics.fanout.active, 0);
  assert.equal(metrics.fanout.queued, 0);
  for (const privateValue of [
    'private-token',
    'private-installation',
    'private-object-id',
    'private-payload',
  ]) {
    assert.equal(JSON.stringify(metrics).includes(privateValue), false);
  }
});

test(
  'in-flight obsolete work increments stale and discarded but never notification counters',
  { timeout: 5000 },
  async t => {
    const f = fixture(t);
    const a = await f.connect('private-token-a');
    await f.subscribe(a, 1);
    const entered = deferred();
    const release = deferred();
    const filter = f.server._filterSensitiveData.bind(f.server);
    f.server._filterSensitiveData = async (...args) => {
      entered.resolve();
      await release.promise;
      return filter(...args);
    };
    const running = f.deliver(f.event());
    await entered.promise;
    f.server._handleUnsubscribe(a, { requestId: 1 });
    release.resolve();
    await running;
    const metrics = f.server.getMetrics();
    assert.equal(metrics.eventsReceived, 1);
    assert.equal(metrics.candidates, 1);
    assert.equal(metrics.stale, 1);
    assert.equal(metrics.discarded, 1);
    assert.equal(metrics.notifications, 0);
    expectCounts(f.server, 1, 0, 0, 0);
  }
);

test('metrics emit on their own unref timer, without HTTP, and shutdown disables collection', async t => {
  const f = fixture(t);
  const old = process.env.MSG_LQ_METRICS_INTERVAL_MS;
  process.env.MSG_LQ_METRICS_INTERVAL_MS = '1000';
  t.after(() => {
    if (old === undefined) {
      delete process.env.MSG_LQ_METRICS_INTERVAL_MS;
    } else {
      process.env.MSG_LQ_METRICS_INTERVAL_MS = old;
    }
  });
  let callback;
  let unreferenced = false;
  const handle = {
    unref: () => {
      unreferenced = true;
    },
  };
  t.mock.method(global, 'setInterval', (fn, interval) => {
    assert.equal(interval, 1000);
    callback = fn;
    return handle;
  });
  const clear = t.mock.method(global, 'clearInterval', timer => {
    assert.equal(timer, handle);
  });
  const emitted = [];
  t.mock.method(logger, 'info', (...args) => emitted.push(args));
  f.server._startMetrics();
  assert.equal(unreferenced, true);
  const disable = t.mock.method(f.server._loopDelay, 'disable');
  callback();
  assert.equal(emitted.length, 1);
  assert.equal(emitted[0][0], 'LiveQuery metrics %j');
  assert.equal(emitted[0][1].pid, process.pid);
  assert.equal(emitted[0][1].clients, 0);
  assert.ok(emitted[0][1].eventLoop);
  f.server.subscriber = { isOpen: false };
  await f.server.shutdown();
  assert.equal(clear.mock.callCount(), 1);
  assert.equal(disable.mock.callCount(), 1);
});
