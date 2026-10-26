'use strict';

// No database or sockets: execute the real authorization/filter/trigger pipeline.
const test = require('node:test');
const assert = require('node:assert/strict');
const Parse = require('parse/node');
const { ParseLiveQueryServer } = require('../../lib/LiveQuery/ParseLiveQueryServer');
const { Client } = require('../../lib/LiveQuery/Client');
const { Subscription } = require('../../lib/LiveQuery/Subscription');
const triggers = require('../../lib/triggers');
const logger = require('../../lib/logger').getLogger();
const logMethods = ['error', 'warn', 'info', 'verbose', 'debug', 'silly', 'log'];
for (const method of logMethods) {
  logger[method] = () => {};
}
test.after(() => {
  for (const method of logMethods) {
    delete logger[method];
  }
});
Parse.initialize('lq-http-safety');
Parse.Object.disableSingleInstance();
global.Parse = Parse;
const OPEN = { find: { '*': true }, get: { '*': true } };
let sequence = 0;
const tick = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => {
  let resolve;
  const promise = new Promise(done => {
    resolve = done;
  });
  return { promise, resolve };
};

function fixture(specs = [{ id: 'a' }, { id: 'b' }], query = {}) {
  const className = `Safety${++sequence}`;
  const server = Object.create(ParseLiveQueryServer.prototype);
  server.config = { cacheTimeout: 5000, collectionPrefix: '', databaseAdapter: {} };
  server.clients = new Map();
  server.subscriptions = new Map();
  server.authCache = new Map();
  server._initSpikadCombined();
  const frames = [];
  const subscription = new Subscription(className, query, className);
  for (const spec of specs) {
    const token = spec.anonymous ? undefined : spec.id;
    const user = token
      ? Parse.Object.fromJSON({ className: '_User', objectId: spec.id })
      : undefined;
    const auth = token
      ? { user, userRoles: spec.roles || [], getUserRoles: async () => spec.roles || [] }
      : undefined;
    if (token) {
      server.authCache.set(token, Promise.resolve({ auth, userId: spec.id }));
    }
    const client = new Client(
      spec.id,
      { send: raw => frames.push({ recipient: spec.id, ...JSON.parse(raw) }) },
      false,
      token
    );
    client.addSubscriptionInfo(1, { sessionToken: token, ...spec.info });
    server.clients.set(spec.id, client);
    subscription.addClientSubscription(spec.id, 1);
  }
  server.getAuthForSessionToken = token => server.authCache.get(token) || Promise.resolve({});
  server.subscriptions.set(className, new Map([[className, subscription]]));
  const object = changes => ({
    className,
    objectId: 'object',
    cloneProbe: 'private-copy',
    score: 1,
    nested: { value: 'source' },
    secret: 'secret',
    ACL: { '*': { read: true } },
    ...changes,
  });
  const message = (current = {}, original, clp = OPEN) => {
    const result = { currentParseObject: object(current), classLevelPermissions: clp };
    if (original !== undefined) {
      result.originalParseObject = object(original);
    }
    server._inflateParseObject(result);
    return result;
  };
  const deliver = async (msg, kind = 'save') => {
    await server[kind === 'delete' ? '_onAfterDelete' : '_onAfterSave'](msg);
    await server._fanout.waitIdle();
  };
  const hook = fn => triggers.addTrigger('afterEvent', className, fn, Parse.applicationId);
  return { server, className, frames, message, deliver, hook, subscription };
}

function spyCopies() {
  const original = JSON.parse;
  let count = 0;
  JSON.parse = function (value, ...args) {
    if (typeof value === 'string' && value.includes('"cloneProbe"') && !value.includes('"op":')) {
      count++;
    }
    return original.call(this, value, ...args);
  };
  return {
    count: () => count,
    restore: () => {
      JSON.parse = original;
    },
  };
}

test('denied recipients allocate no private JSON copy; role readers retain access', async () => {
  for (const kind of ['save', 'delete']) {
    const f = fixture([
      { id: 'a', roles: ['role:staff'] },
      { id: 'b' },
      { id: 'anon', anonymous: true },
    ]);
    const msg = f.message({ ACL: { 'role:staff': { read: true } } });
    const spy = spyCopies();
    try {
      await f.deliver(msg, kind);
      assert.deepEqual(
        f.frames.map(frame => frame.recipient),
        ['a']
      );
      assert.equal(spy.count(), 1, `${kind}: one private copy for the one authorized reader`);
    } finally {
      spy.restore();
    }
  }
});

test('all denied ACLs never reach serialization, filtering or triggers', async () => {
  const f = fixture();
  let hooks = 0;
  f.hook(() => hooks++);
  const spy = spyCopies();
  try {
    await f.deliver(f.message({ ACL: { other: { read: true } } }));
    assert.equal(f.frames.length, 0);
    assert.equal(hooks, 0);
    assert.equal(spy.count(), 0);
  } finally {
    spy.restore();
  }
});

test('CLP pointer permissions and requiresAuthentication retain their full checks', async () => {
  for (const clp of [
    { find: { pointerFields: ['owner'] }, get: { '*': true } },
    { find: {}, readUserFields: ['owner'], get: { '*': true } },
  ]) {
    const f = fixture([{ id: 'a' }, { id: 'b' }, { id: 'anon', anonymous: true }]);
    await f.deliver(
      f.message({ owner: { __type: 'Pointer', className: '_User', objectId: 'a' } }, undefined, clp)
    );
    assert.deepEqual(
      f.frames.filter(x => x.op === 'create').map(x => x.recipient),
      ['a']
    );
  }
  const f = fixture([{ id: 'a' }, { id: 'anon', anonymous: true }]);
  await f.deliver(
    f.message({}, undefined, { find: { requiresAuthentication: true }, get: { '*': true } })
  );
  assert.deepEqual(
    f.frames.filter(x => x.op === 'create').map(x => x.recipient),
    ['a']
  );
  assert.ok(f.frames.some(x => x.recipient === 'anon' && x.op === 'error'));
});

test('protected fields and concurrent nested trigger mutations stay private to each recipient', async () => {
  const f = fixture();
  f.hook(async request => {
    assert.equal(request.object.get('nested').value, 'source');
    request.object.get('nested').value = request.user.id;
    await tick();
    assert.equal(request.object.get('nested').value, request.user.id);
  });
  const msg = f.message({}, undefined, { ...OPEN, protectedFields: { '*': ['secret'], a: [] } });
  await f.deliver(msg);
  assert.equal(f.frames.length, 2);
  for (const frame of f.frames) {
    assert.equal(frame.object.nested.value, frame.recipient);
    assert.equal(frame.object.secret, frame.recipient === 'a' ? 'secret' : undefined);
  }
  assert.equal(msg.currentParseObject.get('nested').value, 'source');
  assert.equal(msg.currentParseObject.get('secret'), 'secret');
});

test('ACL grant and revocation expose only the recipient-authorized state', async () => {
  const f = fixture();
  await f.deliver(
    f.message({ secret: 'new-private', ACL: { a: { read: true } } }, { secret: 'old-public' })
  );
  const revoked = f.frames.find(x => x.recipient === 'b');
  assert.equal(revoked.op, 'leave');
  assert.equal(revoked.object.secret, 'old-public');
  assert.equal(revoked.original.secret, 'old-public');
  f.frames.length = 0;
  await f.deliver(
    f.message({ secret: 'new-public' }, { secret: 'old-private', ACL: { a: { read: true } } })
  );
  const granted = f.frames.find(x => x.recipient === 'b');
  assert.equal(granted.op, 'enter');
  assert.equal(granted.object.secret, 'new-public');
  assert.equal(granted.original, undefined);
});

test('ACL materialization is per event; watch compares nested values, not instance identity', async () => {
  const f = fixture(
    Array.from({ length: 40 }, (_, i) => ({ id: `u${i}`, info: { watch: ['nested'] } }))
  );
  const msg = f.message({}, {});
  let aclReads = 0;
  for (const object of [msg.currentParseObject, msg.originalParseObject]) {
    const getACL = object.getACL.bind(object);
    object.getACL = () => {
      aclReads++;
      return getACL();
    };
  }
  await f.deliver(msg);
  assert.equal(aclReads, 2);
  assert.equal(f.frames.length, 0, 'equal nested content is not a watched change');
  await f.deliver(f.message({ nested: { value: 'changed' } }, {}));
  assert.equal(f.frames.length, 40);
});

for (const kind of ['save', 'delete']) {
  for (const stage of ['auth', 'trigger', 'filter']) {
    for (const change of ['unsubscribe', 'disconnect', 'requestId reuse']) {
      test(
        `in-flight ${kind}: ${change} during ${stage} cannot send the obsolete event`,
        { timeout: 5000 },
        async () => {
          const f = fixture([{ id: 'a' }]);
          const entered = deferred();
          const release = deferred();
          const client = f.server.clients.get('a');
          let paused = false;
          const pause = async () => {
            if (!paused) {
              paused = true;
              entered.resolve();
              await release.promise;
            }
          };
          if (stage === 'trigger') {
            f.hook(pause);
          } else {
            const key = stage === 'auth' ? 'getAuthForSessionToken' : '_filterSensitiveData';
            const original = f.server[key].bind(f.server);
            f.server[key] = async (...args) => {
              await pause();
              return original(...args);
            };
          }
          const running = f.deliver(f.message(), kind);
          await entered.promise;
          if (change === 'disconnect') {
            f.server.clients.delete('a');
          } else {
            client.deleteSubscriptionInfo(1);
            if (change === 'requestId reuse') {
              client.addSubscriptionInfo(1, { sessionToken: 'a', watch: ['score'] });
            }
          }
          release.resolve();
          await running;
          assert.equal(
            f.frames.length,
            0,
            'neither a notification nor an error belongs to the old subscription'
          );
          assert.equal(f.server._fanout.active, 0);
        }
      );
    }
  }
}
