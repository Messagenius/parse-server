'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const Module = require('node:module');
const { default: SpikadFanoutLimiter } = require('../../lib/LiveQuery/FanoutLimiter');
const root = path.resolve(__dirname, '../..');
const file = path.join(root, 'lib/LiveQuery/ParseLiveQueryServer.js');
const patched = fs.readFileSync(path.join(__dirname, 'fixtures/livequery-baseline.cjs'), 'utf8');
const compile = source => {
  const m = new Module(file, module);
  m.filename = file;
  m.paths = Module._nodeModulePaths(path.dirname(file));
  m._compile(source, file);
  return m.exports.ParseLiveQueryServer;
};
const Original = compile(patched),
  Combined = require('../../lib/LiveQuery/ParseLiveQueryServer').ParseLiveQueryServer;
const Parse = require(require.resolve('parse/node', { paths: [root] }));
if (!Parse.applicationId) {
  Parse.initialize('lq-combined-test');
}
global.Parse = Parse;
const { Client } = require('../../lib/LiveQuery/Client');
const { Subscription } = require('../../lib/LiveQuery/Subscription');
const open = { find: { '*': true }, get: { '*': true } };
const make = (Class = Combined) => {
  const server = Object.create(Class.prototype);
  server.config = { cacheTimeout: 5000, collectionPrefix: '', databaseAdapter: {} };
  server.clients = new Map();
  server.subscriptions = new Map();
  server.authCache = new Map();
  server._initSpikadCombined?.();
  return server;
};
const remember = (server, token, userId) => {
  const promise = Promise.resolve({
    auth: { user: { id: userId }, getUserRoles: async () => [] },
    userId,
  });
  server.authCache.set(token, promise);
  server._tokenUser?.set(token, { userId, promise });
};

test('prefilter considers both tokens, cache lifetime, public ACL, roles and restricted CLP', () => {
  const server = make(),
    readers = new Set(['allowed']);
  remember(server, 'deny', 'denied');
  remember(server, 'allow', 'allowed');
  let info = { sessionToken: 'deny' };
  const client = { getSubscriptionInfo: () => info, sessionToken: 'allow' };
  const skip = (clp = open, cur = readers, orig = readers) =>
    server._canSkipClient(client, 1, cur, orig, clp, {});
  assert.equal(skip(), false, 'connection token grants access');
  info = { sessionToken: 'allow' };
  client.sessionToken = 'deny';
  assert.equal(skip(), false, 'subscription token grants access');
  info = { sessionToken: 'deny' };
  assert.equal(skip(), true, 'both cached identities are excluded');
  assert.equal(skip({ find: { admin: true } }), false, 'preserve CLP errors');
  assert.equal(skip({ find: { '*': true, requiresAuthentication: true } }), false);
  assert.equal(skip({ find: { '*': true }, readUserFields: ['owner'] }), false);
  assert.equal(skip(open, readers, null), false, 'old public ACL needs leave');
  assert.equal(skip(open, new Set(['denied']), readers), false, 'ACL grant needs enter');
  client.hasMasterKey = true;
  assert.equal(skip(), false);
  client.hasMasterKey = false;
  server.authCache.delete('deny');
  assert.equal(skip(), false, 'expired auth falls back');
  server.authCache.set('deny', Promise.resolve({}));
  assert.equal(skip(), false, 'replaced auth falls back');
  assert.equal(server._aclReaders(new Parse.ACL({ '*': { read: true } })), null);
  assert.equal(server._aclReaders(new Parse.ACL({ 'role:admins': { read: true } })), null);
});

test('fanout limits active work, drains every task, and continues after callback errors', async () => {
  const limiter = new SpikadFanoutLimiter(3);
  let active = 0,
    peak = 0,
    finished = 0;
  await Promise.all(
    Array.from({ length: 40 }, (_, i) =>
      limiter.run(async () => {
        active++;
        peak = Math.max(peak, active);
        await new Promise(resolve => setImmediate(resolve));
        active--;
        finished++;
        if (i === 5) {
          throw Error('expected test callback error');
        }
      })
    )
  );
  assert.equal(finished, 40);
  assert.equal(peak, 3);
  assert.equal(limiter.stats().maxQueued, 37);
  assert.equal(limiter.active, 0);
  assert.equal(limiter.queued, 0);
  assert.equal(limiter.stats().dropped + limiter.stats().droppedStale, 0);
});

test('queued callback is discarded only when its client or subscription was replaced', async () => {
  for (const replacement of ['subscription', 'client']) {
    const server = make();
    server._fanout = new SpikadFanoutLimiter(1);
    let release,
      info = {};
    const busy = server._fanout.run(
      () =>
        new Promise(resolve => {
          release = resolve;
        })
    );
    await new Promise(resolve => setImmediate(resolve));
    const client = { getSubscriptionInfo: () => info };
    server.clients.set('c', client);
    let calls = 0;
    const task = server._scheduleSpikad('c', client, 1, null, null, open, {}, () => calls++);
    if (replacement === 'subscription') {
      info = {};
    } else {
      server.clients.set('c', {});
    }
    release();
    await Promise.all([busy, task]);
    assert.equal(calls, 0);
  }
});

test('same-token ACL denial verifies once; different tokens retain their independent checks', async () => {
  const server = make(),
    acl = new Parse.ACL({ allowed: { read: true } });
  let calls = [];
  server._verifyACL = async (_acl, token) => {
    calls.push(token);
    return token === 'allow';
  };
  const client = { sessionToken: 'deny', getSubscriptionInfo: () => ({ sessionToken: 'deny' }) };
  assert.equal(await server._matchesACL(acl, client, 1), false);
  assert.deepEqual(calls, ['deny']);
  calls = [];
  client.sessionToken = 'allow';
  assert.equal(await server._matchesACL(acl, client, 1), true);
  assert.deepEqual(calls, ['deny', 'allow']);
});

test('delete delivers identical protected payloads and errors, with and without the prefilter', async () => {
  const run = async (Class, clp, acl) => {
    const server = make(Class),
      frames = [],
      pending = [];
    server.getAuthForSessionToken = token => server.authCache.get(token) || Promise.resolve({});
    for (const user of ['a', 'b']) {
      remember(server, user, user);
    }
    const subscription = new Subscription('X', {}, 'x');
    for (const user of ['a', 'b']) {
      const client = new Client(user, { send: raw => frames.push(JSON.parse(raw)) }, false, user);
      client.addSubscriptionInfo(1, { sessionToken: user });
      server.clients.set(user, client);
      subscription.addClientSubscription(user, 1);
    }
    for (const ids of subscription.clientRequestIds.values()) {
      ids.forEach = fn => {
        for (const id of ids) {
          pending.push(fn(id));
        }
      };
    }
    server.subscriptions.set('X', new Map([['x', subscription]]));
    const message = {
      currentParseObject: { className: 'X', objectId: 'object', secret: 'hidden', ACL: acl },
      classLevelPermissions: clp,
    };
    server._inflateParseObject(message);
    await server._onAfterDelete(message);
    await Promise.all(pending);
    return {
      frames: frames.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))),
      controller: server._dbController,
      skipped: server._prefilter?.skipped,
    };
  };
  for (const clp of [{ ...open, protectedFields: { '*': ['secret'] } }, { find: { a: true } }]) {
    for (const acl of [
      { a: { read: true } },
      { '*': { read: true } },
      { 'role:admins': { read: true } },
    ]) {
      const original = await run(Original, clp, acl),
        combined = await run(Combined, clp, acl);
      assert.deepEqual(combined.frames, original.frames);
      if (clp.find['*'] && acl.a) {
        assert.equal(combined.skipped, 1);
        assert.ok(combined.controller);
        assert.equal(combined.frames[0].object.secret, undefined);
      }
    }
  }
});
