'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { performance } = require('node:perf_hooks');
const Parse = require('parse/node');
const { ParseLiveQueryServer } = require('../../lib/LiveQuery/ParseLiveQueryServer');

Parse.initialize('auth-cache-lifetime-tests');
const user = id => ({ className: '_User', objectId: id, username: id });
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
};
const make = t => {
  // Exercise the production LRU and real Auth materialization without a DB or listener.
  const server = new ParseLiveQueryServer(http.createServer(), {
    appId: Parse.applicationId, authCacheSize: 2, cacheTimeout: 60000,
  });
  server.cacheController = { user: { get: async token => user(token) } };
  t.after(async () => { server.authCache.clear(); await server.shutdown(); });
  return server;
};

test('identity shortcuts are removed on LRU eviction, deletion and clear', async t => {
  const server = make(t);
  for (const token of ['a', 'b', 'c']) {
    await server.getAuthForSessionToken(token);
  }
  assert.equal(server.authCache.size, 2);
  assert.equal(server._tokenUser.size, 2);
  assert.equal(server._tokenUser.has('a'), false);
  server.authCache.delete('b');
  assert.equal(server._tokenUser.has('b'), false);
  server.authCache.clear();
  assert.equal(server._tokenUser.size, 0);
});

test('TTL expiration removes the shortcut and falls back to authorization', async t => {
  const server = make(t);
  await server.getAuthForSessionToken('expired');
  const promise = server.authCache.peek('expired');
  server.authCache.set('expired', promise, { ttl: 1, start: performance.now() - 1000 });
  const client = {
    sessionToken: 'expired',
    getSubscriptionInfo: () => ({ sessionToken: 'expired' }),
  };
  assert.equal(server._canSkipClient(client, 1, new Set(), new Set(), undefined, {}), false);
  assert.equal(server.authCache.size, 0);
  assert.equal(server._tokenUser.size, 0);
});

test('an authentication completing after eviction cannot recreate a retained identity', async t => {
  const server = make(t), pending = deferred();
  server.cacheController.user.get = token => token === 'old' ? pending.promise : user(token);
  const old = server.getAuthForSessionToken('old');
  await server.getAuthForSessionToken('b');
  await server.getAuthForSessionToken('c');
  pending.resolve(user('old'));
  assert.equal((await old).userId, 'old');
  assert.equal(server._tokenUser.has('old'), false);
  assert.equal(server._tokenUser.size, 2);
});

test('late success or failure cannot replace or delete a newer authentication for the same token', async t => {
  for (const outcome of ['success', 'invalid-session', 'other-error']) {
    const server = make(t), pending = deferred();
    let reads = 0;
    server.cacheController.user.get = () => ++reads === 1 ? pending.promise : user('new');
    const old = server.getAuthForSessionToken('same');
    server.authCache.delete('same');
    const current = server.getAuthForSessionToken('same');
    assert.equal((await current).userId, 'new');
    if (outcome === 'success') {
      pending.resolve(user('old'));
    } else {
      pending.reject(outcome === 'invalid-session'
        ? new Parse.Error(Parse.Error.INVALID_SESSION_TOKEN, 'Expired') : new Error('Unavailable'));
    }
    await old;
    assert.equal(server.authCache.get('same'), current);
    assert.equal(server._tokenUser.get('same').promise, current);
    assert.equal(server._tokenUser.get('same').userId, 'new');
    // The current user is allowed, so a stale deny identity must not suppress delivery.
    const client = { sessionToken: 'same', getSubscriptionInfo: () => ({ sessionToken: 'same' }) };
    assert.equal(server._canSkipClient(client, 1, new Set(['new']), new Set(), undefined, {}), false);
  }
});

test('invalid sessions still use the negative cache and never populate the shortcut', async t => {
  const server = make(t);
  let reads = 0;
  server.cacheController.user.get = async () => {
    reads++;
    throw new Parse.Error(Parse.Error.INVALID_SESSION_TOKEN, 'Expired');
  };
  assert.equal((await server.getAuthForSessionToken('bad')).error.code, Parse.Error.INVALID_SESSION_TOKEN);
  assert.equal((await server.getAuthForSessionToken('bad')).error.code, Parse.Error.INVALID_SESSION_TOKEN);
  assert.equal(reads, 1);
  assert.equal(server._tokenUser.size, 0);
});
