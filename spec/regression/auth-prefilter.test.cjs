'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const Module = require('node:module');
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

