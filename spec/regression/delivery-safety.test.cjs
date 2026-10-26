'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const Parse = require('parse/node');
const { ParseLiveQueryServer } = require('../../lib/LiveQuery/ParseLiveQueryServer');
const { Client } = require('../../lib/LiveQuery/Client');
const { Subscription } = require('../../lib/LiveQuery/Subscription');
const triggers = require('../../lib/triggers');
Parse.initialize('parse7-delivery-safety');
global.Parse = Parse;
const open = { find: { '*': true }, get: { '*': true } };
const row = (extra = {}) => ({ className: 'X', objectId: 'x', secret: 'private', value: 'current', ACL: {'*': {read: true}}, ...extra });
async function deliver({ op = 'save', clp = open, current = row(), original = row({value: 'previous'}), hook } = {}) {
  const server = Object.create(ParseLiveQueryServer.prototype), frames = [], pending = [];
  server.config = {}; server.clients = new Map(); server.subscriptions = new Map(); server.authCache = new Map();
  server.getAuthForSessionToken = async token => ({ userId: token, auth: { user: { id: token }, userRoles: [], getUserRoles: async () => [] } });
  const sub = new Subscription('X', {}, 'x');
  for (const id of ['a', 'b']) {
    const client = new Client(id, { send: raw => frames.push({client: id, frame: JSON.parse(raw)}) }, false, id);
    client.addSubscriptionInfo(1, { sessionToken: id });
    server.clients.set(id, client); sub.addClientSubscription(id, 1);
  }
  for (const ids of sub.clientRequestIds.values()) {
    ids.forEach = fn => { for (const id of ids) pending.push(fn(id)); };
  }
  server.subscriptions.set('X', new Map([['x', sub]]));
  if (hook) triggers.addTrigger('afterEvent', 'X', hook, Parse.applicationId);
  try {
    const message = {currentParseObject: current, originalParseObject: op === 'save' ? original : undefined, classLevelPermissions: clp};
    server._inflateParseObject(message);
    await server[op === 'save' ? '_onAfterSave' : '_onAfterDelete'](message);
    await Promise.all(pending);
    return frames;
  } finally {
    if (hook) triggers.removeTrigger('afterEvent', 'X', Parse.applicationId);
  }
}

test('a denied CLP stops save and delete delivery even when the ACL allows it', async () => {
  for (const op of ['save', 'delete']) assert.deepEqual(await deliver({op, clp: {find: {nobody: true}}}), []);
});
test('ACL revocation sends the last readable state; ACL grant hides the unreadable original', async () => {
  const restricted = {a: {read: true}};
  const leave = (await deliver({current: row({ACL: restricted})})).find(x => x.client === 'b').frame;
  assert.equal(leave.op, 'leave'); assert.equal(leave.object.value, 'previous');
  assert.deepEqual(leave.object.ACL, {'*': {read: true}});
  const enter = (await deliver({original: row({ACL: restricted, value: 'previous'})})).find(x => x.client === 'b').frame;
  assert.equal(enter.op, 'enter'); assert.equal(enter.original, undefined);
});
test('protected-field deletion never mutates another recipient payload, including delete', async () => {
  for (const op of ['save', 'delete']) {
    const frames = await deliver({op, clp: {...open, protectedFields: {'*': ['secret'], b: []}}});
    const a = frames.find(x => x.client === 'a').frame, b = frames.find(x => x.client === 'b').frame;
    assert.equal(a.object.secret, undefined); assert.equal(b.object.secret, 'private');
    if (op === 'save') { assert.equal(a.original.secret, undefined); assert.equal(b.original.secret, 'private'); }
  }
});
test('concurrent afterEvent changes remain isolated and are sent as filtered JSON', async () => {
  for (const op of ['save', 'delete']) {
    const frames = await deliver({op, hook: async request => {
      await new Promise(resolve => setImmediate(resolve));
      request.object.set('recipient', request.user.id);
    }});
    assert.equal(frames.length, 2);
    for (const {client, frame} of frames) assert.equal(frame.object.recipient, client);
  }
});
