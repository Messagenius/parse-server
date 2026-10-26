'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { default: FanoutLimiter } = require('../../lib/LiveQuery/FanoutLimiter');
const { default: FanoutRecipients, fanoutGeneration } = require('../../lib/LiveQuery/FanoutRecipients');
const { Subscription } = require('../../lib/LiveQuery/Subscription');
const { default: logger } = require('../../lib/logger');
const immediate = () => new Promise(resolve => setImmediate(resolve));

test('callbacks replenished from microtasks share a budget and yield repeatedly', async () => {
  const limiter = new FanoutLimiter(4, 16);
  let completed = 0;
  const work = () => {
    completed++;
    if (completed < 12000) { limiter.run(work); }
  };
  limiter.run(work);
  const observations = [];
  for (let i = 0; i < 5; i++) {
    await immediate();
    observations.push(completed);
  }
  assert.ok(observations.every(value => value < 12000));
  assert.ok(observations.every((value, i) => !i || value > observations[i - 1]));
  await limiter.waitIdle();
  assert.equal(completed, 12000);
  assert.ok(limiter.stats().yields > 5);
});

test('producer and callbacks spend one budget; newer events cannot bypass a paused producer', async () => {
  const limiter = new FanoutLimiter(4, 8);
  const admitted = [];
  const started = [];
  function* event(name) {
    for (let i = 0; i < 60; i++) {
      admitted.push(`${name}:${i}`);
      limiter.run(() => started.push(`${name}:${i}`));
      yield;
    }
  }
  const first = limiter.produce(event('save'));
  const second = limiter.produce(event('delete'));
  assert.ok(admitted.length <= 4, 'producer and starts must share the initial 8-step budget');
  await Promise.all([first, second]);
  await limiter.waitIdle();
  const expected = ['save', 'delete'].flatMap(name => Array.from({ length: 60 }, (_, i) => `${name}:${i}`));
  assert.deepEqual(admitted, expected);
  assert.deepEqual(started, expected);
  assert.ok(limiter.stats().maxQueued <= 1, 'producer does not allocate all recipient callbacks');
});

test('producer pauses under asynchronous callback backpressure and retains FIFO after errors', async () => {
  const limiter = new FanoutLimiter(2, 16);
  const started = [];
  const release = [];
  function* event() {
    for (let i = 0; i < 20; i++) {
      limiter.run(async () => {
        started.push(i);
        if (i < 2) { await new Promise(resolve => release.push(resolve)); }
        if (i === 4) { throw Error('expected async callback error'); }
      });
      yield;
    }
  }
  limiter.produce(event());
  await immediate();
  assert.deepEqual(started, [0, 1]);
  assert.equal(limiter.active, 2);
  assert.equal(limiter.queued, 1);
  assert.equal(limiter.queuedEvents, 1);
  release.forEach(resolve => resolve());
  await limiter.waitIdle();
  assert.deepEqual(started, Array.from({ length: 20 }, (_, i) => i));
  assert.equal(limiter.stats().callbackErrors, 1);
});

test('producer, callback and logger exceptions never strand the queue', async () => {
  const limiter = new FanoutLimiter(1, 4);
  const original = logger.error;
  logger.error = () => { throw Error('expected logger exception'); };
  let finished = 0;
  try {
    limiter.run(() => { throw Error('expected sync callback error'); });
    limiter.produce((function* () {
      yield;
      throw Error('expected producer error');
    })());
    limiter.produce((function* () {
      limiter.run(() => finished++);
      yield;
    })());
    await limiter.waitIdle();
    assert.equal(finished, 1);
    assert.equal(limiter.stats().callbackErrors, 1);
    assert.equal(limiter.stats().producerErrors, 1);
    assert.equal(limiter.queued, 0);
    assert.equal(limiter.queuedEvents, 0);
  } finally {
    logger.error = original;
  }
});

test('continuous events leave HTTP and timer callbacks serviceable before drain', async () => {
  const limiter = new FanoutLimiter(16, 32);
  const server = http.createServer((req, res) => res.end('ok'));
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  let completed = 0;
  let generated = 0;
  const total = 50000;
  function enqueueEvent() {
    limiter.produce((function* () {
      for (let i = 0; i < 100; i++) {
        limiter.run(() => completed++);
        yield;
      }
      generated += 100;
      if (generated < total) { enqueueEvent(); }
    })());
  }
  try {
    enqueueEvent();
    const timer = new Promise(resolve => setTimeout(() => resolve(completed), 1));
    const request = new Promise((resolve, reject) => {
      http.get(`http://127.0.0.1:${server.address().port}/health`, res => {
        res.resume();
        res.on('end', () => resolve(completed));
      }).on('error', reject);
    });
    const [atTimer, atHttp] = await Promise.all([timer, request]);
    assert.ok(atTimer < total, 'timer must run while fanout is still continuously busy');
    assert.ok(atHttp < total, 'HTTP must respond while fanout is still continuously busy');
    await limiter.waitIdle();
    assert.equal(completed, total);
    assert.equal(limiter.stats().dropped, 0);
  } finally {
    await new Promise(resolve => server.close(resolve));
  }
});

test('recipient cursor survives unsubscribe and requestId reuse without including new memberships', () => {
  const recipients = new FanoutRecipients();
  const first = recipients.add('a', 1);
  const removed = recipients.add('a', 2);
  const third = recipients.add('b', 3);
  const cutoff = fanoutGeneration();
  const iterator = recipients.entries(cutoff);
  assert.equal(iterator.next().value, first);
  recipients.remove('a', 1);
  recipients.remove('a', 2);
  const replacement = recipients.add('a', 1);
  assert.equal(first.active, false);
  assert.equal(removed.active, false);
  assert.notEqual(replacement, first);
  const rest = Array.from(iterator).filter(entry => entry.active);
  assert.deepEqual(rest, [third]);
  assert.equal(recipients.size, 2);
  assert.equal(recipients.clients.size, 2);
});

test('recipient cutoff terminates during continuous new registrations; index retains no deleted nodes', () => {
  const recipients = new FanoutRecipients();
  for (let i = 0; i < 100; i++) { recipients.add('a', i); }
  const cutoff = fanoutGeneration();
  const seen = [];
  for (const entry of recipients.entries(cutoff)) {
    seen.push(entry.requestId);
    recipients.add('b', 1000 + entry.requestId);
    recipients.remove('a', entry.requestId);
  }
  assert.equal(seen.length, 100);
  assert.equal(recipients.clients.has('a'), false);
  assert.equal(recipients.size, 100);
  for (const entry of Array.from(recipients.entries(fanoutGeneration()))) {
    recipients.remove(entry.clientId, entry.requestId);
  }
  assert.equal(recipients.size, 0);
  assert.equal(recipients.clients.size, 0);
  assert.equal(recipients.head, null);
  assert.equal(recipients.tail, null);
});

test('Subscription keeps existing arrays compatible and updates lifetime membership on replacement', () => {
  const subscription = new Subscription('Test', {}, 'hash');
  subscription.addClientSubscription('client', 1);
  subscription.addClientSubscription('client', 1);
  assert.deepEqual(subscription.clientRequestIds.get('client'), [1]);
  assert.equal(subscription.fanoutRecipients.size, 1);
  const old = subscription.fanoutRecipients.head.token;
  subscription.deleteClientSubscription('client', 1);
  assert.equal(old.active, false);
  subscription.addClientSubscription('client', 1);
  assert.notEqual(old, subscription.fanoutRecipients.head.token);
  assert.equal(subscription.fanoutRecipients.size, 1);
});

const Parse = require('parse/node');
if (!Parse.applicationId) { Parse.initialize('lq-fairness-test'); }
global.Parse = Parse;
const { ParseLiveQueryServer } = require('../../lib/LiveQuery/ParseLiveQueryServer');
const { Client } = require('../../lib/LiveQuery/Client');
const openCLP = { find: { '*': true }, get: { '*': true } };
function makeServer(count, options = {}) {
  const server = Object.create(ParseLiveQueryServer.prototype);
  server.config = { cacheTimeout: 5000, collectionPrefix: '', databaseAdapter: {} };
  server.clients = new Map();
  server.subscriptions = new Map();
  server.authCache = new Map();
  server._initSpikadCombined();
  server._fanout = new FanoutLimiter(4, 8);
  const frames = [];
  const subscription = new Subscription('Fairness', options.query || {}, 'query');
  for (let i = 0; i < count; i++) {
    const id = `client-${i}`;
    const client = new Client(id, { send: raw => frames.push({ client: id, frame: JSON.parse(raw) }) }, false);
    client.addSubscriptionInfo(1, { subscription });
    server.clients.set(id, client);
    subscription.addClientSubscription(id, 1);
  }
  server.subscriptions.set('Fairness', new Map([['query', subscription]]));
  server._matchesCLP = async () => true;
  server._matchesACL = async () => true;
  server._filterSensitiveData = async () => {};
  const message = (sequence, acl = { '*': { read: true } }) => {
    const value = {
      currentParseObject: { className: 'Fairness', objectId: 'object', sequence, ACL: acl },
      classLevelPermissions: openCLP,
    };
    server._inflateParseObject(value);
    return value;
  };
  return { server, subscription, frames, message };
}

test('real save/delete producers preserve FIFO callback admission across batch boundaries', async () => {
  const { server, message, frames } = makeServer(50);
  const admission = [];
  server._matchesCLP = async (clp, object) => {
    admission.push(object.sequence);
    return true;
  };
  const first = server._onAfterSave(message(0));
  const second = server._onAfterDelete(message(1));
  const third = server._onAfterSave(message(2));
  await Promise.all([first, second, third]);
  await server._fanout.waitIdle();
  assert.deepEqual(admission, [0, 1, 2].flatMap(sequence => Array(50).fill(sequence)));
  assert.equal(frames.length, 150);
  assert.ok(server._fanout.stats().maxQueued <= 1);
});

test('real producer skips unsubscribe/reused requestId while paused and excludes a new subscription', async () => {
  const { server, subscription, frames, message } = makeServer(3);
  server._fanout = new FanoutLimiter(1, 2);
  const first = server._onAfterSave(message(0));
  // The producer yielded the first recipient, but has not scheduled it yet.
  const client = server.clients.get('client-0');
  subscription.deleteClientSubscription('client-0', 1);
  client.deleteSubscriptionInfo(1);
  client.addSubscriptionInfo(1, { subscription });
  subscription.addClientSubscription('client-0', 1);
  const added = new Subscription('Fairness', {}, 'new-query');
  client.addSubscriptionInfo(2, { subscription: added });
  added.addClientSubscription('client-0', 2);
  server.subscriptions.get('Fairness').set('new-query', added);
  const second = server._onAfterDelete(message(1));
  await Promise.all([first, second]);
  await server._fanout.waitIdle();
  const oldFrames = frames.filter(({ frame }) => frame.object?.sequence === 0);
  assert.deepEqual(oldFrames.map(({ client: id }) => id), ['client-1', 'client-2']);
  const newFrames = frames.filter(({ frame }) => frame.object?.sequence === 1);
  assert.equal(newFrames.length, 4);
  assert.equal(newFrames.filter(({ client: id }) => id === 'client-0').length, 2);
});

test('real producer yields for ACL prefiltered and nonmatching subscription work under continuous events', async () => {
  for (const mode of ['prefiltered', 'nonmatching']) {
    const { server, subscription, message, frames } = makeServer(600);
    if (mode === 'nonmatching') {
      const subscriptions = server.subscriptions.get('Fairness');
      subscriptions.delete('query');
      for (let i = 0; i < 600; i++) {
        const sub = new Subscription('Fairness', { sequence: -1 }, `query-${i}`);
        sub.addClientSubscription(`client-${i}`, 1);
        subscriptions.set(sub.hash, sub);
      }
    }
    let events = 0;
    let complete;
    const done = new Promise(resolve => { complete = resolve; });
    const enqueue = () => {
      const event = message(events, { allowed: { read: true } });
      server._onAfterSave(event).then(() => {
        events++;
        if (events < 20) { enqueue(); } else { complete(); }
      });
    };
    enqueue();
    for (let i = 0; i < 5; i++) { await immediate(); }
    assert.ok(events < 20, `${mode} work must not starve I/O turns`);
    assert.ok(server._fanout.stats().yields >= 5);
    await done;
    await server._fanout.waitIdle();
    assert.equal(frames.length, 0);
    if (mode === 'prefiltered') {
      assert.equal(server._prefilter.skipped, subscription.fanoutRecipients.size * 20);
    }
  }
});

test('unsubscribe during pending auth suppresses delivery and error frames for reused requestIds', async () => {
  for (const event of ['_onAfterSave', '_onAfterDelete']) {
    for (const rejectAuth of [false, true]) {
      const { server, subscription, frames, message } = makeServer(1);
      let release;
      server._matchesCLP = () => new Promise((resolve, reject) => {
        release = () => rejectAuth ? reject(Error('expected stale auth rejection')) : resolve(true);
      });
      const queued = server[event](message(0));
      while (!release) { await immediate(); }
      const client = server.clients.get('client-0');
      subscription.deleteClientSubscription('client-0', 1);
      client.deleteSubscriptionInfo(1);
      client.addSubscriptionInfo(1, { subscription });
      subscription.addClientSubscription('client-0', 1);
      release();
      await queued;
      await server._fanout.waitIdle();
      assert.equal(frames.length, 0);
      assert.equal(server._metrics.stale, 1);
    }
  }
});


test('retained callback lifetime tokens cannot retain recipient-list history during churn', () => {
  const recipients = new FanoutRecipients();
  const pendingCallbackToken = recipients.add('slow', 1);
  const initial = recipients.add('rotating', 0);
  const iterator = recipients.entries(fanoutGeneration());
  const iteratedToken = iterator.next().value;
  assert.equal(iteratedToken, pendingCallbackToken);
  iterator.return();
  assert.equal(recipients.cursors.size, 0);
  recipients.remove('slow', 1);
  let previous = initial;
  for (let i = 1; i <= 1000; i++) {
    const next = recipients.add('rotating', i);
    recipients.remove('rotating', previous.requestId);
    assert.equal(previous.active, false);
    previous = next;
  }
  assert.deepEqual(Object.keys(pendingCallbackToken).sort(),
    ['active', 'clientId', 'generation', 'requestId']);
  assert.equal(pendingCallbackToken.active, false);
  assert.equal(recipients.size, 1);
  assert.equal(recipients.head, recipients.tail);
  assert.equal(recipients.head.previous, null);
  assert.equal(recipients.head.next, null);
  assert.equal(recipients.head.token, previous);
  assert.equal(recipients.clients.get('rotating').size, 1);
  assert.equal(recipients.clients.has('slow'), false);
});


test('suspended recipient cursors do not retain tombstone chains under continuous churn', () => {
  const recipients = new FanoutRecipients();
  recipients.add('first', 0);
  recipients.add('rotating', 0);
  const cutoff = fanoutGeneration();
  const iterator = recipients.entries(cutoff);
  assert.equal(iterator.next().value.clientId, 'first');
  assert.equal(recipients.cursors.size, 1);
  const firstNode = recipients.head;
  recipients.remove('first', 0);
  assert.equal(firstNode.next, null);
  assert.equal(firstNode.previous, null);
  for (let i = 1; i <= 1000; i++) {
    const removed = recipients.head;
    recipients.add('rotating', i);
    recipients.remove('rotating', i - 1);
    assert.equal(removed.previous, null);
    assert.equal(removed.next, null);
    assert.equal(Array.from(recipients.cursors)[0].next, recipients.head);
  }
  assert.equal(iterator.next().done, true, 'new memberships cannot extend the old event');
  assert.equal(recipients.cursors.size, 0);
  assert.equal(recipients.size, 1);
});

test('recipient cursors are released on iterator return and thrown consumer errors', () => {
  const recipients = new FanoutRecipients();
  recipients.add('client', 1);
  recipients.add('client', 2);
  for (const mode of ['return', 'throw']) {
    const iterator = recipients.entries(fanoutGeneration());
    iterator.next();
    assert.equal(recipients.cursors.size, 1);
    if (mode === 'return') {
      iterator.return();
    } else {
      assert.throws(() => iterator.throw(Error('consumer failure')), /consumer failure/);
    }
    assert.equal(recipients.cursors.size, 0);
  }
  assert.equal(Array.from(recipients.entries(fanoutGeneration())).length, 2);
  assert.equal(recipients.cursors.size, 0);
});
