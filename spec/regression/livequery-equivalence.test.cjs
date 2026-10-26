'use strict';

// Parse 7.5.4 differential coverage against the snapshot with ONLY delivery safety
// fixes and stateless protected-field calls. Optimization outputs must be identical.
// The reference intentionally includes CLP denial and recipient/ACL isolation fixes;
// delivery-safety.test.cjs independently tests these corrections to upstream 7.5.4.

const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const Module = require('module');

const PS_ROOT = path.resolve(__dirname, '../..');
const LQ_FILE = path.join(PS_ROOT, 'lib/LiveQuery/ParseLiveQueryServer.js');
// The Parse SDK instance parse-server itself uses (it ships its own copy).
const Parse = require(require.resolve('parse/node', { paths: [PS_ROOT] }));
const { Subscription } = require('../../lib/LiveQuery/Subscription');
const { Client } = require('../../lib/LiveQuery/Client');
const { matchesQuery } = require('../../lib/LiveQuery/QueryTools');
const parseLogger = require('../../lib/logger');
const { isDate } = require('node:util').types;
const deepcopy = require('deepcopy');

// ---------------------------------------------------------------------------------------------
// Sources and variants

const fixture = name =>
  fs.readFileSync(path.join(__dirname, 'fixtures/livequery-' + name + '.cjs'), 'utf8');

// Compiles a ParseLiveQueryServer source as if it were the installed file: its relative requires
// resolve to the installed (shared, cached) parse-server modules.
function compileServerClass(source) {
  const m = new Module(LQ_FILE, module);
  m.filename = LQ_FILE;
  m.paths = Module._nodeModulePaths(path.dirname(LQ_FILE));
  m._compile(source, LQ_FILE);
  return m.exports.ParseLiveQueryServer;
}

const PRISTINE_SOURCE = fixture('baseline');
const THREE_SOURCE = fs.readFileSync(LQ_FILE, 'utf8');
const FALLBACK_LINE = '        if (hasDottedKey(subscription.query)) {';
assert.strictEqual(THREE_SOURCE.split(FALLBACK_LINE).length, 2);
const VARIANTS = {
  pristine: compileServerClass(PRISTINE_SOURCE),
  three: require('../../lib/LiveQuery/ParseLiveQueryServer').ParseLiveQueryServer,
  naive: compileServerClass(THREE_SOURCE.replace(FALLBACK_LINE, '        if (false) {')),
};

// Parse Server's logger, captured: _onAfterSave logs the subscriptions whose match throws and the
// per-client failures; those error lines are part of the compared output. QueryTools holds the
// logger instance it saw at load time, so the methods of that instance are replaced.
const logged = { error: [] };
const parseServerLogger = parseLogger.getLogger();
const LOG_METHODS = ['error', 'warn', 'info', 'verbose', 'debug', 'silly', 'log'];
for (const method of LOG_METHODS) {
  parseServerLogger[method] =
    method === 'error' ? (...args) => logged.error.push(args.map(String).join(' ')) : () => {};
}
test.after(() => {
  for (const method of LOG_METHODS) {
    delete parseServerLogger[method];
  }
});

Parse.Object.disableSingleInstance();
// Parse Server sets the global at startup; SchemaController.validatePermission reads it.
if (!global.Parse) {
  global.Parse = Parse;
}
// getTrigger(className, 'afterEvent', Parse.applicationId) throws without an application id.
if (!Parse.applicationId) {
  Parse.initialize('lq-equivalence-test');
}

// ---------------------------------------------------------------------------------------------
// Fixtures

const D = iso => ({ __type: 'Date', iso });
const PTR = (className, objectId) => ({ __type: 'Pointer', className, objectId });
const GEO = (latitude, longitude) => ({ __type: 'GeoPoint', latitude, longitude });

const CREATED = '2026-01-01T00:00:00.000Z';
const WHEN = '2026-03-01T10:00:00.000Z';
const NESTED_WHEN = '2026-04-01T10:00:00.000Z';

// The publisher's JSON for class X. _inflateParseObject and toJSON() turn createdAt/updatedAt
// into ISO strings; `when`, `nested.when` and `dates` stay Date JSON in what matchesQuery sees.
const BASE = {
  className: 'X',
  objectId: 'o1',
  createdAt: CREATED,
  updatedAt: '2026-01-02T00:00:00.000Z',
  name: 'alice',
  score: 10,
  active: true,
  nothing: null,
  when: D(WHEN),
  owner: PTR('_User', 'userA'),
  location: GEO(45, 9),
  area: {
    __type: 'Polygon',
    coordinates: [
      [0, 0],
      [0, 10],
      [10, 10],
      [10, 0],
    ],
  },
  doc: { __type: 'File', name: 'a.txt', url: 'http://files.invalid/a.txt' },
  blob: { __type: 'Bytes', base64: 'AAEC' },
  tags: ['a', 'b', 'c'],
  dates: [D(WHEN), D('2026-05-01T00:00:00.000Z')],
  members: [PTR('_User', 'userA'), PTR('_User', 'userB')],
  profile: { city: 'Milan', zip: 20100 },
  nested: { when: D(NESTED_WHEN), label: 'x' },
  secret: 's3cr3t',
  ACL: { '*': { read: true } },
};

function variantOf(changes) {
  const o = deepcopy(BASE);
  for (const [k, v] of Object.entries(changes)) {
    if (v === undefined) {
      delete o[k];
    } else {
      o[k] = v;
    }
  }
  return o;
}

const ACL_ONLY_A = { userA: { read: true, write: true } };
const ACL_ADMINS = { 'role:admins': { read: true } };

// Saves as (original, current). Together they produce create, enter, leave, update and no event
// for the queries below, and leave/enter caused by an ACL change rather than by the query.
const SAVES = [
  { name: 'create', original: null, current: BASE },
  { name: 'no change', original: BASE, current: BASE },
  { name: 'score 10 -> 11', original: BASE, current: variantOf({ score: 11 }) },
  {
    name: 'score 10 -> 99, name -> bob',
    original: BASE,
    current: variantOf({ score: 99, name: 'bob' }),
  },
  {
    name: 'when moves to June',
    original: BASE,
    current: variantOf({ when: D('2026-06-01T00:00:00.000Z') }),
  },
  {
    name: 'when moves to 2025',
    original: variantOf({ when: D('2025-06-01T00:00:00.000Z') }),
    current: BASE,
  },
  { name: 'when removed', original: BASE, current: variantOf({ when: undefined }) },
  {
    name: 'owner A -> B, members shrink',
    original: BASE,
    current: variantOf({ owner: PTR('_User', 'userB'), members: [PTR('_User', 'userB')] }),
  },
  { name: 'location moves away', original: BASE, current: variantOf({ location: GEO(-30, 120) }) },
  { name: 'tags change', original: BASE, current: variantOf({ tags: ['a', 'z'] }) },
  {
    name: 'profile.city changes',
    original: BASE,
    current: variantOf({ profile: { city: 'Rome', zip: 100 } }),
  },
  {
    name: 'nested.when changes',
    original: BASE,
    current: variantOf({ nested: { when: D('2027-01-01T00:00:00.000Z'), label: 'y' } }),
  },
  {
    name: 'active false, nothing set',
    original: BASE,
    current: variantOf({ active: false, nothing: 'x' }),
  },
  {
    name: 'dates array changes',
    original: BASE,
    current: variantOf({ dates: [D('2030-01-01T00:00:00.000Z')] }),
  },
  { name: 'ACL public -> userA only', original: BASE, current: variantOf({ ACL: ACL_ONLY_A }) },
  { name: 'ACL userA only -> public', original: variantOf({ ACL: ACL_ONLY_A }), current: BASE },
  {
    name: 'ACL role admins -> public, score 11',
    original: variantOf({ ACL: ACL_ADMINS }),
    current: variantOf({ score: 11 }),
  },
  {
    name: 'ACL public -> admins, when moves',
    original: BASE,
    current: variantOf({ ACL: ACL_ADMINS, when: D('2026-09-01T00:00:00.000Z') }),
  },
  { name: 'area removed', original: BASE, current: variantOf({ area: undefined }) },
];

// Every operator matchesKeyConstraints implements, the ones it rejects, and dotted keys.
const QUERIES = [
  // direct equality
  { name: 'alice' },
  { score: 10 },
  { tags: 'b' },
  { owner: PTR('_User', 'userA') },
  { members: PTR('_User', 'userB') },
  { when: D(WHEN) },
  { dates: D(WHEN) },
  { nothing: null },
  { active: true },
  { objectId: 'o1' },
  // comparisons
  { score: { $lt: 50 } },
  { score: { $lte: 10 } },
  { score: { $gt: 10 } },
  { score: { $gte: 11 } },
  { when: { $gt: D('2026-02-01T00:00:00.000Z') } },
  { when: { $lt: D('2026-05-01T00:00:00.000Z') } },
  { when: { $gte: D(WHEN), $lte: D('2026-12-31T00:00:00.000Z') } },
  { name: { $gte: 'b' } },
  { score: { $eq: 10 } },
  { when: { $eq: D(WHEN) } },
  { name: { $ne: 'alice' } },
  { owner: { $ne: PTR('_User', 'userB') } },
  { when: { $ne: D(WHEN) } },
  { score: { $in: [10, 20] } },
  { owner: { $in: [PTR('_User', 'userA')] } },
  { tags: { $in: ['z', 'c'] } },
  { when: { $in: [D(WHEN)] } },
  { name: { $nin: ['bob'] } },
  { score: { $nin: [11] } },
  { tags: { $all: ['a', 'b'] } },
  { when: { $exists: true } },
  { nothing: { $exists: false } },
  { missing: { $exists: false } },
  { name: { $regex: '^al' } },
  { name: { $regex: 'ALI', $options: 'i' } },
  { name: { $regex: { source: 'ice$', flags: '' } } },
  { name: { $regex: '\\Qal\\E' } },
  { when: { $regex: '2026' } },
  // geo
  { location: { $nearSphere: GEO(45.1, 9.1), $maxDistance: 0.01 } },
  { location: { $within: { $box: [GEO(40, 5), GEO(50, 15)] } } },
  { location: { $geoWithin: { $polygon: [GEO(40, 5), GEO(40, 15), GEO(50, 15), GEO(50, 5)] } } },
  { location: { $geoWithin: { $centerSphere: [[9, 45], 0.01] } } },
  { area: { $geoIntersects: { $point: GEO(5, 5) } } },
  // arrays
  { tags: { $containedBy: ['a', 'b', 'c', 'd'] } },
  { score: { $containedBy: [1] } },
  // not evaluable locally: matchesKeyConstraints returns false (or throws, as for $containedBy
  // on a number above or $geoIntersects on a missing field)
  { name: { $text: { $search: { $term: 'alice' } } } },
  { owner: { $select: { query: { className: '_User', where: {} }, key: 'objectId' } } },
  { owner: { $dontSelect: { query: { className: '_User', where: {} }, key: 'objectId' } } },
  { owner: { $inQuery: { className: '_User', where: {} } } },
  { owner: { $notInQuery: { className: '_User', where: {} } } },
  { $relatedTo: { object: PTR('Y', 'y1'), key: 'xs' } },
  { score: { $unknown: 1 } },
  // logical
  { $or: [{ score: 99 }, { when: { $gte: D(WHEN) } }] },
  { $and: [{ active: true }, { when: { $lte: D('2026-12-31T00:00:00.000Z') } }] },
  { $nor: [{ name: 'bob' }, { when: { $lt: D('2026-01-01T00:00:00.000Z') } }] },
  { $or: [{ $and: [{ score: { $gte: 10 } }, { tags: 'a' }] }, { $nor: [{ active: true }] }] },
  { $or: 'not an array' },
  // dotted keys: these keep the per-subscription clone
  { 'profile.city': 'Milan' },
  { 'createdAt.iso': CREATED },
  { 'when.iso': WHEN },
  { 'when.__type': 'Date' },
  { 'nested.when.iso': NESTED_WHEN },
  { 'nested.when.__type': 'Date' },
  { 'nested.when': { $gt: D('2026-01-01T00:00:00.000Z') } },
  { $or: [{ 'when.iso': WHEN }, { score: 99 }] },
  { $and: [{ 'profile.city': 'Milan' }, { when: { $gt: D('2026-02-01T00:00:00.000Z') } }] },
];

// Groups of up to four subscriptions that share a field, run in every insertion order. The first
// two hold the cases the dotted-key fallback exists for: a subscription that decodes a Date on
// the shared clone (`when $gt`, `nested.when $gt`) next to one that reads into it ('.iso').
const FOCUS_GROUPS = [
  [{ when: { $gt: D('2026-02-01T00:00:00.000Z') } }, { 'when.iso': WHEN }],
  [
    { 'nested.when': { $gt: D('2026-01-01T00:00:00.000Z') } },
    { 'nested.when.iso': NESTED_WHEN },
    { 'nested.when.__type': 'Date' },
    { nested: { $exists: true } },
  ],
  [
    { when: { $gt: D('2026-02-01T00:00:00.000Z') } },
    { 'when.iso': WHEN },
    { 'when.__type': 'Date' },
    { $or: [{ 'when.iso': WHEN }, { score: 99 }] },
  ],
  [
    { when: D(WHEN) },
    { when: { $eq: D(WHEN) } },
    { when: { $exists: true } },
    { when: { $regex: '2026' } },
  ],
  [
    { createdAt: { $gt: D('2025-01-01T00:00:00.000Z') } },
    { 'createdAt.iso': CREATED },
    { createdAt: CREATED },
    { updatedAt: { $exists: true } },
  ],
  [
    { dates: D(WHEN) },
    { dates: { $in: [D(WHEN)] } },
    { when: { $in: [D(WHEN)] } },
    { when: { $ne: D(WHEN) } },
  ],
];

const SESSIONS = {
  'token-a': { userId: 'userA', roles: [] },
  'token-b': { userId: 'userB', roles: [] },
  'token-c': { userId: 'userC', roles: ['role:admins'] },
};
const CLIENTS = [
  { id: 'c-a', token: 'token-a' },
  { id: 'c-b', token: 'token-b' },
  { id: 'c-c', token: 'token-c' },
  { id: 'c-anon' },
  { id: 'c-expired', token: 'token-expired' },
  { id: 'c-master', master: true },
];
// Per subscription: four requests over three clients, one of them with two requestIds; `keys`
// (select) and `watch` vary so that Client._toJSONWithFields and _checkWatchFields run.
const REQUEST_SHAPES = [
  { offset: 0, info: {} },
  { offset: 1, info: { keys: ['name', 'score', 'when'] } },
  { offset: 3, info: { watch: ['score', 'when'] } },
  { offset: 1, info: { watch: ['name'] } },
];

const CLP_OPEN = { find: { '*': true }, get: { '*': true } };
// protectedFields as an object goes through DatabaseController.addProtectedFields.
const CLP_PROTECTED = {
  find: { '*': true },
  get: { '*': true },
  protectedFields: { '*': ['secret', 'blob'], userA: [] },
};
// `find` only for userA and admins: other clients of a find subscription fail validatePermission.
const CLP_RESTRICTED = {
  find: { userA: true, 'role:admins': true },
  get: { '*': true },
  protectedFields: { '*': ['secret'] },
};

// ---------------------------------------------------------------------------------------------
// Harness

// subs: [{id, query}] in insertion order. The requestIds depend on the subscription id, not on
// its position, so that runs in different orders are directly comparable.
// `setup(server)`, when given, runs on the server object before the save (stubs).
async function runSave(ServerClass, subs, save, clp, setup) {
  const server = Object.create(ServerClass.prototype);
  // collectionPrefix and databaseAdapter let _filterSensitiveData build a DatabaseController
  // for protectedFields without a database (it only reads the CLP).
  server.config = { cacheTimeout: 5000, collectionPrefix: '', databaseAdapter: {} };
  server.clients = new Map();
  server.subscriptions = new Map();
  server.getAuthForSessionToken = async sessionToken => {
    const s = SESSIONS[sessionToken];
    if (!s) {
      return {};
    }
    return {
      auth: { user: { id: s.userId }, userRoles: s.roles, getUserRoles: async () => s.roles },
      userId: s.userId,
    };
  };
  // Exercise the conservative prefilter as well as the full authorization path.
  if (server._initSpikadCombined) {
    server._initSpikadCombined();
    server.authCache = new Map();
    for (const [token, session] of Object.entries(SESSIONS)) {
      const promise = server.getAuthForSessionToken(token);
      server.authCache.set(token, promise);
      server._tokenUser.set(token, { userId: session.userId, promise });
    }
  }

  const out = {};
  const record = (clientId, requestId, entry) => {
    const key = clientId + '#' + requestId;
    (out[key] = out[key] || []).push(entry);
  };
  for (const spec of CLIENTS) {
    const ws = { send: frame => record(spec.id, JSON.parse(frame).requestId, ['frame', frame]) };
    const client = new Client(spec.id, ws, !!spec.master, spec.token, 'inst-' + spec.id);
    for (const fn of ['pushCreate', 'pushEnter', 'pushUpdate', 'pushLeave', 'pushDelete']) {
      const real = client[fn];
      client[fn] = function (requestId, object, original) {
        record(spec.id, requestId, [
          fn,
          requestId,
          JSON.stringify(object),
          JSON.stringify(original),
        ]);
        return real.call(this, requestId, object, original);
      };
    }
    server.clients.set(spec.id, client);
  }

  // The per-request callbacks are fire-and-forget (requestIds.forEach(async ...)). Each
  // subscription's requestIds array records the promises its callbacks return, so the run
  // waits for exactly those instead of sleeping.
  const pending = [];
  const classSubscriptions = new Map();
  for (const { id, query } of subs) {
    const subscription = new Subscription('X', query, 'hash-' + id);
    REQUEST_SHAPES.forEach((shape, k) => {
      const spec = CLIENTS[(id + shape.offset) % CLIENTS.length];
      const requestId = id * 10 + k;
      server.clients
        .get(spec.id)
        .addSubscriptionInfo(requestId, { ...shape.info, sessionToken: spec.token });
      subscription.addClientSubscription(spec.id, requestId);
    });
    for (const requestIds of subscription.clientRequestIds.values()) {
      requestIds.forEach = function (fn) {
        for (const requestId of this) {
          pending.push(Promise.resolve(fn(requestId)));
        }
      };
    }
    classSubscriptions.set(subscription.hash, subscription);
  }
  server.subscriptions.set('X', classSubscriptions);

  const message = {
    currentParseObject: deepcopy(save.current),
    originalParseObject: save.original ? deepcopy(save.original) : undefined,
    classLevelPermissions: deepcopy(clp),
  };
  server._inflateParseObject(message);
  if (setup) {
    setup(server);
  }
  const errorsBefore = logged.error.length;
  await server._onAfterSave(message);
  let settled = -1;
  while (settled !== pending.length) {
    settled = pending.length;
    await Promise.all(pending);
    await new Promise(resolve => setImmediate(resolve));
  }
  const sorted = {};
  for (const key of Object.keys(out).sort()) {
    sorted[key] = out[key];
  }
  return { deliveries: sorted, errors: logged.error.slice(errorsBefore).sort() };
}

function* permutations(items) {
  if (items.length <= 1) {
    yield items.slice();
    return;
  }
  for (let i = 0; i < items.length; i++) {
    const rest = items.slice(0, i).concat(items.slice(i + 1));
    for (const p of permutations(rest)) {
      yield [items[i], ...p];
    }
  }
}

// Seeded PRNG (mulberry32): the random parts are reproducible from the seed in the message.
function prng(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const pick = (r, items) => items[Math.floor(r() * items.length)];
function shuffled(r, items) {
  const a = items.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(r() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

const label = (save, clp, subs) =>
  save.name + ' | CLP ' + JSON.stringify(clp) + ' | order ' + subs.map(s => s.id).join(',');

// Compare optimized Parse 7 with the same version plus delivery safety fixes.
async function compareVariants(subs, save, clp) {
  const pristine = await runSave(VARIANTS.pristine, subs, save, clp);
  const three = await runSave(VARIANTS.three, subs, save, clp);
  assert.deepStrictEqual(three, pristine, 'Parse 7 baseline vs optimized: ' + label(save, clp, subs));
  return three;
}

const ALL_SUBS = QUERIES.map((query, id) => ({ id, query }));

// ---------------------------------------------------------------------------------------------
// 1-3. Operators, field types, events, clients, CLP, protected fields

test('every operator, field type and event: same deliveries as the corrected Parse 7 baseline', async t => {
  const kinds = new Set();
  let deliveries = 0;
  for (const clp of [CLP_OPEN, CLP_PROTECTED, CLP_RESTRICTED]) {
    for (const save of SAVES) {
      const three = await compareVariants(ALL_SUBS, save, clp);
      for (const entries of Object.values(three.deliveries)) {
        for (const entry of entries) {
          kinds.add(entry[0] === 'frame' ? 'frame:' + JSON.parse(entry[1]).op : entry[0]);
          deliveries++;
        }
      }
    }
  }
  t.diagnostic(
    JSON.stringify({
      deliveries,
      kinds: [...kinds].sort(),
    })
  );
  // The fixture reaches every event type and the error path, so the comparison is not vacuous.
  for (const kind of [
    'pushCreate',
    'pushEnter',
    'pushUpdate',
    'pushLeave',
    'frame:create',
    'frame:enter',
    'frame:update',
    'frame:leave',
  ]) {
    assert.ok(kinds.has(kind), kind + ' reached');
  }
  assert.ok(deliveries > 1000, 'deliveries compared: ' + deliveries);
  // Subscriptions whose match throws are skipped and logged identically.
  assert.ok(logged.error.some(line => line.includes('Failed matching subscription')));
});

test('leave and enter caused by an ACL change reach the client the same way', async () => {
  const subs = [{ id: 1, query: { score: { $gte: 10 } } }];
  const leave = await compareVariants(
    subs,
    SAVES.find(s => s.name === 'ACL public -> userA only'),
    CLP_OPEN
  );
  // c-b (requestId 10 on subscription 1) loses read access: leave with the original body.
  const b = leave.deliveries['c-b#10'];
  assert.strictEqual(b[0][0], 'pushLeave');
  assert.strictEqual(
    JSON.parse(b[0][2]).ACL['*'].read,
    true,
    'leave carries the last readable state'
  );
  const enter = await compareVariants(
    subs,
    SAVES.find(s => s.name === 'ACL userA only -> public'),
    CLP_OPEN
  );
  assert.strictEqual(enter.deliveries['c-b#10'][0][0], 'pushEnter');
  assert.strictEqual(
    enter.deliveries['c-b#10'][0][3],
    'null',
    'enter without the unreadable original'
  );
});

// ---------------------------------------------------------------------------------------------
// Test seam: Parse Server's own unit specs (spec/ParseLiveQueryServer.spec.js) replace
// server._matchesSubscription with counter-driven stubs. Every evaluation must still go through
// it, original before current, so that such a stub fully drives the outcome.

test('seam: _matchesSubscription is called twice per subscription, original then current, and drives the outcome', async () => {
  const save = SAVES.find(s => s.name === 'score 10 -> 11');
  const patterns = { enter: [false, true], leave: [true, false], none: [false, false] };
  const queries = { plain: { score: { $gte: 0 } }, dotted: { 'profile.city': 'Milan' } };
  const subs = [];
  const expected = {};
  let id = 1;
  for (const [kind, query] of Object.entries(queries)) {
    for (const [pattern, results] of Object.entries(patterns)) {
      subs.push({ id, query, kind, pattern, results });
      expected['hash-' + id] = pattern;
      id++;
    }
  }
  for (const variant of ['pristine', 'three']) {
    const calls = [];
    const stub = server => {
      const queue = new Map(subs.map(s => ['hash-' + s.id, s.results.slice()]));
      server._matchesSubscription = function (parseObject, subscription, ...rest) {
        calls.push({
          hash: subscription.hash,
          parseObject,
          argc: 2 + rest.length,
          cloned: rest[0],
        });
        return queue.get(subscription.hash).shift();
      };
    };
    const run = await runSave(VARIANTS[variant], subs, save, CLP_OPEN, stub);
    for (const sub of subs) {
      const hash = 'hash-' + sub.id;
      const mine = calls.filter(c => c.hash === hash);
      assert.strictEqual(mine.length, 2, variant + ' ' + hash + ': two calls');
      assert.deepStrictEqual(
        mine.map(c => c.parseObject.score),
        [10, 11],
        variant + ' ' + hash + ': original, then current'
      );
      if (variant === 'three' && sub.kind === 'plain') {
        assert.deepStrictEqual(
          mine.map(c => [c.argc, c.cloned]),
          [
            [3, true],
            [3, true],
          ],
          hash + ': shared clone, third argument true'
        );
      } else {
        assert.deepStrictEqual(
          mine.map(c => [c.argc, c.cloned]),
          [
            [2, undefined],
            [2, undefined],
          ],
          variant + ' ' + hash + ': two arguments'
        );
      }
      // The four requests of the subscription receive what the stub decided, and only that.
      const pushes = Object.entries(run.deliveries)
        .filter(([key]) => Math.floor(Number(key.split('#')[1]) / 10) === sub.id)
        .map(([, entries]) => entries.filter(e => e[0] !== 'frame').map(e => e[0]));
      const want = { enter: 'pushEnter', leave: 'pushLeave', none: null }[sub.pattern];
      if (want) {
        assert.strictEqual(
          pushes.length,
          REQUEST_SHAPES.length,
          variant + ' ' + hash + ': every request delivered'
        );
        for (const p of pushes) {
          assert.deepStrictEqual(p, [want], variant + ' ' + hash);
        }
      } else {
        assert.deepStrictEqual(pushes, [], variant + ' ' + hash + ': nothing delivered');
      }
    }
    // The non-dotted subscriptions all receive the same two shared clones.
    if (variant === 'three') {
      const plain = calls.filter(c => c.argc === 3);
      assert.strictEqual(
        new Set(plain.filter(c => c.parseObject.score === 10).map(c => c.parseObject)).size,
        1
      );
      assert.strictEqual(
        new Set(plain.filter(c => c.parseObject.score === 11).map(c => c.parseObject)).size,
        1
      );
    }
  }
  assert.deepStrictEqual(
    await runSave(VARIANTS.three, subs, save, CLP_OPEN, server => {
      const queue = new Map(subs.map(s => ['hash-' + s.id, s.results.slice()]));
      server._matchesSubscription = (parseObject, subscription) =>
        queue.get(subscription.hash).shift();
    }),
    await runSave(VARIANTS.pristine, subs, save, CLP_OPEN, server => {
      const queue = new Map(subs.map(s => ['hash-' + s.id, s.results.slice()]));
      server._matchesSubscription = (parseObject, subscription) =>
        queue.get(subscription.hash).shift();
    }),
    'the same stub gives the same deliveries on pristine and patched'
  );
});

test('seam: the real _matchesSubscription clones with two arguments and decodes in place with true', () => {
  const server = Object.create(VARIANTS.three.prototype);
  const subscription = new Subscription(
    'X',
    { when: { $gt: D('2026-02-01T00:00:00.000Z') } },
    'hash-real'
  );
  const object = { className: 'X', objectId: 'o1', when: D(WHEN) };
  assert.strictEqual(server._matchesSubscription(object, subscription), true);
  assert.deepStrictEqual(object.when, D(WHEN), 'two arguments: the input is not mutated');
  assert.strictEqual(server._matchesSubscription(object, subscription, false), true);
  assert.deepStrictEqual(object.when, D(WHEN), 'false: the input is not mutated');
  assert.strictEqual(server._matchesSubscription(null, subscription, true), false);
  assert.strictEqual(server._matchesSubscription(object, subscription, true), true);
  assert.ok(
    isDate(object.when),
    "true: the Date JSON is decoded in place on the caller's clone"
  );
  assert.strictEqual(object.when.toISOString(), WHEN);
  assert.strictEqual(
    server._matchesSubscription(object, subscription, true),
    true,
    'idempotent on the decoded object'
  );
});

// ---------------------------------------------------------------------------------------------
// 4. Order independence

test('every insertion order of the focus groups gives the pristine output', async () => {
  let runs = 0;
  for (const [g, group] of FOCUS_GROUPS.entries()) {
    const subs = group.map((query, i) => ({ id: 100 + g * 10 + i, query }));
    const reference = new Map();
    for (const order of permutations(subs)) {
      for (const save of SAVES) {
        const three = await compareVariants(order, save, CLP_OPEN);
        if (!reference.has(save.name)) {
          reference.set(save.name, three);
        } else {
          assert.deepStrictEqual(
            three,
            reference.get(save.name),
            'order dependence: ' + label(save, CLP_OPEN, order)
          );
        }
        runs++;
      }
    }
  }
  assert.ok(runs > 1000);
});

test('negative control: without the dotted-key fallback the shared clone changes what clients receive', async () => {
  const decodeFirst = [
    { id: 1, query: { when: { $gt: D('2026-02-01T00:00:00.000Z') } } },
    { id: 2, query: { 'when.iso': WHEN } },
  ];
  const readFirst = decodeFirst.slice().reverse();
  const save = SAVES.find(s => s.name === 'score 10 -> 11');
  const pristine = await runSave(VARIANTS.pristine, decodeFirst, save, CLP_OPEN);
  const naiveDecodeFirst = await runSave(VARIANTS.naive, decodeFirst, save, CLP_OPEN);
  const naiveReadFirst = await runSave(VARIANTS.naive, readFirst, save, CLP_OPEN);
  assert.ok(pristine.deliveries['c-c#20'], 'pristine delivers to the when.iso subscription');
  assert.notDeepStrictEqual(
    naiveDecodeFirst,
    pristine,
    'decoded by the first subscription, unread by the second'
  );
  assert.strictEqual(naiveDecodeFirst.deliveries['c-c#20'], undefined);
  assert.deepStrictEqual(naiveReadFirst, pristine, 'the other order hides the problem');
  assert.deepStrictEqual(await runSave(VARIANTS.three, decodeFirst, save, CLP_OPEN), pristine);
  assert.deepStrictEqual(await runSave(VARIANTS.three, readFirst, save, CLP_OPEN), pristine);
});

test('random 4-subscription subsets in every order, and random orders of all subscriptions', async () => {
  const seed = 20261021;
  const r = prng(seed);
  for (let n = 0; n < 12; n++) {
    const subset = shuffled(r, ALL_SUBS).slice(0, 4);
    const save = pick(r, SAVES);
    const clp = pick(r, [CLP_OPEN, CLP_PROTECTED, CLP_RESTRICTED]);
    let reference = null;
    for (const order of permutations(subset)) {
      const three = await compareVariants(order, save, clp);
      if (!reference) {
        reference = three;
      } else {
        assert.deepStrictEqual(
          three,
          reference,
          'seed ' + seed + ' order dependence: ' + label(save, clp, order)
        );
      }
    }
  }
  const references = new Map();
  for (let n = 0; n < 8; n++) {
    const order = shuffled(r, ALL_SUBS);
    for (const save of SAVES) {
      const three = await compareVariants(order, save, CLP_OPEN);
      if (!references.has(save.name)) {
        references.set(save.name, three);
      } else {
        assert.deepStrictEqual(
          three,
          references.get(save.name),
          'seed ' + seed + ' order dependence: ' + label(save, CLP_OPEN, order)
        );
      }
    }
  }
});

// ---------------------------------------------------------------------------------------------
// 5-6. QueryTools.matchesQuery: property test and mutation audit

const ISOS = [
  '2025-12-31T23:59:59.000Z',
  CREATED,
  WHEN,
  NESTED_WHEN,
  '2026-12-31T00:00:00.000Z',
  'not-a-date',
];
const STRINGS = ['alice', 'bob', 'Alice', 'al.ice', '', '2026-03-01T10:00:00.000Z'];
const NUMBERS = [0, 1, 10, 10.5, -3, 100];

function randomDate(r) {
  return D(pick(r, ISOS));
}
function randomGeo(r) {
  return GEO(pick(r, [0, 5, 45, 45.1, -30]), pick(r, [0, 5, 9, 9.1, 120]));
}
function randomScalar(r) {
  const roll = r();
  if (roll < 0.35) {
    return pick(r, STRINGS);
  }
  if (roll < 0.7) {
    return pick(r, NUMBERS);
  }
  if (roll < 0.85) {
    return r() < 0.5;
  }
  return null;
}
function randomObject(r) {
  const o = { className: 'X', objectId: pick(r, ['o1', 'o2']) };
  const fields = {
    s: () => pick(r, STRINGS),
    n: () => pick(r, NUMBERS),
    b: () => r() < 0.5,
    z: () => null,
    d1: () => randomDate(r),
    d2: () => randomDate(r),
    createdAt: () => (r() < 0.5 ? pick(r, ISOS) : randomDate(r)),
    updatedAt: () => (r() < 0.5 ? pick(r, ISOS) : randomDate(r)),
    p: () => PTR('_User', pick(r, ['userA', 'userB'])),
    g: () => randomGeo(r),
    poly: () => ({
      __type: 'Polygon',
      coordinates: [
        [0, 0],
        [0, 10],
        [10, 10],
        [10, 0],
      ],
    }),
    f: () => ({ __type: 'File', name: 'a.txt', url: 'http://files.invalid/a.txt' }),
    by: () => ({ __type: 'Bytes', base64: 'AAEC' }),
    arrS: () => [pick(r, STRINGS), pick(r, STRINGS)],
    arrN: () => [pick(r, NUMBERS), pick(r, NUMBERS)],
    arrD: () => [randomDate(r), randomDate(r)],
    arrP: () => [PTR('_User', 'userA'), PTR('_User', pick(r, ['userB', 'userC']))],
    nested: () => ({ when: randomDate(r), city: pick(r, STRINGS), inner: { when: randomDate(r) } }),
    ACL: () => ({ '*': { read: r() < 0.5 } }),
  };
  for (const [k, make] of Object.entries(fields)) {
    if (r() < 0.8) {
      o[k] = make();
    }
  }
  return o;
}

const TOP_KEYS = [
  'objectId',
  's',
  'n',
  'b',
  'z',
  'd1',
  'd2',
  'createdAt',
  'updatedAt',
  'p',
  'g',
  'poly',
  'f',
  'by',
  'arrS',
  'arrN',
  'arrD',
  'arrP',
  'nested',
  'ACL',
  'missing',
];
const DOTTED_KEYS = [
  'nested.when',
  'nested.when.iso',
  'nested.when.__type',
  'nested.city',
  'nested.inner.when',
  'nested.inner.when.iso',
  'createdAt.iso',
  'updatedAt.__type',
  'd1.iso',
  'd1.__type',
  'g.latitude',
  'p.objectId',
  'missing.x',
];

function valueAt(object, key) {
  let v = object;
  for (const part of key.split('.')) {
    v = v === null || v === undefined ? undefined : v[part];
  }
  return v;
}

function randomOperand(r, current) {
  const roll = r();
  if (roll < 0.3 && current !== undefined) {
    return deepcopy(current);
  }
  if (roll < 0.55) {
    return randomDate(r);
  }
  if (roll < 0.65) {
    return PTR('_User', pick(r, ['userA', 'userB']));
  }
  return randomScalar(r);
}

function randomConstraint(r, current) {
  const operand = () => randomOperand(r, current);
  const kinds = [
    () => null,
    () => operand(),
    () => ({ [pick(r, ['$lt', '$lte', '$gt', '$gte'])]: operand() }),
    () => ({ $gt: operand(), $lt: operand() }),
    () => ({ [pick(r, ['$eq', '$ne'])]: operand() }),
    () => ({ [pick(r, ['$in', '$nin'])]: [operand(), operand()] }),
    () => ({ $all: [operand()] }),
    () => ({ $exists: pick(r, [true, false, 'yes']) }),
    () => ({
      $regex: pick(r, ['^al', 'ICE', '2026', '\\Qal.\\E', '(']),
      ...(r() < 0.5 ? { $options: 'i' } : {}),
    }),
    () => ({ $regex: { source: pick(r, ['^b', 'e$', '2026']), flags: pick(r, ['', 'i']) } }),
    () => ({
      $nearSphere: randomGeo(r),
      ...(r() < 0.5 ? { $maxDistance: pick(r, [0.01, 1]) } : {}),
    }),
    () => ({ $within: { $box: [GEO(-10, -10), GEO(50, 15)] } }),
    () => ({ $geoWithin: { $polygon: [GEO(40, 5), GEO(40, 15), GEO(50, 15), GEO(50, 5)] } }),
    () => ({ $geoWithin: { $centerSphere: [[9, 45], pick(r, [0.01, 2])] } }),
    () => ({ $geoIntersects: { $point: randomGeo(r) } }),
    () => ({ $containedBy: [operand(), pick(r, STRINGS)] }),
    () => ({ $text: { $search: { $term: 'alice' } } }),
    () => ({
      [pick(r, ['$select', '$dontSelect'])]: {
        query: { className: '_User', where: {} },
        key: 'objectId',
      },
    }),
    () => ({ [pick(r, ['$inQuery', '$notInQuery'])]: { className: '_User', where: {} } }),
  ];
  return pick(r, kinds)();
}

function randomQuery(r, object, depth = 0) {
  const query = {};
  const n = 1 + Math.floor(r() * 3);
  for (let i = 0; i < n; i++) {
    const roll = r();
    if (depth < 2 && roll < 0.15) {
      const op = pick(r, ['$or', '$and', '$nor']);
      query[op] =
        r() < 0.95
          ? Array.from({ length: 1 + Math.floor(r() * 3) }, () => randomQuery(r, object, depth + 1))
          : {};
    } else if (roll < 0.17) {
      query.$relatedTo = { object: PTR('Y', 'y1'), key: 'xs' };
    } else {
      const key = r() < 0.25 ? pick(r, DOTTED_KEYS) : pick(r, TOP_KEYS);
      query[key] = randomConstraint(r, valueAt(object, key));
    }
  }
  return query;
}

// The hasDottedKey of change 3, taken from the patched source, and an independent rewrite; the
// property test asserts they agree on every generated query.
const HAS_DOTTED_KEY_LINE = '    const hasDottedKey = ';
const productionHasDottedKey = (() => {
  const line = THREE_SOURCE.split('\n').find(l => l.startsWith(HAS_DOTTED_KEY_LINE));
  return new Function(line + '\nreturn hasDottedKey;')();
})();
function usesDottedKey(where) {
  if (where === null || typeof where !== 'object') {
    return false;
  }
  return Object.keys(where).some(k =>
    ['$or', '$and', '$nor'].includes(k)
      ? Array.isArray(where[k]) && where[k].some(usesDottedKey)
      : k.includes('.')
  );
}

function evaluate(object, query) {
  try {
    return { result: matchesQuery(object, query) };
  } catch (e) {
    return { threw: e.constructor.name + ': ' + e.message };
  }
}

const isDateJSON = v =>
  v !== null && typeof v === 'object' && !(isDate(v)) && v.__type === 'Date';

// Paths where `after` differs from `before`; throws on any difference other than a Date JSON
// value replaced by the Date it encodes.
function dateDecodes(before, after, at = []) {
  if (isDateJSON(before) && isDate(after)) {
    assert.ok(
      Object.is(after.getTime(), new Date(before.iso).getTime()),
      'decoded to the same instant at ' + at.join('.')
    );
    return [at.join('.')];
  }
  if (before === null || typeof before !== 'object') {
    assert.ok(Object.is(before, after), 'unchanged value at ' + at.join('.'));
    return [];
  }
  assert.ok(
    after !== null && typeof after === 'object' && !(isDate(after)),
    'still an object at ' + at.join('.')
  );
  assert.strictEqual(Array.isArray(after), Array.isArray(before), 'same kind at ' + at.join('.'));
  assert.deepStrictEqual(Object.keys(after), Object.keys(before), 'same keys at ' + at.join('.'));
  const out = [];
  for (const k of Object.keys(before)) {
    out.push(...dateDecodes(before[k], after[k], at.concat(k)));
  }
  return out;
}

function deepFreeze(value) {
  if (value !== null && typeof value === 'object' && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const k of Object.keys(value)) {
      deepFreeze(value[k]);
    }
  }
  return value;
}

const CASES = 3000;
const PROPERTY_SEED = 1021;
function generateCases() {
  const r = prng(PROPERTY_SEED);
  const cases = [];
  for (let i = 0; i < CASES; i++) {
    const object = randomObject(r);
    const queries = Array.from({ length: 2 + Math.floor(r() * 4) }, () => randomQuery(r, object));
    cases.push({ object, queries, r: prng(PROPERTY_SEED * 7919 + i) });
  }
  return cases;
}

test('property: a clone shared across queries answers like a fresh clone per query', t => {
  const stats = {
    cases: 0,
    orders: 0,
    sharedEvaluations: 0,
    dotted: 0,
    decodedOnShared: 0,
    naiveDivergences: 0,
    throws: 0,
    matches: 0,
  };
  for (const [i, { object, queries, r }] of generateCases().entries()) {
    stats.cases++;
    const fresh = queries.map(q => evaluate(deepcopy(object), q));
    for (const [j, q] of queries.entries()) {
      const before = deepcopy(q);
      assert.strictEqual(
        productionHasDottedKey(q),
        usesDottedKey(q),
        'case ' + i + ': hasDottedKey ' + JSON.stringify(q)
      );
      if (usesDottedKey(q)) {
        // Production keeps these on the pristine path: only determinism is required.
        stats.dotted++;
        assert.deepStrictEqual(
          evaluate(deepcopy(object), q),
          fresh[j],
          'case ' + i + ' deterministic'
        );
      }
      assert.deepStrictEqual(q, before, 'case ' + i + ': matchesQuery leaves the query untouched');
    }
    const indices = queries.map((_, j) => j);
    const orders =
      indices.length <= 4
        ? [...permutations(indices)]
        : Array.from({ length: 24 }, () => shuffled(r, indices));
    for (const order of orders) {
      stats.orders++;
      const shared = deepcopy(object);
      const naive = deepcopy(object);
      for (const j of order) {
        const q = queries[j];
        if (fresh[j].threw) {
          stats.throws++;
        }
        if (fresh[j].result) {
          stats.matches++;
        }
        // Negative control: what the shared clone would answer for a dotted query too.
        if (usesDottedKey(q)) {
          if (JSON.stringify(evaluate(naive, q)) !== JSON.stringify(fresh[j])) {
            stats.naiveDivergences++;
          }
          continue;
        }
        evaluate(naive, q);
        stats.sharedEvaluations++;
        assert.deepStrictEqual(
          evaluate(shared, q),
          fresh[j],
          'case ' +
            i +
            ' query ' +
            j +
            ' order ' +
            order +
            ': ' +
            JSON.stringify(q) +
            ' on ' +
            JSON.stringify(object)
        );
      }
      // The shared clone differs from the object only by top-level Date decodes.
      const decoded = dateDecodes(object, shared);
      for (const p of decoded) {
        assert.ok(
          !p.includes('.'),
          'case ' + i + ': only top-level fields are decoded on the shared clone, got ' + p
        );
      }
      stats.decodedOnShared += decoded.length;
    }
  }
  t.diagnostic(JSON.stringify(stats));
  // The generator reaches every interesting region.
  assert.strictEqual(stats.cases, CASES);
  assert.ok(stats.decodedOnShared > 1000, JSON.stringify(stats));
  assert.ok(stats.dotted > 500, JSON.stringify(stats));
  assert.ok(stats.throws > 100, JSON.stringify(stats));
  assert.ok(stats.matches > 1000, JSON.stringify(stats));
  assert.ok(
    stats.naiveDivergences > 0,
    'without the fallback some dotted queries diverge: ' + JSON.stringify(stats)
  );
});

const WRITE_ERROR = /read only property|Cannot add property|not extensible|Cannot delete property/;

// Splits a where into single-key constraints, through $or/$and/$nor.
function atoms(where) {
  const out = [];
  for (const [k, v] of Object.entries(where || {})) {
    if (['$or', '$and', '$nor'].includes(k)) {
      if (Array.isArray(v)) {
        for (const sub of v) {
          out.push(...atoms(sub));
        }
      }
    } else {
      out.push({ [k]: v });
    }
  }
  return out;
}

// The slot matchesKeyConstraints would decode for this single-key constraint, if any: it follows
// a dotted key with `object[part] || {}` exactly as QueryTools does, and decodes the last
// component when the constraint is not null and the value there is Date JSON.
function decodeSlot(object, atom) {
  const [key, constraint] = Object.entries(atom)[0];
  if (constraint === null || key === '$relatedTo') {
    return null;
  }
  const parts = key.split('.');
  let container = object;
  for (const part of parts.slice(0, -1)) {
    container = container[part] || {};
  }
  const last = parts[parts.length - 1];
  const value = container[last];
  return value && value.__type == 'Date' ? parts.join('.') : null;
}

function frozenRun(object, query) {
  const frozen = deepFreeze(deepcopy(object));
  try {
    matchesQuery(frozen, query);
    return null;
  } catch (e) {
    return e instanceof TypeError && WRITE_ERROR.test(e.message) ? e.message : null;
  }
}

test('mutation audit: on a frozen object the only write matchesQuery attempts is the Date decode', t => {
  let writes = 0;
  let atomWrites = 0;
  let atomCount = 0;
  for (const [i, { object, queries }] of generateCases().entries()) {
    for (const q of queries) {
      const where = 'case ' + i + ': ' + JSON.stringify(q) + ' on ' + JSON.stringify(object);
      // Whole query, short-circuits included: a write is attempted exactly when evaluating on
      // a fresh clone changes it, and every change is a Date decode of a constrained slot.
      const write = frozenRun(object, q);
      const fresh = deepcopy(object);
      evaluate(fresh, q);
      const decoded = dateDecodes(object, fresh);
      assert.strictEqual(
        !!write,
        decoded.length > 0,
        'write attempted iff the fresh clone changed: ' + where
      );
      const slots = new Set(
        atoms(q)
          .map(a => decodeSlot(object, a))
          .filter(Boolean)
      );
      for (const p of decoded) {
        assert.ok(slots.has(p), 'decoded slot ' + p + ' is a constrained Date JSON: ' + where);
      }
      if (write) {
        writes++;
        const property = (write.match(/property '([^']+)'/) || [])[1];
        assert.ok(
          property && decoded.some(p => p.split('.').pop() === property),
          'the write names a decoded slot (' + write + '): ' + where
        );
      }
      // Each single-key constraint on its own (no short-circuit): a write happens if and only
      // if it constrains a slot holding Date JSON.
      for (const atom of atoms(q)) {
        atomCount++;
        const atomWrite = frozenRun(object, atom);
        assert.strictEqual(
          !!atomWrite,
          decodeSlot(object, atom) !== null,
          'atom ' + JSON.stringify(atom) + ': ' + where
        );
        if (atomWrite) {
          atomWrites++;
        }
      }
    }
  }
  t.diagnostic(JSON.stringify({ writes, atomWrites, atomCount }));
  assert.ok(
    writes > 1000 && atomWrites > writes,
    'writes ' + writes + ', atom writes ' + atomWrites + ' of ' + atomCount
  );
});
