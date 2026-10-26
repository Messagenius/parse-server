'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { ParseLiveQueryServer } = require('../../lib/LiveQuery/ParseLiveQueryServer');
const Controllers = require('../../lib/Controllers');
const exported = require('../../lib/Controllers/DatabaseController');
const DatabaseController = exported.default || exported;
const fs = require('node:fs'), path = require('node:path'), Module = require('node:module');
const file = require.resolve('../../lib/LiveQuery/ParseLiveQueryServer');
const reference = new Module(file, module);
reference.filename = file;
reference.paths = Module._nodeModulePaths(path.dirname(file));
reference._compile(fs.readFileSync(path.join(__dirname, 'fixtures/livequery-7.5.4.cjs'), 'utf8'), file);
const original = reference.exports.ParseLiveQueryServer.prototype._filterSensitiveData;

function fixture({ user = 'reader', roles = [], master = false, authenticated = true, own = false,
  fields = { '*': ['email', 'private'], authenticated: ['private'], 'role:Admin': [], reader: ['private'] },
  originalObject = true, className = '_User', session = true, query = {} } = {}) {
  const object = { className, objectId: own ? user : 'someone-else', email: 'private@example.test',
    private: 'hidden', public: 'visible', authData: { provider: 'private' }, sessionToken: 'private-token',
    owner: { __type: 'Pointer', className: '_User', objectId: user }, _internal: 'private' };
  const res = { object, original: originalObject ? { ...object, public: 'old' } : null };
  const permissions = fields === undefined ? {} : { protectedFields: fields };
  const client = { hasMasterKey: master, getSubscriptionInfo: () => session ? { sessionToken: 'fixture-token' } : undefined };
  const context = {
    config: { databaseURI: 'mongodb://db1:27017,db2:27017/test', appId: 'test' },
    getAuthForSessionToken: async () => ({ userId: authenticated ? user : undefined,
      auth: { user: authenticated ? { id: user } : null, userRoles: roles } }),
  };
  return { context, client, res, permissions, query };
}

async function run(filter, options) {
  const f = fixture(options);
  await filter.call(f.context, f.permissions, f.res, f.client, 1, 'find', f.query);
  return { res: f.res, permissions: f.permissions, query: f.query };
}

async function outcome(filter, options) {
  try {
    return { value: await run(filter, options) };
  } catch (error) {
    return { error: { name: error.name, message: error.message } };
  }
}

test('matches Parse field filtering for each user, role, class and original object', async (t) => {
  const fast = ParseLiveQueryServer.prototype._filterSensitiveData;
  const cases = [
    {}, { authenticated: false }, { session: false }, { user: 'another-reader' },
    { user: 'admin', roles: ['role:Admin'] }, { own: true }, { master: true },
    { master: true, fields: ['private'] },
    { fields: ['private'] }, { fields: null }, { fields: {} },
    { className: 'Message' }, { originalObject: false },
    { query: { objectId: 'reader' } },
    { fields: { '*': ['email', 'private'], 'userField:owner': ['private'] } },
  ];
  for (const options of cases) {
    assert.deepEqual(await outcome(fast, structuredClone(options)), await outcome(original, structuredClone(options)), JSON.stringify(options));
  }
  const nonOwner = await run(fast, {});
  assert.equal(nonOwner.res.object.private, undefined);
  assert.equal(nonOwner.res.original.private, undefined);
  assert.equal(nonOwner.res.object.authData, undefined);
  assert.equal(nonOwner.res.object.sessionToken, undefined);
  assert.equal(nonOwner.res.object.public, 'visible');
});

test('does not construct controllers/adapters; recomputes decisions across users and policy changes', async (t) => {
  const fast = ParseLiveQueryServer.prototype._filterSensitiveData;
  t.mock.method(Controllers, 'getDatabaseController', () => { throw new Error('Unexpected database-controller allocation'); });
  const policy = { '*': ['private'], 'role:Admin': [] };
  const visible = await run(fast, { user: 'admin', roles: ['role:Admin'], fields: structuredClone(policy) });
  const hidden = await run(fast, { user: 'reader', fields: structuredClone(policy) });
  assert.equal(visible.res.object.private, 'hidden');
  assert.equal(hidden.res.object.private, undefined);
  assert.equal((await run(fast, { fields: { '*': [] } })).res.object.private, 'hidden');
  assert.equal((await run(fast, { fields: { '*': ['private'] } })).res.object.private, undefined);
});

test('keeps separate authorization and decisions for overlapping asynchronous deliveries', async (t) => {
  const fast = ParseLiveQueryServer.prototype._filterSensitiveData;
  const cases = Array.from({ length: 30 }, (_, i) => ({ user: `user-${i}`, own: i % 2 === 0 }));
  const expected = await Promise.all(cases.map(options => run(original, options)));
  assert.deepEqual(await Promise.all(cases.map(options => run(fast, options))), expected);
});

test('preserves authentication rejection without delivering partially filtered data', async (t) => {
  const f = fixture();
  f.context.getAuthForSessionToken = async () => { throw new Error('invalid session'); };
  const before = structuredClone(f.res);
  await assert.rejects(ParseLiveQueryServer.prototype._filterSensitiveData.call(f.context, f.permissions, f.res, f.client, 1, 'find', f.query), /invalid session/);
  assert.deepEqual(f.res, before);
});

