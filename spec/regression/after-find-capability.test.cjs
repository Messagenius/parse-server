'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const Parse = require('parse/node');
const triggers = require('../../lib/triggers');
const key = Symbol.for('messagenius.userAfterFind.raw.v1');
const app = 'native-after-find-regression';
Parse.initialize(app);
Parse.Object.disableSingleInstance();
const config = {
  applicationId: app,
  logLevels: { triggerBeforeSuccess: 'silent', triggerAfter: 'silent' },
};
const rows = [
  { objectId: 'a', name: 'A' },
  { objectId: 'b', name: 'B' },
];
const run = (className = '_User', context = {}) =>
  triggers.maybeRunAfterFindTrigger(
    triggers.Types.afterFind,
    { isMaster: false },
    className,
    rows,
    config,
    { where: {} },
    context,
    false
  );

test('explicit server capability returns normalized output without running the no-op hook', async () => {
  let calls = 0,
    fast = 0;
  const hook = request => {
    calls++;
    return request.objects;
  };
  triggers.addTrigger(triggers.Types.afterFind, '_User', hook, app);
  const expected = await run();
  hook[key] = (request, input, sdk) => {
    fast++;
    assert.equal(request.query.className, '_User');
    assert.equal(sdk, Parse);
    return input.map(row => ({ ...row, className: '_User' }));
  };
  assert.deepEqual(await run(), expected);
  assert.equal(calls, 1);
  assert.equal(fast, 1);
});

test('declined or missing capability and other classes retain ordinary hooks', async () => {
  let calls = 0,
    fast = 0;
  const hook = request => {
    calls++;
    return request.objects;
  };
  triggers.addTrigger(triggers.Types.afterFind, '_User', hook, app);
  await run('_User', { skip: true, 'messagenius.userAfterFind.raw.v1': true });
  hook[key] = () => {
    fast++;
    return null;
  };
  await run();
  triggers.addTrigger(triggers.Types.afterFind, 'Other', hook, app);
  await run('Other');
  assert.equal(calls, 3);
  assert.equal(fast, 1);
});

test('validators execute with SDK objects and preserve rejection', async () => {
  let fast = 0,
    validations = 0;
  const hook = request => request.objects;
  hook[key] = () => {
    fast++;
    return [];
  };
  triggers.addTrigger(triggers.Types.afterFind, '_User', hook, app, request => {
    validations++;
    assert(request.objects.every(o => o instanceof Parse.Object));
  });
  assert.equal((await run()).length, 2);
  assert.equal(validations, 1);
  assert.equal(fast, 0);
  triggers.addTrigger(triggers.Types.afterFind, '_User', hook, app, () => {
    throw new Parse.Error(119, 'Denied');
  });
  await assert.rejects(run(), { code: 119, message: 'Denied' });
  assert.equal(fast, 0);
});
