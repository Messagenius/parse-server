# Spikad optimizations on Parse Server 7

Branch `codex/spikad-optimizations-7` starts at the official **7.5.4** tag,
commit `c9333dd7bc45eb9009649358db78ca66c6b8a2d5`, the version found in the
previous local Spikad installation. Package version: `7.5.4-messagenius.1`.
The dependency versions and Node support range of that tag are retained.
This is a native source fork, compiled by the normal Git dependency `prepare`
script; it does not install a runtime monkey patch or require a source hash.

## Separate changes

1. **Protected fields:** call Parse 7's stateless `addProtectedFields` method
   directly. No database controller/adapter is constructed for this calculation.
   Field policies are recomputed for each recipient and object, never cached.
2. **Delivery safety:** backport the recipient isolation and ACL transition
   handling present in upstream Parse 9.10.0. Each recipient has separate current
   and original objects; `afterEvent` changes are serialized and filtered before
   sending. A false CLP check now stops delivery on save and delete. ACL loss
   delivers only the last readable state; ACL gain does not expose an unreadable
   original. These are intentional fixes to 7.5.4, not performance equivalence.
3. **Matching:** skip subscriptions matching neither state, use one `deepcopy`
   per state for matching, retain fresh copies for dotted predicates (including
   logical branches), and fast-path empty queries. Recipient payloads remain
   separate. The Parse 7 matcher and its copying library are retained.
4. **Authentication and ACL prefilter:** configurable auth cache size (10000 by
   default), conservative rejection using both subscription and connection tokens,
   and deduplication of identical-token ACL checks. Public/role ACLs, restricted
   CLPs, missing/expired identities and uncertainty use the ordinary checks.
   Identity shortcuts belong to the exact LRU promise: disposal removes them and
   late authentication success/failure cannot revive or overwrite newer entries.
5. **Fanout:** at most 256 active callbacks per LiveQuery instance by default.
   The queue drains without expiry or overflow drops; callbacks targeting a
   replaced client/subscription are discarded rather than sent to its replacement.
   This bounds active work, not total queue memory or delivery ordering.

## Configuration

- `authCacheSize`: maximum LiveQuery authentication entries, default 10000.
- `cacheTimeout`: existing Parse TTL, unchanged. Base-api sets 60000 ms.
- `MSG_LQ_FANOUT_CONCURRENCY`: positive integer environment variable, default 256.

The protected-fields optimization is native and always present: the separate
local patch's `MSG_LIVEQUERY_PROTECTED_FIELDS_FAST_PATH` flag is not required.
Rollback is a dependency revision/consumer lockfile change followed by rebuild
and worker restart. No database migration runs when this package is installed.

## Differences from the Parse 9 variant

The counterpart is `codex/spikad-optimizations`, based on 9.10.0. Both forks have
the LiveQuery optimizations and auth-cache lifetime fix above. This branch keeps
Parse 7's dependencies, CLP implementation, matcher and protected-field rules;
it does not pretend to backport the entire Parse 9 security or feature history.
In particular, Parse 9's controller uses `protectedFieldsOwnerExempt` state,
whereas Parse 7's stateless method can be called without an instance.

The Parse 9 User afterFind raw-response capability is **not included**. Parse 7
has a different trigger pipeline and SDK normalization contract; its ordinary
User hook remains in place. The independent base-api permission-query and
relation-index optimization is available on both release paths.

## Validation

`npm ci` builds the fork. `npm run test:messagenius` rebuilds and runs **27**
regressions without a database or listening server, using actual Parse 7 modules.
The differential matching suite compares the optimized code with a frozen
7.5.4 reference containing only protected-field and delivery-safety fixes. It
covers operators, dates, dotted predicates, query order, ACL transitions,
protected fields and concurrent recipients. A deliberately broken dotted-query
variant demonstrates that the tests detect changed deliveries. Separate tests
exercise the production LRU, late authentication, fanout queue draining and
replacement, denied CLPs and concurrent afterEvent hooks.

`fixtures/livequery-7.5.4.cjs` is the original compiled tag;
`fixtures/livequery-baseline.cjs` is the compiled version after the delivery
safety commit. Fixtures are excluded from the published package. ESLint passes
for the changed runtime files. Options are generated with `npm run definitions`.
Validation used Node 22.14.0. No production deployment, database migration or
load benchmark is implied by these tests; the Parse 9 benchmark numbers do not
measure this Parse 7 branch.
