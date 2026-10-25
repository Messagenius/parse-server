# Messagenius optimizations

Branch `codex/spikad-optimizations` starts at upstream Parse Server 9.10.0
(`b5ca12fa`) and publishes version `9.10.0-messagenius.1`. The original fork's
`alpha` branch is unchanged. Base-api pins an exact commit of this branch.

## Native implementation

`src/LiveQuery/ParseLiveQueryServer.ts` contains the combined behavior previously
measured on devspikad3: skip subscriptions matching neither object version,
share matching clones except for dotted-key predicates, conservative cached ACL
recipient filtering, reuse the database controller, avoid duplicate identical-token
ACL checks, and fast-path empty predicates. Parse 9 permission checks, protected
fields and event triggers remain in place for recipients that cannot be skipped.

Authentication cache size is configurable through `authCacheSize` (default
10000); its TTL remains `cacheTimeout`. Cached identity shortcuts are removed
when the auth LRU disposes an entry (eviction, expiration, deletion or clear).
Late authentication results cannot repopulate shortcuts for removed promises or
overwrite a newer authentication for the same token.
`src/LiveQuery/FanoutLimiter.js` limits
active callbacks to `MSG_LQ_FANOUT_CONCURRENCY` (default 256), without evicting
queued events. It does not bound total queue memory. Replaced clients and
subscriptions cannot receive events queued for their previous identities.

`src/triggers.js` supports an explicit server-owned User afterFind capability:
`Symbol.for('messagenius.userAfterFind.raw.v1')`. A synchronous capability gets
`(request, objectsInput, Parse)` and either returns normalized response rows or
`null` to use the ordinary callback pipeline. Only `_User` afterFind is eligible;
validators disable the shortcut. Client request context cannot enable it.
Base-api owns the business-specific eligibility checks and normalization.

These are ordinary source changes, compiled by `npm run build`. There are no
source rewrite installers, monkey-patch assignments or runtime source assertions.
Git dependency installation uses the existing `prepare` script to build `lib/`.
A build environment must install development dependencies for preparation.
Runtime packages contain compiled `lib/`, not the test fixtures or this document.

## Validation

Run `npm ci`, then `npm run test:messagenius`. The latter builds and runs isolated
Node regression tests without a database or network. The tests compare native
LiveQuery events and payloads against frozen historical implementations, and
cover ACL transitions, roles, both session tokens, cache lifetime, CLP errors,
protected fields, deletion, queue draining and subscription replacement. They
also cover afterFind capability selection, ordinary fallback and validators.
Base-api separately tests its actual User callback and complete response values.

`spec/regression/fixtures/` contains test-only compiled reference snapshots:

- `livequery-upstream.cjs`: upstream 9.10.0.
- `livequery-baseline.cjs`: skip unmatched subscriptions and enlarged auth cache.
- `livequery-clone.cjs`: baseline plus shared clones with dotted-key fallback.
- `livequery-measured.cjs`: combined implementation used by base-api before this migration.

Reference modules are evaluated in memory using the current package dependencies;
no installed files are overwritten. Historical comments in them describe their
provenance, not an installation procedure. The negative control deliberately
removes the dotted-key fallback in memory to prove the test detects changed
client payloads. Fixtures are excluded by the package's `files` allowlist.

The source migration has not itself been load-tested or deployed to devspikad3.
Previous performance measurements and their limitations remain in base-api docs.
