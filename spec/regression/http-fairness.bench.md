# Continuous LiveQuery fanout / HTTP fairness benchmark

Run with Node 22 and a built Parse Server checkout (`lib/` and dependencies):

```sh
node --expose-gc spec/regression/http-fairness.bench.cjs --root /absolute/path/to/parse-server --rate 8 --seconds 6 --clients 4000 --label sustainable-1
node --expose-gc spec/regression/http-fairness.bench.cjs --root /absolute/path/to/parse-server --rate 25 --seconds 6 --clients 4000 --label overload-1
# Separate allocation trial; do not mix these latencies with the unprofiled runs.
node --expose-gc spec/regression/http-fairness.bench.cjs --root /absolute/path/to/parse-server --rate 25 --seconds 6 --clients 4000 --label overload-profile --profile
```

Each process warms three complete saves and then runs one trial. Repeat each unprofiled scenario three times, sequentially, with the same Node binary and no competing tests. The recorded baseline is the original combined implementation at `d0fbc892`; the after checkout contains the fairness patch. Results are JSON lines, one object per process. No production connection or external service is used. An ephemeral loopback port is required.

## Workload and measurements

- A worker publishes at a fixed open-loop rate for six seconds. The producer continues while the server event loop is blocked. It catches up delayed ticks to the original schedule, and never waits for a save to complete. This avoids a load generator that accidentally self-throttles with the tested event loop.
- One matching query has 4,000 authenticated clients. Half have the allowed role; half fail the real asynchronous ACL path. Role ACL makes the conservative prefilter unavailable; the harness asserts zero prefilter skips. Cached auth and role results replace external database calls. The real save handler, ACL checks, CLP, protected field filtering, object clones, watch handling and Client JSON serialization run. The transport counts serialized WebSocket frames instead of opening 4,000 sockets. Each published update should produce exactly 2,000 frames; errors and denied-client deliveries fail the run.
- The same independent worker sends 50 HTTP GET requests/s to a simple `node:http` handler returning `ok`. `httpMs` is response latency from request launch; `httpScheduledMs` additionally includes launch delay from the intended schedule. Requests continue to completion after the six-second load interval. The keep-alive agent allows 1,024 sockets, above the total 300 probes, so an agent concurrency cap does not hide the server stall.
- A 10ms main-thread timer records lateness (`actual interval - 10ms`). Its count is included: under starvation a percentile may contain very few samples, so always inspect max and count too.
- The worker samples total unfinished callback-equivalent work every 5ms from shared counters: `generated saves × 4,000 - completed callbacks`. This includes unread ingress messages, event producers, scheduled tasks and active callbacks. Queue growth is a least-squares slope over the final 75% of the source interval. Peak can undercount sub-5ms transients. `atHalf` and `atSourceEnd` use nearest samples. Final backlog is asserted zero; drain is measured from the scheduled source stop until both fanout and HTTP probes finish, with up to one 5ms observer tick of slack.
- Limiter statistics are separate: the old limiter's small callback queue does not include ingress backlog; the new implementation also tracks pending event producers. These internal queue units must not be compared as if equivalent. `limiterOutstandingPeak` counts callbacks submitted through `run()` but not yet resolved; it excludes unenumerated recipients. Throughput divides all completed callbacks by actual load-plus-drain elapsed time.
- CPU is `process.cpuUsage()` over actual elapsed time and includes the worker thread, HTTP probing and instrumentation. It can exceed 100% of one core. This is not isolated server-thread CPU. Forced-GC heap before/after includes the warmed server and harness; negative deltas may reflect warmup garbage/compaction. Only six seconds of load is measured, so this is not a leak or long-term capacity test.
- Allocation trials use inspector `HeapProfiler.startSampling`, interval 32KiB, including objects collected by both minor and major GC. The sum of sample sizes is an estimate of allocation volume, not heap growth. `actualDurationMs` runs from completed sampler activation to the stop-sampling request; it includes actual drain, not merely the configured six seconds. Profile materialization is excluded from that denominator and the returned profile is released before retained-heap GC. Optional `--profile-out /path/file.json` saves the raw profile. Sampling has overhead, so these are separate runs.

The overload case deliberately exceeds service capacity. A fair event loop can preserve HTTP responsiveness while unfinished work still grows. No finite-memory guarantee, admission control, durable queue or dropping policy is implied by this benchmark. Database/Redis latency, real WebSocket backpressure, network topology, cloud-code triggers and multi-process contention are outside this in-process harness. This benchmark validates the diagnosed CPU/microtask fairness failure and unchanged delivery counts under controlled load; it does not predict exact production latency.

## Recorded results, 2026-10-26

Node v22.14.0 on the local macOS host. Three fresh-process unprofiled trials per row; p95 columns show the median trial p95, max columns show the worst trial maximum. Throughput, growth, drain and CPU show trial medians; backlog peak shows the worst trial peak. All 19 trials (15 latency trials + 4 allocation trials) completed all expected fanout work and HTTP requests, with exactly half as many frames as callbacks, zero errors, zero unauthorized frames and zero prefilter skips. Files in `benchmark-results/` contain all trial measurements. `after-overload` retains the original scenario name for the identical 25 saves/s comparison; the corrected implementation can sustain that rate. `after-heavy` is additional overload at 75 saves/s. All after measurements use the final recipient lifetime-token implementation, which does not capture internal linked-list nodes in queued callbacks.

| Version/scenario | Saves/s | HTTP p95 / max ms | Timer p95 / max ms | Completed k tasks/s | Backlog peak tasks | Growth k tasks/s | Drain ms | CPU % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline-sustainable | 8 | 59.26 / 221.46 | 55.59 / 217.78 | 32.0 | 4,976 | ~0 | 3 | 65.7 |
| after-sustainable | 8 | 0.89 / 5.12 | 1.29 / 2.11 | 32.0 | 4,000 | ~0 | 1 | 28.1 |
| baseline-overload | 25 | 8257.53 / 8705.51 | 8529.54 / 8701.52 | 70.0 | 189,110 | 28.6 | 2566 | 112.6 |
| after-overload | 25 | 0.95 / 8.31 | 1.27 / 6.31 | 99.9 | 13,110 | ~0 | 3 | 74.8 |
| after-heavy | 75 | 0.89 / 25.54 | 0.18 / 39.12 | 133.0 | 1,034,563 | 171.6 | 7525 | 95.8 |

At 25 saves/s, baseline timer callbacks ran only four times over roughly 8.4–8.7 seconds; this explains why its timer p95 is essentially its single long stall. The internal baseline limiter reported only 3,744 queued callbacks even while end-to-end backlog reached 189,110: unconsumed producer messages account for the hidden work.

The 75 saves/s trials explicitly exceed corrected service capacity: offered work is 300,000 callbacks/s while completion capacity is 124,897–136,277/s. HTTP p95 remains 0.68–0.98ms, but unfinished work reaches 1,034,563 callback equivalents and requires 7.20–8.41 seconds to drain after publishing stops. This is evidence of event-loop fairness, not a bounded queue or sustainable overload. No events are dropped. Repeat this level using `--rate 75 --seconds 6 --clients 4000`.

### Allocation and retained heap

Separate inspector trial for each before/after workload; decimal MB/GB. These are sampled estimates, not exact byte counters.

| Version/scenario | Completed tasks | Sample duration s | Estimated allocated GB | Estimated GB/s | Estimated bytes/task | Post-GC heap MB | Heap delta MB |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline-sustainable | 192,000 | 6.009 | 3.700 | 0.616 | 19,269 | 42.67 | -3.18 |
| after-sustainable | 192,000 | 6.008 | 2.676 | 0.445 | 13,937 | 48.34 | +1.06 |
| baseline-overload | 600,000 | 9.290 | 11.566 | 1.245 | 19,277 | 43.49 | -2.37 |
| after-overload | 600,000 | 6.007 | 8.389 | 1.397 | 13,981 | 48.37 | +1.09 |

Estimated allocation volume per delivered workload falls 27.5–27.7%. At 25 saves/s, the corrected version finishes work sooner, so total bytes and bytes/task are more useful comparisons than bytes/s alone. Unprofiled post-GC heap is 42.30–42.90MB before and 48.12–48.15MB after. Warmup-to-final heap deltas after the fix are +0.84–+0.86MB. The short run and differing warmup/JIT retention do not establish a leak or long-term memory bound. CPU is whole-process including worker, so percentages above 100% are possible.
