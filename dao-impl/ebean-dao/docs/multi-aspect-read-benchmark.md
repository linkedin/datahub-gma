# Multi-aspect read improvement benchmark (META-24100)

Old-vs-new latency + query-count for a `batchGetUnion`-style multi-aspect read, quantifying the improvement of the
bundled multi-aspect read shipped in [PR #642](https://github.com/linkedin/datahub-gma/pull/642) over master's current
per-aspect read shape. This is the companion "how much do we gain" number to the current-state baseline in
[PR #640](https://github.com/linkedin/datahub-gma/pull/640).

## What it measures

`MultiAspectReadBenchmarkTest` provisions a synthetic entity table (`metadata_entity_foo`) with 73 JSON aspect columns
and one seeded row per URN, then measures two read shapes back-to-back **on the same seeded data** so their latencies
are directly comparable:

- **current** (master today) — one `SELECT` per aspect column with the `JSON_EXTRACT(col, '$.gma_deleted') IS NULL`
  soft-delete filter (exactly as `SQLStatementUtils.createAspectReadSql` emits) => 73 round-trips per logical read.
- **multi-aspect** (PR #642) — a single bundled `SELECT` of all requested aspect columns per entity table with a
  row-level `deleted_ts IS NULL` filter (the shape `SQLStatementUtils.createMultiAspectReadSql` emits) => 1 round-trip
  per logical read.

The read batch URNs are **not hardcoded** — they are loaded dynamically at runtime from the classpath resource
`dao-impl/ebean-dao/src/test/resources/benchmark-urns.txt` (one URN per line; blank lines and `#` comments ignored), so
the URN set can be edited without recompiling the test. Override the resource with `-Dgma.benchmark.urnFile=<name>`.

> **This benchmark runs entirely against a local, in-process database.** It uses an embedded MariaDB (MariaDB4j) started
> inside the test JVM — no shared/remote database and no network hop. Absolute latencies therefore reflect a local
> single-node DB and are **not** prod-representative; the meaningful takeaways are the per-read query-count collapse and
> the relative speedup.

- **DB SELECT count** — MariaDB `Com_select` session-status delta (connection pinned in a txn).
- **Latency** — p50 / p90 / p99 / max over 200 iterations (20 warmup).

## Results

```
URNs=30 aspectsPerUrn=73 totalKeys=2190  (over 200 iters)
path         | DB SELECTs/read  | p50 ms    | p90 ms    | p99 ms    | max ms
current      | 73               | 26.985    | 30.969    | 34.415    | 37.135
multi-aspect | 1                | 1.139     | 1.216     | 1.375     | 4.180
improvement: DB SELECTs/read 73 -> 1 (73.0x fewer)   p50 latency 95.8% lower (23.70x faster)
```

| Path                | DB SELECTs / read | p50       | p90       | p99       | max       |
| ------------------- | ----------------- | --------- | --------- | --------- | --------- |
| current (master)    | **73**            | 26.985 ms | 30.969 ms | 34.415 ms | 37.135 ms |
| multi-aspect (#642) | **1**             | 1.139 ms  | 1.216 ms  | 1.375 ms  | 4.180 ms  |

**Improvement:** query count collapses from **73 SELECTs → 1 SELECT** (73× fewer round-trips); p50 latency drops
**95.8%** (≈**23.7× faster**) and p99 latency drops from **34.415 ms → 1.375 ms** (≈**25× faster**) for a 73-aspect
entity.

Absolute latency is not prod-representative (in-process DB, no network); the meaningful takeaway is the per-read
query-count collapse (73 → 1) and the relative speedup, which is exactly what the multi-aspect read in PR #642 delivers.

## How to run

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 11.0.21)
./gradlew :dao-impl:ebean-dao:test --tests '*MultiAspectReadBenchmarkTest*' -Dgma.benchmark=true
```

> On Apple Silicon, enable the 3 documented `configurationBuilder` lines in `EmbeddedMariaInstance` (points MariaDB4j at
> a Homebrew `mariadb` install) since the bundled x86_64 MariaDB 10.2.11 needs OpenSSL 1.0. This is a local-only test
> harness tweak and is not committed.
