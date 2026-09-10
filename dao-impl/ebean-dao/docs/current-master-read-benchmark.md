# Current-master multi-aspect read baseline (META-24100)

Baseline latency + query-count for a `batchGetUnion`-style multi-aspect read using **only the code on `master` today**
(i.e. before the [PR #622](https://github.com/linkedin/datahub-gma/pull/622) single-query optimization). This is the
current-state number, not an old-vs-new comparison.

## What it measures

`CurrentSchemaReadBenchmarkTest` provisions a synthetic entity table (`metadata_entity_foo`) with 73 JSON aspect columns
and one seeded row per URN, then issues master's current read shape: **one `SELECT` per aspect column** with the
`JSON_EXTRACT(col, '$.gma_deleted') IS NULL` soft-delete filter (exactly as `SQLStatementUtils.createAspectReadSql`
emits) => 73 round-trips per logical read.

The read batch URNs are **not hardcoded** — they are loaded dynamically at runtime from the classpath resource
`dao-impl/ebean-dao/src/test/resources/benchmark-urns.txt` (one URN per line; blank lines and `#` comments ignored), so
the URN set can be edited without recompiling the test. Override the resource with `-Dgma.benchmark.urnFile=<name>`.

> **This benchmark runs entirely against a local, in-process database.** It uses an embedded MariaDB (MariaDB4j) started
> inside the test JVM — no shared/remote database and no network hop. Absolute latencies therefore reflect a local
> single-node DB and are **not** prod-representative; the meaningful takeaway is the per-read query count.

- **DB SELECT count** — MariaDB `Com_select` session-status delta (connection pinned in a txn).
- **Latency** — p50 / p90 / max over 200 iterations (20 warmup).

## Results

```
URNs=50 aspectsPerUrn=73 totalKeys=3650  (over 200 iters)
path     | DB SELECTs/read  | p50 ms    | p90 ms    | max ms
current  | 73               | 28.671    | 32.456    | 42.806
```

| Path             | DB SELECTs / read | p50       | p90       | max       |
| ---------------- | ----------------- | --------- | --------- | --------- |
| current (master) | **73**            | 28.671 ms | 32.456 ms | 42.806 ms |

Absolute latency is not prod-representative (in-process DB, no network); the meaningful takeaway is today's per-read
query count (**73 SELECTs** for a 73-aspect entity) and its latency, which is the baseline PR #622 aims to collapse to 1
SELECT.

## How to run

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 11.0.21)
./gradlew :dao-impl:ebean-dao:test --tests '*CurrentSchemaReadBenchmarkTest*' -Dgma.benchmark=true
```

> On Apple Silicon, enable the 3 documented `configurationBuilder` lines in `EmbeddedMariaInstance` (points MariaDB4j at
> a Homebrew `mariadb` install) since the bundled x86_64 MariaDB 10.2.11 needs OpenSSL 1.0. This is a local-only test
> harness tweak and is not committed.
