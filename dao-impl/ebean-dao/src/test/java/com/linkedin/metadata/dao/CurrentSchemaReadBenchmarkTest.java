package com.linkedin.metadata.dao;

import com.google.common.io.Resources;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.dao.utils.SQLStatementUtils;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.SQLStatementUtils.SOFT_DELETED_CHECK;
import static com.linkedin.metadata.dao.utils.SQLStatementUtils.DELETED_TS_IS_NULL_CHECK;
import static com.linkedin.testing.TestUtils.*;

/**
 * Current-baseline latency + query-count benchmark for a multi-aspect {@code batchGetUnion} read
 * using <b>only the code that exists on {@code master} today</b> (META-24100 context, before the
 * <a href="https://github.com/linkedin/datahub-gma/pull/622">PR #622</a> single-query optimization).
 *
 * <p>This intentionally measures just the <b>current</b> read shape (no comparison against the
 * not-yet-merged multi-aspect path): one {@code SELECT} per aspect column with the
 * {@code JSON_EXTRACT(col, '$.gma_deleted') IS NULL} soft-delete filter, exactly as
 * {@code SQLStatementUtils.createAspectReadSql} emits on master =&gt; {@code NUM_ASPECTS} round-trips
 * per logical read.
 *
 * <p>Metrics for the current path:
 * <ul>
 *   <li><b>DB SELECT count</b> — MariaDB {@code Com_select} session-status delta (connection pinned
 *       in a transaction) for one logical read.</li>
 *   <li><b>Latency</b> — p50 / p90 / max over {@link #ITERATIONS} iterations.</li>
 * </ul>
 *
 * <p>Absolute latency is not prod-representative (in-process DB, no network); the meaningful
 * takeaway is today's per-read query count ({@code NUM_ASPECTS} SELECTs) and its latency. Skipped in
 * the normal suite/CI; run explicitly with {@code -Dgma.benchmark=true}:
 *
 * <pre>
 *   ./gradlew :dao-impl:ebean-dao:test --tests '*CurrentSchemaReadBenchmarkTest*' -Dgma.benchmark=true
 * </pre>
 */
public class CurrentSchemaReadBenchmarkTest {

  private static final int NUM_ASPECTS = 73;   // aspect columns per entity (matches PR #622's Dataset example)
  private static final int NUM_URNS = 50;      // URNs in the batch
  private static final int WARMUP = 20;        // warmup iterations (JIT + connection pool priming)
  private static final int ITERATIONS = 200;   // measured iterations

  private static final String TABLE = "metadata_entity_foo";
  private static final String COLUMN_PREFIX = "a_bench";

  private static EbeanServer _server;

  @BeforeClass
  public void init() {
    _server = EmbeddedMariaInstance.getServer(CurrentSchemaReadBenchmarkTest.class.getSimpleName());
  }

  @BeforeMethod
  public void setup() throws Exception {
    _server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource("ebean-local-access-create-all.sql"), StandardCharsets.UTF_8)));
    for (int a = 0; a < NUM_ASPECTS; a++) {
      _server.execute(Ebean.createSqlUpdate("ALTER TABLE " + TABLE + " ADD COLUMN " + COLUMN_PREFIX + a + " JSON"));
    }
    final String colList = String.join(", ", aspectColumns());
    for (int i = 0; i < NUM_URNS; i++) {
      final String values = aspectColumns().stream()
          .map(c -> "JSON_OBJECT('value', '" + c + "_v')")
          .collect(Collectors.joining(", "));
      _server.execute(Ebean.createSqlUpdate(
          "INSERT INTO " + TABLE + " (urn, lastmodifiedon, lastmodifiedby, " + colList + ") VALUES ('"
              + makeFooUrn(i) + "', NOW(), 'actor', " + values + ")"));
    }
  }

  @Test
  public void benchmarkCurrentMasterRead() {
    if (!Boolean.getBoolean("gma.benchmark")) {
      throw new org.testng.SkipException("benchmark disabled; enable with -Dgma.benchmark=true");
    }

    final Set<String> aspectColumns = new LinkedHashSet<>(aspectColumns());
    final Set<Urn> urns = new LinkedHashSet<>();
    for (int i = 0; i < NUM_URNS; i++) {
      urns.add(makeFooUrn(i));
    }

    final List<String> currentSqls = buildCurrentPerAspectSqls(aspectColumns, urns);

    for (int w = 0; w < WARMUP; w++) {
      runCurrentPath(currentSqls);
    }

    final long currentSelects;
    try (io.ebean.Transaction txn = _server.beginTransaction()) {
      final long before = comSelect();
      runCurrentPath(currentSqls);
      currentSelects = comSelect() - before;
      txn.commit();
    }

    final long[] currentNanos = time(() -> runCurrentPath(currentSqls));

    System.out.println("============ current master multi-aspect read benchmark ============");
    System.out.printf("URNs=%d aspectsPerUrn=%d totalKeys=%d  (over %d iters)%n",
        NUM_URNS, NUM_ASPECTS, NUM_URNS * NUM_ASPECTS, ITERATIONS);
    System.out.printf("%-8s | %-16s | %-9s | %-9s | %-9s%n", "path", "DB SELECTs/read", "p50 ms", "p90 ms", "max ms");
    System.out.printf("%-8s | %-16d | %-9.3f | %-9.3f | %-9.3f%n",
        "current", currentSelects, toMs(pct(currentNanos, 0.50)), toMs(pct(currentNanos, 0.90)),
        toMs(currentNanos[ITERATIONS - 1]));
    System.out.println("====================================================================");
  }

  /**
   * Replicates {@code SQLStatementUtils.createAspectReadSql} output for one aspect column (master's
   * current per-aspect read shape).
   */
  private static List<String> buildCurrentPerAspectSqls(Set<String> aspectColumns, Set<Urn> urns) {
    final String urnList = urns.stream()
        .map(u -> "'" + SQLStatementUtils.escapeReservedCharInUrn(u.toString()) + "'")
        .collect(Collectors.joining(", "));
    final List<String> sqls = new ArrayList<>();
    for (String col : aspectColumns) {
      final String softDeleteCheck = String.format(SOFT_DELETED_CHECK, col);
      sqls.add("SELECT urn, " + col + ", lastmodifiedon, lastmodifiedby FROM " + TABLE
          + " WHERE " + softDeleteCheck + " AND urn IN (" + urnList + ") AND " + DELETED_TS_IS_NULL_CHECK);
    }
    return sqls;
  }

  private static void runCurrentPath(List<String> sqls) {
    for (String sql : sqls) {
      _server.createSqlQuery(sql).findList();
    }
  }

  private static long[] time(Runnable r) {
    final long[] samples = new long[ITERATIONS];
    for (int i = 0; i < ITERATIONS; i++) {
      final long start = System.nanoTime();
      r.run();
      samples[i] = System.nanoTime() - start;
    }
    Arrays.sort(samples);
    return samples;
  }

  private static List<String> aspectColumns() {
    final List<String> cols = new ArrayList<>();
    for (int a = 0; a < NUM_ASPECTS; a++) {
      cols.add(COLUMN_PREFIX + a);
    }
    return cols;
  }

  private static long pct(long[] sorted, double p) {
    return sorted[(int) (sorted.length * p)];
  }

  private static double toMs(long nanos) {
    return nanos / 1_000_000.0;
  }

  private static long comSelect() {
    final List<SqlRow> rows = _server.createSqlQuery("SHOW SESSION STATUS LIKE 'Com_select'").findList();
    return rows.isEmpty() ? -1 : Long.parseLong(rows.get(0).getString("Value"));
  }
}
