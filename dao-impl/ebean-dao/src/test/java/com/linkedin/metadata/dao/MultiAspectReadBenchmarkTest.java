package com.linkedin.metadata.dao;

import com.google.common.io.Resources;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.dao.utils.SQLStatementUtils;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.SQLStatementUtils.SOFT_DELETED_CHECK;
import static com.linkedin.metadata.dao.utils.SQLStatementUtils.DELETED_TS_IS_NULL_CHECK;


/**
 * Old-vs-new latency + query-count benchmark for a multi-aspect {@code batchGetUnion} read that
 * quantifies the improvement of the bundled multi-aspect read (META-24100 /
 * <a href="https://github.com/linkedin/datahub-gma/pull/642">PR #642</a>) over master's current
 * per-aspect read shape.
 *
 * <p>Both paths run against the same seeded synthetic table so their latencies are directly
 * comparable. Two read shapes are measured back-to-back on identical data:
 * <ul>
 *   <li><b>current</b> (master today) — one {@code SELECT} per aspect column with the
 *       {@code JSON_EXTRACT(col, '$.gma_deleted') IS NULL} soft-delete filter (exactly as
 *       {@code SQLStatementUtils.createAspectReadSql} emits) =&gt; {@code NUM_ASPECTS} round-trips
 *       per logical read.</li>
 *   <li><b>multi-aspect</b> (PR #642) — a single bundled {@code SELECT} of all requested aspect
 *       columns per entity table with a row-level {@code deleted_ts IS NULL} filter (the shape
 *       {@code SQLStatementUtils.createMultiAspectReadSql} emits) =&gt; 1 round-trip per logical
 *       read.</li>
 * </ul>
 *
 * <p>Metrics per path:
 * <ul>
 *   <li><b>DB SELECT count</b> — MariaDB {@code Com_select} session-status delta (connection pinned
 *       in a transaction) for one logical read.</li>
 *   <li><b>Latency</b> — p50 / p90 / p99 / max over {@link #ITERATIONS} iterations.</li>
 * </ul>
 *
 * <p>The final report also prints the <b>improvement</b>: query-count reduction (N&nbsp;&rarr;&nbsp;1)
 * and the p50 latency speedup factor.
 *
 * <p>Absolute latency is not prod-representative (in-process DB, no network); the meaningful
 * takeaways are the per-read query-count collapse and the relative speedup. Skipped in the normal
 * suite/CI; run explicitly with {@code -Dgma.benchmark=true}:
 *
 * <pre>
 *   ./gradlew :dao-impl:ebean-dao:test --tests '*MultiAspectReadBenchmarkTest*' -Dgma.benchmark=true
 * </pre>
 */
public class MultiAspectReadBenchmarkTest {

  private static final int NUM_ASPECTS = 73;   // aspect columns per entity (matches PR #622's Dataset example)

  // Classpath resource holding the read batch URNs (one per line; blank lines and '#' comments
  // ignored). Externalized so the URN set can be edited without recompiling the test. Override the
  // resource name with -Dgma.benchmark.urnFile=<name>.
  private static final String URN_RESOURCE =
      System.getProperty("gma.benchmark.urnFile", "benchmark-urns.txt");

  // Real production dataset URNs (metadata identifiers only) loaded dynamically at runtime so the
  // issued queries and logs mirror real-world URN shapes. Data volume is still synthetic.
  private static final String[] REAL_URN_STRINGS = loadUrnStrings(URN_RESOURCE);
  private static final int NUM_URNS = REAL_URN_STRINGS.length;   // URNs in the batch (real dataset URNs)
  private static final int WARMUP = 20;        // warmup iterations (JIT + connection pool priming)
  private static final int ITERATIONS = 200;   // measured iterations

  private static final String TABLE = "metadata_entity_foo";
  private static final String COLUMN_PREFIX = "a_bench";

  private static EbeanServer _server;
  private static PrintWriter _log;
  private static Path _logPath;

  @BeforeClass
  public void init() {
    _server = EmbeddedMariaInstance.getServer(MultiAspectReadBenchmarkTest.class.getSimpleName());
    openLog();
  }

  @AfterClass
  public void tearDown() {
    if (_log != null) {
      _log.flush();
      _log.close();
    }
  }

  @BeforeMethod
  public void setup() throws Exception {
    _server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource("ebean-local-access-create-all.sql"), StandardCharsets.UTF_8)));
    log("=== SCHEMA SETUP ===");
    log("Base table created from ebean-local-access-create-all.sql: " + TABLE);
    // Real dataset URNs exceed the test schema's VARCHAR(100) urn column; widen to match prod-scale urn lengths.
    _server.execute(Ebean.createSqlUpdate("ALTER TABLE " + TABLE + " MODIFY urn VARCHAR(512) NOT NULL"));
    for (int a = 0; a < NUM_ASPECTS; a++) {
      final String ddl = "ALTER TABLE " + TABLE + " ADD COLUMN " + COLUMN_PREFIX + a + " JSON";
      _server.execute(Ebean.createSqlUpdate(ddl));
    }
    log("Added " + NUM_ASPECTS + " JSON aspect columns: " + COLUMN_PREFIX + "0 .. " + COLUMN_PREFIX + (NUM_ASPECTS - 1));
    final String colList = String.join(", ", aspectColumns());
    for (int i = 0; i < NUM_URNS; i++) {
      final String values = aspectColumns().stream()
          .map(c -> "JSON_OBJECT('value', '" + c + "_v')")
          .collect(Collectors.joining(", "));
      _server.execute(Ebean.createSqlUpdate(
          "INSERT INTO " + TABLE + " (urn, lastmodifiedon, lastmodifiedby, " + colList + ") VALUES ('"
              + benchUrn(i) + "', NOW(), 'actor', " + values + ")"));
    }
    log("Seeded " + NUM_URNS + " rows. URNs:");
    for (int i = 0; i < NUM_URNS; i++) {
      log("  [" + i + "] " + benchUrn(i));
    }
  }

  @Test
  public void benchmarkMultiAspectReadImprovement() {
    if (!Boolean.getBoolean("gma.benchmark")) {
      throw new org.testng.SkipException("benchmark disabled; enable with -Dgma.benchmark=true");
    }

    final Set<String> aspectColumns = new LinkedHashSet<>(aspectColumns());
    final Set<Urn> urns = new LinkedHashSet<>();
    for (int i = 0; i < NUM_URNS; i++) {
      urns.add(benchUrn(i));
    }

    final List<String> currentSqls = buildCurrentPerAspectSqls(aspectColumns, urns);
    final String multiAspectSql = buildMultiAspectSql(aspectColumns, urns);

    // Log the SQL issued by each path for one logical read (N per-aspect SELECTs vs 1 bundled SELECT).
    log("");
    log("=== CURRENT PATH QUERIES (one logical read = " + currentSqls.size() + " SELECTs) ===");
    for (int q = 0; q < currentSqls.size(); q++) {
      log("Q" + q + ": " + currentSqls.get(q));
    }
    log("");
    log("=== MULTI-ASPECT PATH QUERY (one logical read = 1 SELECT) ===");
    log("Q0: " + multiAspectSql);

    // Log the actual rows returned by the multi-aspect query as a sample of "what result we get".
    log("");
    log("=== SAMPLE RESULT (rows returned by multi-aspect Q0) ===");
    final List<SqlRow> sample = _server.createSqlQuery(multiAspectSql).findList();
    log("Multi-aspect query returned " + sample.size() + " rows. First up to 5:");
    for (int r = 0; r < Math.min(5, sample.size()); r++) {
      final SqlRow row = sample.get(r);
      log("  row[" + r + "] urn=" + row.getString("urn") + "  " + COLUMN_PREFIX + "0=" + row.getString(COLUMN_PREFIX + "0")
          + "  lastmodifiedon=" + row.get("lastmodifiedon"));
    }

    // Warm up both paths (JIT + connection pool + query plan cache) before measuring.
    for (int w = 0; w < WARMUP; w++) {
      runCurrentPath(currentSqls);
      runMultiAspectPath(multiAspectSql);
    }

    final long currentSelects = countSelects(() -> runCurrentPath(currentSqls));
    final long multiAspectSelects = countSelects(() -> runMultiAspectPath(multiAspectSql));

    final long[] currentNanos = time(() -> runCurrentPath(currentSqls));
    final long[] multiAspectNanos = time(() -> runMultiAspectPath(multiAspectSql));

    final double currentP50 = toMs(pct(currentNanos, 0.50));
    final double multiAspectP50 = toMs(pct(multiAspectNanos, 0.50));
    final double p50Speedup = multiAspectP50 > 0 ? currentP50 / multiAspectP50 : Double.NaN;
    final double p50ReductionPct = currentP50 > 0 ? (1.0 - (multiAspectP50 / currentP50)) * 100.0 : Double.NaN;

    final String header = "============ multi-aspect read improvement benchmark ============";
    final String line1 = String.format("URNs=%d aspectsPerUrn=%d totalKeys=%d  (over %d iters)",
        NUM_URNS, NUM_ASPECTS, NUM_URNS * NUM_ASPECTS, ITERATIONS);
    final String colHdr = String.format("%-12s | %-16s | %-9s | %-9s | %-9s | %-9s",
        "path", "DB SELECTs/read", "p50 ms", "p90 ms", "p99 ms", "max ms");
    final String currentRow = String.format("%-12s | %-16d | %-9.3f | %-9.3f | %-9.3f | %-9.3f",
        "current", currentSelects, currentP50, toMs(pct(currentNanos, 0.90)), toMs(pct(currentNanos, 0.99)),
        toMs(currentNanos[ITERATIONS - 1]));
    final String multiRow = String.format("%-12s | %-16d | %-9.3f | %-9.3f | %-9.3f | %-9.3f",
        "multi-aspect", multiAspectSelects, multiAspectP50, toMs(pct(multiAspectNanos, 0.90)),
        toMs(pct(multiAspectNanos, 0.99)), toMs(multiAspectNanos[ITERATIONS - 1]));
    final String improve = String.format(
        "improvement: DB SELECTs/read %d -> %d (%.1fx fewer)   p50 latency %.1f%% lower (%.2fx faster)",
        currentSelects, multiAspectSelects,
        multiAspectSelects > 0 ? (double) currentSelects / multiAspectSelects : Double.NaN,
        p50ReductionPct, p50Speedup);
    final String footer = "=================================================================";

    log("");
    log("=== BENCHMARK RESULT ===");
    for (String s : new String[] {header, line1, colHdr, currentRow, multiRow, improve, footer}) {
      System.out.println(s);
      log(s);
    }
    if (_logPath != null) {
      System.out.println("Full run log written to: " + _logPath.toAbsolutePath());
    }
  }

  /**
   * Opens a per-run log file under the (git-ignored) Gradle {@code build/} directory.
   */
  private static void openLog() {
    try {
      final Path dir = Paths.get("build", "benchmark-logs");
      Files.createDirectories(dir);
      final String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
      _logPath = dir.resolve("multi-aspect-read-benchmark-" + ts + ".log");
      _log = new PrintWriter(Files.newBufferedWriter(_logPath, StandardCharsets.UTF_8));
      log("multi-aspect-read-benchmark run @ " + LocalDateTime.now());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void log(String msg) {
    if (_log != null) {
      _log.println(msg);
      _log.flush();
    }
  }

  /**
   * Replicates {@code SQLStatementUtils.createAspectReadSql} output for one aspect column (master's
   * current per-aspect read shape).
   */
  private static List<String> buildCurrentPerAspectSqls(Set<String> aspectColumns, Set<Urn> urns) {
    final String urnList = urnInClause(urns);
    final List<String> sqls = new ArrayList<>();
    for (String col : aspectColumns) {
      final String softDeleteCheck = String.format(SOFT_DELETED_CHECK, col);
      sqls.add("SELECT urn, " + col + ", lastmodifiedon, lastmodifiedby FROM " + TABLE
          + " WHERE " + softDeleteCheck + " AND urn IN (" + urnList + ") AND " + DELETED_TS_IS_NULL_CHECK);
    }
    return sqls;
  }

  /**
   * Replicates {@code SQLStatementUtils.createMultiAspectReadSql} output: a single bundled SELECT of
   * all requested aspect columns for the table with a row-level {@code deleted_ts IS NULL} filter.
   */
  private static String buildMultiAspectSql(Set<String> aspectColumns, Set<Urn> urns) {
    final String columnList = String.join(", ", aspectColumns);
    final String urnList = urnInClause(urns);
    return "SELECT urn, " + columnList + ", lastmodifiedon, lastmodifiedby FROM " + TABLE
        + " WHERE urn IN (" + urnList + ") AND " + DELETED_TS_IS_NULL_CHECK;
  }

  private static String urnInClause(Set<Urn> urns) {
    return urns.stream()
        .map(u -> "'" + SQLStatementUtils.escapeReservedCharInUrn(u.toString()) + "'")
        .collect(Collectors.joining(", "));
  }

  private static void runCurrentPath(List<String> sqls) {
    for (String sql : sqls) {
      _server.createSqlQuery(sql).findList();
    }
  }

  private static void runMultiAspectPath(String sql) {
    _server.createSqlQuery(sql).findList();
  }

  /**
   * Counts the MariaDB {@code Com_select} delta for a single logical read, pinning the connection in
   * a transaction so the session-status counter reflects only this path's queries.
   */
  private static long countSelects(Runnable r) {
    try (io.ebean.Transaction txn = _server.beginTransaction()) {
      final long before = comSelect();
      r.run();
      final long delta = comSelect() - before;
      txn.commit();
      return delta;
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

  private static Urn benchUrn(int i) {
    try {
      return Urn.createFromString(REAL_URN_STRINGS[i]);
    } catch (java.net.URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads the read-batch URNs from a classpath resource at runtime (one URN per line; blank lines
   * and lines starting with {@code #} are ignored) so the URN set is editable without recompiling.
   */
  private static String[] loadUrnStrings(String resourceName) {
    try {
      final List<String> lines = Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8);
      final List<String> urns = lines.stream()
          .map(String::trim)
          .filter(l -> !l.isEmpty() && !l.startsWith("#"))
          .collect(Collectors.toList());
      if (urns.isEmpty()) {
        throw new IllegalStateException("No URNs found in benchmark URN resource: " + resourceName);
      }
      return urns.toArray(new String[0]);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read benchmark URN resource: " + resourceName, e);
    }
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
