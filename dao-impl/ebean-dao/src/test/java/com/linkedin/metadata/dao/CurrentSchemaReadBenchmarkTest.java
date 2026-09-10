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
    _server = EmbeddedMariaInstance.getServer(CurrentSchemaReadBenchmarkTest.class.getSimpleName());
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
  public void benchmarkCurrentMasterRead() {
    if (!Boolean.getBoolean("gma.benchmark")) {
      throw new org.testng.SkipException("benchmark disabled; enable with -Dgma.benchmark=true");
    }

    final Set<String> aspectColumns = new LinkedHashSet<>(aspectColumns());
    final Set<Urn> urns = new LinkedHashSet<>();
    for (int i = 0; i < NUM_URNS; i++) {
      urns.add(benchUrn(i));
    }

    final List<String> currentSqls = buildCurrentPerAspectSqls(aspectColumns, urns);

    // Log every SQL query issued for one logical read (one SELECT per aspect column).
    log("");
    log("=== QUERIES (one logical read = " + currentSqls.size() + " SELECTs) ===");
    for (int q = 0; q < currentSqls.size(); q++) {
      log("Q" + q + ": " + currentSqls.get(q));
    }

    // Log the actual rows returned by the first query as a sample of "what result we are getting".
    log("");
    log("=== SAMPLE RESULT (rows returned by Q0) ===");
    final List<SqlRow> sample = _server.createSqlQuery(currentSqls.get(0)).findList();
    log("Q0 returned " + sample.size() + " rows. First up to 5:");
    for (int r = 0; r < Math.min(5, sample.size()); r++) {
      final SqlRow row = sample.get(r);
      log("  row[" + r + "] urn=" + row.getString("urn") + "  " + COLUMN_PREFIX + "0=" + row.getString(COLUMN_PREFIX + "0")
          + "  lastmodifiedon=" + row.get("lastmodifiedon"));
    }

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

    final String header = "============ current master multi-aspect read benchmark ============";
    final String line1 = String.format("URNs=%d aspectsPerUrn=%d totalKeys=%d  (over %d iters)",
        NUM_URNS, NUM_ASPECTS, NUM_URNS * NUM_ASPECTS, ITERATIONS);
    final String colHdr = String.format("%-8s | %-16s | %-9s | %-9s | %-9s", "path", "DB SELECTs/read", "p50 ms", "p90 ms", "max ms");
    final String dataRow = String.format("%-8s | %-16d | %-9.3f | %-9.3f | %-9.3f",
        "current", currentSelects, toMs(pct(currentNanos, 0.50)), toMs(pct(currentNanos, 0.90)),
        toMs(currentNanos[ITERATIONS - 1]));
    final String footer = "====================================================================";

    log("");
    log("=== BENCHMARK RESULT ===");
    for (String s : new String[] {header, line1, colHdr, dataRow, footer}) {
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
      _logPath = dir.resolve("current-master-read-benchmark-" + ts + ".log");
      _log = new PrintWriter(Files.newBufferedWriter(_logPath, StandardCharsets.UTF_8));
      log("current-master-read-benchmark run @ " + LocalDateTime.now());
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
