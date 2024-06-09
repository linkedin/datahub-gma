package com.linkedin.metadata.dao;

import com.google.common.io.Resources;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.producer.GenericMetadataProducer;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import io.ebean.config.ServerConfig;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.common.AuditStamps.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class EbeanGenericLocalDAOTest {

  private static EbeanServer _server;

  private static ServerConfig _serverConfig;

  private static GenericLocalDAO _genericLocalDAO;

  private static GenericMetadataProducer _producer;

  @Nonnull
  private String readSQLfromFile(@Nonnull String resourcePath) {
    try {
      return Resources.toString(Resources.getResource(resourcePath), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public void init() {
    _producer = mock(GenericMetadataProducer.class);
    _server = EmbeddedMariaInstance.getServer(EbeanLocalAccessTest.class.getSimpleName());
    _serverConfig = EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName());
    _genericLocalDAO = new EbeanGenericLocalDAO(_serverConfig, _producer);
  }

  @BeforeMethod
  public void setupTest() {
    _server.execute(Ebean.createSqlUpdate(readSQLfromFile("ebean-generic-local-dao-create-all.sql")));
  }

  @Test
  public void testIngestOne() throws URISyntaxException {

    FooUrn fooUrn = FooUrn.createFromString("urn:li:foo:1");
    AspectFoo aspectFoo = new AspectFoo().setValue("foo");

    _genericLocalDAO.save(fooUrn, AspectFoo.class, RecordUtils.toJsonString(aspectFoo),
        makeAuditStamp("tester"), null, null);

    SqlQuery sqlQuery = _server.createSqlQuery("select * from metadata_aspect");
    List<SqlRow> result = sqlQuery.findList();

    // One record is returned.
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getString("urn"), "urn:li:foo:1");
    assertEquals(result.get(0).getString("metadata"), RecordUtils.toJsonString(aspectFoo)); // {"value":"foo"}
    assertEquals(result.get(0).getString("aspect"), AspectFoo.class.getCanonicalName());
  }

  @Test
  public void testIngestTwoSameValue() throws URISyntaxException {
    FooUrn fooUrn = FooUrn.createFromString("urn:li:foo:1");
    AspectFoo aspectFoo = new AspectFoo().setValue("foo");

    _genericLocalDAO.save(fooUrn, AspectFoo.class, RecordUtils.toJsonString(aspectFoo),
        makeAuditStamp("tester"), null, null);
    _genericLocalDAO.save(fooUrn, AspectFoo.class, RecordUtils.toJsonString(aspectFoo),
        makeAuditStamp("tester"), null, null);

    SqlQuery sqlQuery = _server.createSqlQuery("select * from metadata_aspect");
    List<SqlRow> result = sqlQuery.findList();

    // One record is returned because two aspect are equal, will not invoke db.
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getString("urn"), "urn:li:foo:1");
    assertEquals(result.get(0).getString("metadata"), RecordUtils.toJsonString(aspectFoo)); // {"value":"foo"}
    assertEquals(result.get(0).getString("aspect"), AspectFoo.class.getCanonicalName());
  }

  @Test
  public void testIngestTwoDifferentValue() throws URISyntaxException {
    FooUrn fooUrn = FooUrn.createFromString("urn:li:foo:1");
    AspectFoo aspectFoo1 = new AspectFoo().setValue("foo");
    AspectFoo aspectFoo2 = new AspectFoo().setValue("bar");

    _genericLocalDAO.save(fooUrn, AspectFoo.class, RecordUtils.toJsonString(aspectFoo1),
        makeAuditStamp("tester"), null, null);
    _genericLocalDAO.save(fooUrn, AspectFoo.class, RecordUtils.toJsonString(aspectFoo2),
        makeAuditStamp("tester"), null, null);

    SqlQuery sqlQuery = _server.createSqlQuery("select * from metadata_aspect order by version asc");
    List<SqlRow> result = sqlQuery.findList();

    // Two record is returned because two aspect are different.
    assertEquals(result.size(), 2);
    assertEquals(result.get(0).getString("urn"), "urn:li:foo:1");
    assertEquals(result.get(0).getString("metadata"), RecordUtils.toJsonString(aspectFoo2)); // {"value":"bar"}
    assertEquals(result.get(0).getString("aspect"), AspectFoo.class.getCanonicalName());
    assertEquals(result.get(0).getInteger("version").intValue(), 0);

    assertEquals(result.get(1).getString("urn"), "urn:li:foo:1");
    assertEquals(result.get(1).getString("metadata"), RecordUtils.toJsonString(aspectFoo1)); // {"value":"foo"}
    assertEquals(result.get(1).getString("aspect"), AspectFoo.class.getCanonicalName());
    assertEquals(result.get(1).getInteger("version").intValue(), 1);
  }

  @Test
  public void testQueryLatest() throws URISyntaxException {
    FooUrn fooUrn = FooUrn.createFromString("urn:li:foo:1");
    AspectFoo aspectFoo1 = new AspectFoo().setValue("foo");
    AspectFoo aspectFoo2 = new AspectFoo().setValue("bar");

    _genericLocalDAO.save(fooUrn, AspectFoo.class, RecordUtils.toJsonString(aspectFoo1),
        makeAuditStamp("tester"), null, null);
    _genericLocalDAO.save(fooUrn, AspectFoo.class, RecordUtils.toJsonString(aspectFoo2),
        makeAuditStamp("tester"), null, null);

    Optional<GenericLocalDAO.MetadataWithExtraInfo> metadata = _genericLocalDAO.queryLatest(fooUrn, AspectFoo.class);

    // {"value":"bar"} is inserted later so it is the latest metadata.
    assertEquals(metadata.get().getAspect(), RecordUtils.toJsonString(aspectFoo2));
  }

  @Test
  public void testBackfill() throws URISyntaxException {
    FooUrn fooUrn = FooUrn.createFromString("urn:li:foo:1");
    AspectFoo aspectFoo1 = new AspectFoo().setValue("foo");
    AspectFoo aspectFoo2 = new AspectFoo().setValue("bar");

    _genericLocalDAO.save(fooUrn, AspectFoo.class, RecordUtils.toJsonString(aspectFoo1),
        makeAuditStamp("tester"), null, null);

    verify(_producer, times(1)).produceAspectSpecificMetadataAuditEvent(eq(fooUrn),
        eq(null), eq(aspectFoo1), eq(makeAuditStamp("tester")), eq(null), eq(null));

    _genericLocalDAO.save(fooUrn, AspectFoo.class, RecordUtils.toJsonString(aspectFoo2),
        makeAuditStamp("tester"), null, null);

    verify(_producer, times(1)).produceAspectSpecificMetadataAuditEvent(eq(fooUrn),
        eq(aspectFoo1), eq(aspectFoo2), eq(makeAuditStamp("tester")), eq(null), eq(null));

    verifyNoMoreInteractions(_producer);

    Map<Urn, Set<Class<? extends RecordTemplate>>> aspects = Collections.singletonMap(fooUrn, Collections.singleton(AspectFoo.class));

    Map<Urn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfillResults
        = _genericLocalDAO.backfill(BackfillMode.BACKFILL_ALL, aspects);

    assertEquals(backfillResults.size(), 1);
    assertEquals(backfillResults.get(fooUrn).get(AspectFoo.class).get(), aspectFoo2);
  }
}
