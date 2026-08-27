package com.linkedin.metadata.dao.localrelationship;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.EBeanDAOConfig;
import com.linkedin.metadata.dao.EbeanLocalAccess;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.EbeanLocalRelationshipQueryDAO;
import com.linkedin.metadata.dao.EbeanLocalRelationshipWriterDAO;
import com.linkedin.metadata.dao.IEbeanLocalAccess;
import com.linkedin.metadata.dao.RelationshipKeysetCursor;
import com.linkedin.metadata.dao.RelationshipKeysetPage;
import com.linkedin.metadata.dao.urnpath.EmptyPathExtractor;
import com.linkedin.metadata.dao.utils.EBeanDAOUtils;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.dao.utils.RelationshipLookUpContext;
import com.linkedin.metadata.dao.utils.SQLSchemaUtils;
import com.linkedin.metadata.dao.utils.SQLStatementUtils;
import com.linkedin.metadata.dao.utils.SchemaValidatorUtil;
import com.linkedin.metadata.query.AspectField;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.LocalRelationshipCriterion;
import com.linkedin.metadata.query.LocalRelationshipCriterionArray;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.LocalRelationshipValue;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipField;
import com.linkedin.metadata.query.UrnField;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.BarSnapshot;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityAspectUnionArray;
import com.linkedin.testing.FooSnapshot;
import com.linkedin.testing.localrelationship.AssetRelationship;
import com.linkedin.testing.localrelationship.BelongsTo;
import com.linkedin.testing.localrelationship.BelongsToV2;
import com.linkedin.testing.localrelationship.ConsumeFrom;
import com.linkedin.testing.localrelationship.EnvorinmentType;
import com.linkedin.testing.localrelationship.OwnedBy;
import com.linkedin.testing.localrelationship.PairsWith;
import com.linkedin.testing.localrelationship.ReportsTo;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlUpdate;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.naming.OperationNotSupportedException;
import javax.persistence.PersistenceException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterion;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterionArray;
import pegasus.com.linkedin.metadata.query.LogicalOperation;
import pegasus.com.linkedin.metadata.query.innerLogicalOperation.Operator;

import static com.linkedin.metadata.dao.EbeanLocalRelationshipQueryDAO.*;
import static com.linkedin.metadata.dao.utils.LogicalExpressionLocalRelationshipCriterionUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class EbeanLocalRelationshipQueryDAOTest {
  public Urn fooEntityUrn;
  public Urn barEntityUrn;
  public Urn crewEntityUrn;

  private EbeanServer _server;
  private EbeanLocalRelationshipWriterDAO _localRelationshipWriterDAO;
  private EbeanLocalRelationshipQueryDAO _localRelationshipQueryDAO;
  private IEbeanLocalAccess<FooUrn> _fooUrnEBeanLocalAccess;
  private IEbeanLocalAccess<BarUrn> _barUrnEBeanLocalAccess;
  private final EBeanDAOConfig _eBeanDAOConfig = new EBeanDAOConfig();

  @Factory(dataProvider = "inputList")
  public EbeanLocalRelationshipQueryDAOTest(boolean nonDollarVirtualColumnsEnabled) {
    _eBeanDAOConfig.setNonDollarVirtualColumnsEnabled(nonDollarVirtualColumnsEnabled);
  }

  @DataProvider(name = "inputList")
  public static Object[][] inputList() {
    return new Object[][] {
        { true },
        { false }
    };
  }

  @BeforeClass
  public void init() throws URISyntaxException {
    _server = EmbeddedMariaInstance.getServer(EbeanLocalRelationshipQueryDAOTest.class.getSimpleName());
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
    _fooUrnEBeanLocalAccess = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()),
        FooUrn.class, new EmptyPathExtractor<>(), _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled());
    _barUrnEBeanLocalAccess = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()),
        BarUrn.class, new EmptyPathExtractor<>(), _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled());
    _localRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server, _eBeanDAOConfig);

    fooEntityUrn = new Urn("urn:li:foo");
    barEntityUrn = new Urn("urn:li:bar");
    crewEntityUrn = new Urn("urn:li:crew");
  }

  @BeforeMethod
  public void recreateTables() throws IOException {
    if (!_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled()) {
      _server.execute(Ebean.createSqlUpdate(
          Resources.toString(Resources.getResource("ebean-local-relationship-dao-create-all.sql"), StandardCharsets.UTF_8)));
    } else {
      _server.execute(Ebean.createSqlUpdate(
          Resources.toString(Resources.getResource("ebean-local-relationship-create-all-with-non-dollar-virtual-column-names.sql"), StandardCharsets.UTF_8)));
    }

    // also reset the schema mode to NEW_SCHEMA_ONLY
    _localRelationshipQueryDAO.setSchemaConfig(EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY);
  }

  @Test
  public void testFindOneEntity() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("foo"));
  }

  @Test
  public void testFindOneEntityTwoAspects() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectBar().setValue("bar"), AspectBar.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 2);
    EntityAspectUnion fooAspectUnion = new EntityAspectUnion();
    fooAspectUnion.setAspectFoo(new AspectFoo().setValue("foo"));
    EntityAspectUnion barAspectUnion = new EntityAspectUnion();
    barAspectUnion.setAspectBar(new AspectBar().setValue("bar"));

    EntityAspectUnionArray expected = new EntityAspectUnionArray(fooAspectUnion, barAspectUnion);

    assertEquals(fooSnapshotList.get(0).getAspects(), expected);
  }

  @Test
  public void testFindEntitiesV2WithV1Filter() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    assertThrows(IllegalArgumentException.class, () -> _localRelationshipQueryDAO.findEntitiesV2(FooSnapshot.class, filter, 0, 10));
  }

  @Test
  public void testFindOneEntityV2() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setLogicalExpressionCriteria(
        wrapCriterionAsLogicalExpression(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntitiesV2(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("foo"));
  }

  @Test
  public void testFindOneEntityV2WithStartWithCondition() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(new FooUrn(2), new AspectFoo().setValue("fooTwo"), AspectFoo.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.START_WITH,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setLogicalExpressionCriteria(
        wrapCriterionAsLogicalExpression(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntitiesV2(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 2);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("foo"));

    assertEquals(fooSnapshotList.get(1).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(1).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("fooTwo"));

    // Prepare filter
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("fooT"),
        Condition.START_WITH,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter2 = new LocalRelationshipFilter().setLogicalExpressionCriteria(
        wrapCriterionAsLogicalExpression(filterCriterion2));
    List<FooSnapshot> fooSnapshotList2 = _localRelationshipQueryDAO.findEntitiesV2(FooSnapshot.class, filter2, 0, 10);

    assertEquals(fooSnapshotList2.size(), 1);
    assertEquals(fooSnapshotList2.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList2.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("fooTwo"));
  }

  @Test
  public void testFindOneEntityTwoAspectsV2() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectBar().setValue("bar"), AspectBar.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setLogicalExpressionCriteria(
        wrapCriterionAsLogicalExpression(filterCriterion));

    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntitiesV2(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 2);
    EntityAspectUnion fooAspectUnion = new EntityAspectUnion();
    fooAspectUnion.setAspectFoo(new AspectFoo().setValue("foo"));
    EntityAspectUnion barAspectUnion = new EntityAspectUnion();
    barAspectUnion.setAspectBar(new AspectBar().setValue("bar"));

    EntityAspectUnionArray expected = new EntityAspectUnionArray(fooAspectUnion, barAspectUnion);

    assertEquals(fooSnapshotList.get(0).getAspects(), expected);
  }

  @DataProvider(name = "schemaConfig")
  public static Object[][] schemaConfig() {
    return new Object[][] {
        { EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY },
        { EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY }
    };
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationship(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);

    // Add Alice, Bob and Jack into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobReportsToAlice), false);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(jack, AspectFoo.class, Collections.singletonList(jackReportsToAlice), false);

    // Find all reports-to relationship for Alice.
    LocalRelationshipFilter filter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(alice.toString()),
          Condition.EQUAL,
          new UrnField());
      filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);
    List<ReportsTo> reportsToAlice = _localRelationshipQueryDAO.findRelationships(FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()), FooSnapshot.class, filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.OUTGOING),
        0, 10);

    // Asserts
    assertEquals(reportsToAlice.size(), 2);
    Set<FooUrn> actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    Set<FooUrn> expected = ImmutableSet.of(jack, bob);
    assertEquals(actual, expected);

    // Soft (set delete_ts = now()) Delete Jack reports-to ALice relationship
    SqlUpdate deletionSQL = _server.createSqlUpdate(
        SQLStatementUtils.deleteLocalRelationshipSQL(SQLSchemaUtils.getRelationshipTableName(jackReportsToAlice), false));
    deletionSQL.setParameter("source", jack.toString());
    deletionSQL.execute();

    reportsToAlice = _localRelationshipQueryDAO.findRelationships(FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()), FooSnapshot.class, filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.OUTGOING),
        0, 10);

    // Expect: only bob reports to Alice
    assertEquals(reportsToAlice.size(), 1);
    actual = reportsToAlice.stream()
        .map(reportsTo -> makeFooUrn(reportsTo.getSource().toString()))
        .collect(Collectors.toSet());
    expected = ImmutableSet.of(bob);
    assertEquals(actual, expected);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipWithFilter(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    BarUrn spark = new BarUrn(1);
    BarUrn samza = new BarUrn(2);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
          null, false);

      // Add Spark and Samza into entity tables.
      _barUrnEBeanLocalAccess.add(spark, new AspectFoo().setValue("Spark"), AspectFoo.class, new AuditStamp(), null, false);
      _barUrnEBeanLocalAccess.add(samza, new AspectFoo().setValue("Samza"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Spark consume-from hdfs relationship
    ConsumeFrom sparkConsumeFromHdfs = new ConsumeFrom().setSource(spark).setDestination(hdfs).setEnvironment(EnvorinmentType.OFFLINE);
    _localRelationshipWriterDAO.addRelationships(spark, AspectFoo.class, Collections.singletonList(sparkConsumeFromHdfs), false);

    // Add Samza consume-from kafka and Samza consume-from restli relationships
    ConsumeFrom samzaConsumeFromKafka = new ConsumeFrom().setSource(samza).setDestination(kafka).setEnvironment(EnvorinmentType.NEARLINE);
    ConsumeFrom samzaConsumeFromRestli = new ConsumeFrom().setSource(samza).setDestination(restli).setEnvironment(EnvorinmentType.ONLINE);

    _localRelationshipWriterDAO.addRelationships(samza, AspectFoo.class, ImmutableList.of(samzaConsumeFromKafka, samzaConsumeFromRestli), false);

    // Find all consume-from relationship for Samza.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(samza.toString()),
        Condition.EQUAL,
        new UrnField());

    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);
    List<ConsumeFrom> consumeFromSamza = _localRelationshipQueryDAO.findRelationships(
        BarSnapshot.class,
        filterUrn,
        FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        ConsumeFrom.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.OUTGOING),
        0, 10);

    // Assert
    assertEquals(consumeFromSamza.size(), 2); // Because Samza consume from 1. kafka and 2. restli

    // Find all consume-from relationship for Samza which happens in NEARLINE. Not supported in old schema mode.
    if (schemaConfig != EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      LocalRelationshipCriterion filterRelationshipCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("NEARLINE"),
          Condition.EQUAL,
          new RelationshipField().setPath("/environment"));

      LocalRelationshipFilter filterRelationship = new LocalRelationshipFilter().setCriteria(
          new LocalRelationshipCriterionArray(filterRelationshipCriterion)).setDirection(RelationshipDirection.OUTGOING);

      List<ConsumeFrom> consumeFromSamzaInNearline = _localRelationshipQueryDAO.findRelationships(
          BarSnapshot.class,
          filterUrn,
          FooSnapshot.class,
          new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
          ConsumeFrom.class,
          filterRelationship,
          0, 10);

      // Assert
      assertEquals(consumeFromSamzaInNearline.size(), 1); // Because Samza only consumes kafka in NEARLINE.
    }
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipWithEntityUrn(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);

    // Add Alice, Bob and Jack into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobReportsToAlice), false);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(jack, AspectFoo.class, Collections.singletonList(jackReportsToAlice), false);

    // Find all reports-to relationship for Alice.
    LocalRelationshipFilter destFilter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(alice.toString()),
          Condition.EQUAL,
          new UrnField());
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    List<ReportsTo> reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV2(
        null, null, "foo", destFilter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    // Asserts
    assertEquals(reportsToAlice.size(), 2);
    Set<FooUrn> actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    Set<FooUrn> expected = ImmutableSet.of(jack, bob);
    assertEquals(actual, expected);

    // Soft (set delete_ts = now()) Delete Jack reports-to ALice relationship
    SqlUpdate deletionSQL = _server.createSqlUpdate(
        SQLStatementUtils.deleteLocalRelationshipSQL(SQLSchemaUtils.getRelationshipTableName(jackReportsToAlice), false));
    deletionSQL.setParameter("source", jack.toString());
    deletionSQL.execute();

    reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV2(
        null, null, "foo", destFilter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    // Expect: only bob reports to Alice
    assertEquals(reportsToAlice.size(), 1);
    actual = reportsToAlice.stream()
        .map(reportsTo -> makeFooUrn(reportsTo.getSource().toString()))
        .collect(Collectors.toSet());
    expected = ImmutableSet.of(bob);
    assertEquals(actual, expected);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipWithFilterWithEntityUrn(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    BarUrn spark = new BarUrn(1);
    BarUrn samza = new BarUrn(2);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
          null, false);

      // Add Spark and Samza into entity tables.
      _barUrnEBeanLocalAccess.add(spark, new AspectFoo().setValue("Spark"), AspectFoo.class, new AuditStamp(), null, false);
      _barUrnEBeanLocalAccess.add(samza, new AspectFoo().setValue("Samza"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Spark consume-from hdfs relationship
    ConsumeFrom sparkConsumeFromHdfs = new ConsumeFrom().setSource(spark).setDestination(hdfs).setEnvironment(EnvorinmentType.OFFLINE);
    _localRelationshipWriterDAO.addRelationships(spark, AspectFoo.class, Collections.singletonList(sparkConsumeFromHdfs), false);

    // Add Samza consume-from kafka and Samza consume-from restli relationships
    ConsumeFrom samzaConsumeFromKafka = new ConsumeFrom().setSource(samza).setDestination(kafka).setEnvironment(EnvorinmentType.NEARLINE);
    ConsumeFrom samzaConsumeFromRestli = new ConsumeFrom().setSource(samza).setDestination(restli).setEnvironment(EnvorinmentType.ONLINE);

    _localRelationshipWriterDAO.addRelationships(samza, AspectFoo.class, ImmutableList.of(samzaConsumeFromRestli, samzaConsumeFromKafka), false);

    // Find all consume-from relationship for Samza.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:bar:2"), // 2 is Samza as defined at very beginning.
        Condition.EQUAL,
        new UrnField());
    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));
    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    List<ConsumeFrom> consumeFromSamza = _localRelationshipQueryDAO.findRelationshipsV2("bar", filterUrn, "foo", null,
        ConsumeFrom.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    assertEquals(consumeFromSamza.size(), 2); // Because Samza consumes from 1. kafka and 2. restli

    // Find all consume-from relationship for Samza which happens in NEARLINE. Not supported in OLD_SCHEMA_ONLY mode.
    if (schemaConfig != EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      LocalRelationshipCriterion filterRelationshipCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
          LocalRelationshipValue.create("NEARLINE"), Condition.EQUAL, new RelationshipField().setPath("/environment"));

      LocalRelationshipFilter filterRelationship = new LocalRelationshipFilter().setCriteria(
          new LocalRelationshipCriterionArray(filterRelationshipCriterion)).setDirection(RelationshipDirection.OUTGOING);

      List<ConsumeFrom> consumeFromSamzaInNearline = _localRelationshipQueryDAO.findRelationshipsV2("bar", filterUrn, "foo", null,
          ConsumeFrom.class,
          filterRelationship,
          -1, -1, new RelationshipLookUpContext());

      // Assert
      assertEquals(consumeFromSamzaInNearline.size(), 1); // Because Samza only consumes kafka in NEARLINE.
    }
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipForCrewUsage(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    BarUrn spark = new BarUrn(1);
    BarUrn samza = new BarUrn(2);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
          null, false);

      // Add Spark and Samza into entity tables.
      _barUrnEBeanLocalAccess.add(spark, new AspectFoo().setValue("Spark"), AspectFoo.class, new AuditStamp(), null, false);
      _barUrnEBeanLocalAccess.add(samza, new AspectFoo().setValue("Samza"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // crew1 is a non-mg entity
    FooUrn crew1 = new FooUrn(4);
    FooUrn crew2 = new FooUrn(5);

    // add kafka owned by crew1
    OwnedBy kafkaOwnedByCrew1 = new OwnedBy().setSource(kafka).setDestination(crew1);
    _localRelationshipWriterDAO.addRelationships(kafka, AspectFoo.class, Collections.singletonList(kafkaOwnedByCrew1), false);

    // add hdfs owned by crew1
    OwnedBy hdfsOwnedByCrew1 = new OwnedBy().setSource(hdfs).setDestination(crew1);
    _localRelationshipWriterDAO.addRelationships(hdfs, AspectFoo.class, Collections.singletonList(hdfsOwnedByCrew1), false);

    // add restli owned by crew1
    OwnedBy restliOwnedByCrew1 = new OwnedBy().setSource(restli).setDestination(crew1);
    _localRelationshipWriterDAO.addRelationships(restli, AspectFoo.class, Collections.singletonList(restliOwnedByCrew1), false);

    // add spark owned by crew2
    OwnedBy sparkOwnedByCrew2 = new OwnedBy().setSource(spark).setDestination(crew2);
    _localRelationshipWriterDAO.addRelationships(spark, AspectFoo.class, Collections.singletonList(sparkOwnedByCrew2), false);

    // add samza owned by crew2
    OwnedBy samzaOwnedByCrew2 = new OwnedBy().setSource(samza).setDestination(crew2);
    _localRelationshipWriterDAO.addRelationships(samza, AspectFoo.class, Collections.singletonList(samzaOwnedByCrew2), false);

    // Find all owned-by relationship for crew1.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:4"), // 4 is crew1 as defined at very beginning.
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    // test owned by of crew1 can be found
    List<OwnedBy> ownedByCrew1 = _localRelationshipQueryDAO.findRelationshipsV2(null, null, "crew", filterUrn,
        OwnedBy.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    assertEquals(ownedByCrew1.size(), 3);

    // Find all owned-by relationship for crew2.
    LocalRelationshipCriterion filterUrnCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:5"), // 5 is crew2 as defined at very beginning.
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LocalRelationshipFilter filterUrn2 = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion2));

    // test owned by of crew2 can be found
    List<OwnedBy> ownedByCrew2 = _localRelationshipQueryDAO.findRelationshipsV2(null, null, "crew", filterUrn2,
        OwnedBy.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    assertEquals(ownedByCrew2.size(), 2);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipWithFilterOnSourceEntityForCrewUsage(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
          null, false);
    }

    // crew is a non-mg entity
    FooUrn crew = new FooUrn(4);

    // add kafka owned by crew
    OwnedBy kafkaOwnedByCrew = new OwnedBy().setSource(kafka).setDestination(crew);
    _localRelationshipWriterDAO.addRelationships(kafka, AspectFoo.class, Collections.singletonList(kafkaOwnedByCrew), false);

    // add hdfs owned by crew
    OwnedBy hdfsOwnedByCrew = new OwnedBy().setSource(hdfs).setDestination(crew);
    _localRelationshipWriterDAO.addRelationships(hdfs, AspectFoo.class, Collections.singletonList(hdfsOwnedByCrew), false);

    // add restli owned by crew
    OwnedBy restliOwnedByCrew = new OwnedBy().setSource(restli).setDestination(crew);
    _localRelationshipWriterDAO.addRelationships(restli, AspectFoo.class, Collections.singletonList(restliOwnedByCrew), false);

    // Find all owned-by relationship for crew.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:4"), // 4 is crew as defined at very beginning.
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));

    LocalRelationshipCriterion filterUrnCriterion1 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:1"), // 1 is kafka as defined at very beginning.
        Condition.EQUAL,
        new UrnField());
    LocalRelationshipFilter filterUrn1 = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion1));

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    // test owned by of crew can be filtered by source entity, e.g. only include kafka
    List<OwnedBy> ownedByCrew1 = _localRelationshipQueryDAO.findRelationshipsV2("foo", filterUrn1, "crew", filterUrn,
        OwnedBy.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    assertEquals(ownedByCrew1.size(), 1);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipWithNonUrnFilterOnSourceEntityForCrewUsage(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    // Find all owned-by relationship for crew.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:4"), // 4 is crew as defined at very beginning.
        Condition.EQUAL,
        new AspectField());
    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));

    LocalRelationshipCriterion filterUrnCriterion1 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:1"), // 1 is kafka as defined at very beginning.
        Condition.EQUAL,
        new UrnField());
    LocalRelationshipFilter filterUrn1 = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion1));

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    // non-mg entity cannot be filtered by non-urn filter. This will throw an exception.
    assertThrows(IllegalArgumentException.class, () -> {
      _localRelationshipQueryDAO.findRelationshipsV2("foo", filterUrn1, "crew", filterUrn,
          OwnedBy.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
          -1, -1, new RelationshipLookUpContext());
    });
  }

  @Test(dataProvider = "schemaConfig")
  void testFindRelationshipsWithEntityUrnOffsetAndCount(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);
    FooUrn lisa = new FooUrn(4);
    FooUrn rose = new FooUrn(5);
    FooUrn jenny = new FooUrn(6);

    // Add Alice, Bob, Jack, Lisa, Rose, and Jenny into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(lisa, new AspectFoo().setValue("Lisa"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(rose, new AspectFoo().setValue("Rose"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(jenny, new AspectFoo().setValue("Jenny"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobReportsToAlice), false);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(jack, AspectFoo.class, Collections.singletonList(jackReportsToAlice), false);

    // Add Lisa reports-to ALice relationship
    ReportsTo lisaReportsToAlice = new ReportsTo().setSource(lisa).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(lisa, AspectFoo.class, Collections.singletonList(lisaReportsToAlice), false);

    // Add Rose reports-to ALice relationship
    ReportsTo roseReportsToAlice = new ReportsTo().setSource(rose).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(rose, AspectFoo.class, Collections.singletonList(roseReportsToAlice), false);

    // Add Jenny reports-to ALice relationship
    ReportsTo jennyReportsToAlice = new ReportsTo().setSource(jenny).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(jenny, AspectFoo.class, Collections.singletonList(jennyReportsToAlice), false);

    // Find all reports-to relationship for Alice.
    LocalRelationshipFilter filter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(alice.toString()),
          Condition.EQUAL,
          new UrnField());
      filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    List<ReportsTo> reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV2(
        null, null, "foo", filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, 3, new RelationshipLookUpContext());

    // Asserts only 3 reports-to relationships are returned
    assertEquals(reportsToAlice.size(), 3);

    reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV2(
        null, null, "foo", filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        2, 10, new RelationshipLookUpContext());

    // Asserts 3 returns, and the content starts from the 3rd report (Lisa)
    assertEquals(reportsToAlice.size(), 3);
    Set<FooUrn> actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    Set<FooUrn> expected = ImmutableSet.of(lisa, rose, jenny);
    assertEquals(actual, expected);

    reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV2(
        null, null, "foo", filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        2, -1, new RelationshipLookUpContext());

    // Asserts 5 returns, because offset cannot be applied when count isn't specified.
    assertEquals(reportsToAlice.size(), 5);
    actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    expected = ImmutableSet.of(bob, jack, lisa, rose, jenny);
    assertEquals(actual, expected);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV3WithRelationshipV1(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);

    // Add Alice, Bob and Jack into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobReportsToAlice), false);

    // Find all reports-to relationship for Alice.
    LocalRelationshipFilter destFilter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(alice.toString()),
          Condition.EQUAL,
          new UrnField());
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV3(
        null, null, "foo", destFilter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    AssetRelationship expected = reportsToAlice.get(0);
    assertEquals(expected.getSource(), "urn:li:foo:2");

    ReportsTo expectedReportsTo = expected.getRelatedTo().getReportsTo();

    assertNotNull(expectedReportsTo);
    assertEquals(expectedReportsTo.getSource().toString(), "urn:li:foo:2");
    assertEquals(expectedReportsTo.getDestination().toString(), "urn:li:foo:1");
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV3WithRelationshipV2(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn car = new FooUrn(2);

    // Add car and owner into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Find all belongs-to relationship for owner.
    LocalRelationshipFilter destFilter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(owner.toString()),
          Condition.EQUAL,
          new UrnField());
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV3(
        null, null, "foo", destFilter,
        BelongsToV2.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    AssetRelationship expected = belongsToOwner.get(0);
    assertEquals(expected.getSource(), "urn:li:foo:2");

    BelongsToV2 expectedBelongsToV2 = expected.getRelatedTo().getBelongsToV2();
    assertEquals(expectedBelongsToV2.getDestination().getString(), owner.toString());
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV3WithRelationshipV2WithHistory(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn car = new FooUrn(2);

    // Add car and owner into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // IMPORTANT: remove the relationship so that we can test history.
    _localRelationshipWriterDAO.removeRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner));

    // Find all belongs-to relationship for owner.
    LocalRelationshipFilter destFilter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(owner.toString()),
          Condition.EQUAL,
          new UrnField());
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV3(
        null, null, "foo", destFilter,
        BelongsToV2.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext(true));

    AssetRelationship expected = belongsToOwner.get(0);
    assertEquals(expected.getSource(), "urn:li:foo:2");

    BelongsToV2 expectedBelongsToV2 = expected.getRelatedTo().getBelongsToV2();
    assertEquals(expectedBelongsToV2.getDestination().getString(), owner.toString());
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV4WithNullFilter(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car2"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        null, null, null, null,
        BelongsToV2.class, new LocalRelationshipFilter().setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 2);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV4WithOneCriterion(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car2"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Find all belongs-to relationship for owner.
    LocalRelationshipFilter destFilter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters, and only support one urn filter.
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(owner.toString()),
          Condition.EQUAL,
          new UrnField());
      LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(oldSchemaFilterCriterion);

      destFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(logicalExpressionCriterion);
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

      destFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(logicalExpressionCriterion);
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        null, null, "foo", destFilter,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 1);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:2");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner.toString());
  }

  /**
   * Old schema does not support multiple criteria in the same filter. Skipped in this test.
   */
  @Test
  public void testFindRelationshipsV4WithTwoOrCriterion() throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car2"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Find all belongs-to relationship for owner.
    // criterion 1: owner = "Owner"
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    // criterion 2: owner = "Owner2"
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner2"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);
    array.add(logicalExpressionCriterion2);

    // or criterion: (owner = "Owner" OR owner = "Owner2")
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = buildLogicalGroup(Operator.OR, array);

    // Filter: (owner = "Owner" OR owner = "Owner2")
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        null, null, "foo", destFilter,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 2);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:2");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner.toString());

    AssetRelationship actual2 = belongsToOwner.get(1);
    assertEquals(actual2.getSource(), "urn:li:foo:4");

    BelongsToV2 actual2BelongsToV2 = actual2.getRelatedTo().getBelongsToV2();
    assertEquals(actual2BelongsToV2.getDestination().getString(), owner2.toString());
  }

  /**
   * Old schema does not support multiple criteria in the same filter. Skipped in this test.
   */
  @Test
  public void testFindRelationshipsV4WithTwoAndCriterion() throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car, new AspectBar().setValue("Bike"), AspectBar.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Find all belongs-to relationship for owner.
    // criterion 1: foo = "Car"
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Car"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    // criterion 2: bar = "Bike"
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Bike"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);
    array.add(logicalExpressionCriterion2);

    // and criterion: (foo = "Car" AND bar = "Bike")
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = buildLogicalGroup(Operator.AND, array);

    // Filter: (foo = "Car" AND bar = "Bike")
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        "foo", srcFilter, null, null,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 1);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:2");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner.toString());
  }

  /**
   * Old schema does not support multiple criteria in the same filter. Skipped in this test.
   */
  @Test
  public void testFindRelationshipsV4WithOneNotCriterion() throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car2"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car2 belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Find all belongs-to relationship for owner.
    // criterion 1: foo = "Car"
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Car"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);

    // not criterion: ( NOT foo = "Car")
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = buildLogicalGroup(Operator.NOT, array);

    // Filter: ( NOT foo = "Car")
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        "foo", srcFilter, null, null,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 1);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:4");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner2.toString());
  }

  /**
   * Old schema does not support multiple criteria in the same filter. Skipped in this test.
   */
  @Test
  public void testFindRelationshipsV4WithOneNotCriterionInNested() throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car, new AspectBar().setValue("Bike"), AspectBar.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectBar().setValue("Bike2"), AspectBar.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car2 belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Find all belongs-to relationship for owner.
    // criterion 1: foo = "Car"
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Car"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    // criterion 2: bar = "Bike"
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Bike"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion2);

    // not criterion: (NOT bar = "Bike")
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = buildLogicalGroup(Operator.NOT, array);

    LogicalExpressionLocalRelationshipCriterionArray array2 = new LogicalExpressionLocalRelationshipCriterionArray();
    array2.add(localRelationshipCriterion);
    array2.add(logicalExpressionCriterion);

    // and criterion: (foo = "Car" AND (NOT bar = "Bike"))
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion2 = buildLogicalGroup(Operator.AND, array2);

    // Filter: (foo = "Car" AND (NOT bar = "Bike"))
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion2);


    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        "foo", srcFilter, null, null,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 1);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:4");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner2.toString());
  }

  /**
   * Old schema does not support multiple criteria in the same filter. Skipped in this test.
   */
  @Test
  public void testFindRelationshipsV4WithNestedCriterion() throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn owner3 = new FooUrn(5);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);
    FooUrn car3 = new FooUrn(6);

    // Add car and owner into entity tables.
    _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car, new AspectBar().setValue("Bike"), AspectBar.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car2"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car3, new AspectFoo().setValue("Car3"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner3, new AspectFoo().setValue("Owner3"), AspectFoo.class, new AuditStamp(), null, false);

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car2 belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Add car3 belongs-to owner3 relationship
    BelongsToV2 carBelongsToOwner3 = new BelongsToV2();
    carBelongsToOwner3.setDestination(BelongsToV2.Destination.create(owner3.toString()));
    _localRelationshipWriterDAO.addRelationships(car3, AspectFoo.class, Collections.singletonList(carBelongsToOwner3), false);

    // Find all belongs-to relationship for owner.
    // criterion 1: foo = "Car"
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Car"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    // criterion 2: bar = "Bike"
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Bike"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);
    array.add(logicalExpressionCriterion2);

    // and criterion: (foo = "Car" AND bar = "Bike")
    LogicalExpressionLocalRelationshipCriterion andCriterion = buildLogicalGroup(Operator.AND, array);

    // criterion 3: foo = "Car2"
    LocalRelationshipCriterion filterCriterion3 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Car2"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion3 = wrapCriterionAsLogicalExpression(filterCriterion3);

    LogicalExpressionLocalRelationshipCriterionArray array2 = new LogicalExpressionLocalRelationshipCriterionArray();
    array2.add(andCriterion);
    array2.add(logicalExpressionCriterion3);

    // or criterion: ((foo = "Car" AND bar = "Bike") OR (foo = "Car2"))
    LogicalExpressionLocalRelationshipCriterion orCriterion = buildLogicalGroup(Operator.OR, array2);

    // Filter: (foo = "Car" AND bar = "Bike") OR (foo = "Car2")
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(orCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        "foo", srcFilter, null, null,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 2);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:2");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner.toString());

    AssetRelationship actual2 = belongsToOwner.get(1);
    assertEquals(actual2.getSource(), "urn:li:foo:4");

    BelongsToV2 actual2BelongsToV2 = actual2.getRelatedTo().getBelongsToV2();
    assertEquals(actual2BelongsToV2.getDestination().getString(), owner2.toString());
  }

  @Test
  public void testFindRelationshipsV4WithUnsetOp() {
    // Find all belongs-to relationship for owner.
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner2"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);
    array.add(logicalExpressionCriterion2);

    // unset operator
    LogicalOperation operation = new LogicalOperation();
    operation.setExpressions(array);

    LogicalExpressionLocalRelationshipCriterion.Expr expr = new LogicalExpressionLocalRelationshipCriterion.Expr();
    expr.setLogical(operation);

    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = new LogicalExpressionLocalRelationshipCriterion()
        .setExpr(expr);

    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    // illegalArgumentException is expected here because the filter contains the unset operator.
    assertThrows(IllegalArgumentException.class, () -> {
      _localRelationshipQueryDAO.findRelationshipsV4(
          null, null, "foo", destFilter,
          BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
              new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
          AssetRelationship.class, wrapOptions,
          -1, -1, new RelationshipLookUpContext());
    });
  }

  @Test
  public void testFindRelationshipsV4WithUnknownOp() {
    // Find all belongs-to relationship for owner.
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner2"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);
    array.add(logicalExpressionCriterion2);

    // unknown operator
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = buildLogicalGroup(Operator.UNKNOWN, array);

    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    // illegalArgumentException is expected here because the filter contains the unknown operator.
    assertThrows(IllegalArgumentException.class, () -> {
      _localRelationshipQueryDAO.findRelationshipsV4(
          null, null, "foo", destFilter,
          BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
              new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
          AssetRelationship.class, wrapOptions,
          -1, -1, new RelationshipLookUpContext());
    });

    // $unknown operator
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion1 = buildLogicalGroup(Operator.$UNKNOWN, array);

    LocalRelationshipFilter destFilter1 = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion1);

    // illegalArgumentException is expected here because the filter contains the unknown operator.
    assertThrows(IllegalArgumentException.class, () -> {
      _localRelationshipQueryDAO.findRelationshipsV4(
          null, null, "foo", destFilter,
          BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
              new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
          AssetRelationship.class, wrapOptions,
          -1, -1, new RelationshipLookUpContext());
    });
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV4WithCriteriaField(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn car = new FooUrn(2);

    // Add car and owner into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Find all belongs-to relationship for owner.
    LocalRelationshipFilter destFilter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(owner.toString()),
          Condition.EQUAL,
          new UrnField());
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    // illegalArgumentException is expected here because the filter contains the criteria field for v2/v3 api.
    assertThrows(IllegalArgumentException.class, () -> {
      _localRelationshipQueryDAO.findRelationshipsV4(
          null, null, "foo", destFilter,
          BelongsToV2.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
          AssetRelationship.class, wrapOptions,
          -1, -1, new RelationshipLookUpContext());
    });
  }

  @Test
  public void testIsMgEntityType() throws Exception {
    // EbeanLocalRelationshipQueryDAOTest does not have the same package as EbeanLocalRelationshipQueryDAO (cant access protected method directly).
    Method isMgEntityTypeMethod = EbeanLocalRelationshipQueryDAO.class.getDeclaredMethod("isMgEntityType", String.class);
    isMgEntityTypeMethod.setAccessible(true);

    // assert foo is an MG entity (has metadata_entity_foo table in db)
    assertTrue((Boolean) isMgEntityTypeMethod.invoke(_localRelationshipQueryDAO, "foo"));

    // assert crew is not an MG entity (does not have metadata_entity_crew table in db)
    assertFalse((Boolean) isMgEntityTypeMethod.invoke(_localRelationshipQueryDAO, "crew"));
  }

  @Test
  public void testFindEntitiesOneHopAwayIncomingDirection() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);

    // Add Alice, Bob and Jack into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null, false);

    // Add Bob reports-to Alice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobReportsToAlice), false);

    // Add Jack reports-to Alice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(jack, AspectFoo.class, Collections.singletonList(jackReportsToAlice), false);

    // Find all Alice's direct reports.
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<RecordTemplate> aliceDirectReports = _localRelationshipQueryDAO.findEntities(
        FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        FooSnapshot.class,
        filter,
        ReportsTo.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.INCOMING),
        1, 1, 0, 10);

    // Asserts Alice has two direct reports
    assertEquals(aliceDirectReports.size(), 2);

    Set<FooUrn> actual = aliceDirectReports.stream().map(result -> {
      FooSnapshot person = (FooSnapshot) result;
      return makeFooUrn(person.data().get("urn").toString());
    }).collect(Collectors.toSet());

    // Asserts Alice's direct reports are Jack and Bob.
    Set<FooUrn> expected = ImmutableSet.of(jack, bob);
    assertEquals(actual, expected);
  }

  @Test
  public void testFindEntitiesOneHopAwayOutgoingDirection() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    BarUrn stanford = new BarUrn(1);
    BarUrn mit = new BarUrn(2);

    // Add Alice and Bob into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);

    // Add Stanford and MIT into entity tables.
    _barUrnEBeanLocalAccess.add(stanford, new AspectFoo().setValue("Stanford"), AspectFoo.class, new AuditStamp(), null, false);
    _barUrnEBeanLocalAccess.add(mit, new AspectFoo().setValue("MIT"), AspectFoo.class, new AuditStamp(), null, false);

    // Add Alice belongs to MIT and Stanford.
    BelongsTo aliceBelongsToMit = new BelongsTo().setSource(alice).setDestination(mit);
    BelongsTo aliceBelongsToStanford = new BelongsTo().setSource(alice).setDestination(stanford);
    _localRelationshipWriterDAO.addRelationships(alice, AspectFoo.class, ImmutableList.of(aliceBelongsToStanford, aliceBelongsToMit), false);

    // Add Bob belongs to Stanford.
    BelongsTo bobBelongsToStandford = new BelongsTo().setSource(bob).setDestination(stanford);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobBelongsToStandford), false);

    // Alice filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter aliceFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    // Find all the schools Alice has attended.
    List<RecordTemplate> schoolsAliceAttends = _localRelationshipQueryDAO.findEntities(
        FooSnapshot.class,
        aliceFilter,
        BarSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        BelongsTo.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.OUTGOING),
        1, 1, 0, 10);

    // Asserts Alice attends two schools
    assertEquals(schoolsAliceAttends.size(), 2);

    Set<BarUrn> actual = schoolsAliceAttends.stream().map(result -> {
      BarSnapshot school = (BarSnapshot) result;
      return makeBarUrn(school.data().get("urn").toString());
    }).collect(Collectors.toSet());

    // Asserts Alice attends Stanford and MIT
    Set<BarUrn> expected = ImmutableSet.of(stanford, mit);
    assertEquals(actual, expected);
  }

  @Test
  public void testFindEntitiesOneHopAwayUndirected() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);
    FooUrn john = new FooUrn(4);

    // Add Alice, Bob, Jack and John into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(john, new AspectFoo().setValue("John"), AspectFoo.class, new AuditStamp(), null, false);

    _fooUrnEBeanLocalAccess.add(alice, new AspectBar().setValue("32"), AspectBar.class, new AuditStamp(), null, false); // Alice 32 years old

    _fooUrnEBeanLocalAccess.add(bob, new AspectBar().setValue("52"), AspectBar.class, new AuditStamp(), null, false); // Bob 52 years old

    _fooUrnEBeanLocalAccess.add(jack, new AspectBar().setValue("16"), AspectBar.class, new AuditStamp(), null, false); // Jack 16 years old

    _fooUrnEBeanLocalAccess.add(john, new AspectBar().setValue("42"), AspectBar.class, new AuditStamp(), null, false); // John 42 years old

    // Add Alice pair-with Jack relationships. Alice --> Jack.
    PairsWith alicePairsWithJack = new PairsWith().setSource(alice).setDestination(jack);
    _localRelationshipWriterDAO.addRelationships(alice, AspectFoo.class, Collections.singletonList(alicePairsWithJack), false);

    // Add Bob pair-with Alice relationships. Bob --> Alice.
    PairsWith bobPairsWithAlice = new PairsWith().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobPairsWithAlice), false);

    // Add Alice pair-with John relationships. Alice --> John.
    PairsWith alicePairsWithJohn = new PairsWith().setSource(alice).setDestination(john);
    _localRelationshipWriterDAO.addRelationships(alice, AspectFoo.class, Collections.singletonList(alicePairsWithJohn), false);

    // Alice filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter aliceFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    // Age filter
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("30"),
        Condition.GREATER_THAN,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter ageFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion2));


    // Find all the persons that are paired with Alice and also more than 30 years old.
    List<RecordTemplate> personsPairedWithAlice = _localRelationshipQueryDAO.findEntities(
        FooSnapshot.class,
        aliceFilter,
        FooSnapshot.class,
        ageFilter,
        PairsWith.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        1, 1, 0, 10);

    // Asserts Alice pairs with two persons
    assertEquals(personsPairedWithAlice.size(), 2);

    Set<FooUrn> actual = personsPairedWithAlice.stream().map(result -> {
      FooSnapshot school = (FooSnapshot) result;
      return makeFooUrn(school.data().get("urn").toString());
    }).collect(Collectors.toSet());

    // Asserts Alice paired with Bob and John
    Set<FooUrn> expected = ImmutableSet.of(bob, john);
    assertEquals(actual, expected);
  }

  @Test
  public void testFindOneEntityWithInCondition() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(new StringArray("foo")),
        Condition.IN,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("foo"));
  }

  @Test
  public void testFindNoEntityWithInCondition() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(new StringArray("bar")),
        Condition.IN,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 0);
  }

  @Test
  public void testFindEntitiesWithEmptyRelationshipFilter() throws URISyntaxException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

    // Create empty filter
    LocalRelationshipFilter emptyFilter = new LocalRelationshipFilter();

    try {
      _localRelationshipQueryDAO.findEntities(FooSnapshot.class, emptyFilter, FooSnapshot.class, emptyFilter, PairsWith.class, emptyFilter, 1, 1, 0, 10);
    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalArgumentException);
      assertEquals(ex.getMessage(), "Relationship direction cannot be null or UNKNOWN.");
    }
  }

  @Test
  public void testFindEntitiesWithSingleInCondition() throws OperationNotSupportedException, URISyntaxException {
    // Added 20 FooUrn entities with aspect AspectFoo and value "foo1" to "foo20"
    for (int i = 1; i <= 20; i++) {
      _fooUrnEBeanLocalAccess.add(new FooUrn(i), new AspectFoo().setValue("foo" + i), AspectFoo.class, new AuditStamp(),
          null, false);
    }

    // Created one more FooUrn entity with aspect AspectBar and value "bar" and AspectFoo with value "foo5"
    FooUrn one = new FooUrn(21);
    _fooUrnEBeanLocalAccess.add(one, new AspectFoo().setValue("foo5"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(one, new AspectBar().setValue("bar"), AspectBar.class, new AuditStamp(), null, false);

    // Prepare the filter values for AspectFoo
    List<String> values = Arrays.asList("foo1", "foo2", "foo3", "foo4", "foo5");

    // Create a single criterion with all values in one IN clause for AspectFoo
    LocalRelationshipCriterion filterCriterion =
        EBeanDAOUtils.buildRelationshipFieldCriterion(
            LocalRelationshipValue.create(new StringArray(values)),
            Condition.IN,
            new AspectField()
                .setAspect(AspectFoo.class.getCanonicalName())
                .setPath("/value")
        );

    // Create the EQUAL criterion for AspectBar
    LocalRelationshipCriterion filterCriterion1 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("bar"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value")
    );

    LocalRelationshipFilter filter = new LocalRelationshipFilter();
    filter.setCriteria(new LocalRelationshipCriterionArray(Arrays.asList(filterCriterion, filterCriterion1)));

    // Retrieve entities (limit to 100 results for testing)
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 100);

    // Assertions
    assertEquals(fooSnapshotList.size(), 1); // Only one entity should match the criteria
  }


  /**
   * Same as {@link #testFindEntitiesWithSingleInCondition} but with multiple IN conditions.
   */
  @Test
  public void testFindEntitiesWithMultipleInConditions()
      throws OperationNotSupportedException, URISyntaxException, NoSuchFieldException, IllegalAccessException {
    // Added 20 FooUrn entities with aspect AspectFoo and value "foo1" to "foo20"
    for (int i = 1; i <= 20; i++) {
      _fooUrnEBeanLocalAccess.add(new FooUrn(i), new AspectFoo().setValue("foo" + i), AspectFoo.class, new AuditStamp(),
          null, false);
    }
    // Created one more FooUrn entity with aspect AspectBar and value "bar" and AspectFoo with value "foo5"
    FooUrn one = new FooUrn(21);
    _fooUrnEBeanLocalAccess.add(one, new AspectFoo().setValue("foo5"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(one, new AspectBar().setValue("bar"), AspectBar.class, new AuditStamp(), null, false);


    List<LocalRelationshipCriterion> criteriaList = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      LocalRelationshipCriterion filterCriterion =
          EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(new StringArray("foo" + i)),
              Condition.IN, new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      criteriaList.add(filterCriterion);
    }
    // Create the EQUAL criterion for AspectBar
    criteriaList.add(EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("bar"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value")
    ));

    LocalRelationshipFilter filter = new LocalRelationshipFilter();
    filter.setCriteria(new LocalRelationshipCriterionArray(criteriaList));

    // Retrieve entities (limit to 100 results for testing)
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 100);

    // Assertions
    assertEquals(fooSnapshotList.size(), 1); // Only one entity should match the criteria
  }

  @Test
  public void testBuildFindRelationshipSQL() {
    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
        + "INNER JOIN source_table_name st ON st.urn=rt.source WHERE rt.deleted_ts is NULL");
  }

  @Test
  public void testBuildFindRelationshipSQLWithSource() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "metadata_entity_foo", srcFilter, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN metadata_entity_foo st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND st.i_aspectfoo"
            + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "value='Alice'");
  }

  @Test
  public void testBuildFindRelationshipSQLWithDestination() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "metadata_entity_bar", destFilter,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN metadata_entity_bar dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND dt.i_aspectfoo"
            + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "value='Alice'");
  }

  @Test
  public void testBuildFindRelationshipSQLWithSourceAndDestination() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Bob"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion2));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "metadata_entity_foo", srcFilter, "metadata_entity_bar", destFilter,
        -1, -1, new RelationshipLookUpContext());

    char virtualColumnDelimiter = _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? '0' : '$';
    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN metadata_entity_bar dt ON dt.urn=rt.destination "
            + "INNER JOIN metadata_entity_foo st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND (dt.i_aspectfoo"
            + virtualColumnDelimiter + "value='Bob') AND (st.i_aspectfoo" + virtualColumnDelimiter + "value='Alice')");
  }

  @Test
  public void testBuildFindRelationshipSQLWithHistory() {
    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext(true));

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source ) ranked_rows WHERE row_num = 1");
  }

  @Test
  public void testBuildFindRelationshipSQLWithHistoryWithRelationshipWithSubtype() {
    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("metadata_relationship_belongstov2",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext(true));

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source"
            + (", rt.metadata" + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "type")
            + ", rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM metadata_relationship_belongstov2 rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source ) ranked_rows WHERE row_num = 1");
  }

  @Test
  public void testBuildFindRelationshipSQLWithHistoryWithSource() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "metadata_entity_foo", srcFilter, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext(true));

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN metadata_entity_foo st ON st.urn=rt.source  WHERE st.i_aspectfoo"
            + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "value='Alice') ranked_rows WHERE row_num = 1");
  }

  @Test
  public void testBuildFindRelationshipSQLWithHistoryWithDestination() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "metadata_entity_bar", destFilter,
        -1, -1, new RelationshipLookUpContext(true));

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM relationship_table_name rt INNER JOIN metadata_entity_bar dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source  WHERE dt.i_aspectfoo"
            + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "value='Alice') ranked_rows WHERE row_num = 1");
  }

  @Test
  public void testBuildFindRelationshipSQLWithHistoryWithSourceAndDestination() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
            LocalRelationshipValue.create("urn:li:foo:4"),
            Condition.EQUAL,
            new UrnField());
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Bob"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion2));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "metadata_entity_foo", srcFilter, "metadata_entity_bar", destFilter,
        -1, -1, new RelationshipLookUpContext(true));

    char virtualColumnDelimiter = _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? '0' : '$';

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM relationship_table_name rt INNER JOIN metadata_entity_bar dt ON dt.urn=rt.destination "
            + "INNER JOIN metadata_entity_foo st ON st.urn=rt.source  WHERE (dt.i_aspectfoo" + virtualColumnDelimiter
            + "value='Bob') AND (st.urn='urn:li:foo:4')) ranked_rows WHERE row_num = 1");
  }

  @Test
  public void testBuildFindRelationshipSQLWithLogicalExpression() {
    LocalRelationshipFilter srcFilter = createLocalRelationshipFilterWithAndLogicalExpression();

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", srcFilter, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND (st.urn='urn:li:foo:4' AND st.urn='urn:li:foo:5')");
  }

  private static LocalRelationshipFilter createLocalRelationshipFilterWithAndLogicalExpression() {
    LocalRelationshipCriterion urnCriterion1 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:4"),
        Condition.EQUAL,
        new UrnField());
    LogicalExpressionLocalRelationshipCriterion logical1 =
        wrapCriterionAsLogicalExpression(urnCriterion1);

    LocalRelationshipCriterion urnCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:5"),
        Condition.EQUAL,
        new UrnField());
    LogicalExpressionLocalRelationshipCriterion logical2 =
        wrapCriterionAsLogicalExpression(urnCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray andArray = new LogicalExpressionLocalRelationshipCriterionArray();
    andArray.add(logical1);
    andArray.add(logical2);

    LogicalExpressionLocalRelationshipCriterion andCriterion = buildLogicalGroup(Operator.AND, andArray);

    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter();
    srcFilter.setLogicalExpressionCriteria(andCriterion);
    return srcFilter;
  }

  @Test
  public void testBuildFindRelationshipSQLWithSingleQuote() {
    // The bug case raised in META-22917
    String problematicUrn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/jobs/dsotnt/lix_evaluations/premium_custom_button_acq_convoad_trex_eval_results',PROD)";

    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(problematicUrn),
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(logicalExpressionCriterion)
        .setDirection(RelationshipDirection.OUTGOING);

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL(
        "metadata_relationship_belongsto",
        filter,
        "metadata_entity_foo", null,
        null, null,
        -1, -1, new RelationshipLookUpContext());

    // Verify the SQL is valid and the special characters are properly escaped
    assertNotNull(sql);
    assertTrue(sql.contains("metadata_relationship_belongsto"));
    assertTrue(sql.contains("metadata_entity_foo"));
    // The problematic URN should be properly escaped in the WHERE clause
    assertTrue(sql.contains("rt.destination="));

    // Most importantly, execute the SQL to ensure it doesn't throw syntax errors
    try {
      _server.createSqlQuery(sql).findList();
    } catch (PersistenceException e) {
      fail("SQL query with special characters should not throw syntax errors: " + e.getMessage());
    }
  }


  @Test
  public void testFindRelationshipsWithSingleQuoteInUrn() throws Exception {
    // The original bug case raised in META-22917
    String problematicUrn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/jobs/dsotnt/lix_evaluations/premium_custom_button_acq_convoad_trex_eval_results',PROD)";

    FooUrn source = new FooUrn(1);

    // Add source entity
    _fooUrnEBeanLocalAccess.add(source, new AspectFoo().setValue("Source"), AspectFoo.class, new AuditStamp(), null, false);

    // Create relationship with problematic destination URN
    BelongsToV2 relationship = new BelongsToV2();
    relationship.setDestination(BelongsToV2.Destination.create(problematicUrn));
    _localRelationshipWriterDAO.addRelationships(source, AspectFoo.class, Collections.singletonList(relationship), false);

    // Query with the problematic URN
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(problematicUrn),
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(logicalExpressionCriterion)
        .setDirection(RelationshipDirection.OUTGOING);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    // This should work without SQL exceptions
    List<AssetRelationship> results = _localRelationshipQueryDAO.findRelationshipsV4(
        "foo", null, null, null,  // No destination entity type/filter since it's non-MG
        BelongsToV2.class,
        filter,
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    // Should find the relationship
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).getRelatedTo().getBelongsToV2().getDestination().getString(), problematicUrn);
  }

  // -------------------------------------------------------------------------
  // Keyset (seek) pagination: typed API, model and baseline SQL builder
  // -------------------------------------------------------------------------

  private LocalRelationshipFilter emptyFilter() {
    return new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray());
  }

  private LocalRelationshipFilter outgoingEmptyFilter() {
    return new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray())
        .setDirection(RelationshipDirection.OUTGOING);
  }

  private List<FooUrn> addReportsToChain(int count, FooUrn destination) throws URISyntaxException {
    List<FooUrn> sources = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      FooUrn source = new FooUrn(100 + i);
      _localRelationshipWriterDAO.addRelationships(source, AspectFoo.class,
          Collections.singletonList(new ReportsTo().setSource(source).setDestination(destination)), false);
      sources.add(source);
    }
    return sources;
  }

  private RelationshipKeysetPage<ReportsTo> keysetPage(int pageSize, RelationshipKeysetCursor cursor) {
    return keysetPage(_localRelationshipQueryDAO, pageSize, cursor);
  }

  private RelationshipKeysetPage<ReportsTo> keysetPage(EbeanLocalRelationshipQueryDAO queryDAO, int pageSize,
      RelationshipKeysetCursor cursor) {
    return queryDAO.findRelationshipsByKeyset(
        null, emptyFilter(), null, emptyFilter(), ReportsTo.class, outgoingEmptyFilter(), pageSize, cursor);
  }

  private List<ReportsTo> drainKeyset(int pageSize) {
    return drainKeyset(pageSize, null);
  }

  private List<ReportsTo> drainKeyset(int pageSize, RelationshipKeysetCursor cursor) {
    List<ReportsTo> all = new ArrayList<>();
    do {
      RelationshipKeysetPage<ReportsTo> page = keysetPage(pageSize, cursor);
      all.addAll(page.getRelationships());
      cursor = page.getNextCursor();
    } while (cursor != null);
    return all;
  }

  private String captureScanStartTime() {
    return _server.createSqlQuery("SELECT DATE_FORMAT(NOW(6), '%Y-%m-%d %H:%i:%s.%f') AS scan_start_time")
        .findOne()
        .getString("scan_start_time");
  }

  private long maxRelationshipId(String relationshipTableName) {
    return _server.createSqlQuery("SELECT COALESCE(MAX(id), 0) AS max_id FROM " + relationshipTableName)
        .findOne()
        .getLong("max_id");
  }

  private void softDeleteReportsToAfterScanStart(String scanStartTime, FooUrn... sources) {
    for (FooUrn source : sources) {
      _server.createSqlUpdate("UPDATE metadata_relationship_reportsto "
          + "SET deleted_ts=DATE_ADD(STR_TO_DATE(:scanStartTime, '%Y-%m-%d %H:%i:%s.%f'), INTERVAL 1 MICROSECOND) "
          + "WHERE source=:source AND deleted_ts IS NULL")
          .setParameter("scanStartTime", scanStartTime)
          .setParameter("source", source.toString())
          .execute();
    }
  }

  private RelationshipKeysetCursor reportsToCursorAtScanStart(long lastId, String scanStartTime) {
    String relationshipTableName = SQLSchemaUtils.getRelationshipTableName(ReportsTo.class);
    return new RelationshipKeysetCursor(lastId, maxRelationshipId(relationshipTableName), scanStartTime,
        relationshipTableName);
  }

  private void assertSourcesInOrder(List<ReportsTo> relationships, FooUrn... expectedSources) {
    assertEquals(relationships.size(), expectedSources.length);
    for (int i = 0; i < expectedSources.length; i++) {
      assertEquals(makeFooUrn(relationships.get(i).getSource().toString()), expectedSources[i]);
    }
  }

  private void assertSourcesExactlyOnce(List<ReportsTo> relationships, Set<FooUrn> expectedSources) {
    assertEquals(relationships.size(), expectedSources.size());
    Set<FooUrn> actualSources = relationships.stream()
        .map(r -> makeFooUrn(r.getSource().toString()))
        .collect(Collectors.toSet());
    assertEquals(actualSources.size(), relationships.size());
    assertEquals(actualSources, expectedSources);
  }

  @Test
  public void testBuildFindRelationshipKeysetCurrentSQL() {
    // Unlike the existing offset builder, the keyset builder adds `id > lastId`, `id <= maxId`,
    // `ORDER BY rt.id ASC` and a finite LIMIT. The current-row query keeps the original
    // `deleted_ts is NULL` predicate so the hot path keeps the old index plan.
    String sql = _localRelationshipQueryDAO.buildFindRelationshipKeysetCurrentSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "destination_table_name", null,
        10, 5, 20);

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source WHERE rt.deleted_ts is NULL "
            + "AND rt.id > 5 AND rt.id <= 20 ORDER BY rt.id ASC LIMIT 10");
  }

  @Test
  public void testBuildFindRelationshipKeysetDeletedSinceScanStartSQL() {
    String sql = _localRelationshipQueryDAO.buildFindRelationshipKeysetDeletedSinceScanStartSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "destination_table_name", null,
        10, 5, 20, "2026-08-05 12:34:56.789000");

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source WHERE "
            + "rt.deleted_ts > STR_TO_DATE(:scanStartTime, '%Y-%m-%d %H:%i:%s.%f') "
            + "AND rt.id > 5 AND rt.id <= 20 ORDER BY rt.id ASC LIMIT 10");
  }

  @Test
  public void testBuildFindRelationshipKeysetSQLWithSource() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipKeysetCurrentSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "metadata_entity_foo", srcFilter, "destination_table_name", null,
        10, 5, 20);

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN metadata_entity_foo st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND st.i_aspectfoo"
            + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "value='Alice' "
            + "AND rt.id > 5 AND rt.id <= 20 ORDER BY rt.id ASC LIMIT 10");
  }

  @Test
  public void testBuildFindRelationshipKeysetSQLRejectsInvalidPageSize() {
    // Zero and negative are rejected.
    expectThrows(IllegalArgumentException.class, () ->
        _localRelationshipQueryDAO.buildFindRelationshipKeysetCurrentSQL("relationship_table_name",
            new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
            null, null, null, null, 0, 0, 20));
    expectThrows(IllegalArgumentException.class, () ->
        _localRelationshipQueryDAO.buildFindRelationshipKeysetCurrentSQL("relationship_table_name",
            new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
            null, null, null, null, -1, 0, 20));
    // Above the hard upper bound of 1000 is rejected.
    expectThrows(IllegalArgumentException.class, () ->
        _localRelationshipQueryDAO.buildFindRelationshipKeysetCurrentSQL("relationship_table_name",
            new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
            null, null, null, null, 1001, 0, 20));
    expectThrows(IllegalArgumentException.class, () ->
        _localRelationshipQueryDAO.buildFindRelationshipKeysetDeletedSinceScanStartSQL("relationship_table_name",
            new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
            null, null, null, null, 10, 0, 20, null));
  }

  @Test
  public void testKeysetPaginationRejectedInNonNewSchemaModes() throws URISyntaxException {
    addReportsToChain(2, new FooUrn(1));

    // Keyset pagination is supported only in NEW_SCHEMA_ONLY. Both OLD_SCHEMA_ONLY and the
    // deprecated DUAL_SCHEMA must be rejected before building/executing any SQL, on the typed API
    // and the public SQL builder alike. Restore NEW_SCHEMA_ONLY after each assertion so the mode
    // cannot leak into subsequent assertions or tests.
    for (EbeanLocalDAO.SchemaConfig rejectedConfig : new EbeanLocalDAO.SchemaConfig[]{
        EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY, EbeanLocalDAO.SchemaConfig.DUAL_SCHEMA}) {
      _localRelationshipQueryDAO.setSchemaConfig(rejectedConfig);

      // Typed API rejects the mode before building/executing any SQL.
      expectThrows(UnsupportedOperationException.class, () -> keysetPage(3, null));

      // The public SQL builder rejects the mode as well.
      expectThrows(UnsupportedOperationException.class, () ->
          _localRelationshipQueryDAO.buildFindRelationshipKeysetCurrentSQL("relationship_table_name",
              new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
              null, null, null, null, 10, 5, 20));

      _localRelationshipQueryDAO.setSchemaConfig(EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY);
    }
  }

  @Test
  public void testFindRelationshipsByKeysetEmptyTable() {
    RelationshipKeysetPage<ReportsTo> page = keysetPage(3, null);
    assertTrue(page.getRelationships().isEmpty());
    assertNull(page.getNextCursor());
    assertEquals(page.getMaxId(), 0L);
  }

  @Test
  public void testFindRelationshipsByKeysetRejectsInvalidPageSize() {
    expectThrows(IllegalArgumentException.class, () -> keysetPage(0, null));
    expectThrows(IllegalArgumentException.class, () -> keysetPage(-1, null));
    // Above the hard upper bound of 1000 is rejected.
    expectThrows(IllegalArgumentException.class, () -> keysetPage(1001, null));
  }

  @Test
  public void testFindRelationshipsByKeysetFewerThanPageSize() throws URISyntaxException {
    addReportsToChain(2, new FooUrn(1));
    RelationshipKeysetPage<ReportsTo> page = keysetPage(5, null);
    assertEquals(page.getRelationships().size(), 2);
    assertNull(page.getNextCursor());
    assertEquals(page.getMaxId(), 2L);
  }

  @Test
  public void testFindRelationshipsByKeysetExactlyPageSize() throws URISyntaxException {
    addReportsToChain(3, new FooUrn(1));

    // A full page whose last matching row equals maxId ends the scan: no next cursor.
    RelationshipKeysetPage<ReportsTo> first = keysetPage(3, null);
    assertEquals(first.getRelationships().size(), 3);
    assertNull(first.getNextCursor());
    assertEquals(first.getMaxId(), 3L);
  }

  @Test
  public void testFindRelationshipsByKeysetFullPageBelowMaxIdYieldsFinalEmptyQuery()
      throws URISyntaxException {
    FooUrn dest = new FooUrn(1);
    // Insert 3 rows (ids 1..3), then soft-delete the last two by re-pointing their sources.
    List<FooUrn> sources = addReportsToChain(3, dest);
    // Re-adding source #2 and #3 soft-deletes ids 2 and 3 and inserts ids 4 and 5.
    _localRelationshipWriterDAO.addRelationships(sources.get(1), AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(sources.get(1)).setDestination(new FooUrn(2))), false);
    _localRelationshipWriterDAO.addRelationships(sources.get(2), AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(sources.get(2)).setDestination(new FooUrn(2))), false);
    _server.createSqlUpdate("UPDATE metadata_relationship_reportsto SET deleted_ts='2000-01-01 00:00:00' "
        + "WHERE deleted_ts IS NOT NULL").execute();

    // Current rows toward dest #1: only id 1. Max id is 5 because later nonmatching rows
    // (ids 4/5 pointing elsewhere) set maxId. A page size of 1 fills fully with id 1 (below maxId
    // 5), so a next cursor is produced; that continuation query is empty because ids 2 and 3 are
    // soft-deleted and 4/5 point elsewhere, so one final empty page is needed to end the scan.
    LocalRelationshipCriterion relCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(dest.toString()), Condition.EQUAL, new UrnField().setName("destination"));
    LocalRelationshipFilter relationshipFilter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray(relCriterion))
        .setDirection(RelationshipDirection.OUTGOING);

    RelationshipKeysetPage<ReportsTo> first = _localRelationshipQueryDAO.findRelationshipsByKeyset(
        null, emptyFilter(), null, emptyFilter(), ReportsTo.class, relationshipFilter, 1, null);
    assertEquals(first.getRelationships().size(), 1);
    assertNotNull(first.getNextCursor());
    assertEquals(first.getNextCursor().getMaxId(), 5L);

    RelationshipKeysetPage<ReportsTo> second = _localRelationshipQueryDAO.findRelationshipsByKeyset(
        null, emptyFilter(), null, emptyFilter(), ReportsTo.class, relationshipFilter, 1, first.getNextCursor());
    assertTrue(second.getRelationships().isEmpty());
    assertNull(second.getNextCursor());
  }

  @Test
  public void testFindRelationshipsByKeysetMultiPageOrderedComplete() throws URISyntaxException {
    Set<FooUrn> expectedSources = new java.util.HashSet<>(addReportsToChain(7, new FooUrn(1)));

    List<ReportsTo> drained = new ArrayList<>();
    RelationshipKeysetCursor cursor = null;
    String scanStartTime = null;
    String relationshipTableName = SQLSchemaUtils.getRelationshipTableName(ReportsTo.class);
    do {
      RelationshipKeysetPage<ReportsTo> page = keysetPage(2, cursor);
      drained.addAll(page.getRelationships());
      cursor = page.getNextCursor();
      if (cursor != null) {
        assertNotNull(cursor.getScanStartTime());
        assertEquals(cursor.getRelationshipTableName(), relationshipTableName);
        if (scanStartTime == null) {
          scanStartTime = cursor.getScanStartTime();
        } else {
          assertEquals(cursor.getScanStartTime(), scanStartTime);
        }
      }
    } while (cursor != null);

    assertEquals(drained.size(), 7);
    Set<FooUrn> actualSources = drained.stream()
        .map(r -> makeFooUrn(r.getSource().toString()))
        .collect(Collectors.toSet());
    // No duplicates and all sources present.
    assertEquals(actualSources.size(), 7);
    assertEquals(actualSources, expectedSources);
  }

  @Test
  public void testFindRelationshipsByKeysetMaxIdExcludesLaterInsert() throws URISyntaxException {
    addReportsToChain(5, new FooUrn(1));

    // Capture the max id on the first page.
    RelationshipKeysetCursor cursor = null;
    List<ReportsTo> drained = new ArrayList<>();
    RelationshipKeysetPage<ReportsTo> page = keysetPage(2, null);
    drained.addAll(page.getRelationships());
    cursor = page.getNextCursor();
    assertEquals(page.getMaxId(), 5L);

    // Insert more rows after the scan started; they must not be observed. These later inserts are
    // deterministically excluded by the fixed maxId (best effort only covers updates/deletes of
    // existing ids). Use distinct sources (200+) so no existing relationship is superseded.
    for (int i = 1; i <= 3; i++) {
      FooUrn src = new FooUrn(200 + i);
      _localRelationshipWriterDAO.addRelationships(src, AspectFoo.class,
          Collections.singletonList(new ReportsTo().setSource(src).setDestination(new FooUrn(2))), false);
    }

    while (cursor != null) {
      page = keysetPage(2, cursor);
      drained.addAll(page.getRelationships());
      cursor = page.getNextCursor();
    }

    assertEquals(drained.size(), 5);
  }

  @Test
  public void testFindRelationshipsByKeysetSoftDeletedGaps() throws URISyntaxException {
    FooUrn dest = new FooUrn(1);
    FooUrn a = new FooUrn(101);
    FooUrn b = new FooUrn(102);
    FooUrn c = new FooUrn(103);
    _localRelationshipWriterDAO.addRelationships(a, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(a).setDestination(dest)), false);
    _localRelationshipWriterDAO.addRelationships(b, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(b).setDestination(dest)), false);
    _localRelationshipWriterDAO.addRelationships(c, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(c).setDestination(dest)), false);

    // Re-adding B soft-deletes its first row and inserts a new one, creating an id gap.
    _localRelationshipWriterDAO.addRelationships(b, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(b).setDestination(dest)), false);
    _server.createSqlUpdate("UPDATE metadata_relationship_reportsto SET deleted_ts='2000-01-01 00:00:00' "
        + "WHERE source=:source AND deleted_ts IS NOT NULL")
        .setParameter("source", b.toString()).execute();

    List<ReportsTo> drained = drainKeyset(2);

    // Verifies a soft-deleted id sitting between live rows does not stop or skip the later live
    // rows (distinct from the final-empty case: the gap is mid-scan, not a trailing empty page).
    // Only the three current (non-deleted) relationships are returned, gap skipped.
    assertEquals(drained.size(), 3);
    Set<FooUrn> actual = drained.stream()
        .map(r -> makeFooUrn(r.getSource().toString()))
        .collect(Collectors.toSet());
    assertEquals(actual, ImmutableSet.of(a, b, c));
  }

  @Test
  public void testFindRelationshipsByKeysetWithSourceFilter() throws URISyntaxException {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);

    FooUrn boss = new FooUrn(9);
    _localRelationshipWriterDAO.addRelationships(alice, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(alice).setDestination(boss)), false);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(bob).setDestination(boss)), false);

    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    RelationshipKeysetPage<ReportsTo> page = _localRelationshipQueryDAO.findRelationshipsByKeyset(
        FooSnapshot.class, srcFilter, null, emptyFilter(), ReportsTo.class,
        outgoingEmptyFilter(), 5, null);

    assertEquals(page.getRelationships().size(), 1);
    assertEquals(makeFooUrn(page.getRelationships().get(0).getSource().toString()), alice);
  }

  @Test
  public void testFindRelationshipsByKeysetWithDestinationFilter() throws URISyntaxException {
    FooUrn worker = new FooUrn(1);
    FooUrn alice = new FooUrn(2);
    FooUrn bob = new FooUrn(3);
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);

    _localRelationshipWriterDAO.addRelationships(worker, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(worker).setDestination(alice)), false);
    // A second source reporting to bob so the destination filter has something to exclude.
    _localRelationshipWriterDAO.addRelationships(new FooUrn(4), AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(new FooUrn(4)).setDestination(bob)), false);

    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    RelationshipKeysetPage<ReportsTo> page = _localRelationshipQueryDAO.findRelationshipsByKeyset(
        null, emptyFilter(), FooSnapshot.class, destFilter, ReportsTo.class,
        outgoingEmptyFilter(), 5, null);

    assertEquals(page.getRelationships().size(), 1);
    assertEquals(makeFooUrn(page.getRelationships().get(0).getDestination().toString()), alice);
  }

  @Test
  public void testFindRelationshipsByKeysetWithRelationshipFilter() throws URISyntaxException {
    FooUrn worker = new FooUrn(1);
    FooUrn alice = new FooUrn(2);
    _fooUrnEBeanLocalAccess.add(worker, new AspectFoo().setValue("Worker"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);

    _localRelationshipWriterDAO.addRelationships(worker, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(worker).setDestination(alice)), false);
    _localRelationshipWriterDAO.addRelationships(new FooUrn(3), AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(new FooUrn(3)).setDestination(new FooUrn(4))), false);

    LocalRelationshipCriterion relCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(alice.toString()), Condition.EQUAL, new UrnField().setName("destination"));
    LocalRelationshipFilter relationshipFilter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray(relCriterion))
        .setDirection(RelationshipDirection.OUTGOING);

    RelationshipKeysetPage<ReportsTo> page = _localRelationshipQueryDAO.findRelationshipsByKeyset(
        null, emptyFilter(), null, emptyFilter(), ReportsTo.class, relationshipFilter, 5, null);

    assertEquals(page.getRelationships().size(), 1);
    assertEquals(makeFooUrn(page.getRelationships().get(0).getDestination().toString()), alice);
  }

  @Test
  public void testFindRelationshipsByKeysetPreservesScanStartMembershipUnderConcurrentReplacement()
      throws URISyntaxException {
    FooUrn dest = new FooUrn(1);
    FooUrn a = new FooUrn(101);
    FooUrn b = new FooUrn(102);
    FooUrn c = new FooUrn(103);
    FooUrn d = new FooUrn(104);
    // Ids 1..4, all current, all pointing at dest.
    _localRelationshipWriterDAO.addRelationships(a, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(a).setDestination(dest)), false);
    _localRelationshipWriterDAO.addRelationships(b, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(b).setDestination(dest)), false);
    _localRelationshipWriterDAO.addRelationships(c, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(c).setDestination(dest)), false);
    _localRelationshipWriterDAO.addRelationships(d, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(d).setDestination(dest)), false);

    // Page 1 (size 2) captures the largest row id maxId = 4 and returns ids 1,2 (a, b).
    RelationshipKeysetPage<ReportsTo> first = keysetPage(2, null);
    assertEquals(first.getMaxId(), 4L);
    assertNotNull(first.getNextCursor());
    List<ReportsTo> drained = new ArrayList<>(first.getRelationships());

    // Between pages, "concurrently" replace c: this soft-deletes id 3 (which is <= maxId) and
    // inserts a fresh current row id 5 (which is > maxId). The replacement row is excluded by the
    // fixed maxId bound, but the original id 3 is retained because it was alive at scan start.
    _localRelationshipWriterDAO.addRelationships(c, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(c).setDestination(dest)), false);

    RelationshipKeysetCursor cursor = first.getNextCursor();
    while (cursor != null) {
      RelationshipKeysetPage<ReportsTo> page = keysetPage(2, cursor);
      drained.addAll(page.getRelationships());
      cursor = page.getNextCursor();
    }

    Set<FooUrn> actual = drained.stream()
        .map(r -> makeFooUrn(r.getSource().toString()))
        .collect(Collectors.toSet());
    assertEquals(actual, ImmutableSet.of(a, b, c, d));
  }

  @Test
  public void testFindRelationshipsByKeysetDedupsRowDeletedBetweenCurrentAndDeletedQueries()
      throws URISyntaxException {
    FooUrn dest = new FooUrn(1);
    FooUrn source = new FooUrn(101);
    _localRelationshipWriterDAO.addRelationships(source, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(source).setDestination(dest)), false);

    boolean[] injected = {false};
    EbeanLocalRelationshipQueryDAO racyQueryDAO = new EbeanLocalRelationshipQueryDAO(_server, _eBeanDAOConfig) {
      @Override
      protected void afterKeysetCurrentRowsFetched(String relationshipTableName,
          List<io.ebean.SqlRow> currentRows) {
        if (!injected[0] && !currentRows.isEmpty()
            && relationshipTableName.equals(SQLSchemaUtils.getRelationshipTableName(ReportsTo.class))) {
          injected[0] = true;
          _server.createSqlUpdate("UPDATE metadata_relationship_reportsto "
              + "SET deleted_ts='2099-01-01 00:00:00.000000' WHERE id=:id")
              .setParameter("id", currentRows.get(0).getLong("id"))
              .execute();
        }
      }
    };

    RelationshipKeysetPage<ReportsTo> page = racyQueryDAO.findRelationshipsByKeyset(
        null, emptyFilter(), null, emptyFilter(), ReportsTo.class, outgoingEmptyFilter(), 10, null);

    assertTrue(injected[0]);
    assertEquals(page.getRelationships().size(), 1);
    assertEquals(makeFooUrn(page.getRelationships().get(0).getSource().toString()), source);
    assertNull(page.getNextCursor());
  }

  @Test
  public void testFindRelationshipsByKeysetDoesNotDuplicateRelationshipRewrittenImmediatelyBeforeScan()
      throws URISyntaxException {
    FooUrn dest = new FooUrn(1);
    FooUrn source = new FooUrn(101);
    _localRelationshipWriterDAO.addRelationships(source, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(source).setDestination(dest)), false);

    // This rewrite completes before the first page captures scanStartTime. Only the replacement row
    // was current at scan start, so the old soft-deleted row must not be returned too.
    _localRelationshipWriterDAO.addRelationships(source, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(source).setDestination(dest)), false);

    io.ebean.SqlRow liveRow = _server.createSqlQuery("SELECT COUNT(*) AS live_count FROM metadata_relationship_reportsto "
        + "WHERE source=:source AND deleted_ts IS NULL")
        .setParameter("source", source.toString())
        .findOne();
    assertEquals(liveRow.getLong("live_count").longValue(), 1L);

    List<ReportsTo> drained = drainKeyset(10);
    assertEquals(drained.size(), 1);
    assertEquals(makeFooUrn(drained.get(0).getSource().toString()), source);
  }

  @Test
  public void testFindRelationshipsByKeysetDefersCurrentRowsWhenDeletedRowsTruncateMergedPage()
      throws URISyntaxException {
    List<FooUrn> sources = addReportsToChain(6, new FooUrn(1));
    String scanStartTime = captureScanStartTime();
    RelationshipKeysetCursor cursor = reportsToCursorAtScanStart(0, scanStartTime);
    softDeleteReportsToAfterScanStart(scanStartTime, sources.get(0), sources.get(1));
    long[] firstDeletedUpperId = {-1L};
    EbeanLocalRelationshipQueryDAO observingQueryDAO = new EbeanLocalRelationshipQueryDAO(_server, _eBeanDAOConfig) {
      @Override
      public String buildFindRelationshipKeysetDeletedSinceScanStartSQL(String relationshipTableName,
          LocalRelationshipFilter relationshipFilter, String sourceTableName,
          LocalRelationshipFilter sourceEntityFilter, String destTableName,
          LocalRelationshipFilter destinationEntityFilter, int pageSize, long lastId, long maxId,
          String scanStartTime) {
        if (firstDeletedUpperId[0] == -1L) {
          firstDeletedUpperId[0] = maxId;
        }
        return super.buildFindRelationshipKeysetDeletedSinceScanStartSQL(relationshipTableName, relationshipFilter,
            sourceTableName, sourceEntityFilter, destTableName, destinationEntityFilter, pageSize, lastId, maxId,
            scanStartTime);
      }
    };

    List<ReportsTo> drained = new ArrayList<>();
    RelationshipKeysetPage<ReportsTo> first = keysetPage(observingQueryDAO, 2, cursor);
    // Direct enforcement of Query B's frontier. This pins the per-page work bound; it is not a
    // proof of data correctness, since the merge would select the same rows without the cap.
    assertEquals(firstDeletedUpperId[0], 4L);
    assertSourcesInOrder(first.getRelationships(), sources.get(0), sources.get(1));
    assertNotNull(first.getNextCursor());
    assertEquals(first.getNextCursor().getLastId(), 2L);
    drained.addAll(first.getRelationships());

    RelationshipKeysetPage<ReportsTo> second = keysetPage(observingQueryDAO, 2, first.getNextCursor());
    assertSourcesInOrder(second.getRelationships(), sources.get(2), sources.get(3));
    assertNotNull(second.getNextCursor());
    assertEquals(second.getNextCursor().getLastId(), 4L);
    drained.addAll(second.getRelationships());

    RelationshipKeysetPage<ReportsTo> third = keysetPage(observingQueryDAO, 2, second.getNextCursor());
    assertSourcesInOrder(third.getRelationships(), sources.get(4), sources.get(5));
    assertNull(third.getNextCursor());
    drained.addAll(third.getRelationships());

    assertSourcesExactlyOnce(drained, new java.util.HashSet<>(sources));
  }

  @Test
  public void testFindRelationshipsByKeysetDeletedRowsCanFillWholePage()
      throws URISyntaxException {
    List<FooUrn> sources = addReportsToChain(4, new FooUrn(1));
    String scanStartTime = captureScanStartTime();
    RelationshipKeysetCursor cursor = reportsToCursorAtScanStart(0, scanStartTime);
    softDeleteReportsToAfterScanStart(scanStartTime, sources.toArray(new FooUrn[sources.size()]));

    RelationshipKeysetPage<ReportsTo> first = keysetPage(2, cursor);
    assertSourcesInOrder(first.getRelationships(), sources.get(0), sources.get(1));
    assertNotNull(first.getNextCursor());

    RelationshipKeysetPage<ReportsTo> second = keysetPage(2, first.getNextCursor());
    assertSourcesInOrder(second.getRelationships(), sources.get(2), sources.get(3));
    assertNull(second.getNextCursor());

    List<ReportsTo> drained = new ArrayList<>();
    drained.addAll(first.getRelationships());
    drained.addAll(second.getRelationships());
    assertSourcesExactlyOnce(drained, new java.util.HashSet<>(sources));
  }

  @Test
  public void testFindRelationshipsByKeysetInterleavesDeletedRowsAroundCurrentRows()
      throws URISyntaxException {
    List<FooUrn> sources = addReportsToChain(6, new FooUrn(1));
    String scanStartTime = captureScanStartTime();
    RelationshipKeysetCursor cursor = reportsToCursorAtScanStart(0, scanStartTime);
    softDeleteReportsToAfterScanStart(scanStartTime, sources.get(1), sources.get(3), sources.get(5));

    RelationshipKeysetPage<ReportsTo> first = keysetPage(4, cursor);
    assertSourcesInOrder(first.getRelationships(), sources.get(0), sources.get(1), sources.get(2), sources.get(3));
    assertNotNull(first.getNextCursor());

    RelationshipKeysetPage<ReportsTo> second = keysetPage(4, first.getNextCursor());
    assertSourcesInOrder(second.getRelationships(), sources.get(4), sources.get(5));
    assertNull(second.getNextCursor());

    List<ReportsTo> drained = new ArrayList<>();
    drained.addAll(first.getRelationships());
    drained.addAll(second.getRelationships());
    assertSourcesExactlyOnce(drained, new java.util.HashSet<>(sources));
  }

  @Test
  public void testFindRelationshipsByKeysetFinalMergedPageCanLandOnMaxId()
      throws URISyntaxException {
    List<FooUrn> sources = addReportsToChain(4, new FooUrn(1));
    String scanStartTime = captureScanStartTime();
    RelationshipKeysetCursor cursor = reportsToCursorAtScanStart(0, scanStartTime);
    softDeleteReportsToAfterScanStart(scanStartTime, sources.get(3));

    RelationshipKeysetPage<ReportsTo> first = keysetPage(2, cursor);
    assertSourcesInOrder(first.getRelationships(), sources.get(0), sources.get(1));
    assertNotNull(first.getNextCursor());

    RelationshipKeysetPage<ReportsTo> second = keysetPage(2, first.getNextCursor());
    assertSourcesInOrder(second.getRelationships(), sources.get(2), sources.get(3));
    assertNull(second.getNextCursor());

    List<ReportsTo> drained = new ArrayList<>();
    drained.addAll(first.getRelationships());
    drained.addAll(second.getRelationships());
    assertSourcesExactlyOnce(drained, new java.util.HashSet<>(sources));
  }

  @Test
  public void testRelationshipKeysetCursorValidationAndGetters() {
    String scanStartTime = "2026-08-05 12:34:56.789000";
    RelationshipKeysetCursor cursor =
        new RelationshipKeysetCursor(3, 10, scanStartTime, "metadata_relationship_reportsto");
    assertEquals(cursor.getLastId(), 3L);
    assertEquals(cursor.getMaxId(), 10L);
    assertEquals(cursor.getScanStartTime(), scanStartTime);
    assertEquals(cursor.getRelationshipTableName(), "metadata_relationship_reportsto");

    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetCursor(-1, 10, "2026-08-05 12:34:56.789000", "metadata_relationship_reportsto"));
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetCursor(3, -1, "2026-08-05 12:34:56.789000", "metadata_relationship_reportsto"));
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetCursor(11, 10, "2026-08-05 12:34:56.789000", "metadata_relationship_reportsto"));
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetCursor(3, 10, null, "metadata_relationship_reportsto"));
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetCursor(3, 10, "", "metadata_relationship_reportsto"));
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetCursor(3, 10, "2026-08-05 12:34:56.789", "metadata_relationship_reportsto"));
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetCursor(3, 10, "2026-02-30 12:34:56.789000", "metadata_relationship_reportsto"));
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetCursor(3, 10, "2026-08-05 12:34:56.789000", null));
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetCursor(3, 10, "2026-08-05 12:34:56.789000", ""));
  }

  @Test
  public void testFindRelationshipsByKeysetRejectsCursorForDifferentRelationshipTable() {
    String belongsToTable = SQLSchemaUtils.getRelationshipTableName(BelongsToV2.class);
    String reportsToTable = SQLSchemaUtils.getRelationshipTableName(ReportsTo.class);
    RelationshipKeysetCursor wrongTypeCursor =
        new RelationshipKeysetCursor(0, 1, "2026-08-05 12:34:56.789000", belongsToTable);

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> keysetPage(2, wrongTypeCursor));
    assertTrue(exception.getMessage().contains(belongsToTable));
    assertTrue(exception.getMessage().contains(reportsToTable));
  }

  @Test
  public void testRelationshipKeysetPageDefensiveAndValidation() throws URISyntaxException {
    List<ReportsTo> source = new ArrayList<>();
    source.add(new ReportsTo().setSource(new FooUrn(1)).setDestination(new FooUrn(2)));
    RelationshipKeysetCursor nextCursor =
        new RelationshipKeysetCursor(5, 10, "2026-08-05 12:34:56.789000", "metadata_relationship_reportsto");
    RelationshipKeysetPage<ReportsTo> page =
        new RelationshipKeysetPage<>(source, 10, nextCursor);

    // Defensive copy: mutating the input list cannot alter the returned page contents.
    source.clear();
    assertEquals(page.getRelationships().size(), 1);
    assertNotNull(page.getNextCursor());
    expectThrows(UnsupportedOperationException.class, () -> page.getRelationships().add(null));

    // next cursor maxId must match maxId.
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetPage<>(new ArrayList<ReportsTo>(), 10,
            new RelationshipKeysetCursor(5, 9, "2026-08-05 12:34:56.789000", "metadata_relationship_reportsto")));
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetPage<ReportsTo>(null, 10, null));
  }

  // -------------------------------------------------------------------------
  // V4 keyset (seek) pagination: behavioral tests
  // -------------------------------------------------------------------------

  @Test
  public void testFindRelationshipsV4ByKeysetMultiPageWrappedRecordsWithLogicalFilters()
      throws URISyntaxException {
    FooUrn owner = new FooUrn(1000);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);

    // Five cars, each with aspect value "Car", each belongs-to the same owner.
    List<FooUrn> cars = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      FooUrn car = new FooUrn(i);
      _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
      BelongsToV2 belongsTo = new BelongsToV2();
      belongsTo.setDestination(BelongsToV2.Destination.create(owner.toString()));
      _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(belongsTo), false);
      cars.add(car);
    }

    // Source nonmatch: a "Truck" belongs-to the owner; excluded by the source aspect filter.
    FooUrn truck = new FooUrn(2000);
    _fooUrnEBeanLocalAccess.add(truck, new AspectFoo().setValue("Truck"), AspectFoo.class, new AuditStamp(), null, false);
    BelongsToV2 truckBelongsTo = new BelongsToV2();
    truckBelongsTo.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(truck, AspectFoo.class, Collections.singletonList(truckBelongsTo), false);

    // Destination nonmatch: a "Car" belongs-to a "Stranger"; excluded solely by the destination
    // aspect filter (destination value != "Owner"). The relationship filter is intentionally an
    // empty logical OUTGOING filter here, so this row is excluded by the destination filter alone;
    // relationship-filter behavior is covered independently by the non-MG destination test.
    FooUrn stranger = new FooUrn(3000);
    _fooUrnEBeanLocalAccess.add(stranger, new AspectFoo().setValue("Stranger"), AspectFoo.class, new AuditStamp(), null, false);
    FooUrn strayCar = new FooUrn(6);
    _fooUrnEBeanLocalAccess.add(strayCar, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    BelongsToV2 strayBelongsTo = new BelongsToV2();
    strayBelongsTo.setDestination(BelongsToV2.Destination.create(stranger.toString()));
    _localRelationshipWriterDAO.addRelationships(strayCar, AspectFoo.class, Collections.singletonList(strayBelongsTo), false);

    // Logical source filter: source foo aspect value == "Car".
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(
        wrapCriterionAsLogicalExpression(EBeanDAOUtils.buildRelationshipFieldCriterion(
            LocalRelationshipValue.create("Car"), Condition.EQUAL,
            new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"))));

    // Logical destination filter: destination foo aspect value == "Owner".
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(
        wrapCriterionAsLogicalExpression(EBeanDAOUtils.buildRelationshipFieldCriterion(
            LocalRelationshipValue.create("Owner"), Condition.EQUAL,
            new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"))));

    // Empty logical OUTGOING relationship filter: matches every belongs-to row, so each row is
    // included or excluded purely by the source and destination entity filters. This keeps the
    // multi-page test focused on the entity filters; relationship-filter behavior is exercised
    // independently by the non-MG destination test.
    LocalRelationshipFilter relationshipFilter = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(new LogicalExpressionLocalRelationshipCriterion())
        .setDirection(RelationshipDirection.OUTGOING);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> drained = new ArrayList<>();
    int pages = 0;
    RelationshipKeysetCursor cursor = null;
    do {
      RelationshipKeysetPage<AssetRelationship> page = _localRelationshipQueryDAO.findRelationshipsV4ByKeyset(
          "foo", srcFilter, "foo", destFilter, BelongsToV2.class, relationshipFilter,
          AssetRelationship.class, wrapOptions, 2, cursor);
      assertEquals(page.getMaxId(), 7L);
      drained.addAll(page.getRelationships());
      cursor = page.getNextCursor();
      pages++;
    } while (cursor != null);

    // 5 matching rows (nonmatching source/destination rows excluded) over pages of size 2
    // => 3 pages, no duplicates, all wrapped. maxId spans the two nonmatching rows too.
    assertEquals(pages, 3);
    assertEquals(drained.size(), 5);
    Set<String> actualSources = new java.util.HashSet<>();
    for (AssetRelationship rel : drained) {
      actualSources.add(rel.getSource());
      assertEquals(rel.getRelatedTo().getBelongsToV2().getDestination().getString(), owner.toString());
    }
    Set<String> expectedSources = cars.stream().map(FooUrn::toString).collect(Collectors.toSet());
    assertEquals(actualSources, expectedSources);
  }

  @Test
  public void testFindRelationshipsV4ByKeysetNonMgDestinationTypeResolvesNoTable() throws URISyntaxException {
    // GQS non-MG destination usage: GQS passes the literal destination entity type "NON_MG_ASSET"
    // with a null destination filter. "NON_MG_ASSET" is not a registered MG entity type, so
    // getMgEntityTableName returns null and no destination entity table is joined. Guards against
    // accidentally requiring a destination entity table for the V4 keyset wrapper.
    FooUrn source = new FooUrn(1);
    _fooUrnEBeanLocalAccess.add(source, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);

    // Non-MG destination: an external dataset urn with no metadata_entity table.
    String nonMgDestination = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/tracking/events,PROD)";
    BelongsToV2 belongsTo = new BelongsToV2();
    belongsTo.setDestination(BelongsToV2.Destination.create(nonMgDestination));
    _localRelationshipWriterDAO.addRelationships(source, AspectFoo.class, Collections.singletonList(belongsTo), false);

    // Source aspect filter still applies; the relationship filter pins the non-MG destination urn.
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(
        wrapCriterionAsLogicalExpression(EBeanDAOUtils.buildRelationshipFieldCriterion(
            LocalRelationshipValue.create("Car"), Condition.EQUAL,
            new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"))));
    LocalRelationshipFilter relationshipFilter = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(wrapCriterionAsLogicalExpression(
            EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(nonMgDestination),
                Condition.EQUAL, new UrnField().setName("destination"))))
        .setDirection(RelationshipDirection.OUTGOING);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    // Non-MG destination type "NON_MG_ASSET" (as GQS passes) with a null destination filter.
    RelationshipKeysetPage<AssetRelationship> page = _localRelationshipQueryDAO.findRelationshipsV4ByKeyset(
        "foo", srcFilter, "NON_MG_ASSET", null, BelongsToV2.class, relationshipFilter,
        AssetRelationship.class, wrapOptions, 10, null);

    assertEquals(page.getMaxId(), 1L);
    assertNull(page.getNextCursor());
    assertEquals(page.getRelationships().size(), 1);
    AssetRelationship wrapped = page.getRelationships().get(0);
    assertEquals(wrapped.getSource(), source.toString());
    assertEquals(wrapped.getRelatedTo().getBelongsToV2().getDestination().getString(), nonMgDestination);
  }

  @Test
  public void testFindRelationshipsV4ByKeysetRejectsInvalidWrapOptions() {
    LocalRelationshipFilter emptyLogical = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(new LogicalExpressionLocalRelationshipCriterion())
        .setDirection(RelationshipDirection.OUTGOING);

    // null wrapOptions.
    expectThrows(IllegalArgumentException.class, () ->
        _localRelationshipQueryDAO.findRelationshipsV4ByKeyset(
            null, null, null, null, ReportsTo.class, emptyLogical, AssetRelationship.class, null, 2, null));

    // wrapOptions missing the AssetRelationship return-type marker.
    Map<String, Object> badWrapOptions = new HashMap<>();
    badWrapOptions.put(RELATIONSHIP_RETURN_TYPE, "SomethingElse");
    expectThrows(IllegalArgumentException.class, () ->
        _localRelationshipQueryDAO.findRelationshipsV4ByKeyset(
            null, null, null, null, ReportsTo.class, emptyLogical, AssetRelationship.class, badWrapOptions, 2, null));
  }

  @Test
  public void testFindRelationshipsV4ByKeysetRejectsCriteriaField() {
    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    // V4 requires logical-expression filters; the legacy criteria field must be rejected.
    LocalRelationshipFilter criteriaRelationshipFilter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray())
        .setDirection(RelationshipDirection.OUTGOING);

    expectThrows(IllegalArgumentException.class, () ->
        _localRelationshipQueryDAO.findRelationshipsV4ByKeyset(
            null, null, null, null, ReportsTo.class, criteriaRelationshipFilter,
            AssetRelationship.class, wrapOptions, 2, null));
  }

  // Keyset (seek) pagination: META-24159 single-destination FORCE INDEX hint.

  // Mirrors the private constant of the same name on EbeanLocalRelationshipQueryDAO. Declared here rather
  // than widening the production constant's visibility, so a change to that constant fails these tests.
  private static final String IDX_DESTINATION_DELETED_TS = "idx_destination_deleted_ts";
  private static final String IDX_SOURCE_DELETED_TS = "idx_source_deleted_ts";
  private static final String TEST_RELATIONSHIP_TABLE = "relationship_table_name";

  // A DAO whose SchemaValidatorUtil is a mock so tests drive indexExists deterministically (the embedded
  // relationship tables carry no idx_destination_deleted_ts). Uses the constructor that wires the validator
  // and the SQL generator from the same instance.
  private EbeanLocalRelationshipQueryDAO daoWithMockedIndex(boolean indexPresent) {
    return daoWithMockedIndexes(indexPresent, false);
  }

  // Same, with the destination and source indexes controlled independently so the precedence and
  // fall-through cases between the two hints can be driven directly.
  private EbeanLocalRelationshipQueryDAO daoWithMockedIndexes(boolean destinationIndexPresent,
      boolean sourceIndexPresent) {
    SchemaValidatorUtil mockValidator = mock(SchemaValidatorUtil.class);
    // Pinned to the exact table and index, so a typo in either fails rather than matching anything.
    when(mockValidator.indexExists(eq(TEST_RELATIONSHIP_TABLE), eq(IDX_DESTINATION_DELETED_TS)))
        .thenReturn(destinationIndexPresent);
    when(mockValidator.indexExists(eq(TEST_RELATIONSHIP_TABLE), eq(IDX_SOURCE_DELETED_TS)))
        .thenReturn(sourceIndexPresent);
    EbeanLocalRelationshipQueryDAO dao =
        new EbeanLocalRelationshipQueryDAO(_server, _eBeanDAOConfig, mockValidator);
    dao.setSchemaConfig(EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY);
    return dao;
  }

  private static LocalRelationshipFilter emptyLogicalRelationshipFilter() {
    return new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(new LogicalExpressionLocalRelationshipCriterion())
        .setDirection(RelationshipDirection.OUTGOING);
  }

  private static UrnField urnField(String name) {
    UrnField field = new UrnField();
    return name == null ? field : field.setName(name);
  }

  private static LocalRelationshipCriterion urnEqual(String urn, String fieldName) {
    return EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(urn), Condition.EQUAL, urnField(fieldName));
  }

  // EQUAL paired with a one-element array instead of a scalar string: a degenerate, hint-ineligible shape.
  private static LocalRelationshipCriterion urnEqualArray(String fieldName, String urn) {
    return EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(new StringArray(urn)), Condition.EQUAL, urnField(fieldName));
  }

  private static LocalRelationshipCriterion urnIn(String fieldName, String... urns) {
    return EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(new StringArray(Arrays.asList(urns))), Condition.IN, urnField(fieldName));
  }

  private static LocalRelationshipFilter leafFilter(LocalRelationshipCriterion criterion) {
    return new LocalRelationshipFilter().setLogicalExpressionCriteria(wrapCriterionAsLogicalExpression(criterion));
  }

  private static LocalRelationshipFilter groupFilter(Operator op, LocalRelationshipCriterion... criteria) {
    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    for (LocalRelationshipCriterion criterion : criteria) {
      array.add(wrapCriterionAsLogicalExpression(criterion));
    }
    return new LocalRelationshipFilter().setLogicalExpressionCriteria(buildLogicalGroup(op, array));
  }

  private static void assertForceIndexHint(String sql, boolean expected) {
    if (expected) {
      assertTrue(sql.contains("FORCE INDEX (" + IDX_DESTINATION_DELETED_TS + ")"), sql);
      assertTrue(sql.indexOf("FROM " + TEST_RELATIONSHIP_TABLE + " rt") < sql.indexOf("FORCE INDEX"), sql);
    } else {
      assertFalse(sql.contains("FORCE INDEX"), sql);
    }
    assertTrue(sql.endsWith("ORDER BY rt.id ASC LIMIT 10"), sql);
  }

  // Structural eligibility matrix on the relationship-filter `destination` path (no destination entity
  // join, so predicates land on rt). The hint fires only for a direct, non-negated, single-value urn
  // leaf named "destination" when the index exists; every other shape emits no hint. NOT/AND/OR collapse
  // to the same ineligible branch, so one grouped case (OR) plus NOT cover the grouped path.
  @DataProvider(name = "relationshipFilterDestinationCases")
  public Object[][] relationshipFilterDestinationCases() {
    return new Object[][]{
        {"destination EQUAL, index present", leafFilter(urnEqual("urn:li:foo:1", "destination")), true, true,
            "rt.destination='urn:li:foo:1'"},
        {"single-value destination IN, index present", leafFilter(urnIn("destination", "urn:li:foo:1")), true, true,
            "rt.destination IN ('urn:li:foo:1')"},
        {"NOT-wrapped destination", groupFilter(Operator.NOT, urnEqual("urn:li:foo:1", "destination")), true, false,
            "(NOT rt.destination='urn:li:foo:1')"},
        {"OR-grouped destinations", groupFilter(Operator.OR, urnEqual("urn:li:foo:1", "destination"),
            urnEqual("urn:li:foo:2", "destination")), true, false,
            "rt.destination='urn:li:foo:1' OR rt.destination='urn:li:foo:2'"},
        {"multi-value destination IN", leafFilter(urnIn("destination", "urn:li:foo:1", "urn:li:foo:2")), true, false,
            "rt.destination IN ('urn:li:foo:1', 'urn:li:foo:2')"},
        {"single-array destination EQUAL", leafFilter(urnEqualArray("destination", "urn:li:foo:1")), true, false,
            "rt.destination='('urn:li:foo:1')'"},
        {"urn field not named destination", leafFilter(urnEqual("urn:li:foo:1", null)), true, false,
            "rt.urn='urn:li:foo:1'"}
    };
  }

  @Test(dataProvider = "relationshipFilterDestinationCases")
  public void testKeysetSqlRelationshipFilterDestinationHint(String desc, LocalRelationshipFilter relationshipFilter,
      boolean indexPresent, boolean expectHint, String expectedPredicate) {
    String sql = daoWithMockedIndex(indexPresent).buildFindRelationshipKeysetCurrentSQL("relationship_table_name",
        relationshipFilter, null, null, null, null, 10, 5, 20);

    assertForceIndexHint(sql, expectHint);
    assertFalse(sql.contains(" dt "), sql);
    assertTrue(sql.contains(expectedPredicate), sql);
  }

  /**
   * Reverse-lineage reads pass no destination entity class and an empty destination entity filter, pinning the
   * destination through the relationship filter instead. That is the shape this hint exists for, so it must be
   * hinted even though a non-null destination entity filter is present.
   */
  @Test
  public void testKeysetSqlHintsWhenDestinationEntityFilterIsEmptyAndRelationshipFilterPins() {
    final LocalRelationshipFilter emptyDestinationEntityFilter =
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray());

    String sql = daoWithMockedIndex(true).buildFindRelationshipKeysetCurrentSQL("relationship_table_name",
        leafFilter(urnEqual("urn:li:foo:1", "destination")), null, null, null, emptyDestinationEntityFilter, 10, 5, 20);

    assertForceIndexHint(sql, true);
    assertFalse(sql.contains(" dt "), sql);
    assertTrue(sql.contains("rt.destination='urn:li:foo:1'"), sql);
  }

  // Destination entity-table path: the INNER JOIN on dt is always retained, and the guard here requires
  // the entity urn leaf to be named "urn" (the relationship path requires "destination"). These cases
  // prove join retention, the indexExists guard, and the "urn" name requirement.
  @DataProvider(name = "destinationEntityFilterCases")
  public Object[][] destinationEntityFilterCases() {
    return new Object[][]{
        {"urn EQUAL, index present", leafFilter(urnEqual("urn:li:foo:1", null)), true, null, true,
            "dt.urn='urn:li:foo:1'"},
        {"single-value urn IN with source join, index present", leafFilter(urnIn(null, "urn:li:foo:1")), true,
            "source_table_name", true, "dt.urn IN ('urn:li:foo:1')"},
        {"eligible shape but index missing", leafFilter(urnEqual("urn:li:foo:1", null)), false, null, false,
            "dt.urn='urn:li:foo:1'"},
        {"urn field named destination", leafFilter(urnEqual("urn:li:foo:1", "destination")), true, null, false,
            "dt.destination='urn:li:foo:1'"}
    };
  }

  @Test(dataProvider = "destinationEntityFilterCases")
  public void testKeysetSqlDestinationEntityFilterJoinAndHint(String desc, LocalRelationshipFilter destFilter,
      boolean indexPresent, String sourceTable, boolean expectHint, String expectedPredicate) {
    String sql = daoWithMockedIndex(indexPresent).buildFindRelationshipKeysetCurrentSQL("relationship_table_name",
        emptyLogicalRelationshipFilter(), sourceTable, null, "metadata_entity_bar", destFilter, 10, 5, 20);

    String join = "INNER JOIN metadata_entity_bar dt ON dt.urn=rt.destination";
    assertTrue(sql.contains(join), sql);
    assertForceIndexHint(sql, expectHint);
    if (expectHint) {
      assertTrue(sql.indexOf("FORCE INDEX") < sql.indexOf(join), sql);
    }
    assertTrue(sql.contains(expectedPredicate), sql);
    if (sourceTable != null) {
      assertTrue(sql.contains("INNER JOIN " + sourceTable + " st ON st.urn=rt.source"), sql);
    }
  }

  @Test
  public void testKeysetSqlNonUrnDestinationRetainsJoinNoHint() {
    // A non-urn (aspect) destination filter is ineligible: keep the join, add no hint. Real validator
    // so the aspect virtual column resolves.
    LocalRelationshipCriterion aspectCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("Alice"), Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipKeysetCurrentSQL("relationship_table_name",
        emptyLogicalRelationshipFilter(), null, null, "metadata_entity_bar", leafFilter(aspectCriterion), 10, 5, 20);

    assertTrue(sql.contains("INNER JOIN metadata_entity_bar dt ON dt.urn=rt.destination"), sql);
    assertFalse(sql.contains("FORCE INDEX"), sql);
    assertTrue(sql.contains("dt.i_aspectfoo"
        + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "value='Alice'"), sql);
  }

  @Test
  public void testFindRelationshipsV4ByKeysetSingleUrnDestinationRetainsJoinExcludesOrphans()
      throws URISyntaxException {
    // Owner entity plus five cars belonging to it; every destination entity exists.
    FooUrn owner = new FooUrn(1000);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    Set<String> expectedSources = new java.util.HashSet<>();
    for (int i = 1; i <= 5; i++) {
      FooUrn car = new FooUrn(i);
      _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
      BelongsToV2 belongsTo = new BelongsToV2();
      belongsTo.setDestination(BelongsToV2.Destination.create(owner.toString()));
      _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(belongsTo), false);
      expectedSources.add(car.toString());
    }

    // Orphan: source exists but its destination foo entity (foo:9999) was never created, so the
    // destination INNER JOIN must exclude it.
    FooUrn orphanSource = new FooUrn(4242);
    _fooUrnEBeanLocalAccess.add(orphanSource, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    BelongsToV2 orphan = new BelongsToV2();
    orphan.setDestination(BelongsToV2.Destination.create(new FooUrn(9999).toString()));
    _localRelationshipWriterDAO.addRelationships(orphanSource, AspectFoo.class, Collections.singletonList(orphan), false);

    // No filter: the destination INNER JOIN alone drops the orphan and returns the five cars.
    assertEquals(drainV4Sources(null, owner), expectedSources);

    // Single-urn destination entity filter (the hint-eligible shape) returns the same five, orphan excluded.
    assertEquals(drainV4Sources(leafFilter(urnEqual(owner.toString(), null)), owner), expectedSources);
  }

  // Drains every AssetRelationship page for a BelongsToV2 keyset scan over destination entity "foo",
  // asserts each row's destination equals expectedDestination, returns the source urns.
  private Set<String> drainV4Sources(LocalRelationshipFilter destFilter, FooUrn expectedDestination) {
    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);
    Set<String> sources = new java.util.HashSet<>();
    RelationshipKeysetCursor cursor = null;
    do {
      RelationshipKeysetPage<AssetRelationship> page = _localRelationshipQueryDAO.findRelationshipsV4ByKeyset(
          "foo", null, "foo", destFilter, BelongsToV2.class, emptyLogicalRelationshipFilter(),
          AssetRelationship.class, wrapOptions, 2, cursor);
      for (AssetRelationship rel : page.getRelationships()) {
        sources.add(rel.getSource());
        assertEquals(rel.getRelatedTo().getBelongsToV2().getDestination().getString(), expectedDestination.toString());
      }
      cursor = page.getNextCursor();
    } while (cursor != null);
    return sources;
  }

  // --------------------------------------------------------------------------------------------
  // Keyset (seek) pagination: META-24386 single-source FORCE INDEX hint.
  // Mirrors the destination cases above; only the pinned side and the expected index differ.
  // --------------------------------------------------------------------------------------------

  private static void assertHintIndex(String sql, String expectedIndex) {
    if (expectedIndex == null) {
      assertFalse(sql.contains("FORCE INDEX"), sql);
    } else {
      assertTrue(sql.contains("FORCE INDEX (" + expectedIndex + ")"), sql);
      assertTrue(sql.indexOf("FROM " + TEST_RELATIONSHIP_TABLE + " rt") < sql.indexOf("FORCE INDEX"), sql);
    }
    assertTrue(sql.endsWith("ORDER BY rt.id ASC LIMIT 10"), sql);
  }

  // Structural eligibility matrix on the relationship-filter `source` path, matching the destination
  // matrix case for case so the two sides cannot drift apart.
  @DataProvider(name = "relationshipFilterSourceCases")
  public Object[][] relationshipFilterSourceCases() {
    return new Object[][]{
        {"source EQUAL, index present", leafFilter(urnEqual("urn:li:foo:1", "source")), true, true,
            "rt.source='urn:li:foo:1'"},
        {"single-value source IN, index present", leafFilter(urnIn("source", "urn:li:foo:1")), true, true,
            "rt.source IN ('urn:li:foo:1')"},
        {"eligible shape but index missing", leafFilter(urnEqual("urn:li:foo:1", "source")), false, false,
            "rt.source='urn:li:foo:1'"},
        {"NOT-wrapped source", groupFilter(Operator.NOT, urnEqual("urn:li:foo:1", "source")), true, false,
            "(NOT rt.source='urn:li:foo:1')"},
        {"OR-grouped sources", groupFilter(Operator.OR, urnEqual("urn:li:foo:1", "source"),
            urnEqual("urn:li:foo:2", "source")), true, false,
            "rt.source='urn:li:foo:1' OR rt.source='urn:li:foo:2'"},
        {"multi-value source IN", leafFilter(urnIn("source", "urn:li:foo:1", "urn:li:foo:2")), true, false,
            "rt.source IN ('urn:li:foo:1', 'urn:li:foo:2')"},
        {"single-array source EQUAL", leafFilter(urnEqualArray("source", "urn:li:foo:1")), true, false,
            "rt.source='('urn:li:foo:1')'"},
        {"urn field not named source", leafFilter(urnEqual("urn:li:foo:1", null)), true, false,
            "rt.urn='urn:li:foo:1'"}
    };
  }

  @Test(dataProvider = "relationshipFilterSourceCases")
  public void testKeysetSqlRelationshipFilterSourceHint(String desc, LocalRelationshipFilter relationshipFilter,
      boolean indexPresent, boolean expectHint, String expectedPredicate) {
    String sql = daoWithMockedIndexes(false, indexPresent).buildFindRelationshipKeysetCurrentSQL(
        TEST_RELATIONSHIP_TABLE, relationshipFilter, null, null, null, null, 10, 5, 20);

    assertHintIndex(sql, expectHint ? IDX_SOURCE_DELETED_TS : null);
    assertFalse(sql.contains(" st "), sql);
    assertTrue(sql.contains(expectedPredicate), sql);
  }

  /**
   * Source entity-table path: the INNER JOIN on st is always retained, and the entity urn leaf must be
   * named "urn" because it is rendered against st, not rt.
   */
  @Test
  public void testKeysetSqlSourceEntityFilterJoinAndHint() {
    String sql = daoWithMockedIndexes(false, true).buildFindRelationshipKeysetCurrentSQL(
        TEST_RELATIONSHIP_TABLE, emptyLogicalRelationshipFilter(), "metadata_entity_foo",
        leafFilter(urnEqual("urn:li:foo:1", null)), null, null, 10, 5, 20);

    String join = "INNER JOIN metadata_entity_foo st ON st.urn=rt.source";
    assertTrue(sql.contains(join), sql);
    assertHintIndex(sql, IDX_SOURCE_DELETED_TS);
    assertTrue(sql.indexOf("FORCE INDEX") < sql.indexOf(join), sql);
    assertTrue(sql.contains("st.urn='urn:li:foo:1'"), sql);
  }

  /**
   * The source side has only two pinning shapes where the destination has three. With no source entity
   * class the builder never renders sourceEntityFilter at all (the destination has an `else if` that moves
   * its filter onto rt; the source has no such branch), so an otherwise hint-eligible source entity filter
   * must not produce a hint here. Hinting would drive the plan off a predicate absent from the WHERE clause.
   */
  @Test
  public void testKeysetSqlNoSourceHintWhenSourceEntityFilterIsNotRendered() {
    String sql = daoWithMockedIndexes(false, true).buildFindRelationshipKeysetCurrentSQL(
        TEST_RELATIONSHIP_TABLE, emptyLogicalRelationshipFilter(), null,
        leafFilter(urnEqual("urn:li:foo:1", "source")), null, null, 10, 5, 20);

    assertHintIndex(sql, null);
    assertFalse(sql.contains(" st "), sql);
    // The filter really is absent from the query, which is what makes the hint unsound.
    assertFalse(sql.contains("urn:li:foo:1"), sql);
  }

  /**
   * Only one FORCE INDEX can be emitted. Destination wins so every query hinted before META-24386 keeps
   * exactly the plan it had.
   */
  @Test
  public void testKeysetSqlDestinationWinsWhenBothSidesPinned() {
    String sql = daoWithMockedIndexes(true, true).buildFindRelationshipKeysetCurrentSQL(
        TEST_RELATIONSHIP_TABLE,
        groupFilter(Operator.AND, urnEqual("urn:li:foo:1", "destination"), urnEqual("urn:li:foo:2", "source")),
        null, null, null, null, 10, 5, 20);

    // An AND group is not a direct single leaf, so neither side is eligible through the relationship filter.
    assertHintIndex(sql, null);

    // Pin the destination through the relationship filter and the source through the entity join: both are
    // eligible, and the destination hint is the one emitted.
    String bothEligible = daoWithMockedIndexes(true, true).buildFindRelationshipKeysetCurrentSQL(
        TEST_RELATIONSHIP_TABLE, leafFilter(urnEqual("urn:li:foo:1", "destination")),
        "metadata_entity_foo", leafFilter(urnEqual("urn:li:foo:2", null)), null, null, 10, 5, 20);

    assertHintIndex(bothEligible, IDX_DESTINATION_DELETED_TS);
    assertFalse(bothEligible.contains(IDX_SOURCE_DELETED_TS), bothEligible);
    assertTrue(bothEligible.contains("INNER JOIN metadata_entity_foo st ON st.urn=rt.source"), bothEligible);
  }

  /**
   * When the destination is pinned but its index is absent, an eligible source still gets its hint rather
   * than the query falling back to a full primary-key scan.
   */
  @Test
  public void testKeysetSqlFallsThroughToSourceWhenDestinationIndexMissing() {
    String sql = daoWithMockedIndexes(false, true).buildFindRelationshipKeysetCurrentSQL(
        TEST_RELATIONSHIP_TABLE, leafFilter(urnEqual("urn:li:foo:1", "destination")),
        "metadata_entity_foo", leafFilter(urnEqual("urn:li:foo:2", null)), null, null, 10, 5, 20);

    assertHintIndex(sql, IDX_SOURCE_DELETED_TS);
  }
}
