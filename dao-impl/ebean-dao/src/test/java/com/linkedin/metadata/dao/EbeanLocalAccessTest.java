package com.linkedin.metadata.dao;

import com.google.common.io.Resources;
import com.linkedin.common.AuditStamp;
import com.linkedin.metadata.dao.localrelationship.SampleLocalRelationshipRegistryImpl;
import com.linkedin.metadata.dao.scsi.EmptyPathExtractor;
import com.linkedin.metadata.dao.utils.BarUrnPathExtractor;
import com.linkedin.metadata.dao.utils.FooUrnPathExtractor;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dao.utils.SQLIndexFilterUtils;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.LocalRelationshipCriterionArray;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.BarSnapshot;
import com.linkedin.testing.BarUrnArray;
import com.linkedin.testing.FooSnapshot;
import com.linkedin.testing.localrelationship.AspectFooBar;
import com.linkedin.testing.localrelationship.BelongsTo;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.BurgerUrn;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.common.AuditStamps.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.AssertJUnit.*;

public class EbeanLocalAccessTest {
  private static EbeanServer _server;
  private static IEbeanLocalAccess<FooUrn> _ebeanLocalAccessFoo;
  private static IEbeanLocalAccess<BarUrn> _ebeanLocalAccessBar;
  private static IEbeanLocalAccess<BurgerUrn> _ebeanLocalAccessBurger;
  private static long _now;
  private static final LocalRelationshipFilter EMPTY_FILTER = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray());

  @BeforeClass
  public void init() {
    _server = EmbeddedMariaInstance.getServer();
    _ebeanLocalAccessFoo = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG, FooUrn.class, new FooUrnPathExtractor());
    _ebeanLocalAccessBar = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG, BarUrn.class, new BarUrnPathExtractor());
    _ebeanLocalAccessBurger = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG, BurgerUrn.class, new EmptyPathExtractor<>());
    _ebeanLocalAccessFoo.setLocalRelationshipBuilderRegistry(new SampleLocalRelationshipRegistryImpl());
    _now = System.currentTimeMillis();
  }

  @BeforeMethod
  public void setupTest() throws IOException {
    _server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource("ebean-local-access-create-all.sql"), StandardCharsets.UTF_8)));

    // initialize data with metadata_entity_foo table with fooUrns from 0 ~ 99
    int numOfRecords = 100;
    for (int i = 0; i < numOfRecords; i++) {
      FooUrn fooUrn = makeFooUrn(i);
      AspectFoo aspectFoo = new AspectFoo();
      aspectFoo.setValue(String.valueOf(i));
      AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());
      _ebeanLocalAccessFoo.add(fooUrn, aspectFoo, AspectFoo.class, auditStamp);
    }
  }

  @Test
  public void testGetAspect() {

    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99

    FooUrn fooUrn = makeFooUrn(0);
    AspectKey<FooUrn, AspectFoo> aspectKey = new AspectKey(AspectFoo.class, fooUrn, 0L);

    // When get AspectFoo from urn:li:foo:0
    List<EbeanMetadataAspect> ebeanMetadataAspectList =
        _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(aspectKey), 1000, 0);
    assertEquals(1, ebeanMetadataAspectList.size());

    EbeanMetadataAspect ebeanMetadataAspect = ebeanMetadataAspectList.get(0);

    // Expect: the content of aspect foo is returned
    assertEquals(AspectFoo.class.getCanonicalName(), ebeanMetadataAspect.getKey().getAspect());
    assertEquals(fooUrn.toString(), ebeanMetadataAspect.getKey().getUrn());
    assertEquals("{\"value\":\"0\"}", ebeanMetadataAspect.getMetadata());
    assertEquals("urn:li:testActor:foo", ebeanMetadataAspect.getCreatedBy());

    // Make sure json can be deserialized to Aspect.
    assertNotNull(RecordUtils.toRecordTemplate(AspectFoo.class, ebeanMetadataAspect.getMetadata()));

    // When get AspectFoo from urn:li:foo:9999 (does not exist)
    FooUrn nonExistFooUrn = makeFooUrn(9999);
    AspectKey<FooUrn, AspectFoo> nonExistKey = new AspectKey(AspectFoo.class, nonExistFooUrn, 0L);
    ebeanMetadataAspectList = _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(nonExistKey), 1000, 0);

    // Expect: get AspectFoo from urn:li:foo:9999 returns empty result
    assertTrue(ebeanMetadataAspectList.isEmpty());
  }

  @Test
  public void testListUrnsWithOffset() {

    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99
    // When: finding urns where ids >= 25 and id < 50 sorting by ASC

    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.GREATER_THAN_OR_EQUAL_TO,
            IndexValue.create(25));
    IndexCriterion indexCriterion2 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.LESS_THAN, IndexValue.create(50));

    indexCriterionArray.add(indexCriterion1);
    indexCriterionArray.add(indexCriterion2);
    indexFilter.setCriteria(indexCriterionArray);

    IndexSortCriterion indexSortCriterion =
        SQLIndexFilterUtils.createIndexSortCriterion(AspectFoo.class, "value", SortOrder.ASCENDING);

    // When: list out results with start = 5 and pageSize = 5

    ListResult<FooUrn> listUrns = _ebeanLocalAccessFoo.listUrns(indexFilter, indexSortCriterion, 5, 5);

    assertEquals(5, listUrns.getValues().size());
    assertEquals(5, listUrns.getPageSize());
    assertEquals(10, listUrns.getNextStart());
    assertEquals(25, listUrns.getTotalCount());
    assertEquals(5, listUrns.getTotalPageCount());
  }

  @Test
  public void testListUrnsWithLastUrn() throws URISyntaxException {

    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99
    // When: finding urns where ids >= 25 and id < 50 sorting by ASC

    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.GREATER_THAN_OR_EQUAL_TO,
            IndexValue.create(25));
    IndexCriterion indexCriterion2 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.LESS_THAN, IndexValue.create(50));

    indexCriterionArray.add(indexCriterion1);
    indexCriterionArray.add(indexCriterion2);
    indexFilter.setCriteria(indexCriterionArray);

    IndexSortCriterion indexSortCriterion =
        SQLIndexFilterUtils.createIndexSortCriterion(AspectFoo.class, "value", SortOrder.ASCENDING);

    FooUrn lastUrn = new FooUrn(29);

    // When: list out results with lastUrn = 'urn:li:foo:29' and pageSize = 5
    List<FooUrn> result1 = _ebeanLocalAccessFoo.listUrns(indexFilter, indexSortCriterion, lastUrn, 5);

    // Expect: 5 rows are returns (30~34) and the first element is 'urn:li:foo:30'
    assertEquals(5, result1.size());
    assertEquals("30", result1.get(0).getId());

    lastUrn = result1.get(result1.size() - 1);

    // When: list out results with lastUrn = 'urn:li:foo:34' and pageSize = 5, but with only a filter on the aspect
    IndexCriterion indexCriterion3 = new IndexCriterion().setAspect(FooUrn.class.getCanonicalName());
    indexCriterionArray = new IndexCriterionArray(Collections.singleton(indexCriterion3));
    IndexFilter filter = new IndexFilter().setCriteria(indexCriterionArray);
    List<FooUrn> result2 = _ebeanLocalAccessFoo.listUrns(filter, indexSortCriterion, lastUrn, 5);

    // Expect: 5 rows are returns (35~39) and the first element is 'urn:li:foo:35'
    assertEquals(5, result2.size());
    assertEquals("35", result2.get(0).getId());
  }

  @Test
  public void testExists() throws URISyntaxException {
    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99

    // When: check whether urn:li:foo:0 exist
    FooUrn foo0 = new FooUrn(0);

    // Expect: urn:li:foo:0 exists
    assertTrue(_ebeanLocalAccessFoo.exists(foo0));

    // When: check whether urn:li:foo:9999 exist
    FooUrn foo9999 = new FooUrn(9999);

    // Expect: urn:li:foo:9999 does not exists
    assertFalse(_ebeanLocalAccessFoo.exists(foo9999));
  }

  @Test
  public void testListUrns() throws URISyntaxException {
    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99

    // When: list urns from the 1st record, with 50 page size
    ListResult<FooUrn> fooUrnListResult = _ebeanLocalAccessFoo.listUrns(AspectFoo.class, 0, 50);

    // Expect: 50 results is returned and 100 total records
    assertEquals(50, fooUrnListResult.getValues().size());
    assertEquals(100, fooUrnListResult.getTotalCount());

    // When: list urns from the 55th record, with 50 page size
    fooUrnListResult = _ebeanLocalAccessFoo.listUrns(AspectFoo.class, 55, 50);

    // Expect: 45 results is returned and 100 total records
    assertEquals(45, fooUrnListResult.getValues().size());
    assertEquals(100, fooUrnListResult.getTotalCount());

    // When: list urns from the 101th record, with 50 page size
    fooUrnListResult = _ebeanLocalAccessFoo.listUrns(AspectFoo.class, 101, 50);

    // Expect: 0 results is returned and 100 total records
    assertEquals(0, fooUrnListResult.getValues().size());
    assertEquals(100, fooUrnListResult.getTotalCount());
  }

  @Test
  public void testCountAggregate() {
    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99

    // When: count aggregate with filter value = 25
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.EQUAL, IndexValue.create(25));

    indexCriterionArray.add(indexCriterion1);
    indexFilter.setCriteria(indexCriterionArray);

    IndexGroupByCriterion indexGroupByCriterion = new IndexGroupByCriterion();
    indexGroupByCriterion.setPath("/value");
    indexGroupByCriterion.setAspect(AspectFoo.class.getCanonicalName());
    Map<String, Long> countMap = _ebeanLocalAccessFoo.countAggregate(indexFilter, indexGroupByCriterion);

    // Expect: there is 1 count for value 25
    assertEquals(countMap.get("25"), Long.valueOf(1));

    // When: change foo:26's value to be 25

    FooUrn fooUrn = makeFooUrn(26);
    AspectFoo aspectFoo = new AspectFoo();
    aspectFoo.setValue(String.valueOf(25));
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());
    _ebeanLocalAccessFoo.add(fooUrn, aspectFoo, AspectFoo.class, auditStamp);
    countMap = _ebeanLocalAccessFoo.countAggregate(indexFilter, indexGroupByCriterion);

    // Expect: there are 2 counts for value 25
    assertEquals(countMap.get("25"), Long.valueOf(2));
  }

  @Test
  public void testEscapeSpecialCharInUrn() {
    AspectFoo aspectFoo = new AspectFoo().setValue("test");
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    // Single quote is a special char in SQL.
    BurgerUrn johnsBurgerUrn1 = makeBurgerUrn("urn:li:burger:John's burger");
    _ebeanLocalAccessBurger.add(johnsBurgerUrn1, aspectFoo, AspectFoo.class, auditStamp);

    AspectKey aspectKey1 = new AspectKey(AspectFoo.class, johnsBurgerUrn1, 0L);
    List<EbeanMetadataAspect> ebeanMetadataAspectList = _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(aspectKey1), 1, 0);
    assertEquals(1, ebeanMetadataAspectList.size());
    assertEquals(ebeanMetadataAspectList.get(0).getKey().getUrn(), johnsBurgerUrn1.toString());

    // Double quote is a special char in SQL.
    BurgerUrn johnsBurgerUrn2 = makeBurgerUrn("urn:li:burger:John\"s burger");
    _ebeanLocalAccessBurger.add(johnsBurgerUrn2, aspectFoo, AspectFoo.class, auditStamp);

    AspectKey aspectKey2 = new AspectKey(AspectFoo.class, johnsBurgerUrn2, 0L);
    ebeanMetadataAspectList = _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(aspectKey2), 1, 0);
    assertEquals(1, ebeanMetadataAspectList.size());
    assertEquals(ebeanMetadataAspectList.get(0).getKey().getUrn(), johnsBurgerUrn2.toString());

    // Backslash is a special char in SQL.
    BurgerUrn johnsBurgerUrn3 = makeBurgerUrn("urn:li:burger:John\\s burger");
    _ebeanLocalAccessBurger.add(johnsBurgerUrn3, aspectFoo, AspectFoo.class, auditStamp);

    AspectKey aspectKey3 = new AspectKey(AspectFoo.class, johnsBurgerUrn3, 0L);
    ebeanMetadataAspectList = _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(aspectKey3), 1, 0);
    assertEquals(1, ebeanMetadataAspectList.size());
    assertEquals(ebeanMetadataAspectList.get(0).getKey().getUrn(), johnsBurgerUrn3.toString());
  }

  @Test
  public void testAddWithLocalRelationshipBuilder() throws URISyntaxException {
    FooUrn fooUrn = makeFooUrn(1);
    BarUrn barUrn1 = BarUrn.createFromString("urn:li:bar:1");
    BarUrn barUrn2 = BarUrn.createFromString("urn:li:bar:2");
    BarUrn barUrn3 = BarUrn.createFromString("urn:li:bar:3");
    AspectFooBar aspectFooBar = new AspectFooBar().setBars(new BarUrnArray(barUrn1, barUrn2, barUrn3));
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    _ebeanLocalAccessFoo.add(fooUrn, aspectFooBar, AspectFooBar.class, auditStamp);
    _ebeanLocalAccessBar.add(barUrn1, new AspectFoo().setValue("1"), AspectFoo.class, auditStamp);
    _ebeanLocalAccessBar.add(barUrn2, new AspectFoo().setValue("2"), AspectFoo.class, auditStamp);
    _ebeanLocalAccessBar.add(barUrn3, new AspectFoo().setValue("3"), AspectFoo.class, auditStamp);

    // Verify local relationships and entity are added.
    EbeanLocalRelationshipQueryDAO ebeanLocalRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    List<BelongsTo> relationships = ebeanLocalRelationshipQueryDAO.findRelationships(
        BarSnapshot.class, EMPTY_FILTER, FooSnapshot.class, EMPTY_FILTER, BelongsTo.class, EMPTY_FILTER, 0, 10);

    AspectKey<FooUrn, AspectFooBar> key = new AspectKey<>(AspectFooBar.class, fooUrn, 0L);
    List<EbeanMetadataAspect> aspects = _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(key), 10, 0);

    assertEquals(3, relationships.size());
    assertEquals(1, aspects.size());
  }

  @Test
  public void testAtomicityWithLocalRelationshipBuilder() throws URISyntaxException {
    // Drop the entity table should fail add operation.
    _server.createSqlUpdate("DROP TABLE metadata_entity_foo").execute();

    AspectFooBar aspectFooBar = new AspectFooBar().setBars(new BarUrnArray(
        BarUrn.createFromString("urn:li:bar:123"),
        BarUrn.createFromString("urn:li:bar:456"),
        BarUrn.createFromString("urn:li:bar:789")));

    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    try {
      _ebeanLocalAccessFoo.add(makeFooUrn(1), aspectFooBar, AspectFooBar.class, auditStamp);
    } catch (Exception exception) {
      // Verify no relationship is added.
      List<SqlRow> relationships = _server.createSqlQuery("SELECT * FROM metadata_relationship_belongsto").findList();
      assertEquals(0, relationships.size());
    }
  }

  @Test
  public void testUrnExtraction() {
    FooUrn urn1 = makeFooUrn(1);
    AspectFoo foo1 = new AspectFoo().setValue("foo");
    _ebeanLocalAccessFoo.add(urn1, foo1, AspectFoo.class, makeAuditStamp("actor", _now));

    // get content of virtual column
    List<SqlRow> results = _server.createSqlQuery("SELECT i_urn$fooId as id FROM metadata_entity_foo").findList();
    assertEquals(100, results.size());

    // ensure content is as expected
    SqlRow firstResult = results.get(0);
    assertEquals("0", firstResult.getString("id"));
  }
}