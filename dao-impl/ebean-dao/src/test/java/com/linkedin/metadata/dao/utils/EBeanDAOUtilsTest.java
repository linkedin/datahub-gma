package com.linkedin.metadata.dao.utils;

import com.google.common.io.Resources;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.AuditedAspect;
import com.linkedin.metadata.dao.EbeanLocalAccess;
import com.linkedin.metadata.dao.EbeanMetadataAspect;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.query.AspectField;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.query.LocalRelationshipCriterion;
import com.linkedin.metadata.query.LocalRelationshipValue;
import com.linkedin.metadata.query.RelationshipField;
import com.linkedin.metadata.query.UrnField;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.urn.BurgerUrn;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;


public class EBeanDAOUtilsTest {


  @Nonnull
  private String readSQLfromFile(@Nonnull String resourcePath) {
    try {
      return Resources.toString(Resources.getResource(resourcePath), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testCeilDiv() {
    assertEquals(EBeanDAOUtils.ceilDiv(1, 1), 1);
    assertEquals(EBeanDAOUtils.ceilDiv(2, 1), 2);
    assertEquals(EBeanDAOUtils.ceilDiv(3, 1), 3);
    assertEquals(EBeanDAOUtils.ceilDiv(3, 2), 2);
    assertEquals(EBeanDAOUtils.ceilDiv(1, 2), 1);
    assertEquals(EBeanDAOUtils.ceilDiv(-3, 2), -1);
    assertEquals(EBeanDAOUtils.ceilDiv(-3, -2), 2);
  }

  @Test
  public void testGetEntityType() {
    assertEquals(ModelUtils.getEntityTypeFromUrnClass(FooUrn.class), "foo");
  }

  @Test
  public void testGetUrn() throws URISyntaxException {
    assertEquals(EBeanDAOUtils.getUrn("urn:li:foo:123", FooUrn.class), new FooUrn(123));
  }

  @Test
  public void testCompareResultsListEbeanMetadataAspectSingleton() {
    // test equality between two instances of EbeanMetadataAspect
    EbeanMetadataAspect ema1 = new EbeanMetadataAspect();
    ema1.setKey(new EbeanMetadataAspect.PrimaryKey("urn1", "aspect1", 0L));
    ema1.setMetadata("{\"metadata\": \"value1\"}");
    ema1.setCreatedBy("tester");
    ema1.setCreatedFor("tester");
    ema1.setCreatedOn(new Timestamp(1234567890L));

    EbeanMetadataAspect ema2 = new EbeanMetadataAspect();
    ema2.setKey(new EbeanMetadataAspect.PrimaryKey("urn1", "aspect1", 0L));
    ema2.setMetadata("{\"metadata\": \"value1\"}");
    ema2.setCreatedBy("tester");
    ema2.setCreatedFor("tester");
    ema2.setCreatedOn(new Timestamp(1234567890L));

    assertTrue(EBeanDAOUtils.compareResults(Collections.singletonList(ema1), Collections.singletonList(ema2), "testMethod"));

    // different urn in key
    EbeanMetadataAspect ema3 = new EbeanMetadataAspect();
    ema3.setKey(new EbeanMetadataAspect.PrimaryKey("urn2", "aspect1", 0L));
    ema3.setMetadata("{\"metadata\": \"value1\"}");
    ema3.setCreatedBy("tester");
    ema3.setCreatedFor("tester");
    ema3.setCreatedOn(new Timestamp(1234567890L));

    assertFalse(EBeanDAOUtils.compareResults(Collections.singletonList(ema1), Collections.singletonList(ema3), "testMethod"));

    // different metadata
    EbeanMetadataAspect ema4 = new EbeanMetadataAspect();
    ema4.setKey(new EbeanMetadataAspect.PrimaryKey("urn1", "aspect1", 0L));
    ema4.setMetadata("{\"metadata\": \"value2\"}");
    ema4.setCreatedBy("tester");
    ema4.setCreatedFor("tester");
    ema4.setCreatedOn(new Timestamp(1234567890L));

    // TODO (@jphui) META-18962 De-deduplicity investigation
    // This test case is only used for new schema migration dual-mode validation
    // assertFalse(EBeanDAOUtils.compareResults(Collections.singletonList(ema1), Collections.singletonList(ema4), "testMethod"));

    // same metadata, different order
    EbeanMetadataAspect ema4Different = new EbeanMetadataAspect();
    ema4Different.setKey(new EbeanMetadataAspect.PrimaryKey("urn1", "aspect1", 0L));
    ema4Different.setMetadata("{\"field1\": \"value1\", \"field2\": \"value2\"}");
    ema4Different.setCreatedBy("tester");
    ema4Different.setCreatedFor("tester");
    ema4Different.setCreatedOn(new Timestamp(1234567890L));

    ema4.setMetadata("{\"field2\": \"value2\", \"field1\": \"value1\"}");
    assertTrue(EBeanDAOUtils.compareResults(Collections.singletonList(ema4), Collections.singletonList(ema4Different), "testMethod"));

    // different createdon, but within 1s of each other
    EbeanMetadataAspect ema5 = new EbeanMetadataAspect();
    ema5.setKey(new EbeanMetadataAspect.PrimaryKey("urn1", "aspect1", 0L));
    ema5.setMetadata("{\"metadata\": \"value1\"}");
    ema5.setCreatedBy("tester");
    ema5.setCreatedFor("tester");
    ema5.setCreatedOn(new Timestamp(1234567000L)); // 890 ms different than Timestamp(1234567890L)

    assertTrue(EBeanDAOUtils.compareResults(Collections.singletonList(ema1), Collections.singletonList(ema5), "testMethod"));

    // TODO (@jphui) META-18962 De-deduplicity investigation
    // This test case is only used for new schema migration dual-mode validation
    // different createdon
    ema5.setCreatedOn(new Timestamp(1234560000L)); // 7890 ms different than Timestamp(1234567890L)
    // assertFalse(EBeanDAOUtils.compareResults(Collections.singletonList(ema1), Collections.singletonList(ema5), "testMethod"));

    // TODO (@jphui) META-18962 De-deduplicity investigation
    // This test case is only used for new schema migration dual-mode validation
    // createdFor is nullable, set one EbeanMetadataAspect's createdFor field to null
    ema1.setCreatedFor(null);
    // assertFalse(EBeanDAOUtils.compareResults(Collections.singletonList(ema1), Collections.singletonList(ema2), "testMethod"));

    // both createdFor fields being null should return true
    ema2.setCreatedFor(null);
    assertTrue(EBeanDAOUtils.compareResults(Collections.singletonList(ema1), Collections.singletonList(ema2), "testMethod"));

    // null
    assertFalse(EBeanDAOUtils.compareResults(Collections.singletonList(ema1), Collections.singletonList(null), "testMethod"));
  }

  @Test
  public void testCompareResultsListEbeanMetadataAspectMultiple() {
    // test equality where lists contain the same elements but in different order
    EbeanMetadataAspect ema1 = new EbeanMetadataAspect();
    ema1.setKey(new EbeanMetadataAspect.PrimaryKey("urn1", "aspect1", 0L));
    ema1.setMetadata("{\"metadata\": \"value1\"}");
    ema1.setCreatedBy("tester");
    ema1.setCreatedFor("tester");
    ema1.setCreatedOn(new Timestamp(1234567890L));

    EbeanMetadataAspect ema2 = new EbeanMetadataAspect();
    ema2.setKey(new EbeanMetadataAspect.PrimaryKey("urn2", "aspect2", 0L));
    ema2.setMetadata("{\"metadata\": \"value2\"}");
    ema2.setCreatedBy("tester");
    ema2.setCreatedFor("tester");
    ema2.setCreatedOn(new Timestamp(1234567890L));

    List<EbeanMetadataAspect> list1 = new ArrayList<>();
    list1.add(ema1);
    list1.add(ema2);

    EbeanMetadataAspect ema1Copy = new EbeanMetadataAspect();
    ema1Copy.setKey(new EbeanMetadataAspect.PrimaryKey("urn1", "aspect1", 0L));
    ema1Copy.setMetadata("{\"metadata\": \"value1\"}");
    ema1Copy.setCreatedBy("tester");
    ema1Copy.setCreatedFor("tester");
    ema1Copy.setCreatedOn(new Timestamp(1234567890L));

    EbeanMetadataAspect ema2Copy = new EbeanMetadataAspect();
    ema2Copy.setKey(new EbeanMetadataAspect.PrimaryKey("urn2", "aspect2", 0L));
    ema2Copy.setMetadata("{\"metadata\": \"value2\"}");
    ema2Copy.setCreatedBy("tester");
    ema2Copy.setCreatedFor("tester");
    ema2Copy.setCreatedOn(new Timestamp(1234567890L));

    List<EbeanMetadataAspect> list2 = new ArrayList<>();
    list2.add(ema2Copy);
    list2.add(ema1Copy);

    assertTrue(EBeanDAOUtils.compareResults(list1, list2, "testMethod"));

    // different urn in key
    EbeanMetadataAspect ema3 = new EbeanMetadataAspect();
    ema3.setKey(new EbeanMetadataAspect.PrimaryKey("urn2", "aspect1", 0L));
    ema3.setMetadata("{\"metadata\": \"value1\"}");
    ema3.setCreatedBy("tester");
    ema3.setCreatedFor("tester");
    ema3.setCreatedOn(new Timestamp(1234567890L));

    assertFalse(EBeanDAOUtils.compareResults(Collections.singletonList(ema1), Collections.singletonList(ema3), "testMethod"));

    // different urn in key
    EbeanMetadataAspect ema3DifferentCopy = new EbeanMetadataAspect();
    ema3DifferentCopy.setKey(new EbeanMetadataAspect.PrimaryKey("urn3", "aspect1", 0L));
    ema3DifferentCopy.setMetadata("{\"metadata\": \"value1\"}");
    ema3DifferentCopy.setCreatedBy("tester");
    ema3DifferentCopy.setCreatedFor("tester");
    ema3DifferentCopy.setCreatedOn(new Timestamp(1234567890L));

    list1.add(ema3);
    list2.add(ema3DifferentCopy);

    assertFalse(EBeanDAOUtils.compareResults(list1, list2, "testMethod"));

    // remove different elements so list1 and list2 should be equal again
    list1.remove(ema3);
    list2.remove(ema3DifferentCopy);

    // different size lists
    EbeanMetadataAspect ema4 = new EbeanMetadataAspect();
    ema4.setKey(new EbeanMetadataAspect.PrimaryKey("urn1", "aspect1", 0L));
    ema4.setMetadata("{\"metadata\": \"value1\"}");
    ema4.setCreatedBy("tester");
    ema4.setCreatedFor("tester");
    ema4.setCreatedOn(new Timestamp(1234567890L));

    // add one more element to the first list
    list1.add(ema4);

    assertFalse(EBeanDAOUtils.compareResults(list1, list2, "testMethod"));

    // check when lists are null (in theory, this should never happen)
    assertFalse(EBeanDAOUtils.compareResults(list1, null, "testMethod"));
    assertFalse(EBeanDAOUtils.compareResults(null, list1, "testMethod"));

    // check when lists contain null
    list1.remove(ema4); // remove extra element from previous test so lists are equal again
    list1.add(null);
    list2.add(null);
    assertTrue(EBeanDAOUtils.compareResults(list1, list2, "testMethod"));
  }

  @Test
  public void testCompareResultsListUrnSingleton()  throws URISyntaxException {
    FooUrn urn1 = new FooUrn(1);
    FooUrn urn2 = new FooUrn(1);
    assertTrue(EBeanDAOUtils.compareResults(Collections.singletonList(urn1), Collections.singletonList(urn2), "testMethod"));

    FooUrn urn3 = new FooUrn(2);
    assertFalse(EBeanDAOUtils.compareResults(Collections.singletonList(urn1), Collections.singletonList(urn3), "testMethod"));

    BurgerUrn urn4 = makeBurgerUrn("urn:li:burger:1");
    BurgerUrn urn5 = makeBurgerUrn("urn:li:burger:1");
    assertTrue(EBeanDAOUtils.compareResults(Collections.singletonList(urn4), Collections.singletonList(urn5), "testMethod"));

    BurgerUrn urn6 = makeBurgerUrn("urn:li:burger:2");
    assertFalse(EBeanDAOUtils.compareResults(Collections.singletonList(urn4), Collections.singletonList(urn6), "testMethod"));


    BurgerUrn urn7 = makeBurgerUrn("urn:li:burger:cheeseburger");
    BurgerUrn urn8 = makeBurgerUrn("urn:li:burger:CHEESEburger");
    assertFalse(EBeanDAOUtils.compareResults(Collections.singletonList(urn7), Collections.singletonList(urn8), "testMethod"));
  }

  @Test
  public void testCompareResultsListUrnMultiple()  throws URISyntaxException {
    FooUrn urn1 = new FooUrn(1);
    FooUrn urn2 = new FooUrn(2);
    List<FooUrn> list1 = new ArrayList<>();
    list1.add(urn1);
    list1.add(urn2);

    FooUrn urn1Copy = new FooUrn(1);
    FooUrn urn2Copy = new FooUrn(2);
    List<FooUrn> list2 = new ArrayList<>();
    list2.add(urn2Copy);
    list2.add(urn1Copy);
    assertTrue(EBeanDAOUtils.compareResults(list1, list2, "testMethod"));

    FooUrn urn3 = new FooUrn(3);
    FooUrn urn3DifferentCopy = new FooUrn(4);
    list1.add(urn3);
    list2.add(urn3DifferentCopy);
    assertFalse(EBeanDAOUtils.compareResults(list1, list2, "testMethod"));
  }

  @Test
  public void testCompareResultsListResultUrnSingleton() throws URISyntaxException {
    // both list results should be equal with empty values fields
    ListResult<FooUrn> listResult1 = ListResult.<FooUrn>builder()
        .values(new ArrayList<FooUrn>())
        .metadata(null)
        .nextStart(-1)
        .havingMore(false)
        .totalCount(0)
        .totalPageCount(0)
        .pageSize(1)
        .build();

    ListResult<FooUrn> listResult2 = ListResult.<FooUrn>builder()
        .values(new ArrayList<FooUrn>())
        .metadata(null)
        .nextStart(-1)
        .havingMore(false)
        .totalCount(0)
        .totalPageCount(0)
        .pageSize(1)
        .build();

    assertTrue(EBeanDAOUtils.compareResults(listResult1, listResult2, "testMethod"));

    // both list results have a list of a single value that are equal
    FooUrn urn1 = new FooUrn(1);
    FooUrn urn2 = new FooUrn(1);
    List<FooUrn> list1 = Collections.singletonList(urn1);
    List<FooUrn> list2 = Collections.singletonList(urn2);

    listResult1 = ListResult.<FooUrn>builder()
        .values(list1)
        .metadata(null)
        .nextStart(-1)
        .havingMore(false)
        .totalCount(0)
        .totalPageCount(0)
        .pageSize(1)
        .build();

    listResult2 = ListResult.<FooUrn>builder()
        .values(list2)
        .metadata(null)
        .nextStart(-1)
        .havingMore(false)
        .totalCount(0)
        .totalPageCount(0)
        .pageSize(1)
        .build();

    assertTrue(EBeanDAOUtils.compareResults(listResult1, listResult2, "testMethod"));

    // both list results have a list of a single value that are not equal
    FooUrn urn3 = new FooUrn(3);
    List<FooUrn> list3 = Collections.singletonList(urn3);

    // values set to a singleton list with a different value
    ListResult<FooUrn> listResult3 = ListResult.<FooUrn>builder()
        .values(list3)
        .metadata(null)
        .nextStart(-1)
        .havingMore(false)
        .totalCount(0)
        .totalPageCount(0)
        .pageSize(1)
        .build();

    assertFalse(EBeanDAOUtils.compareResults(listResult1, listResult3, "testMethod"));

    // metadata set to something non-null
    ListResult<FooUrn> listResult4 = ListResult.<FooUrn>builder()
        .values(list2)
        .metadata(new ListResultMetadata())
        .nextStart(-1)
        .havingMore(false)
        .totalCount(0)
        .totalPageCount(0)
        .pageSize(1)
        .build();

    assertFalse(EBeanDAOUtils.compareResults(listResult1, listResult4, "testMethod"));

    // different nextStart
    ListResult<FooUrn> listResult5 = ListResult.<FooUrn>builder()
        .values(list2)
        .metadata(null)
        .nextStart(1)
        .havingMore(false)
        .totalCount(0)
        .totalPageCount(0)
        .pageSize(1)
        .build();

    assertFalse(EBeanDAOUtils.compareResults(listResult1, listResult5, "testMethod"));

    // different havingMore
    ListResult<FooUrn> listResult6 = ListResult.<FooUrn>builder()
        .values(list2)
        .metadata(null)
        .nextStart(-1)
        .havingMore(true)
        .totalCount(0)
        .totalPageCount(0)
        .pageSize(1)
        .build();

    assertFalse(EBeanDAOUtils.compareResults(listResult1, listResult6, "testMethod"));

    // different totalCount
    ListResult<FooUrn> listResult7 = ListResult.<FooUrn>builder()
        .values(list2)
        .metadata(null)
        .nextStart(-1)
        .havingMore(false)
        .totalCount(1)
        .totalPageCount(0)
        .pageSize(1)
        .build();

    assertFalse(EBeanDAOUtils.compareResults(listResult1, listResult7, "testMethod"));

    // different totalPageCount
    ListResult<FooUrn> listResult8 = ListResult.<FooUrn>builder()
        .values(list2)
        .metadata(null)
        .nextStart(-1)
        .havingMore(false)
        .totalCount(0)
        .totalPageCount(1)
        .pageSize(1)
        .build();

    assertFalse(EBeanDAOUtils.compareResults(listResult1, listResult8, "testMethod"));

    // different pageSize
    ListResult<FooUrn> listResult9 = ListResult.<FooUrn>builder()
        .values(list2)
        .metadata(null)
        .nextStart(-1)
        .havingMore(false)
        .totalCount(0)
        .totalPageCount(0)
        .pageSize(2)
        .build();

    assertFalse(EBeanDAOUtils.compareResults(listResult1, listResult9, "testMethod"));

    // return false if one of the list results is null
    assertFalse(EBeanDAOUtils.compareResults(listResult1, null, "testMethod"));
    assertFalse(EBeanDAOUtils.compareResults(null, listResult1, "testMethod"));
  }

  @Test
  public void testCompareResultsListResultUrnMultiple() throws URISyntaxException {
    // each list result has a list of multiple values which are equal, but in different order
    FooUrn urn1 = new FooUrn(1);
    FooUrn urn2 = new FooUrn(2);
    FooUrn urn3 = new FooUrn(1);
    FooUrn urn4 = new FooUrn(2);
    List<FooUrn> list1 = new ArrayList<>();
    List<FooUrn> list2 = new ArrayList<>();
    list1.add(urn1);
    list1.add(urn2);
    list2.add(urn4);
    list2.add(urn3);

    ListResult<FooUrn> listResult1 = ListResult.<FooUrn>builder()
        .values(list1)
        .metadata(null)
        .nextStart(-1)
        .havingMore(false)
        .totalCount(0)
        .totalPageCount(0)
        .pageSize(1)
        .build();

    ListResult<FooUrn> listResult2 = ListResult.<FooUrn>builder()
        .values(list2)
        .metadata(null)
        .nextStart(-1)
        .havingMore(false)
        .totalCount(0)
        .totalPageCount(0)
        .pageSize(1)
        .build();

    assertTrue(EBeanDAOUtils.compareResults(listResult1, listResult2, "testMethod"));
  }

  @Test
  public void testToAndFromJson() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    AspectFoo aspectFoo = new AspectFoo();
    aspectFoo.setValue("test");
    AuditedAspect auditedAspect = new AuditedAspect();

    auditedAspect.setLastmodifiedby("0");
    auditedAspect.setLastmodifiedon("1");
    auditedAspect.setAspect(RecordUtils.toJsonString(aspectFoo));
    String toJson = EbeanLocalAccess.toJsonString(auditedAspect);

    Method extractAspectJsonString = EBeanDAOUtils.class.getDeclaredMethod("extractAspectJsonString", String.class);
    extractAspectJsonString.setAccessible(true);
    assertEquals("{\"lastmodifiedby\":\"0\",\"lastmodifiedon\":\"1\",\"aspect\":{\"value\":\"test\"}}", toJson);
    assertNotNull(RecordUtils.toRecordTemplate(AspectFoo.class, (String) extractAspectJsonString.invoke(EBeanDAOUtils.class, toJson)));
  }

  @Test
  public void testGetFromJdbc() {

    EbeanServer server = EmbeddedMariaInstance.getServer(EBeanDAOUtilsTest.class.getSimpleName());
    server.execute(Ebean.createSqlUpdate(readSQLfromFile("ebean-dao-utils-create-all.sql")));

    long now = Instant.now().getEpochSecond() * 1000;
    FooUrn fooUrn = makeFooUrn(1);
    AspectFoo fooAspect = new AspectFoo().setValue("foo");

    // create bean
    EbeanMetadataAspect aspect = new EbeanMetadataAspect();
    EbeanMetadataAspect.PrimaryKey key =
        new EbeanMetadataAspect.PrimaryKey(fooUrn.toString(), AspectFoo.class.getCanonicalName(), 0);
    aspect.setKey(key);
    aspect.setMetadata(RecordUtils.toJsonString(fooAspect));
    aspect.setCreatedOn(new Timestamp(now - 100));
    aspect.setCreatedBy("fooActor");
    aspect.setEmitTime(12345L);

    // add aspect to the db
    server.insert(aspect);

    // sanity test on JDBC functions
    assertNotNull(EBeanDAOUtils.getWithJdbc(fooUrn.toString(), fooAspect.getClass().getCanonicalName(), server, key));
  }

  @Test
  public void testIsSoftDeletedAspect() {
    SqlRow sqlRow = mock(SqlRow.class);
    when(sqlRow.getString("a_aspectfoo")).thenReturn("{\"gma_deleted\": true}");
    assertTrue(EBeanDAOUtils.isSoftDeletedAspect(sqlRow, "a_aspectfoo"));

    when(sqlRow.getString("a_aspectbar")).thenReturn(
        "{\"aspect\": {\"value\": \"bar\"}, \"lastmodifiedby\": \"urn:li:tester\"}");
    assertFalse(EBeanDAOUtils.isSoftDeletedAspect(sqlRow, "a_aspectbar"));

    when(sqlRow.getString("a_aspectbaz")).thenReturn("{\"random_value\": \"baz\"}");
    assertFalse(EBeanDAOUtils.isSoftDeletedAspect(sqlRow, "a_aspectbaz"));
  }

  @Test
  public void testBuildRelationshipFieldCriterionWithAspectField() {
    LocalRelationshipValue localRelationshipValue = LocalRelationshipValue.create(new StringArray("bar"));
    Condition condition = Condition.IN;
    AspectField aspectField = new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value");

    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(localRelationshipValue,
        condition,
        aspectField);

    assertEquals(aspectField, filterCriterion.getField().getAspectField());
    assertEquals(condition, filterCriterion.getCondition());
    assertEquals(localRelationshipValue, filterCriterion.getValue());

    // to test with other type of fields, don't think there's a need to have a separate test case
    UrnField urnField = new UrnField();
    filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(localRelationshipValue,
        condition,
        urnField);
    assertEquals(urnField, filterCriterion.getField().getUrnField());

    RelationshipField relationshipField = new RelationshipField().setPath("/environment");
    filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(localRelationshipValue,
        condition,
        relationshipField);
    assertEquals(relationshipField, filterCriterion.getField().getRelationshipField());
  }
}