package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.backfill.BackfillItem;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.restli.dao.DefaultLocalDaoRegistryImpl;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.ModelUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseEntityAgnosticResourceTest extends BaseEngineTest {

  private BaseLocalDAO<EntityAspectUnion, FooUrn> _fooLocalDAO;

  private BaseLocalDAO<EntityAspectUnion, BarUrn> _barLocalDAO;

  private DefaultLocalDaoRegistryImpl _registry;

  private Set<String> fooUrnSet;

  private Set<String> singleAspectSet;

  private Set<String> multiAspectsSet;

  class TestResource extends BaseEntityAgnosticResource {

    @Nonnull
    @Override
    protected DefaultLocalDaoRegistryImpl getLocalDaoRegistry() {
      return _registry;
    }
  }

  @BeforeMethod
  void setup() {
    _fooLocalDAO = mock(BaseLocalDAO.class);
    _barLocalDAO = mock(BaseLocalDAO.class);
    when(_barLocalDAO.getUrnClass()).thenReturn(BarUrn.class);
    when(_fooLocalDAO.getUrnClass()).thenReturn(FooUrn.class);
    _registry = DefaultLocalDaoRegistryImpl.init(ImmutableMap.of("foo", _fooLocalDAO, "bar", _barLocalDAO));
    fooUrnSet = ImmutableSet.of(makeFooUrn(1).toString(), makeFooUrn(2).toString(), makeFooUrn(3).toString());
    singleAspectSet = Collections.singleton(getAspectName(AspectFoo.class));
    multiAspectsSet = ImmutableSet.of(getAspectName(AspectBar.class), getAspectName(AspectFoo.class));
  }

  @Test
  public void testBackfillMAESpecificAspectSuccess() {
    TestResource testResource = new TestResource();
    for (String urn : fooUrnSet) {
      when(_fooLocalDAO.backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX, multiAspectsSet, Collections.singleton(urn)))
          .thenReturn(ImmutableMap.of(urn, singleAspectSet));
    }

    BackfillItem[] result = runAndWait(testResource.backfillMAE(provideBackfillItems(fooUrnSet, multiAspectsSet), IngestionMode.BACKFILL));
    for (String urn : fooUrnSet) {
      verify(_fooLocalDAO, times(1)).backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX,
          multiAspectsSet, Collections.singleton(urn));
    }
    assertEqualBackfillItemArrays(result, provideBackfillItems(fooUrnSet, singleAspectSet));
  }

  @Test
  public void testBackfillMAENullAspectSuccess() {
    TestResource testResource = new TestResource();
    for (String urn : fooUrnSet) {
      when(_fooLocalDAO.backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX, null, Collections.singleton(urn)))
          .thenReturn(ImmutableMap.of(urn, multiAspectsSet));
    }

    BackfillItem[] result = runAndWait(testResource.backfillMAE(provideBackfillItems(fooUrnSet, null), IngestionMode.BACKFILL));
    for (String urn : fooUrnSet) {
      verify(_fooLocalDAO, times(1)).backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX,
          null, Collections.singleton(urn));
    }
    assertEqualBackfillItemArrays(result, provideBackfillItems(fooUrnSet, multiAspectsSet));
  }

  @Test
  public void testBackfillMAEMultiEntitiesSuccess() {
    // mockito stubbing
    Set<String> barUrnSet = ImmutableSet.of(makeBarUrn(1).toString(), makeBarUrn(2).toString(), makeBarUrn(3).toString());
    for (String urn : fooUrnSet) {
      when(_fooLocalDAO.backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX, null, Collections.singleton(urn)))
          .thenReturn(ImmutableMap.of(urn, multiAspectsSet));
    }
    for (String urn : barUrnSet) {
      when(_barLocalDAO.backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX, null, Collections.singleton(urn)))
          .thenReturn(ImmutableMap.of(urn, multiAspectsSet));
    }

    // merge urn sets
    Set<String> allUrnSet = new HashSet<>(barUrnSet);
    allUrnSet.addAll(fooUrnSet);

    TestResource testResource = new TestResource();
    BackfillItem[] result = runAndWait(testResource.backfillMAE(provideBackfillItems(allUrnSet, null), IngestionMode.BACKFILL));

    // verify all aspects are backfilled for each urn
    for (String urn : fooUrnSet) {
      verify(_fooLocalDAO, times(1)).backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX,
          null, Collections.singleton(urn));
    }
    for (String urn : barUrnSet) {
      verify(_barLocalDAO, times(1)).backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX,
          null, Collections.singleton(urn));
    }
    BackfillItem[] expectedItems = provideBackfillItems(allUrnSet, multiAspectsSet);
    assertEqualBackfillItemArrays(result, expectedItems);
    verify(_fooLocalDAO, times(1)).getUrnClass();
    verify(_barLocalDAO, times(1)).getUrnClass();
    verifyNoMoreInteractions(_fooLocalDAO);
    verifyNoMoreInteractions(_barLocalDAO);
  }

  @Test
  public void testBackfillMAEEmptyBackfillResult() {
    TestResource testResource = new TestResource();
    // no mockito stubbing, so dao.backfillMAE will return null
    assertEquals(
        runAndWait(testResource.backfillMAE(provideBackfillItems(fooUrnSet, null), IngestionMode.BACKFILL)),
        new BackfillItem[0]
    );
    verify(_fooLocalDAO, times(3)).backfillMAE(any(), any(), any());
  }

  @Test
  public void testBackfillMAEFilterEmptyAspectUrn() {
    TestResource testResource = new TestResource();
    Set<String> urnSet = ImmutableSet.of(makeFooUrn(1).toString(), makeFooUrn(2).toString());
    when(_fooLocalDAO.backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX, null, Collections.singleton(makeFooUrn(1).toString())))
        .thenReturn(ImmutableMap.of(makeFooUrn(1).toString(), multiAspectsSet));
    BackfillItem[] result = runAndWait(testResource.backfillMAE(provideBackfillItems(urnSet, null), IngestionMode.BACKFILL));
    assertEqualBackfillItemArrays(result, provideBackfillItems(ImmutableSet.of(makeFooUrn(1).toString()), multiAspectsSet));
  }

  @Test
  public void testBackfillMAENoSuchEntity() {
    TestResource testResource = new TestResource();
    Set<String> badUrnSet = ImmutableSet.of(makeBazUrn(1).toString(), makeBazUrn(2).toString(), makeBazUrn(3).toString());
    assertEquals(
        runAndWait(testResource.backfillMAE(provideBackfillItems(badUrnSet, null), IngestionMode.BACKFILL)),
        new BackfillItem[0]
    );
    verify(_fooLocalDAO, times(0)).backfillMAE(any(), any(), any());
    verify(_barLocalDAO, times(0)).backfillMAE(any(), any(), any());
  }

  @Test
  public void testBackfillMAENoopMode() {
    TestResource testResource = new TestResource();
    assertEquals(
        runAndWait(testResource.backfillMAE(provideBackfillItems(fooUrnSet, null), IngestionMode.LIVE)),
        new BackfillItem[0]
    );
    verify(_fooLocalDAO, times(0)).backfillMAE(any(), any(), any());
  }

  @Test
  public void testBackfillMAEException() {
    TestResource testResource = new TestResource();
    for (String urn : fooUrnSet) {
      when(_fooLocalDAO.backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX, multiAspectsSet, Collections.singleton(urn)))
          .thenReturn(ImmutableMap.of(urn, multiAspectsSet));
    }
    doThrow(IllegalArgumentException.class).when(_fooLocalDAO).backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX, multiAspectsSet,
        Collections.singleton(makeFooUrn(1).toString()));

    BackfillItem[] result = runAndWait(testResource.backfillMAE(provideBackfillItems(fooUrnSet, multiAspectsSet), IngestionMode.BACKFILL));
    for (String urn : fooUrnSet) {
      verify(_fooLocalDAO, times(1)).backfillMAE(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX,
          multiAspectsSet, Collections.singleton(urn));
    }
    BackfillItem[] expectedItems =
        provideBackfillItems(ImmutableSet.of(makeFooUrn(2).toString(), makeFooUrn(3).toString()), multiAspectsSet);
    assertEqualBackfillItemArrays(result, expectedItems);
  }

  private BackfillItem[] provideBackfillItems(Set<String> urnSet, Set<String> aspects) {
    return urnSet.stream().map(urn -> {
      BackfillItem item = new BackfillItem();
      item.setUrn(urn);
      if (aspects != null) {
        item.setAspects(new StringArray(aspects));
      }
      return item;
    }).toArray(BackfillItem[]::new);
  }

  private void assertEqualBackfillItemArrays(BackfillItem[] actual, BackfillItem[] expected) {
    List<BackfillItem> expectedList = Arrays.asList(expected);
    List<BackfillItem> actualList = Arrays.asList(actual);
    assertEquals(actualList.size(), expectedList.size());
    assertTrue(actualList.containsAll(expectedList));
  }
}
