package com.linkedin.metadata.testing;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.dao.search.ESBulkWriterDAO;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import com.linkedin.testing.PizzaSearchDocument;
import com.linkedin.testing.PizzaSize;
import com.linkedin.testing.urn.PizzaUrn;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import static org.assertj.core.api.Assertions.*;


/**
 * Integration tests for {@link ESSearchDAO} that run against a real Elasticsearch instance.
 */
@ElasticsearchIntegrationTest
public class ESSearchDaoIntegTest {
  @SearchIndexType(PizzaSearchDocument.class)
  @SearchIndexSettings("/pizza/settings.json")
  @SearchIndexMappings("/pizza/mappings.json")
  public SearchIndex<PizzaSearchDocument> _searchIndex;

  ESBulkWriterDAO<PizzaSearchDocument> _bulkDao;
  ESSearchDAO<PizzaSearchDocument> _searchDao;

  private static final class PizzaSearchConfig extends BaseSearchConfig<PizzaSearchDocument> {
    private static final String QUERY_TEMPLATE;
    private static final String AUTOCOMPLETE_TEMPLATE;

    static {
      try {
        QUERY_TEMPLATE = IOUtils.resourceToString("/pizza/queryTemplate.json", StandardCharsets.UTF_8);
        AUTOCOMPLETE_TEMPLATE = IOUtils.resourceToString("/pizza/autocompleteTemplate.json", StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Nonnull
    @Override
    public Set<String> getLowCardinalityFields() {
      return Collections.singleton("size");
    }

    @Nonnull
    @Override
    public Set<String> getFacetFields() {
      return Collections.emptySet();
    }

    @Nonnull
    @Override
    public Class<PizzaSearchDocument> getSearchDocument() {
      return PizzaSearchDocument.class;
    }

    @Nonnull
    @Override
    public String getDefaultAutocompleteField() {
      return "toppings";
    }

    @Nonnull
    @Override
    public String getSearchQueryTemplate() {
      return QUERY_TEMPLATE;
    }

    @Nonnull
    @Override
    public String getAutocompleteQueryTemplate() {
      return AUTOCOMPLETE_TEMPLATE;
    }
  }

  @BeforeEach
  public void setup() {
    _bulkDao = _searchIndex.getWriteDao();
    _searchDao = _searchIndex.createReadDao(new PizzaSearchConfig());
  }

  @Test
  public void autocompletePrefix() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setSize(PizzaSize.MEDIUM).setToppings(new StringArray("Sausage Links")).setUrn(urn0),
        urn0.toString());
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Sausage crumble")).setUrn(urn1),
        urn1.toString());
    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final AutoCompleteResult result = _searchDao.autoComplete("Sau", null, null, 10);

    // then
    assertThat(result.getSuggestions()).containsExactly("Sausage Links", "Sausage crumble");
  }

  @Test
  public void autocompleteSuffix() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setSize(PizzaSize.MEDIUM).setToppings(new StringArray("Mild Sausage")).setUrn(urn0),
        urn0.toString());
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Spicy Sausage")).setUrn(urn1),
        urn1.toString());
    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final AutoCompleteResult result = _searchDao.autoComplete("Sausage", null, null, 10);

    // then
    assertThat(result.getSuggestions()).containsExactlyInAnyOrder("Mild Sausage", "Spicy Sausage");
  }

  @Test
  public void autocompleteUsesDefaultField() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setSize(PizzaSize.MEDIUM)
        .setToppings(new StringArray("Large Meatballs"))
        .setUrn(urn0), urn0.toString());
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Pineapple")).setUrn(urn1),
        urn1.toString());
    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final AutoCompleteResult result = _searchDao.autoComplete("Large", null, null, 10);

    // then
    assertThat(result.getSuggestions()).containsExactly("Large Meatballs");
  }

  @Test
  public void autocompleteNonDefaultField() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setSize(PizzaSize.LARGE)
        .setToppings(new StringArray("Large Meatballs"))
        .setMadeBy("New York Pizzas")
        .setUrn(urn0), urn0.toString());
    _searchIndex.getRequestContainer().flushAndSettle();

    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setSize(PizzaSize.LARGE)
        .setToppings(new StringArray("Pesto"))
        .setMadeBy("New Wave Pizza")
        .setUrn(urn1), urn1.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final AutoCompleteResult result = _searchDao.autoComplete("New", "madeBy", null, 10);

    // then
    assertThat(result.getSuggestions()).containsExactlyInAnyOrder("New York Pizzas", "New Wave Pizza");
  }

  @Test
  public void autocompleteFilter() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setSize(PizzaSize.MEDIUM)
        .setToppings(new StringArray("Small pepperoni"))
        .setUrn(urn0), urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Big pepperoni")).setUrn(urn1),
        urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Pepperoni")).setUrn(urn2),
        urn2.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final AutoCompleteResult result = _searchDao.autoComplete("pepperoni", null, new Filter().setCriteria(
        new CriterionArray(
            new Criterion().setCondition(Condition.EQUAL).setField("size").setValue(PizzaSize.LARGE.toString()))), 10);

    // then
    assertThat(result.getSuggestions()).containsExactlyInAnyOrder("Big pepperoni", "Pepperoni");
  }

  @Test
  public void autocompleteLimit() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setSize(PizzaSize.MEDIUM).setToppings(new StringArray("Mild Sausage")).setUrn(urn0),
        urn0.toString());
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Spicy Sausage")).setUrn(urn1),
        urn1.toString());
    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final AutoCompleteResult result = _searchDao.autoComplete("Sausage", null, null, 1);

    // then
    assertThat(result.getSuggestions()).containsExactlyInAnyOrder("Mild Sausage");
  }

  @Test
  public void search() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    final PizzaSearchDocument pizza0 =
        new PizzaSearchDocument().setSize(PizzaSize.MEDIUM).setToppings(new StringArray("Pepperoni")).setUrn(urn0);
    _bulkDao.upsertDocument(pizza0, urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    final PizzaSearchDocument pizza1 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Big pepperoni")).setUrn(urn1);
    _bulkDao.upsertDocument(pizza1, urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    final PizzaSearchDocument pizza2 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Pineapple")).setUrn(urn2);
    _bulkDao.upsertDocument(pizza2, urn2.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final SearchResult<PizzaSearchDocument> result = _searchDao.search("pepperoni", null, null, 0, 10);

    // then
    assertThat(result.getDocumentList()).containsExactly(pizza0, pizza1);
    assertThat(result.isHavingMore()).isFalse();
  }

  @Test
  public void searchLimit() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    final PizzaSearchDocument pizza0 =
        new PizzaSearchDocument().setSize(PizzaSize.MEDIUM).setToppings(new StringArray("Pepperoni")).setUrn(urn0);
    _bulkDao.upsertDocument(pizza0, urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    final PizzaSearchDocument pizza1 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Big pepperoni")).setUrn(urn1);
    _bulkDao.upsertDocument(pizza1, urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    final PizzaSearchDocument pizza2 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Small Pepperoni")).setUrn(urn2);
    _bulkDao.upsertDocument(pizza2, urn2.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final SearchResult<PizzaSearchDocument> result = _searchDao.search("pepperoni", null, null, 0, 2);

    // then
    assertThat(result.getDocumentList()).containsExactly(pizza0, pizza1);
    assertThat(result.isHavingMore()).isTrue();
  }

  @Test
  public void searchPaging() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    final PizzaSearchDocument pizza0 =
        new PizzaSearchDocument().setSize(PizzaSize.MEDIUM).setToppings(new StringArray("Pepperoni")).setUrn(urn0);
    _bulkDao.upsertDocument(pizza0, urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    final PizzaSearchDocument pizza1 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Big pepperoni")).setUrn(urn1);
    _bulkDao.upsertDocument(pizza1, urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    final PizzaSearchDocument pizza2 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Small Pepperoni")).setUrn(urn2);
    _bulkDao.upsertDocument(pizza2, urn2.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final SearchResult<PizzaSearchDocument> result = _searchDao.search("pepperoni", null, null, 2, 10);

    // then
    assertThat(result.getDocumentList()).containsExactly(pizza2);
    assertThat(result.isHavingMore()).isFalse();
  }

  @Test
  public void searchFilter() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    final PizzaSearchDocument pizza0 =
        new PizzaSearchDocument().setSize(PizzaSize.MEDIUM).setToppings(new StringArray("Pepperoni")).setUrn(urn0);
    _bulkDao.upsertDocument(pizza0, urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    final PizzaSearchDocument pizza1 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Big pepperoni")).setUrn(urn1);
    _bulkDao.upsertDocument(pizza1, urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    final PizzaSearchDocument pizza2 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Small Pepperoni")).setUrn(urn2);
    _bulkDao.upsertDocument(pizza2, urn2.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final SearchResult<PizzaSearchDocument> result = _searchDao.search("pepperoni", new Filter().setCriteria(
        new CriterionArray(new Criterion().setCondition(Condition.EQUAL).setField("size").setValue("LARGE"))), null, 0,
        10);

    // then
    assertThat(result.getDocumentList()).containsExactly(pizza1, pizza2);
    assertThat(result.isHavingMore()).isFalse();
  }

  @Test
  public void searchSort() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    final PizzaSearchDocument pizza0 =
        new PizzaSearchDocument().setSize(PizzaSize.MEDIUM).setToppings(new StringArray("Pepperoni")).setUrn(urn0);
    _bulkDao.upsertDocument(pizza0, urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    final PizzaSearchDocument pizza1 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Big pepperoni")).setUrn(urn1);
    _bulkDao.upsertDocument(pizza1, urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    final PizzaSearchDocument pizza2 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Small Pepperoni")).setUrn(urn2);
    _bulkDao.upsertDocument(pizza2, urn2.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final SearchResult<PizzaSearchDocument> result =
        _searchDao.search("pepperoni", null, new SortCriterion().setField("toppings").setOrder(SortOrder.ASCENDING), 0,
            10);

    // then
    assertThat(result.getDocumentList()).containsExactly(pizza1, pizza0, pizza2);
    assertThat(result.isHavingMore()).isFalse();
  }

  @Test
  public void filter() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    final PizzaSearchDocument pizza0 =
        new PizzaSearchDocument().setSize(PizzaSize.MEDIUM).setToppings(new StringArray("Pepperoni")).setUrn(urn0);
    _bulkDao.upsertDocument(pizza0, urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    final PizzaSearchDocument pizza1 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Big pepperoni")).setUrn(urn1);
    _bulkDao.upsertDocument(pizza1, urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    final PizzaSearchDocument pizza2 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Small Pepperoni")).setUrn(urn2);
    _bulkDao.upsertDocument(pizza2, urn2.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final SearchResult<PizzaSearchDocument> result = _searchDao.filter(new Filter().setCriteria(new CriterionArray(
            new Criterion().setCondition(Condition.EQUAL).setField("size").setValue(PizzaSize.LARGE.toString()))), null, 0,
        10);

    // then
    assertThat(result.getDocumentList()).containsExactly(pizza1, pizza2);
    assertThat(result.isHavingMore()).isFalse();
  }

  @Test
  public void filterSort() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    final PizzaSearchDocument pizza0 =
        new PizzaSearchDocument().setSize(PizzaSize.MEDIUM).setToppings(new StringArray("Pepperoni")).setUrn(urn0);
    _bulkDao.upsertDocument(pizza0, urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    final PizzaSearchDocument pizza1 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Big pepperoni")).setUrn(urn1);
    _bulkDao.upsertDocument(pizza1, urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    final PizzaSearchDocument pizza2 =
        new PizzaSearchDocument().setSize(PizzaSize.LARGE).setToppings(new StringArray("Small Pepperoni")).setUrn(urn2);
    _bulkDao.upsertDocument(pizza2, urn2.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final SearchResult<PizzaSearchDocument> result = _searchDao.filter(new Filter().setCriteria(new CriterionArray(
            new Criterion().setCondition(Condition.EQUAL).setField("size").setValue(PizzaSize.LARGE.toString()))),
        new SortCriterion().setField("toppings").setOrder(SortOrder.DESCENDING), 0, 10);

    // then
    assertThat(result.getDocumentList()).containsExactly(pizza2, pizza1);
    assertThat(result.isHavingMore()).isFalse();
  }
}
