package com.linkedin.metadata.testing;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.browse.BaseBrowseConfig;
import com.linkedin.metadata.dao.browse.ESBrowseDAO;
import com.linkedin.metadata.dao.search.ESBulkWriterDAO;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.BrowseResultEntity;
import com.linkedin.metadata.query.BrowseResultGroup;
import com.linkedin.metadata.query.BrowseResultGroupArray;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import com.linkedin.testing.PizzaSearchDocument;
import com.linkedin.testing.PizzaSize;
import com.linkedin.testing.urn.PizzaUrn;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;


/**
 * Integration tests for {@link ESBrowseDAO} that run against a real Elasticsearch instance.
 */
@ElasticsearchIntegrationTest
public class ESBrowseDaoIntegTest {
  @SearchIndexType(PizzaSearchDocument.class)
  @SearchIndexSettings("/pizza/settings.json")
  @SearchIndexMappings("/pizza/mappings.json")
  public SearchIndex<PizzaSearchDocument> _searchIndex;

  ESBulkWriterDAO<PizzaSearchDocument> _bulkDao;
  ESBrowseDAO _browseDao;

  private static final class PizzaBrowseConfig extends BaseBrowseConfig<PizzaSearchDocument> {
    @Override
    public Class<PizzaSearchDocument> getSearchDocument() {
      return PizzaSearchDocument.class;
    }
  }

  @BeforeEach
  public void setup() {
    _bulkDao = _searchIndex.getWriteDao();
    _browseDao = _searchIndex.createBrowseDao(new PizzaBrowseConfig());
  }

  @Test
  public void getPaths() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn0).setBrowsePaths(new StringArray("/New York/pizza shop")),
        urn0.toString());
    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final List<String> actualPaths = _browseDao.getBrowsePaths(urn0);

    // then
    assertThat(actualPaths).containsExactly("/New York/pizza shop");
  }

  @Test
  public void getPathsForDocumentWithoutPaths() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn0), urn0.toString());
    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final List<String> actualPaths = _browseDao.getBrowsePaths(urn0);

    // then
    assertThat(actualPaths).isEmpty();
  }

  @Test
  public void browse() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn0).setBrowsePaths(new StringArray("/nyc/Mario's")),
        urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn1).setBrowsePaths(new StringArray("/nyc/Luigi's")),
        urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn2).setBrowsePaths(new StringArray("/nyc/brooklyn/Peach's")),
        urn2.toString());

    final PizzaUrn urn3 = new PizzaUrn(3);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn3).setBrowsePaths(new StringArray("/sunnyvale/A Slice of New York")),
        urn3.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final BrowseResult result = _browseDao.browse("/nyc", null, 0, 10);

    // then
    assertThat(result.getEntities()).containsExactly(new BrowseResultEntity().setName("Mario's").setUrn(urn0),
        new BrowseResultEntity().setName("Luigi's").setUrn(urn1));
    // Does not contain peach's because it is nested another level.
    // Does not contain a slice of new york because it is not under /nyc.
  }

  @Test
  public void browseMetadata() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn0).setBrowsePaths(new StringArray("/nyc/Mario's")),
        urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn1).setBrowsePaths(new StringArray("/nyc/Luigi's")),
        urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn2).setBrowsePaths(new StringArray("/nyc/brooklyn/Peach's")),
        urn2.toString());

    final PizzaUrn urn3 = new PizzaUrn(3);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn3).setBrowsePaths(new StringArray("/nyc/manhattan/Bowser's")),
        urn3.toString());

    final PizzaUrn urn4 = new PizzaUrn(4);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn4).setBrowsePaths(new StringArray("/nyc/brooklyn/south/Toad's")),
        urn4.toString());

    final PizzaUrn urn5 = new PizzaUrn(5);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn5).setBrowsePaths(new StringArray("/sunnyvale/A Slice of New York")),
        urn5.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final BrowseResult result = _browseDao.browse("/nyc", null, 0, 10);

    // then
    assertThat(result.getMetadata().getPath()).isEqualTo("/nyc");
    assertThat(result.getMetadata().getTotalNumEntities()).isEqualTo(5); // Mario, Luigi's, Peach's, Bowser's, Toad's
    assertThat(result.getMetadata().getGroups()).isEqualTo(
        new BrowseResultGroupArray(new BrowseResultGroup().setName("brooklyn").setCount(2), // Peach's, Toad's
            new BrowseResultGroup().setName("manhattan").setCount(1))); // Bowser's
  }

  @Test
  public void browseNested() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn0).setBrowsePaths(new StringArray("/nyc/Mario's")),
        urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn1).setBrowsePaths(new StringArray("/nyc/Luigi's")),
        urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn2).setBrowsePaths(new StringArray("/nyc/brooklyn/Peach's")),
        urn2.toString());

    final PizzaUrn urn3 = new PizzaUrn(3);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn3).setBrowsePaths(new StringArray("/sunnyvale/A Slice of New York")),
        urn3.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final BrowseResult result = _browseDao.browse("/nyc/brooklyn", null, 0, 10);

    // then
    assertThat(result.getEntities()).containsExactly(new BrowseResultEntity().setName("Peach's").setUrn(urn2));
  }

  @Test
  public void browseRoot() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn0).setBrowsePaths(new StringArray("/nyc/Mario's")),
        urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn1).setBrowsePaths(new StringArray("/nyc/Luigi's")),
        urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn2).setBrowsePaths(new StringArray("/Peach's")),
        urn2.toString());

    final PizzaUrn urn3 = new PizzaUrn(3);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn3).setBrowsePaths(new StringArray("/sunnyvale/A Slice of New York")),
        urn3.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final BrowseResult result = _browseDao.browse("", null, 0, 10);

    // then
    assertThat(result.getEntities()).containsExactly(new BrowseResultEntity().setName("Peach's").setUrn(urn2));
  }

  @Test
  public void browseForBadPath() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn0).setBrowsePaths(new StringArray("/nyc/Mario's")),
        urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn1).setBrowsePaths(new StringArray("/nyc/Luigi's")),
        urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn2).setBrowsePaths(new StringArray("/nyc/brooklyn/Peach's")),
        urn2.toString());

    final PizzaUrn urn3 = new PizzaUrn(3);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn3).setBrowsePaths(new StringArray("/sunnyvale/A Slice of New York")),
        urn3.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final BrowseResult result = _browseDao.browse("/sanfrancisco", null, 0, 10);

    // then
    assertThat(result.getEntities()).isEmpty();
  }

  @Test
  public void browseForPathWithNoEntities() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn0).setBrowsePaths(new StringArray("/nyc/brooklyn/Mario's")),
        urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn1).setBrowsePaths(new StringArray("/nyc/brooklyn/Luigi's")),
        urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    _bulkDao.upsertDocument(
        new PizzaSearchDocument().setUrn(urn2).setBrowsePaths(new StringArray("/nyc/brooklyn/Peach's")),
        urn2.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final BrowseResult result = _browseDao.browse("/nyc", null, 0, 10);

    // then
    assertThat(result.getEntities()).isEmpty();
  }

  @Test
  public void browseWithFilter() throws Exception {
    // given
    final PizzaUrn urn0 = new PizzaUrn(0);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn0)
        .setBrowsePaths(new StringArray("/nyc/brooklyn/Mario's"))
        .setSize(PizzaSize.LARGE), urn0.toString());

    final PizzaUrn urn1 = new PizzaUrn(1);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn1)
        .setBrowsePaths(new StringArray("/nyc/brooklyn/Luigi's"))
        .setSize(PizzaSize.MEDIUM), urn1.toString());

    final PizzaUrn urn2 = new PizzaUrn(2);
    _bulkDao.upsertDocument(new PizzaSearchDocument().setUrn(urn2)
        .setBrowsePaths(new StringArray("/nyc/brooklyn/Peach's"))
        .setSize(PizzaSize.LARGE), urn2.toString());

    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final BrowseResult result = _browseDao.browse("/nyc/brooklyn", new Filter().setCriteria(
        new CriterionArray(new Criterion().setValue("LARGE").setField("size").setCondition(Condition.EQUAL))), 0, 10);

    // then
    assertThat(result.getEntities()).containsExactly(new BrowseResultEntity().setName("Mario's").setUrn(urn0),
        new BrowseResultEntity().setName("Peach's").setUrn(urn2));
  }
}
