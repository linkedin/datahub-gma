package com.linkedin.metadata.testing;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.dao.browse.BaseBrowseConfig;
import com.linkedin.metadata.dao.browse.ESBrowseDAO;
import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.dao.search.ESBulkWriterDAO;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.common.xcontent.XContentType;


/**
 * Abstraction over an Elasticsearch index, which can be written to and read from.
 *
 * <p>Instances only ever have a single bulk listener and writer DAO, but may create many reader daos, as each reader
 * can be configured differently.
 */
public final class SearchIndex<DOCUMENT extends RecordTemplate> {
  private final Class<DOCUMENT> _documentClass;
  private final ElasticsearchConnection _connection;
  private final String _name;
  private final ESBulkWriterDAO<DOCUMENT> _bulkWriterDAO;
  private final BulkRequestsContainer _requestContainer;

  public SearchIndex(@Nonnull Class<DOCUMENT> documentClass, @Nonnull ElasticsearchConnection connection,
      @Nonnull String name) {
    _documentClass = documentClass;
    _connection = connection;
    _name = name;

    final TestBulkListener bulkListener = new TestBulkListener();
    final BulkProcessor bulkProcessor = BulkProcessor.builder(connection.getTransportClient(), bulkListener).build();
    _requestContainer = new BulkRequestsContainer(bulkProcessor, bulkListener);

    _bulkWriterDAO = new ESBulkWriterDAO<DOCUMENT>(documentClass, bulkProcessor, name);
  }

  /**
   * Sets the settings of this index.
   *
   * @param settingsJson the new Elasticsearch settings as a JSON string
   */
  public void setSettings(@Nonnull String settingsJson) {
    _connection.getTransportClient()
        .admin()
        .indices()
        .prepareUpdateSettings(_name)
        .setSettings(settingsJson, XContentType.JSON)
        .get();
  }

  /**
   * Sets the mappings of this index.
   *
   * @param mappingsJson the new Elasticsearch mappings as a JSON string
   */
  public void setMappings(@Nonnull String mappingsJson) {
    _connection.getTransportClient()
        .admin()
        .indices()
        .preparePutMapping(_name)
        .setSource(mappingsJson, XContentType.JSON)
        .get();
  }

  /**
   * Creates a new {@link ESSearchDAO} with the given configuration that will talk to this index.
   */
  @Nonnull
  public ESSearchDAO<DOCUMENT> createReadDao(@Nonnull BaseSearchConfig<DOCUMENT> config) {
    return new ESSearchDAO<>(_connection.getRestHighLevelClient(), _documentClass,
        new TestSearchConfig<>(config, _name));
  }

  /**
   * Creates a new {@link ESBrowseDAO} with the given configuration that will talk to this index.
   */
  @Nonnull
  public ESBrowseDAO<DOCUMENT> createBrowseDao(@Nonnull BaseBrowseConfig<DOCUMENT> config) {
    return new ESBrowseDAO<>(_connection.getRestHighLevelClient(), new TestBrowseConfig<>(config, _name));
  }

  /**
   * Creates a new {@link ESSearchDAO} with will talk to this index, and where all search and autocomplete queries will
   * return all documents.
   */
  @Nonnull
  public ESSearchDAO<DOCUMENT> createReadAllDocumentsDao() {
    return createReadDao(new ReadAllDocumentsConfig<>(_documentClass));
  }

  @Nonnull
  public Collection<DOCUMENT> readAllDocuments() {
    final ESSearchDAO<DOCUMENT> dao = createReadAllDocumentsDao();
    final List<DOCUMENT> docs = new ArrayList<>();

    int from = 0;
    SearchResult<DOCUMENT> result;

    do {
      result = dao.search("", null, null, from, 10);
      docs.addAll(result.getDocumentList());
      from += 10;
    } while (result.isHavingMore());

    return docs;
  }

  @Nonnull
  public ESBulkWriterDAO<DOCUMENT> getWriteDao() {
    return _bulkWriterDAO;
  }

  @Nonnull
  public BulkRequestsContainer getRequestContainer() {
    return _requestContainer;
  }

  /**
   * Returns the name of this index.
   *
   * <p>Should be used for informational purposes only (i.e. printing error messages, debugging, etc).
   */
  @Nonnull
  public String getName() {
    return _name;
  }

  /**
   * Returns the {@link ElasticsearchConnection} object, which contains lower level access to the Elasticsearch cluster.
   *
   * <p>Note that these are objects to interact with the entire cluster, not this index specifically. You'll need to
   * specifiy that this is in the index you want to interact with in many requests using {@link #getName()}.
   *
   * <p>If you perform any extra mutation operations on the cluster (e.g. create an alias), be sure to clean them up
   * after the test.
   */
  @Nonnull
  public ElasticsearchConnection getConnection() {
    return _connection;
  }
}
