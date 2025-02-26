package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.aspect.AuditedAspect;
import com.linkedin.metadata.dao.urnpath.EmptyPathExtractor;
import com.linkedin.metadata.dao.urnpath.UrnPathExtractor;
import com.linkedin.metadata.dao.utils.EBeanDAOUtils;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dao.utils.SQLSchemaUtils;
import com.linkedin.metadata.dao.utils.SQLStatementUtils;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.ListResultMetadata;
import io.ebean.EbeanServer;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import io.ebean.SqlUpdate;
import io.ebean.Transaction;
import io.ebean.annotation.Transactional;
import io.ebean.config.ServerConfig;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.PersistenceException;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;

import static com.linkedin.metadata.dao.EbeanLocalDAO.*;
import static com.linkedin.metadata.dao.utils.EBeanDAOUtils.*;
import static com.linkedin.metadata.dao.utils.SQLIndexFilterUtils.*;
import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static com.linkedin.metadata.dao.utils.SQLStatementUtils.*;


/**
 * EBeanLocalAccess provides model agnostic data access (read / write) to MySQL database.
 */
@Slf4j
public class EbeanLocalAccess<URN extends Urn> implements IEbeanLocalAccess<URN> {
  private final EbeanServer _server;
  private final Class<URN> _urnClass;
  private final String _entityType;
  private UrnPathExtractor<URN> _urnPathExtractor;
  private final SchemaEvolutionManager _schemaEvolutionManager;
  private final boolean _nonDollarVirtualColumnsEnabled;

  // TODO confirm if the default page size is 1000 in other code context.
  private static final int DEFAULT_PAGE_SIZE = 1000;
  private static final String ASPECT_JSON_PLACEHOLDER = "__PLACEHOLDER__";
  private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
  private static final String SERVICE_IDENTIFIER = "SERVICE_IDENTIFIER";

  // key: table_name,
  // value: Set(column1, column2, column3 ...)
  private final Map<String, Set<String>> tableColumns = new ConcurrentHashMap<>();

  public EbeanLocalAccess(EbeanServer server, ServerConfig serverConfig, @Nonnull Class<URN> urnClass,
      UrnPathExtractor<URN> urnPathExtractor, boolean nonDollarVirtualColumnsEnabled) {
    _server = server;
    _urnClass = urnClass;
    _urnPathExtractor = urnPathExtractor;
    _entityType = ModelUtils.getEntityTypeFromUrnClass(_urnClass);
    _schemaEvolutionManager = createSchemaEvolutionManager(serverConfig);
    _nonDollarVirtualColumnsEnabled = nonDollarVirtualColumnsEnabled;
  }

  public void setUrnPathExtractor(@Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    _urnPathExtractor = urnPathExtractor;
  }

  public void ensureSchemaUpToDate() {
    _schemaEvolutionManager.ensureSchemaUpToDate();
  }

  @Override
  @Transactional
  public <ASPECT extends RecordTemplate> int add(@Nonnull URN urn, @Nullable ASPECT newValue, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull AuditStamp auditStamp, @Nullable IngestionTrackingContext ingestionTrackingContext, boolean isTestMode) {
    return addWithOptimisticLocking(urn, newValue, aspectClass, auditStamp, null, ingestionTrackingContext, isTestMode);
  }

  @Override
  public <ASPECT extends RecordTemplate> int addWithOptimisticLocking(
      @Nonnull URN urn,
      @Nullable ASPECT newValue,
      @Nonnull Class<ASPECT> aspectClass,
      @Nonnull AuditStamp auditStamp,
      @Nullable Timestamp oldTimestamp,
      @Nullable IngestionTrackingContext ingestionTrackingContext,
      boolean isTestMode) {

    final long timestamp = auditStamp.hasTime() ? auditStamp.getTime() : System.currentTimeMillis();
    final String actor = auditStamp.hasActor() ? auditStamp.getActor().toString() : DEFAULT_ACTOR;
    final String impersonator = auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null;
    final boolean urnExtraction = _urnPathExtractor != null && !(_urnPathExtractor instanceof EmptyPathExtractor);

    final SqlUpdate sqlUpdate;
    if (oldTimestamp != null) {
      sqlUpdate = _server.createSqlUpdate(
          SQLStatementUtils.createAspectUpdateWithOptimisticLockSql(urn, aspectClass, urnExtraction, isTestMode));
      sqlUpdate.setParameter("oldTimestamp", oldTimestamp.toString());
    } else {
      sqlUpdate = _server.createSqlUpdate(SQLStatementUtils.createAspectUpsertSql(urn, aspectClass, urnExtraction, isTestMode));
    }
    sqlUpdate.setParameter("urn", urn.toString())
        .setParameter("lastmodifiedon", new Timestamp(timestamp).toString())
        .setParameter("lastmodifiedby", actor);

    // If a non-default UrnPathExtractor is provided, the user MUST specify in their schema generation scripts
    // 'ALTER TABLE <table> ADD COLUMN a_urn JSON'.
    if (urnExtraction) {
      sqlUpdate.setParameter("a_urn", toJsonString(urn));
    }

    // newValue is null if aspect is to be soft-deleted.
    if (newValue == null) {
      return sqlUpdate.setParameter("metadata", DELETED_VALUE).execute();
    }

    AuditedAspect auditedAspect = new AuditedAspect()
        .setAspect(RecordUtils.toJsonString(newValue))
        .setCanonicalName(aspectClass.getCanonicalName())
        .setLastmodifiedby(actor)
        .setLastmodifiedon(new Timestamp(timestamp).toString())
        .setCreatedfor(impersonator, SetMode.IGNORE_NULL);
    if (ingestionTrackingContext != null) {
      auditedAspect.setEmitTime(ingestionTrackingContext.getEmitTime(), SetMode.IGNORE_NULL);
      auditedAspect.setEmitter(ingestionTrackingContext.getEmitter(), SetMode.IGNORE_NULL);
    }

      final String metadata = toJsonString(auditedAspect);
      return sqlUpdate.setParameter("metadata", metadata).execute();
  }

  /**
   * Create aspect from entity table.
   * By this point the callbacks are processed, and the aspect value is validated and ready to be written to database.
   * Race condition on insert is handled at the query level. If the URN already exists, the insert will fail with
   * Duplicate Key exception.
   * All the aspects are inserted in a single query. If the query fails, none of the aspects will be inserted.
   *
   * @param urn                      entity urn
   * @param aspectValues             list of aspect value in {@link RecordTemplate}
   * @param aspectCreateLambdas      class of the aspect
   * @param auditStamp               audit timestamp
   * @param ingestionTrackingContext the ingestionTrackingContext of the MCE responsible for this update
   * @param isTestMode               whether the test mode is enabled or not
   * @return number of rows inserted or updated
   */
  @Override
  public <ASPECT_UNION extends RecordTemplate> int create(
      @Nonnull URN urn,
      @Nonnull List<? extends RecordTemplate> aspectValues,
      @Nonnull List<BaseLocalDAO.AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas,
      @Nonnull AuditStamp auditStamp,
      @Nullable IngestionTrackingContext ingestionTrackingContext,
      boolean isTestMode) {

    aspectValues.forEach(aspectValue -> {
      if (aspectValue == null) {
        throw new IllegalArgumentException("Aspect value cannot be null");
      }
    });

    final long timestamp = auditStamp.hasTime() ? auditStamp.getTime() : System.currentTimeMillis();
    final String actor = auditStamp.hasActor() ? auditStamp.getActor().toString() : DEFAULT_ACTOR;
    final String impersonator = auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null;
    final boolean urnExtraction = _urnPathExtractor != null && !(_urnPathExtractor instanceof EmptyPathExtractor);

    final SqlUpdate sqlUpdate;

    List<String> classNames = aspectCreateLambdas.stream()
        .map(aspectCreateLamdba -> aspectCreateLamdba.getAspectClass().getCanonicalName())
        .collect(Collectors.toList());

    // Create insert statement with variable number of aspect columns
    // For example: INSERT INTO <table_name> (<columns>)
    StringBuilder insertIntoSql = new StringBuilder(SQL_INSERT_INTO_ASPECT_WITH_URN);

    // Create part of insert statement with variable number of aspect values
    // For example: VALUES (<values>);
    StringBuilder insertSqlValues = new StringBuilder(SQL_INSERT_ASPECT_VALUES_WITH_URN);

    for (int i = 0; i < classNames.size(); i++) {
      insertIntoSql.append(getAspectColumnName(urn.getEntityType(), classNames.get(i)));
      // Add parameterization for aspect values
      insertSqlValues.append(":aspect").append(i);
      // Add comma if not the last column
      if (i != classNames.size() - 1) {
        insertIntoSql.append(", ");
        insertSqlValues.append(", ");
      }
    }
    insertIntoSql.append(CLOSING_BRACKET);
    insertSqlValues.append(CLOSING_BRACKET_WITH_SEMICOLON);

    // Build the final insert statement
    // For example: INSERT INTO <table_name> (<columns>) VALUES (<values>);
    String insertStatement = insertIntoSql.toString() + insertSqlValues.toString();
    insertStatement = String.format(insertStatement, getTableName(urn));

    sqlUpdate = _server.createSqlUpdate(insertStatement);

    // Set parameters for each aspect value
    for (int i = 0; i < aspectValues.size(); i++) {
      AuditedAspect auditedAspect = new AuditedAspect()
          .setAspect(RecordUtils.toJsonString(aspectValues.get(i)))
          .setCanonicalName(aspectCreateLambdas.get(i).getAspectClass().getCanonicalName())
          .setLastmodifiedby(actor)
          .setLastmodifiedon(new Timestamp(timestamp).toString())
          .setCreatedfor(impersonator, SetMode.IGNORE_NULL);
      if (ingestionTrackingContext != null) {
        auditedAspect.setEmitTime(ingestionTrackingContext.getEmitTime(), SetMode.IGNORE_NULL);
        auditedAspect.setEmitter(ingestionTrackingContext.getEmitter(), SetMode.IGNORE_NULL);
      }
      sqlUpdate.setParameter("aspect" + i, toJsonString(auditedAspect));
    }

    // If a non-default UrnPathExtractor is provided, the user MUST specify in their schema generation scripts
    // 'ALTER TABLE <table> ADD COLUMN a_urn JSON'.
    if (urnExtraction) {
      sqlUpdate.setParameter("a_urn", toJsonString(urn));
    }
    sqlUpdate.setParameter("urn", urn.toString())
        .setParameter("lastmodifiedon", new Timestamp(timestamp).toString())
        .setParameter("lastmodifiedby", actor);

    return sqlUpdate.execute();
  }

  /**
   * Construct and execute a SQL statement as follows.
   * SELECT urn, aspect1, lastmodifiedon, lastmodifiedby FROM metadata_entity_foo WHERE urn = 'urn:1' AND JSON_EXTRACT(aspect1, '$.gma_deleted') IS NULL
   * UNION ALL
   * SELECT urn, aspect2, lastmodifiedon, lastmodifiedby FROM metadata_entity_foo WHERE urn = 'urn:1' AND JSON_EXTRACT(aspect2, '$.gma_deleted') IS NULL
   * UNION ALL
   * SELECT urn, aspect1, lastmodifiedon, lastmodifiedby FROM metadata_entity_foo WHERE urn = 'urn:2' AND JSON_EXTRACT(aspect1, '$.gma_deleted') IS NULL
   * @param aspectKeys a List of keys (urn, aspect pairings) to query for
   * @param keysCount number of keys to query
   * @param position position of the key to start from
   * @param includeSoftDeleted whether to include soft deleted aspect in the query
   * @param isTestMode whether the operation is in test mode or not
   */
  @Override
  public <ASPECT extends RecordTemplate> List<EbeanMetadataAspect> batchGetUnion(
      @Nonnull List<AspectKey<URN, ? extends RecordTemplate>> aspectKeys, int keysCount, int position,
      boolean includeSoftDeleted, boolean isTestMode) {

    final int end = Math.min(aspectKeys.size(), position + keysCount);
    final Map<Class<ASPECT>, Set<Urn>> keysToQueryMap = new HashMap<>();
    for (int index = position; index < end; index++) {
      final Urn entityUrn = aspectKeys.get(index).getUrn();
      final Class<ASPECT> aspectClass = (Class<ASPECT>) aspectKeys.get(index).getAspectClass();
      if (checkColumnExists(isTestMode ? getTestTableName(entityUrn) : getTableName(entityUrn),
          getAspectColumnName(entityUrn.getEntityType(), aspectClass))) {
        keysToQueryMap.computeIfAbsent(aspectClass, unused -> new HashSet<>()).add(entityUrn);
      }
    }

    // each statement is for a single aspect class
    Map<String, Class<ASPECT>> selectStatements = keysToQueryMap.entrySet()
        .stream()
        .collect(Collectors.toMap(
            entry -> SQLStatementUtils.createAspectReadSql(entry.getKey(), entry.getValue(), includeSoftDeleted,
                isTestMode), entry -> entry.getKey()));

    // consolidate/join the results
    final Map<SqlRow, Class<ASPECT>> sqlRows = new LinkedHashMap<>();
    for (Map.Entry<String, Class<ASPECT>> entry : selectStatements.entrySet()) {
      for (SqlRow sqlRow : _server.createSqlQuery(entry.getKey()).findList()) {
        sqlRows.put(sqlRow, entry.getValue());
      }
    }
    return EBeanDAOUtils.readSqlRows(sqlRows);
  }

  @Override
  public List<URN> listUrns(@Nullable IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      @Nullable URN lastUrn, int pageSize) {
    SqlQuery sqlQuery = createFilterSqlQuery(indexFilter, indexSortCriterion, lastUrn, pageSize);
    final List<SqlRow> sqlRows = sqlQuery.setFirstRow(0).findList();
    return sqlRows.stream().map(sqlRow -> getUrn(sqlRow.getString("urn"), _urnClass)).collect(Collectors.toList());
  }

  @Override
  public ListResult<URN> listUrns(@Nullable IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      int start, int pageSize) {
    final SqlQuery sqlQuery = createFilterSqlQuery(indexFilter, indexSortCriterion, start, pageSize);
    final List<SqlRow> sqlRows = sqlQuery.findList();
    if (sqlRows.size() == 0) {
      final List<SqlRow> totalCountResults = createFilterSqlQuery(indexFilter, indexSortCriterion, 0, DEFAULT_PAGE_SIZE).findList();
      final int actualTotalCount = totalCountResults.isEmpty() ? 0 : totalCountResults.get(0).getInteger("_total_count");
      return toListResult(actualTotalCount, start, pageSize);
    }
    final List<URN> values = sqlRows.stream().map(sqlRow -> getUrn(sqlRow.getString("urn"), _urnClass)).collect(Collectors.toList());
    return toListResult(values, sqlRows, null, start, pageSize);
  }

  @Override
  public boolean exists(@Nonnull URN urn) {
    final String existSql = SQLStatementUtils.createExistSql(urn);
    final SqlQuery sqlQuery = _server.createSqlQuery(existSql);
    return sqlQuery.findList().size() > 0;
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<URN> listUrns(@Nonnull Class<ASPECT> aspectClass, int start,
      int pageSize) {
    final String browseSql = SQLStatementUtils.createAspectBrowseSql(_entityType, aspectClass, start, pageSize);
    final SqlQuery sqlQuery = _server.createSqlQuery(browseSql);

    final List<SqlRow> sqlRows = sqlQuery.findList();
    if (sqlRows.size() == 0) {
      final List<SqlRow> totalCountResults = _server.createSqlQuery(
          SQLStatementUtils.createAspectBrowseSql(_entityType, aspectClass, 0, DEFAULT_PAGE_SIZE)).findList();
      final int actualTotalCount = totalCountResults.isEmpty() ? 0 : totalCountResults.get(0).getInteger("_total_count");
      return toListResult(actualTotalCount, start, pageSize);
    }
    final List<URN> values = sqlRows.stream()
        .map(sqlRow -> getUrn(sqlRow.getString("urn"), _urnClass))
        .collect(Collectors.toList());
    return toListResult(values, sqlRows, null, start, pageSize);
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, @Nonnull URN urn,
      int start, int pageSize) {
    // start / pageSize will be ignored since there will be at most one record returned from entity table.
    final String listAspectByUrnSql = SQLStatementUtils.createListAspectByUrnSql(aspectClass, urn, false);
    final SqlQuery sqlQuery = _server.createSqlQuery(listAspectByUrnSql);

    try {
      final SqlRow sqlRow = sqlQuery.findOne();
      if (sqlRow == null) {
        return toListResult(0, start, pageSize);
      } else {
        sqlRow.set("_total_count", 1);
        final ASPECT aspect = RecordUtils.toRecordTemplate(aspectClass,
            extractAspectJsonString(sqlRow.getString(getAspectColumnName(urn.getEntityType(), aspectClass))));
        final ListResultMetadata listResultMetadata = new ListResultMetadata().setExtraInfos(new ExtraInfoArray());
        final ExtraInfo extraInfo = new ExtraInfo().setUrn(urn)
            .setVersion(LATEST_VERSION)
            .setAudit(makeAuditStamp(sqlRow.getTimestamp("lastmodifiedon"), sqlRow.getString("lastmodifiedby"),
                sqlRow.getString("createdfor")));
        listResultMetadata.getExtraInfos().add(extraInfo);
        return toListResult(Collections.singletonList(aspect), Collections.singletonList(sqlRow), listResultMetadata,
            start, pageSize);
      }
    } catch (PersistenceException pe) {
      throw new RuntimeException(
          String.format("Expect at most 1 aspect value per entity per aspect type . Sql: %s", listAspectByUrnSql));
    }
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, int start,
      int pageSize) {


    final String listAspectSql = SQLStatementUtils.createListAspectWithPaginationSql(aspectClass, _entityType, false, start, pageSize);
    final SqlQuery sqlQuery = _server.createSqlQuery(listAspectSql);
    final List<SqlRow> sqlRows = sqlQuery.findList();
    if (sqlRows.isEmpty()) {
      return toListResult(0, start, pageSize);
    }
    final ListResultMetadata listResultMetadata = new ListResultMetadata().setExtraInfos(new ExtraInfoArray());
    final List<ASPECT> aspectList = sqlRows.stream().map(sqlRow -> {
      final ExtraInfo extraInfo = new ExtraInfo().setUrn(getUrn(sqlRow.getString("urn"), _urnClass))
          .setVersion(LATEST_VERSION).setAudit(
              makeAuditStamp(sqlRow.getTimestamp("lastmodifiedon"), sqlRow.getString("lastmodifiedby"),
                  sqlRow.getString("createdfor")));
      listResultMetadata.getExtraInfos().add(extraInfo);
      return RecordUtils.toRecordTemplate(aspectClass,
          extractAspectJsonString(sqlRow.getString(getAspectColumnName(_entityType, aspectClass))));
    }).collect(Collectors.toList());
    return toListResult(aspectList, sqlRows, listResultMetadata, start, pageSize);
  }


  @Nonnull
  @Override
  public Map<String, Long> countAggregate(@Nullable IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion) {
    final String tableName = SQLSchemaUtils.getTableName(_entityType);
    final String groupByColumn =
        getGeneratedColumnName(_entityType, indexGroupByCriterion.getAspect(), indexGroupByCriterion.getPath(),
            _nonDollarVirtualColumnsEnabled);
    // first, check for existence of the column we want to GROUP BY
    if (!checkColumnExists(tableName, groupByColumn)) {
      // if we are trying to GROUP BY the results on a column that does not exist, just return an empty map
      return Collections.emptyMap();
    }

    // now run the actual GROUP BY query
    final String groupBySql = SQLStatementUtils.createGroupBySql(_entityType, indexFilter, indexGroupByCriterion, _nonDollarVirtualColumnsEnabled);
    final SqlQuery sqlQuery = _server.createSqlQuery(groupBySql);
    final List<SqlRow> sqlRows = sqlQuery.findList();
    Map<String, Long> resultMap = new HashMap<>();
    for (SqlRow sqlRow : sqlRows) {
      final Long count = sqlRow.getLong("count");
      String value = null;
      for (Map.Entry<String, Object> entry : sqlRow.entrySet()) {
        if (!entry.getKey().equalsIgnoreCase("count")) {
          value = String.valueOf(entry.getValue());
          break;
        }
      }
      resultMap.put(value, count);
    }
    return resultMap;
  }

  /**
   * Produce {@link SqlQuery} for list urn by offset (start) and limit (pageSize).
   * @param indexFilter index filter conditions
   * @param indexSortCriterion sorting criterion, default ACS
   * @return SqlQuery a SQL query which can be executed by ebean server.
   */
  private SqlQuery createFilterSqlQuery(@Nullable IndexFilter indexFilter,
      @Nullable IndexSortCriterion indexSortCriterion, int offset, int pageSize) {
    StringBuilder filterSql = new StringBuilder();
    filterSql.append(SQLStatementUtils.createFilterSql(_entityType, indexFilter, true, _nonDollarVirtualColumnsEnabled));
    filterSql.append("\n");
    filterSql.append(parseSortCriteria(_entityType, indexSortCriterion, _nonDollarVirtualColumnsEnabled));
    filterSql.append(String.format(" LIMIT %d", Math.max(pageSize, 0)));
    filterSql.append(String.format(" OFFSET %d", Math.max(offset, 0)));
    return _server.createSqlQuery(filterSql.toString());
  }

  /**
   * Produce {@link SqlQuery} for list urns by last urn.
   */
  private SqlQuery createFilterSqlQuery(@Nullable IndexFilter indexFilter,
      @Nullable IndexSortCriterion indexSortCriterion, @Nullable URN lastUrn, int pageSize) {
    StringBuilder filterSql = new StringBuilder();
    filterSql.append(SQLStatementUtils.createFilterSql(_entityType, indexFilter, false, _nonDollarVirtualColumnsEnabled));

    if (lastUrn != null) {
      // because createFilterSql will only include a WHERE clause if there are non-urn filters, we need to make sure
      // that we add a WHERE if it wasn't added already.
      String operator = "AND";

      if (indexFilter == null
          || !indexFilter.hasCriteria()
          || indexFilter.getCriteria().stream().allMatch(criteria -> isUrn(criteria.getAspect()))) {
        operator = "WHERE";
      }

      filterSql.append(String.format(" %s URN > '%s'", operator, lastUrn));
    }

    filterSql.append("\n");
    filterSql.append(parseSortCriteria(_entityType, indexSortCriterion, _nonDollarVirtualColumnsEnabled));
    filterSql.append(String.format(" LIMIT %d", Math.max(pageSize, 0)));
    return _server.createSqlQuery(filterSql.toString());
  }

  /**
   * Convert sqlRows into {@link ListResult}. This version of toListResult is used when the original SQL query
   * returned nothing, but that doesn't necessarily mean that _total_count is 0 (thought it still could be). For example:
   *
   * <p>
   * If &lt;start&gt; (e.g. 5) is greater than _total_count (e.g. 2), the SQL query will return an empty list, but _total_count
   * is not 0. If we pass in the empty list into the other toListResult method, we will not be able to get the _total_count
   * value (since that is stored within each SqlRow of that list. We must use a second query (with &lt;start&gt; = 0) to check for
   * the actual _total_count, then pass that into this toListResult method.
   * </p>
   * @param totalCount total count from ebean query execution
   * @param start starting position
   * @param pageSize number of rows in a page
   * @param <T> type of query response
   * @return {@link ListResult} which contains paging metadata information
   */
  @Nonnull
  protected <T> ListResult<T> toListResult(int totalCount, int start, int pageSize) {
    if (pageSize == 0) {
      pageSize = DEFAULT_PAGE_SIZE;
    }
    final int totalPageCount = ceilDiv(totalCount, pageSize);
    boolean hasNext;
    int nextStart;
    if (totalCount - start > 0) {
      hasNext = true;
      nextStart = start;
    } else {
      hasNext = false;
      nextStart = ListResult.INVALID_NEXT_START;
    }
    return ListResult.<T>builder()
        .values(Collections.emptyList())
        .metadata(null)
        .nextStart(nextStart)
        .havingMore(hasNext)
        .totalCount(totalCount)
        .totalPageCount(totalPageCount)
        .pageSize(pageSize)
        .build();
  }

  /**
   * Convert sqlRows into {@link ListResult}.
   * @param values a list of query response result
   * @param sqlRows list of {@link SqlRow} from ebean query execution
   * @param listResultMetadata {@link ListResultMetadata} with {@link com.linkedin.metadata.query.ExtraInfo}
   * @param start starting position
   * @param pageSize number of rows in a page
   * @param <T> type of query response
   * @return {@link ListResult} which contains paging metadata information
   */
  @Nonnull
  protected <T> ListResult<T> toListResult(@Nonnull List<T> values, @Nonnull List<SqlRow> sqlRows, @Nullable ListResultMetadata listResultMetadata,
      int start, int pageSize) {
    if (pageSize == 0) {
      pageSize = DEFAULT_PAGE_SIZE;
    }
    final int totalCount = sqlRows.get(0).getInteger("_total_count");
    final int totalPageCount = ceilDiv(totalCount, pageSize);
    boolean hasNext;
    int nextStart;
    if (sqlRows.size() < totalCount - start) {
      hasNext = true;
      nextStart = sqlRows.size() + start;
    } else if (sqlRows.size() == totalCount - start || totalCount == 0 || totalCount - start < 0) {
      hasNext = false;
      nextStart = ListResult.INVALID_NEXT_START;
    } else {
      throw new RuntimeException(
          String.format("Row count (%d) is more than total count of (%d) starting from offset of (%s)", sqlRows.size(),
              totalCount, start));
    }
    return ListResult.<T>builder()
        .values(values)
        .metadata(listResultMetadata)
        .nextStart(nextStart)
        .havingMore(hasNext)
        .totalCount(totalCount)
        .totalPageCount(totalPageCount)
        .pageSize(pageSize)
        .build();
  }

  /**
   * Given an AuditedAspect object, serialize it into a json string in a format that will be saved in DB.
   * @param auditedAspect AuditedAspect object to be serialized
   * @return A json string that can be saved to DB.
   */
  @Nonnull
  public static String toJsonString(@Nonnull final AuditedAspect auditedAspect) {
    String aspect = auditedAspect.getAspect();
    auditedAspect.setAspect(ASPECT_JSON_PLACEHOLDER);
    return RecordUtils.toJsonString(auditedAspect).replace("\"" + ASPECT_JSON_PLACEHOLDER + "\"",  aspect);
  }

  /**
   * Extract paths from urn into a map using the UrnPathExtractor, and convert this map into a JSON string so that it
   * can be used to index urn paths in the MySQL tables.
   * For example, assuming a FooUrnPathExtractor is implemented in a certain way, "urn:li:foo:(urn:li:bar,baz)" can be converted to
   * "{"/name":"foo", "/field1":"urn:li:bar", "/field1/value":"bar", "/field2":"baz"}".
   * @param urn urn
   * @return JSON string representation of the urn
   */
  @Nonnull
  private String toJsonString(@Nonnull URN urn) {
    final Map<String, Object> pathValueMap = _urnPathExtractor.extractPaths(urn);
    return JSONObject.toJSONString(pathValueMap);
  }

  @Nonnull
  private SchemaEvolutionManager createSchemaEvolutionManager(@Nonnull ServerConfig serverConfig) {
    String identifier = serverConfig.getDataSourceConfig().getCustomProperties() != null
        ? serverConfig.getDataSourceConfig().getCustomProperties().getOrDefault(SERVICE_IDENTIFIER, null)
        : null;
    SchemaEvolutionManager.Config config = new SchemaEvolutionManager.Config(
        serverConfig.getDataSourceConfig().getUrl(),
        serverConfig.getDataSourceConfig().getPassword(),
        serverConfig.getDataSourceConfig().getUsername(),
        identifier);

    return new FlywaySchemaEvolutionManager(config);
  }

  /**
   * Check column exists in table.
   */
  public boolean checkColumnExists(@Nonnull String tableName, @Nonnull String columnName) {
    // Fetch table columns on very first read and cache it in tableColumns
    if (!tableColumns.containsKey(tableName)) {
      final List<SqlRow> rows = _server.createSqlQuery(SQLStatementUtils.getAllColumnForTable(tableName)).findList();
      Set<String> columns = new HashSet<>();
      for (SqlRow row : rows) {
        columns.add(row.getString("COLUMN_NAME").toLowerCase());
      }
      tableColumns.put(tableName, columns);
    }

    return tableColumns.get(tableName).contains(columnName.toLowerCase());
  }

  /**
   * SQL implementation of find the latest {@link EbeanMetadataAspect}.
   * @param connection {@link Connection} get from the current transaction, it should not be closed manually
   * @param urn entity urn
   * @param aspectClass aspect class
   * @param <ASPECT> aspect class type
   * @return {@link EbeanMetadataAspect} of the first record or NULL if there's no such an record.
   */
  private static <ASPECT extends RecordTemplate, URN> EbeanMetadataAspect findLatestMetadataAspect(
      @Nonnull Connection connection, @Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {
    final String aspectName = ModelUtils.getAspectName(aspectClass);
    PreparedStatement preparedStatement;
    try {
      preparedStatement = connection.prepareStatement(FIND_LATEST_SQL_TEMPLATE);
      preparedStatement.setString(1, urn.toString());
      preparedStatement.setString(2, aspectName);
      preparedStatement.setInt(3, 0); // version = 0
      ResultSet resultSet = preparedStatement.executeQuery();
      if (resultSet.next()) {
        EbeanMetadataAspect ebeanMetadataAspect = new EbeanMetadataAspect();
        ebeanMetadataAspect.setKey(new EbeanMetadataAspect.PrimaryKey(urn.toString(), aspectName, 0L));
        ebeanMetadataAspect.setMetadata(resultSet.getString("metadata"));
        ebeanMetadataAspect.setCreatedFor(resultSet.getString("createdFor"));
        ebeanMetadataAspect.setCreatedBy(resultSet.getString("createdBy"));
        ebeanMetadataAspect.setCreatedOn(resultSet.getTimestamp("createdOn"));
        return ebeanMetadataAspect;
      } else {
        // return null if there is no such a record in the Database
        return null;
      }
    } catch (SQLException throwables) {
      // throw exception when SQL execution failed.
      log.error("SQL execution failure on urn: {}, error message: {}", urn.toString(), throwables.getMessage());
      throw new RuntimeException(throwables);
    }
  }

  /**
   * SQL template to get the latest record from the metadata_aspect table, order by createdOn with descending order.
   */
  private static final String FIND_LATEST_SQL_TEMPLATE =
      "select * from metadata_aspect t0"
          + " where t0.urn = ? and t0.aspect = ?  and t0.version = ? order by t0.createdOn desc limit 1";

  /**
   * SQL implementation of find the latest {@link EbeanMetadataAspect}.
   * @param urn entity urn
   * @param aspectClass aspect class
   * @param <ASPECT> aspect class type
   * @return {@link EbeanMetadataAspect} of the first record or NULL if there's no such an record.
   */
  static <ASPECT extends RecordTemplate, URN> EbeanMetadataAspect findLatestMetadataAspect(
      @Nonnull EbeanServer ebeanServer, @Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {
    if (ebeanServer.currentTransaction() == null) {
      log.warn("cannot get transaction from the query context: {}", urn.toString());
      // no context transaction, create a new one and recycle after. Should be used in unit test only
      try (Transaction newTransaction = ebeanServer.beginTransaction()) {
        if (newTransaction.getConnection() == null) {
          log.warn("cannot get connection from transaction: {}", urn.toString());
          // if there is no connection on the transaction, rollback to the ebean implementation. Backward compatible
          // for EbeanLocalDAOTest.testAddFailedAfterRetry)
          return ebeanServer.find(EbeanMetadataAspect.class,
              new EbeanMetadataAspect.PrimaryKey(urn.toString(), ModelUtils.getAspectName(aspectClass), 0));
        } else {
          EbeanMetadataAspect ebeanMetadataAspect =
              findLatestMetadataAspect(newTransaction.getConnection(), urn, aspectClass);
          newTransaction.commit();
          return ebeanMetadataAspect;
        }
      } finally {
      }
    } else {
      // has context transaction, get the connection object and execute the query.
      return findLatestMetadataAspect(ebeanServer.currentTransaction().getConnection(), urn, aspectClass);
    }
  }
}
