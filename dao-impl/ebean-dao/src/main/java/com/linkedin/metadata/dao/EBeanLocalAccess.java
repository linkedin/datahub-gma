package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AuditedAspect;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dao.utils.SQLSchemaUtils;
import com.linkedin.metadata.dao.utils.SQLStatementUtils;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import io.ebean.EbeanServer;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import io.ebean.SqlUpdate;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static com.linkedin.metadata.dao.utils.EBeanDAOUtils.*;


/**
 * EBeanLocalAccess provides model agnostic data access (read / write) to MySQL database.
 */
public class EBeanLocalAccess<URN extends Urn> implements IEBeanLocalAccess<URN> {
  private final EbeanServer _server;

  // a cache of (column_name, aspect_name)
  private static final ConcurrentHashMap<String, String> COLUMN_ASPECT_MAP = new ConcurrentHashMap();
  private static final JSONParser JSON_PARSER = new JSONParser();
  protected final Class<URN> _urnClass;
  protected final String _entityType;

  // TODO confirm if the default page size is 1000 in other code context.
  private static final int DEFAULT_PAGE_SIZE = 1000;
  private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
  private static final long LATEST_VERSION = 1L;

  public EBeanLocalAccess(EbeanServer server, @Nonnull Class<URN> urnClass) {
    _server = server;
    _urnClass = urnClass;
    _entityType = getEntityType(_urnClass);
  }

  @Override
  public <ASPECT extends RecordTemplate> int add(@Nonnull URN urn, @Nullable ASPECT newValue, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull AuditStamp auditStamp) {

    long timestamp = auditStamp.hasTime() ? auditStamp.getTime() : System.currentTimeMillis();
    LocalDateTime localDateTime = LocalDateTime.from(Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()));
    String actor = auditStamp.hasActor() ? auditStamp.getActor().toString() : DEFAULT_ACTOR;

    AuditedAspect auditedAspect = new AuditedAspect()
        .setAspect(RecordUtils.toJsonString(newValue))
        .setLastmodifiedby(actor)
        .setLastmodifiedon(localDateTime.toString());

    final SqlUpdate sqlUpdate = _server.createSqlUpdate(SQLStatementUtils.createAspectUpsertSql(urn, aspectClass))
        .setParameter("urn", urn.toString())
        .setParameter("lastmodifiedon", localDateTime.toString())
        .setParameter("lastmodifiedby", actor)
        .setParameter("metadata", toJsonString(auditedAspect));

    return sqlUpdate.execute();
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> List<EbeanMetadataAspect> batchGetOr(
      @Nonnull List<AspectKey<URN, ? extends RecordTemplate>> aspectKeys, int keysCount, int position) {
    // batchGetOr is discouraged due to its performance disadvantage comparing to batchGetUnion approach
    // TODO one optimization could be done is to group queries for the same entity type, to reduce union query statements,
    // TODO but it should be done on batchGetUnion() method.
    return batchGetUnion(aspectKeys, keysCount, position);
  }

  @Override
  public <ASPECT extends RecordTemplate> List<EbeanMetadataAspect> batchGetUnion(
      @Nonnull List<AspectKey<URN, ? extends RecordTemplate>> aspectKeys, int keysCount, int position) {
    /*
    TODO: change statement building logic in the case of getting multiple aspects with multiple urns.
    current:
      select aspect1 from table where urn=urn1
      union
      select aspect2 from table where urn=urn1
      union
      select aspect1 from table where urn=urn2
      union
      select aspect2 from table where urn=urn2
    should be:
      select aspect1, aspect2 from table where urn=urn1
      union
      select aspect1, aspect2 from table where urn=urn2
     */
    final StringBuilder sqlBuilder = new StringBuilder();
    final int end = Math.min(aspectKeys.size(), position + keysCount);
    for (int index = position; index < end; index++) {
      final Urn entityUrn = aspectKeys.get(index).getUrn();
      final Class<? extends RecordTemplate> aspectClass = aspectKeys.get(index).getAspectClass();
      final String columnName = SQLSchemaUtils.getColumnName(aspectClass);
      COLUMN_ASPECT_MAP.putIfAbsent(columnName, aspectClass.getCanonicalName());
      sqlBuilder.append(SQLStatementUtils.createAspectReadSql(entityUrn, aspectClass));
      if (index != end - 1) {
        sqlBuilder.append(" UNION ALL ");
      } else {
        sqlBuilder.append(";");
      }
    }
    SqlQuery sqlQuery = _server.createSqlQuery(sqlBuilder.toString());
    List<SqlRow> sqlRows = sqlQuery.findList();
    return readSqlRows(sqlRows);
  }

  @Override
  public List<URN> listUrns(@Nonnull IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      @Nullable URN lastUrn, int pageSize) {
    SqlQuery sqlQuery = createFilterSqlQuery(indexFilter, indexSortCriterion, lastUrn);
    final List<SqlRow> sqlRows = sqlQuery.setFirstRow(0).setMaxRows(pageSize).findList();
    return sqlRows.stream().map(sqlRow -> getUrn(sqlRow.getString("urn"), _urnClass)).collect(Collectors.toList());
  }

  @Override
  public ListResult<URN> listUrns(@Nonnull IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      int start, int pageSize) {
    SqlQuery sqlQuery = createFilterSqlQuery(indexFilter, indexSortCriterion, null);

    final List<SqlRow> sqlRows = sqlQuery.setFirstRow(start).setMaxRows(pageSize).findList();
    final List<URN> values =
        sqlRows.stream().map(sqlRow -> getUrn(sqlRow.getString("urn"), _urnClass)).collect(Collectors.toList());
    return toListResult(values, sqlRows, start, pageSize);
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
    final String browseSql = SQLStatementUtils.createAspectBrowseSql(_entityType, aspectClass);
    final SqlQuery sqlQuery = _server.createSqlQuery(browseSql);

    final List<SqlRow> sqlRows = sqlQuery.setFirstRow(start).setMaxRows(pageSize).findList();
    final List<URN> values =
        sqlRows.stream().map(sqlRow -> getUrn(sqlRow.getString("urn"), _urnClass)).collect(Collectors.toList());
    return toListResult(values, sqlRows, start, pageSize);
  }

  @Nonnull
  @Override
  public Map<String, Long> countAggregate(@Nonnull IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion) {
    final String tableName = SQLSchemaUtils.getTableName(_entityType);
    final String groupBySql = SQLStatementUtils.createGroupBySql(tableName, indexFilter, indexGroupByCriterion);
    final SqlQuery sqlQuery = _server.createSqlQuery(groupBySql);
    final List<SqlRow> sqlRows = sqlQuery.findList();
    Map<String, Long> resultMap = new HashMap<>();
    for (SqlRow sqlRow : sqlRows) {
      Long count = sqlRow.getLong("count");
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
   * Produce {@link SqlQuery} for list urn by offset (start) and by lastUrn.
   * @param indexFilter index filter conditions
   * @param indexSortCriterion sorting criterion, default ACS
   * @param lastUrn
   * @param <URN>
   * @return
   */
  private <URN> SqlQuery createFilterSqlQuery(@Nonnull IndexFilter indexFilter,
      @Nullable IndexSortCriterion indexSortCriterion, @Nullable URN lastUrn) {
    if (indexFilter.hasCriteria() && indexFilter.getCriteria().isEmpty()) {
      throw new UnsupportedOperationException("Empty Index Filter is not supported by EbeanLocalDAO");
    }

    final String tableName = SQLSchemaUtils.getTableName(_entityType);
    String filterSql = SQLStatementUtils.createFilterSql(tableName, indexFilter, indexSortCriterion);

    // append last urn where condition
    if (lastUrn != null) {
      filterSql = filterSql + " WHERE urn > '" + lastUrn.toString() + "'";
    }
    return _server.createSqlQuery(filterSql);
  }

  /**
   * Read {@link SqlRow} list into a {@link EbeanMetadataAspect} list.
   * @param sqlRows list of {@link SqlRow}
   * @return list of {@link EbeanMetadataAspect}
   */
  private List<EbeanMetadataAspect> readSqlRows(List<SqlRow> sqlRows) {
    return sqlRows.stream().map(sqlRow -> {
      // TODO we expect only one aspect per SqlRow
      final String columnName =
          sqlRow.keySet().stream().filter(key -> key.startsWith(SQLSchemaUtils.ASPECT_PREFIX)).findFirst().get();
      final String aspectName = COLUMN_ASPECT_MAP.get(columnName);
      EbeanMetadataAspect ebeanMetadataAspect = new EbeanMetadataAspect();
      String urn = sqlRow.getString("urn");
      EbeanMetadataAspect.PrimaryKey primaryKey = new EbeanMetadataAspect.PrimaryKey(urn, aspectName, LATEST_VERSION);
      AuditedAspect auditedAspect = RecordUtils.toRecordTemplate(AuditedAspect.class, sqlRow.getString(columnName));
      ebeanMetadataAspect.setKey(primaryKey);
      ebeanMetadataAspect.setCreatedBy(auditedAspect.getLastmodifiedby());
      ebeanMetadataAspect.setCreatedOn(Timestamp.valueOf(LocalDateTime.parse(auditedAspect.getLastmodifiedon())));
      ebeanMetadataAspect.setMetadata(extractAspectJsonString(sqlRow.getString(columnName)));
      return ebeanMetadataAspect;
    }).collect(Collectors.toList());
  }

  /**
   * Convert sqlRows into {@link ListResult}.
   * @param values a list of query response result
   * @param sqlRows list of {@link SqlRow} from ebean query execution
   * @param start starting position
   * @param pageSize number of rows in a page
   * @param <T> type of query response
   * @return {@link ListResult} which contains paging metadata information
   */
  @Nonnull
  protected <T> ListResult<T> toListResult(@Nonnull List<T> values, @Nonnull List<SqlRow> sqlRows,
      @Nullable Integer start, @Nullable Integer pageSize) {

    boolean hasNext = false;
    int nextStart = -1;
    int totalPageCount = 0;
    int totalCount = 0;
    if (pageSize == null) {
      pageSize = DEFAULT_PAGE_SIZE;
    }
    if (sqlRows.isEmpty()) {
      // TODO, sqlRows could be empty but totalCount is not necessarily 0.  It requires a secondary query to get the actual total count
      hasNext = false;
      return ListResult.<T>builder()
          // Format
          .values(Collections.EMPTY_LIST)
          .metadata(null)
          .nextStart(nextStart)
          .havingMore(hasNext)
          .totalCount(totalCount)
          .totalPageCount(totalPageCount)
          .pageSize(pageSize)
          .build();
    }

    totalCount = sqlRows.get(0).getInteger("_total_count");
    totalPageCount = ceilDiv(totalCount, pageSize);
    if (start == null) {
      if (sqlRows.size() < totalCount) {
        hasNext = true;
        nextStart = sqlRows.size();
      } else if (sqlRows.size() == totalCount) {
        hasNext = false;
        nextStart = -1;
      } else {
        throw new RuntimeException(
            String.format("Row count (%d) is more than total count of (%d) started from (%s)", sqlRows.size(),
                totalCount));
      }
    } else {
      if (sqlRows.size() < totalCount - start) {
        hasNext = true;
        nextStart = sqlRows.size() + start;
      } else if (sqlRows.size() == totalCount - start) {
        hasNext = false;
        nextStart = -1;
      } else {
        throw new RuntimeException(
            String.format("Row count (%d) is more than total count of (%d) started from (%s)", sqlRows.size(),
                totalCount, start));
      }
    }
    return ListResult.<T>builder()
        // Format
        .values(values)
        .metadata(null)
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
    auditedAspect.setAspect("PLACEHOLDER");
    return RecordUtils.toJsonString(auditedAspect).replace("\"PLACEHOLDER\"",  aspect);
  }

  /**
   * Extract aspect json string from an AuditedAspect string in its DB format.
   * @param auditedAspect an AuditedAspect string in its DB format
   * @return A string which can be deserialized into Aspect object.
   */
  @Nonnull
  public static String extractAspectJsonString(@Nonnull final String auditedAspect) {
    try {
      JSONObject map = (JSONObject) JSON_PARSER.parse(auditedAspect);
      return map.get("aspect").toString();
    } catch (ParseException e) {
      throw new RuntimeException(String.format("Failed to parse string %s,", auditedAspect));
    }
  }
}
