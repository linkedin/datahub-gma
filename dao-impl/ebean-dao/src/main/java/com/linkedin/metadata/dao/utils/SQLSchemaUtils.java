package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectColumnMetadata;
import com.linkedin.metadata.dao.GlobalAssetRegistry;
import com.linkedin.metadata.dao.exception.MissingAnnotationException;
import com.linkedin.metadata.dao.exception.ModelValidationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;


/**
 * Generate schema related SQL script, such as normalized table / column names ..etc
 */
@Slf4j
public class SQLSchemaUtils {
  private static final String GMA = "gma";
  public static final String ENTITY_TABLE_PREFIX = "metadata_entity_";
  public static final String RELATIONSHIP_TABLE_PREFIX = "metadata_relationship_";
  public static final String TEST_TABLE_SUFFIX = "_test";
  public static final String ASPECT_PREFIX = "a_";
  public static final String INDEX_PREFIX = "i_";

  private static final int MYSQL_MAX_COLUMN_NAME_LENGTH = 64 - ASPECT_PREFIX.length();

  /**
   * This field is used when asset field in {@link com.linkedin.metadata.query.AspectField} is not provided in the
   * legacy implementation. When this is field is set, the getColumnNameFromAnnotation() will retrieve
   * "column" from the "column" annotation from the Aspect. However, the going forward way is to retrieve "column"
   * information from aspect alias defined in the asset.
   *
   * <p>For more context, see: Decision - Using Proto Field Name as Aspect URI
   * go/mg/aspect-alias-decision
   */
  protected static final String UNKNOWN_ASSET = "UNKNOWN_ASSET";

  private SQLSchemaUtils() {
  }

  /**
   * Get MySQL table name from entity urn, e.g. urn:li:dataset to metadata_entity_dataset.
   * @param urn {@link Urn} of the entity
   * @return entity table name
   */
  @Nonnull
  public static String getTableName(@Nonnull Urn urn) {
    return getTableName(urn.getEntityType());
  }

  /**
   * Get MySQL test table name from entity urn, e.g. urn:li:dataset to metadata_entity_dataset_test.
   * @param urn {@link Urn} of the entity
   * @return entity table name
   */
  @Nonnull
  public static String getTestTableName(@Nonnull Urn urn) {
    return getTableName(urn) + TEST_TABLE_SUFFIX;
  }

  /**
   * Get MySQL table name from entity type string.
   * @param entityType entity type as string, such as "dataset", "chart" ..etc
   * @return entity table name
   */
  @Nonnull
  public static String getTableName(@Nonnull String entityType) {
    return ENTITY_TABLE_PREFIX + entityType.toLowerCase();
  }

  /**
   * Get MySQL test table name from entity type string.
   * @param entityType entity type as string, such as "dataset", "chart" ..etc
   * @return entity table name
   */
  @Nonnull
  public static String getTestTableName(@Nonnull String entityType) {
    return getTableName(entityType) + TEST_TABLE_SUFFIX;
  }

  /**
   * Derive the local relationship table name from RELATIONSHIP record.
   * @param relationship The RELATIONSHIP record.
   * @return Local relationship table name as a string.
   */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> String getRelationshipTableName(@Nonnull final RELATIONSHIP relationship) {
    return RELATIONSHIP_TABLE_PREFIX + relationship.getClass().getSimpleName().toLowerCase();
  }

  /**
   * Derive the local relationship table name from RELATIONSHIP record class.
   * @param relationship The RELATIONSHIP record.
   * @return Local relationship table name as a string.
   */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> String getRelationshipTableName(@Nonnull final Class<RELATIONSHIP> relationship) {
    return RELATIONSHIP_TABLE_PREFIX + relationship.getSimpleName().toLowerCase();
  }

  /**
   * Derive the test local relationship table name from RELATIONSHIP record.
   * @param relationship The RELATIONSHIP record.
   * @return Local relationship table name as a string.
   */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> String getTestRelationshipTableName(
      @Nonnull final RELATIONSHIP relationship) {
    return getRelationshipTableName(relationship) + TEST_TABLE_SUFFIX;
  }

  /**
   * Derive the test local relationship table name from RELATIONSHIP record class.
   * @param relationship The RELATIONSHIP record.
   * @return Local relationship table name as a string.
   */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> String getTestRelationshipTableName(
      @Nonnull final Class<RELATIONSHIP> relationship) {
    return getRelationshipTableName(relationship) + TEST_TABLE_SUFFIX;
  }

  /**
   * Get column name from aspect class canonical name.
   */
  @Nonnull
  public static String getAspectColumnName(@Nonnull final String entityType, @Nonnull final String aspectCanonicalName) {
    return ASPECT_PREFIX + getColumnName(entityType, aspectCanonicalName);
  }

  /**
   * DEPRECATED, use getAspectColumnName(String entityType, String aspectCanonicalName) instead.
   */
  @Deprecated
  @Nonnull
  public static String getAspectColumnName(@Nonnull final String aspectCanonicalName) {
    return ASPECT_PREFIX + getColumnNameFromAnnotation(aspectCanonicalName);
  }

  /**
   * Get column name from aspect class.
   * @param aspectClass aspect class
   * @param <ASPECT> aspect that extends {@link RecordTemplate}
   * @return aspect column name
   */
  public static <ASPECT extends RecordTemplate> String getAspectColumnName(@Nonnull final String entityType,
      @Nonnull Class<ASPECT> aspectClass) {
    return getAspectColumnName(entityType, aspectClass.getCanonicalName());
  }

  /**
   * Get generated column name from aspect and path.
   */
  @Nonnull
  public static String getGeneratedColumnName(@Nonnull String assetType, @Nonnull String aspect, @Nonnull String path,
      boolean nonDollarVirtualColumnsEnabled) {
    char delimiter = nonDollarVirtualColumnsEnabled ? '0' : '$';
    if (isUrn(aspect)) {
      return INDEX_PREFIX + "urn" + processPath(path, delimiter);
    }
    if (UNKNOWN_ASSET.equals(assetType)) {
      log.warn("query with unknown asset type. aspect =  {}, path ={}, delimiter = {}", aspect, path, delimiter);
    }
    return INDEX_PREFIX + getColumnName(assetType, aspect) + processPath(path, delimiter);
  }

  /**
   * DEPRECATED, use getGeneratedColumnName(assetType, aspect, path, nonDollarVirtualColumnsEnabled) instead.
   */
  @Deprecated
  @Nonnull
  public static String getGeneratedColumnName(@Nonnull String aspect, @Nonnull String path,
      boolean nonDollarVirtualColumnsEnabled) {
    char delimiter = nonDollarVirtualColumnsEnabled ? '0' : '$';
    if (isUrn(aspect)) {
      return INDEX_PREFIX + "urn" + processPath(path, delimiter);
    }
    return INDEX_PREFIX + getColumnNameFromAnnotation(aspect) + processPath(path, delimiter);
  }

  /**
   * Check the given class name is for urn class.
   */
  public static boolean isUrn(@Nonnull String className) {
    return Urn.class.isAssignableFrom(ClassUtils.loadClass(className));
  }

  /**
   * process 'path' into mysql column name convention.
   * @param path path in string e.g. /name/value, /name
   * @param delimiter delimiter i.e '$' or '0'
   * @return $name$value or $name or 0name$value or 0name
   */
  @Nonnull
  public static String processPath(@Nonnull String path, char delimiter) {
    path = path.replace("/", String.valueOf(delimiter));
    if (!path.startsWith(String.valueOf(delimiter))) {
      path = delimiter + path;
    }
    return path;
  }

  /**
   * Get Column name from aspect canonical name.
   *
   * @param assetType entity type from Urn definition.
   * @param aspectCanonicalName aspect name in canonical form.
   * @return aspect column name
   */
  @Nonnull
  public static String getColumnName(@Nonnull final String assetType,
      @Nonnull final String aspectCanonicalName) {

    Class<? extends RecordTemplate> assetClass = GlobalAssetRegistry.get(assetType);
    if (assetClass == null) {
      log.warn("loading column name from legacy 'column' annotation. asset: {}, aspect: {}", assetType,
          aspectCanonicalName);
      return getColumnNameFromAnnotation(aspectCanonicalName);
    } else {
      String aspectAlias = ModelUtils.getAspectAlias(assetClass, aspectCanonicalName);
      if (aspectAlias == null) {
        throw new ModelValidationException(
            "failed to get aspect alias for: " + aspectCanonicalName + " from asset: " + assetClass);
      } else {
        return aspectAlias;
      }
    }
  }

  /**
   * Get Column name from aspect column annotation (legacy).
   *
   * @param aspectCanonicalName aspect name in canonical form.
   * @return aspect column name
   */
  @Nonnull
  private static String getColumnNameFromAnnotation(@Nonnull final String aspectCanonicalName) {
    // load column from Aspect annotation (legacy way)
    try {
      final RecordDataSchema schema =
          (RecordDataSchema) DataTemplateUtil.getSchema(ClassUtils.loadClass(aspectCanonicalName));
      final Map<String, Object> properties = schema.getProperties();
      final Object gmaObj = properties.get(GMA);
      final AspectColumnMetadata gmaAnnotation = DataTemplateUtil.wrap(gmaObj, AspectColumnMetadata.class);
      return gmaAnnotation.getAspect().getColumn().getName();
    } catch (Exception e) {
      throw new MissingAnnotationException(
          String.format("Aspect %s should be annotated with @gma.aspect.column.name.", aspectCanonicalName), e);
    }
  }

  /**
   * Given a sql script that has one or more SQL statements, detect all potentially high-risk SQL statements.
   * @param sqlScript A string contains one or more SQL statements.
   * @return A list of potentially high-risk SQL statements
   * @throws JSQLParserException if the sql script cannot be properly parsed.
   */
  public static List<String> detectPotentialHighRiskSQL(String sqlScript) throws JSQLParserException {

    // Split SQL into individual statements
    String[] statements = sqlScript.split(";\\s*"); // Splits on semicolons.
    List<String> potentialHighRiskSQL = new ArrayList<>();

    for (String statement : statements) {
      Statement sql = CCJSqlParserUtil.parse(statement);

      if (sql instanceof CreateTable) {
        log.info("Create table is generally low risk. SQL {}", statement);
      } else if (sql instanceof Alter) {
        Alter alterStatement = (Alter) sql;
        List<AlterExpression> expressions = alterStatement.getAlterExpressions();

        // Check if the statement is trying to alter table ... add index ...
        // Note that alter table ... add column ... is considered low-risk though.
        for (AlterExpression expression : expressions) {
          if (expression.getIndex() != null) {
            log.info("Add index to table is potentially high risk. SQL {}", statement);
            potentialHighRiskSQL.add(statement);
            break;
          }
        }

      } else if (sql instanceof CreateIndex) {
        log.info("Create index is potentially high risk. SQL {}", statement);
        potentialHighRiskSQL.add(statement);
      } else {
        // catch all bucket. Any SQL that is not create table, create index, alter table is considered high-risk.
        log.info("SQL is potentially high-risk {}", statement);
        potentialHighRiskSQL.add(statement);
      }
    }

    return potentialHighRiskSQL;
  }

  /**
   * For a SQL statement, extract the table name used in the SQL.
   * @param sql such as "alter table foo add index bar (abc);"
   * @return The table name such as "foo"
   * @throws JSQLParserException if the SQL statement is not one of the supported type.
   */
  public static String extractTableName(String sql) throws JSQLParserException {
    Statement statement = CCJSqlParserUtil.parse(sql);

    if (statement instanceof CreateTable) {

      CreateTable createTable = (CreateTable) statement;
      return createTable.getTable().getName();

    } else if (statement instanceof Alter) {

      Alter alterStatement = (Alter) statement;
      return alterStatement.getTable().getName();

    } else if (statement instanceof CreateIndex) {
      CreateIndex createIndex = (CreateIndex) statement;
      return createIndex.getTable().getName();

    } else if (statement instanceof Drop) {
      Drop dropStatement = (Drop) statement;
      return  dropStatement.getName().getName();
    }

    throw new JSQLParserException("Unsupported statement type: " + statement.getClass().getName());
  }
}
