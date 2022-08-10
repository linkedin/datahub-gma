package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectColumnMetadata;
import com.linkedin.metadata.dao.exception.MissingAnnotationException;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Generate schema related SQL script, such as normalized table / column names ..etc
 */
public class SQLSchemaUtils {

  private static final String GMA = "gma";
  public static final String ENTITY_TABLE_PREFIX = "metadata_entity_";
  public static final String RELATIONSHIP_TABLE_PREFIX = "metadata_relationship_";
  public static final String ASPECT_PREFIX = "a_";
  public static final String INDEX_PREFIX = "i_";

  private static final int MYSQL_MAX_COLUMN_NAME_LENGTH = 64 - ASPECT_PREFIX.length();

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
   * Get MySQL table name from entity type string.
   * @param entityType entity type as string, such as "dataset", "chart" ..etc
   * @return entity table name
   */
  @Nonnull
  public static String getTableName(@Nonnull String entityType) {
    return ENTITY_TABLE_PREFIX + entityType.toLowerCase();
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
   * Get column name from aspect class canonical name.
   */
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
  public static <ASPECT extends RecordTemplate> String getAspectColumnName(@Nonnull Class<ASPECT> aspectClass) {
    return getAspectColumnName(aspectClass.getCanonicalName());
  }

  /**
   * Get generated column name from aspect and path.
   */
  @Nonnull
  public static String getGeneratedColumnName(@Nonnull String aspect, @Nonnull String path) {
    if (isUrn(aspect)) {
      return INDEX_PREFIX + "urn" + processPath(path);
    }

    return INDEX_PREFIX + getColumnNameFromAnnotation(aspect) + processPath(path);
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
   * @return $name$value or $name
   */
  @Nonnull
  private static String processPath(@Nonnull String path) {
    path = path.replace("/", "$");
    if (!path.startsWith("$")) {
      path = "$" + path;
    }
    return path;
  }

  /**
   * Get Column name from aspect canonical name.
   *
   * @param aspectCanonicalName aspect name in canonical form.
   * @return aspect column name
   */
  @Nonnull
  private static String getColumnNameFromAnnotation(@Nonnull final String aspectCanonicalName) {
    try {
      final RecordDataSchema schema = (RecordDataSchema) DataTemplateUtil.getSchema(ClassUtils.loadClass(aspectCanonicalName));
      final Map<String, Object> properties = schema.getProperties();
      final Object gmaObj = properties.get(GMA);
      final AspectColumnMetadata gmaAnnotation = DataTemplateUtil.wrap(gmaObj, AspectColumnMetadata.class);
      return gmaAnnotation.getAspect().getColumn().getName();
    } catch (Exception e) {
      throw new MissingAnnotationException(String.format("Aspect %s should be annotated with @gma.aspect.column.name.",
          aspectCanonicalName), e);
    }
  }
}
