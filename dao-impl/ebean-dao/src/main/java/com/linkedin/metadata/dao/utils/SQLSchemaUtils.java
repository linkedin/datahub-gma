package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nonnull;


/**
 * Generate schema related SQL script, such as normalized table / column names ..etc
 */
public class SQLSchemaUtils {

  private static final String LI_DOMAIN = "com.linkedin.";

  public static final String ENTITY_TABLE_PREFIX = "metadata_entity_";
  public static final String ASPECT_PREFIX = "a_";
  public static final String INDEX_PREFIX = "i_";

  private static final int MYSQL_MAX_COLUMN_NAME_LENGTH = 64 - ASPECT_PREFIX.length();

  private SQLSchemaUtils() {
  }

  /**
   * Get a column name from aspect object follow MySQL column naming convention.
   * @param aspect aspect value
   * @param <ASPECT> aspect that extends {@link RecordTemplate}
   * @return converted column name follow MySQL naming convention.
   */
  public static <ASPECT extends RecordTemplate> String getColumnName(@Nonnull ASPECT aspect) {
    return getColumnName(aspect.getClass());
  }

  /**
   * Get MySQL table name from entity urn, e.g. urn:li:dataset to metadata_entity_dataset.
   * @param urn {@link Urn} of the entity
   * @return entity table name
   */
  public static String getTableName(@Nonnull Urn urn) {
    return getTableName(urn.getEntityType());
  }

  /**
   * Get MySQL table name from entity type string.
   * @param entityType entity type as string, such as "dataset", "chart" ..etc
   * @return entity table name
   */
  public static String getTableName(@Nonnull String entityType) {
    return ENTITY_TABLE_PREFIX + entityType.toLowerCase();
  }

  /**
   * Get normalized aspect name. Normalization Rules:
   * 1. remove LI prefix (com.linkedin.) to save column length
   * 2. all lower cases
   * 3. substitute "." with "_"
   * 4. If length is longer than 64, chopping namespace from left to right
   *
   * @param aspectCanonicalName aspect name in canonical form.
   * @return normalized aspect name
   */
  static String getNormalizedAspectName(@Nonnull String aspectCanonicalName) {
    aspectCanonicalName = aspectCanonicalName.toLowerCase(Locale.ROOT);
    if (aspectCanonicalName.startsWith(LI_DOMAIN)) {
      aspectCanonicalName = aspectCanonicalName.substring(LI_DOMAIN.length());
    }
    if (aspectCanonicalName.length() > MYSQL_MAX_COLUMN_NAME_LENGTH) {
      throw new IllegalArgumentException("Aspect name is too long to be normalized: " + aspectCanonicalName);
    }
    aspectCanonicalName = trimColumnName(aspectCanonicalName);
    return aspectCanonicalName.replace(".", "_");
  }

  /**
   * Get Column name from aspect canonical name.
   *
   * @param aspectCanonicalName aspect name in canonical form.
   * @return aspect column name
   */
  public static String getColumnName(@Nonnull String aspectCanonicalName) {
    return ASPECT_PREFIX + getNormalizedAspectName(aspectCanonicalName);
  }

  /**
   * Get MySQL column name from aspect class.
   *
   * @param aspectClass aspect class
   * @param <ASPECT> aspect that extends {@link RecordTemplate}
   * @return aspect column name
   */
  public static <ASPECT extends RecordTemplate> String getColumnName(@Nonnull Class<ASPECT> aspectClass) {
    return getColumnName(aspectClass.getCanonicalName());
  }

  /**
   * Trim column name to keep it within 64 characters.
   * TODO: it has the restriction to trim class with longer than 64 chars in the class name and resolve the
   * TODO: different classes has the same classname and package prefix to resolve the above restriction, a
   * TODO: smarter trim algorithm or naming registry is required.
   *
   * @param aspectCanonicalName column name in canonical format, e.g: com.linkedin.foo.ClassName
   * @return
   */
  static String trimColumnName(String aspectCanonicalName) {
    if (aspectCanonicalName.length() <= MYSQL_MAX_COLUMN_NAME_LENGTH) {
      return aspectCanonicalName;
    }
    final List<String> packageTokens = Arrays.asList(aspectCanonicalName.split("\\."));
    int charsToChop = aspectCanonicalName.length() - MYSQL_MAX_COLUMN_NAME_LENGTH;
    int curToken = 0;
    while (charsToChop > 0 && curToken < packageTokens.size() - 1) {
      charsToChop -= packageTokens.get(curToken).length() + 1;
      curToken++;
    }
    if (charsToChop > 0) {
      throw new RuntimeException("unable to further trim column name: " + aspectCanonicalName);
    }
    return String.join(".", packageTokens.subList(curToken, packageTokens.size()));
  }
}
