package com.linkedin.mxe;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;


/**
 * Default implementation of a {@link TopicConventionV5}, which is fully customizable for event names.
 *
 * <p>The newer aspect-entity specific event names are based on a pattern that can also be configured. The pattern is a
 * string, which can use {@link #EVENT_TYPE_PLACEHOLDER}, {@link #ENTITY_PLACEHOLDER}, and {@link #ASPECT_PLACEHOLDER}
 * as placeholders for the event type (MCE, MAE, FMCE, etc), entity name, and aspect name, respectively.
 *
 * <p>The default pattern is {@code %EVENT%_%ENTITY%_%ASPECT%}. So, for example, given an URN like
 * {@code urn:li:pizza:0} and an aspect like {@code PizzaInfo}, you would ge the following topic names by defalut:
 *
 * <ul>
 *   <li>{@code MCE_Pizza_PizzaInfo}
 *   <li>{@code MAE_Pizza_PizzaInfo}
 *   <li>{@code FMCE_Pizza_PizzaInfo}
 * </ul>
 */
public final class TopicConventionV5Impl implements TopicConventionV5 {
  // Placeholders
  public static final String EVENT_TYPE_PLACEHOLDER = "%EVENT%";
  public static final String ENTITY_PLACEHOLDER = "%ENTITY%";
  public static final String ASPECT_PLACEHOLDER = "%ASPECT%";

  // v5 defaults
  public static final String DEFAULT_EVENT_PATTERN = "%EVENT%_%ENTITY%_%ASPECT%";

  // V5 event name placeholder replacements
  private static final String METADATA_CHANGE_EVENT_TYPE = "MCE";
  private static final String METADATA_AUDIT_EVENT_TYPE = "MAE";
  private static final String FAILED_METADATA_CHANGE_EVENT_TYPE = "FMCE";

  private static final String V5_EVENT_ROOT_NAMESPACE = "com.linkedin.pegasus2avro.mxe";
  // should be %entityName%.%aspectName%.%EventClass%
  private static final String V5_EVENT_NAMESPACE_TEMPLATE = V5_EVENT_ROOT_NAMESPACE + ".%s.%s.%s";
  private static final String METADATA_CHANGE_EVENT = "MetadataChangeEvent";
  private static final String METADATA_AUDIT_EVENT = "MetadataAuditEvent";
  private static final String FAILED_METADATA_CHANGE_EVENT = "FailedMetadataChangeEvent";

  // v5 patterns
  private final String _eventPattern;

  private final Map<String, String> _overrides;

  public TopicConventionV5Impl(@Nonnull String eventPattern, @Nonnull Map<String, String> overrides) {
    _eventPattern = eventPattern;
    _overrides = overrides;
  }

  public TopicConventionV5Impl(@Nonnull String eventPattern) {
    this(eventPattern, Collections.emptyMap());
  }

  public TopicConventionV5Impl() {
    this(DEFAULT_EVENT_PATTERN);
  }

  /**
   * Given a map from the default topic name to a topic name to use, returns a new convention that will respect these
   * specific per-topic overrides.
   *
   * <p>Needed for LI internally as we made some off-pattern names before we finalized v5. Most users should not be
   * using this. If you need to change topic names in your organization, try to do so consistently and change the
   * {@code eventPattern} constructor parameter instead.
   */
  public TopicConventionV5Impl withOverrides(@Nonnull Map<String, String> overrides) {
    final Map<String, String> merged = new HashMap<>(_overrides);

    for (String key : overrides.keySet()) {
      merged.put(key, overrides.get(key));
    }

    return new TopicConventionV5Impl(_eventPattern, merged);
  }

  public TopicConventionV5Impl withOverride(@Nonnull String schemaFqcn, @Nonnull String topicName) {
    return withOverrides(new HashMap<String, String>() {{
      put(schemaFqcn, topicName);
    }});
  }

  /**
   * Given a map from v5 event type to a topic name to use, returns a new convention that will respect these specific
   * per-topic overrides.
   *
   * <p>Needed for LI internally as we made some off-pattern names before we finalized v5. Most users should not be
   * using this. If you need to change topic names in your organization, try to do so consistently and change the
   * {@code eventPattern} constructor parameter instead.
   */
  public TopicConventionV5Impl withSchemaOverrides(@Nonnull Map<Class<?>, String> overrides) {
    final Map<String, String> merged = new HashMap<>(_overrides);

    for (Class<?> key : overrides.keySet()) {
      final String name = getDefaultEventName(key);
      merged.put(name, overrides.get(key));
    }

    return new TopicConventionV5Impl(_eventPattern, merged);
  }

  public TopicConventionV5Impl withOverride(@Nonnull Class<?> schema, @Nonnull String topicName) {
    return withSchemaOverrides(new HashMap<Class<?>, String>() {{
      put(schema, topicName);
    }});
  }

  @Nonnull
  private String getDefaultEventName(@Nonnull Class<?> eventSchema) {
    final Pattern pattern = Pattern.compile(
        "com\\.linkedin\\.pegasus2avro\\.mxe\\.(?<entity>[A-z]+)\\.(?<aspect>[A-z]+)\\.(?<className>[A-z]+)");
    final Matcher matcher = pattern.matcher(eventSchema.getName());

    if (!matcher.find()) {
      throw new IllegalArgumentException(String.format(
          "Expected all events FCQN to match `com.linkedin.pegasus2avro.mxe.<entity>.<aspect>.<className>`. Got `%s`.",
          eventSchema.getName()));
    }

    final String className = matcher.group("className");

    String eventType;

    if (METADATA_CHANGE_EVENT.equals(className)) {
      eventType = METADATA_CHANGE_EVENT_TYPE;
    } else if (METADATA_AUDIT_EVENT.equals(className)) {
      eventType = METADATA_AUDIT_EVENT_TYPE;
    } else if (FAILED_METADATA_CHANGE_EVENT.equals(className)) {
      eventType = FAILED_METADATA_CHANGE_EVENT_TYPE;
    } else {
      throw new IllegalArgumentException(String.format("Unrecognized MXE class name: %s", className));
    }

    return buildEventName(eventType, toUpperCamelCase(matcher.group("entity")),
        toUpperCamelCase(matcher.group("aspect")));
  }

  @Nonnull
  private String buildEventName(@Nonnull String eventType, @Nonnull String entityName, @Nonnull String aspectName) {
    final String name = _eventPattern.replace(EVENT_TYPE_PLACEHOLDER, eventType)
        .replace(ENTITY_PLACEHOLDER, entityName)
        .replace(ASPECT_PLACEHOLDER, aspectName);

    return _overrides.getOrDefault(name, name);
  }

  @Nonnull
  private String toUpperCamelCase(@Nonnull String str) {
    if (str.isEmpty()) {
      return str;
    }

    if (str.length() == 1) {
      return str.toUpperCase();
    }

    return Character.toUpperCase(str.charAt(0)) + str.substring(1);
  }

  @Nonnull
  private String toLowerCamelCase(@Nonnull String str) {
    if (str.isEmpty()) {
      return str;
    }

    if (str.length() == 1) {
      return str.toLowerCase();
    }

    return Character.toLowerCase(str.charAt(0)) + str.substring(1);
  }

  private String buildEventName(@Nonnull String eventType, @Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    final String entityType = toUpperCamelCase(urn.getEntityType());
    final String aspectName = aspect.getClass().getSimpleName();

    return buildEventName(eventType, entityType, aspectName);
  }

  @Nonnull
  @Override
  public String getMetadataChangeEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    return buildEventName(METADATA_CHANGE_EVENT_TYPE, urn, aspect);
  }

  private Class<?> getClass(@Nonnull Urn urn, @Nonnull RecordTemplate aspect, @Nonnull String eventType)
      throws ClassNotFoundException {
    final String entityType = urn.getEntityType();
    final String aspectName = toLowerCamelCase(aspect.getClass().getSimpleName());

    final String className = String.format(V5_EVENT_NAMESPACE_TEMPLATE, entityType, aspectName, eventType);

    return Class.forName(className);
  }

  @Override
  public Class<?> getMetadataChangeEventSchema(@Nonnull Urn urn, @Nonnull RecordTemplate aspect)
      throws ClassNotFoundException {
    return getClass(urn, aspect, METADATA_CHANGE_EVENT);
  }

  @Nonnull
  @Override
  public String getMetadataAuditEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    return buildEventName(METADATA_AUDIT_EVENT_TYPE, urn, aspect);
  }

  @Override
  public Class<?> getMetadataAuditEventSchema(@Nonnull Urn urn, @Nonnull RecordTemplate aspect)
      throws ClassNotFoundException {
    return getClass(urn, aspect, METADATA_AUDIT_EVENT);
  }

  @Nonnull
  @Override
  public String getFailedMetadataChangeEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    return buildEventName(FAILED_METADATA_CHANGE_EVENT_TYPE, urn, aspect);
  }

  @Override
  public Class<?> getFailedMetadataChangeEventSchema(@Nonnull Urn urn, @Nonnull RecordTemplate aspect)
      throws ClassNotFoundException {
    return getClass(urn, aspect, FAILED_METADATA_CHANGE_EVENT);
  }
}
