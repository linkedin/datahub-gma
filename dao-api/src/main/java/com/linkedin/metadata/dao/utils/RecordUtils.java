package com.linkedin.metadata.dao.utils;

import com.linkedin.data.DataMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.template.AbstractArrayTemplate;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.validator.InvalidSchemaException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;


public class RecordUtils {

  private static final JacksonDataTemplateCodec DATA_TEMPLATE_CODEC = new JacksonDataTemplateCodec();
  private static final String ARRAY_WILDCARD = "*";
  private static final Pattern LEADING_SPACESLASH_PATTERN = Pattern.compile("^[/ ]+");
  private static final Pattern TRAILING_SPACESLASH_PATTERN = Pattern.compile("[/ ]+$");
  private static final Pattern SLASH_PATERN = Pattern.compile("/");

  /**
   * Using in-memory hash map to store the get/is methods of the schema fields of RecordTemplate and UnionTemplate, respectively.
   * These maps have RecordTemplate/UnionTemplate class as the key, with the value being another map of field name with the associated get/is methods.
   */
  private static final ConcurrentHashMap<Class<? extends RecordTemplate>, Map<String, Method>> RECORD_METHOD_CACHE = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<Class<? extends UnionTemplate>, Map<String, Method>> UNION_METHOD_CACHE = new ConcurrentHashMap<>();

  private RecordUtils() {
    // Util class
  }

  @Nonnull
  static String capitalizeFirst(@Nonnull String name) {
    if (name.length() > 0) {
      return Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }
    return name;
  }

  /**
   * Serializes a {@link RecordTemplate} to JSON string.
   *
   * @param recordTemplate the record template to serialize
   * @return the JSON string serialized using {@link JacksonDataTemplateCodec}.
   */
  @Nonnull
  public static String toJsonString(@Nonnull RecordTemplate recordTemplate) {
    try {
      return DATA_TEMPLATE_CODEC.mapToString(recordTemplate.data());
    } catch (IOException e) {
      throw new ModelConversionException("Failed to serialize RecordTemplate: " + recordTemplate.toString());
    }
  }

  /**
   * Creates a {@link RecordTemplate} object from a serialized JSON string.
   *
   * @param type the type of {@link RecordTemplate} to create
   * @param jsonString a JSON string serialized using {@link JacksonDataTemplateCodec}
   * @param <T> must be a valid {@link RecordTemplate} class
   * @return the created {@link RecordTemplate}
   */
  @Nonnull
  public static <T extends RecordTemplate> T toRecordTemplate(@Nonnull Class<T> type, @Nonnull String jsonString) {
    DataMap dataMap;
    try {
      dataMap = DATA_TEMPLATE_CODEC.stringToMap(jsonString);
    } catch (IOException e) {
      throw new ModelConversionException("Failed to deserialize DataMap: " + jsonString);
    }

    return toRecordTemplate(type, dataMap);
  }

  /**
   * Creates a {@link RecordTemplate} object from a {@link DataMap}.
   *
   * @param type the type of {@link RecordTemplate} to create
   * @param dataMap a {@link DataMap} of the record
   * @param <T> must be a valid {@link RecordTemplate} class
   * @return the created {@link RecordTemplate}
   */
  @Nonnull
  public static <T extends RecordTemplate> T toRecordTemplate(@Nonnull Class<T> type, @Nonnull DataMap dataMap) {
    Constructor<T> constructor;
    try {
      constructor = type.getConstructor(DataMap.class);
    } catch (NoSuchMethodException e) {
      throw new ModelConversionException("Unable to find constructor for " + type.getCanonicalName(), e);
    }

    try {
      return constructor.newInstance(dataMap);
    } catch (Exception e) {
      throw new ModelConversionException("Failed to invoke constructor for " + type.getCanonicalName(), e);
    }
  }

  /**
   * Creates a {@link RecordTemplate} object from class FQCN and a {@link DataMap}.
   *
   * @param className FQCN of the record class extending RecordTemplate
   * @param dataMap a {@link DataMap} of the record
   * @return the created {@link RecordTemplate}
   */
  @Nonnull
  public static RecordTemplate toRecordTemplate(@Nonnull String className, @Nonnull DataMap dataMap) {
    Class<? extends RecordTemplate> clazz;
    try {
      clazz = Class.forName(className).asSubclass(RecordTemplate.class);
    } catch (ClassNotFoundException e) {
      throw new ModelConversionException("Unable to find class " + className, e);
    }

    return toRecordTemplate(clazz, dataMap);
  }

  /**
   * Gets the aspect from the aspect class.
   *
   * @param aspectClass the aspect class
   * @return the aspect corresponding to the aspect class
   */
  public static <ASPECT extends RecordTemplate> ASPECT getAspectFromClass(@Nonnull Class<ASPECT> aspectClass) {
    // Create an empty aspect to extract its field names
    final Constructor<ASPECT> constructor;
    try {
      @SuppressWarnings("rawtypes")
      final Class[] constructorParamArray = new Class[]{};
      constructor = aspectClass.getConstructor(constructorParamArray);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Exception occurred while trying to get the default constructor for the aspect. ", e);
    }

    final ASPECT aspect;
    try {
      aspect = constructor.newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Exception occurred while creating an instance of the aspect. ", e);
    }
    return aspect;
  }

  /**
   * Gets aspect from a string representing the class name.
   *
   * @param aspectClassString string representation of an aspect class
   * @return the aspect corresponding to the aspect class
   */
  @Nonnull
  public static <ASPECT extends RecordTemplate> ASPECT getAspectFromString(@Nonnull String aspectClassString) {
    final Class<ASPECT> aspectClass;
    try {
      aspectClass = (Class<ASPECT>) Class.forName(aspectClassString);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Exception occurred while trying to get the aspect class. ", e);
    }

    return getAspectFromClass(aspectClass);
  }

  /**
   * Extracts the aspect from an entity value which includes a single aspect.
   *
   * @param entity the entity value.
   * @param aspectClass the aspect class.
   * @return the aspect which is included in the entity.
   * */
  @Nonnull
  public static <ASPECT extends RecordTemplate, ENTITY extends RecordTemplate> ASPECT
  extractAspectFromSingleAspectEntity(@Nonnull ENTITY entity, @Nonnull Class<ASPECT> aspectClass) {

    final ASPECT aspect = getAspectFromClass(aspectClass);

    final Set<String> aspectFields = aspect.schema().getFields()
        .stream()
        .map(RecordDataSchema.Field::getName)
        .collect(Collectors.toSet());

    // Get entity's field names and only keep fields which occur in the entity and not in the aspect
    final Set<String> entityFields = entity.schema().getFields()
        .stream()
        .map(RecordDataSchema.Field::getName)
        .collect(Collectors.toSet());
    entityFields.removeAll(aspectFields);

    // remove non aspect fields from entity's cloned datamap and use it to create an aspect
    final DataMap entityDataMap;
    try {
      entityDataMap = entity.data().clone();
      entityFields.forEach(entityDataMap::remove);
      return toRecordTemplate(aspectClass, entityDataMap);
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets a {@link com.linkedin.data.schema.RecordDataSchema.Field} from a {@link RecordTemplate}.
   *
   * @param recordTemplate the {@link RecordTemplate} containing the field
   * @param fieldName the name of the field
   * @return the field
   */
  @Nonnull
  public static <T extends RecordTemplate> RecordDataSchema.Field getRecordDataSchemaField(@Nonnull T recordTemplate,
      @Nonnull String fieldName) {

    RecordDataSchema.Field field = recordTemplate.schema().getField(fieldName);
    if (field == null) {
      throw new InvalidSchemaException(
          String.format("Missing expected field '%s' in %s", fieldName, recordTemplate.getClass().getCanonicalName()));
    }
    return field;
  }

  /**
   * Sets the value of a primitive field in a {@link RecordTemplate} using reflection.
   *
   * @param recordTemplate the {@link RecordTemplate} containing the field
   * @param fieldName the name of the field to update
   * @param value the value to set
   */
  public static <T extends RecordTemplate, V> void setRecordTemplatePrimitiveField(@Nonnull T recordTemplate,
      @Nonnull String fieldName, @Nonnull V value) {

    final RecordDataSchema.Field field = getRecordDataSchemaField(recordTemplate, fieldName);
    final Method putDirect =
        getProtectedMethod(RecordTemplate.class, "putDirect", RecordDataSchema.Field.class, Class.class, Object.class,
            SetMode.class);
    invokeProtectedMethod(recordTemplate, putDirect, field, value.getClass(), value, SetMode.DISALLOW_NULL);
  }

  /**
   * Sets the value of a {@link RecordTemplate} field in a {@link RecordTemplate} using reflection.
   *
   * @param recordTemplate the {@link RecordTemplate} containing the field
   * @param fieldName the name of the field to update
   * @param value the value to set
   */
  public static <T extends RecordTemplate, V extends DataTemplate> void setRecordTemplateComplexField(
      @Nonnull T recordTemplate, @Nonnull String fieldName, @Nonnull V value) {

    final RecordDataSchema.Field field = getRecordDataSchemaField(recordTemplate, fieldName);
    final Method putWrapped =
        getProtectedMethod(RecordTemplate.class, "putWrapped", RecordDataSchema.Field.class, Class.class,
            DataTemplate.class, SetMode.class);
    invokeProtectedMethod(recordTemplate, putWrapped, field, value.getClass(), value, SetMode.DISALLOW_NULL);
  }

  /**
   * Gets the value of a non-wrapped field from a {@link RecordTemplate} using reflection.
   *
   * @param recordTemplate the {@link RecordTemplate} to get the value from
   * @param fieldName the name of the field
   * @param valueClass the expected type for the value
   * @return the value for the field
   */
  @Nonnull
  public static <T extends RecordTemplate, V> V getRecordTemplateField(@Nonnull T recordTemplate,
      @Nonnull String fieldName, @Nonnull Class<V> valueClass) {

    final RecordDataSchema.Field field = getRecordDataSchemaField(recordTemplate, fieldName);
    final Method obtainCustomType =
        getProtectedMethod(RecordTemplate.class, "obtainCustomType", RecordDataSchema.Field.class, Class.class,
            GetMode.class);
    return (V) invokeProtectedMethod(recordTemplate, obtainCustomType, field, valueClass, GetMode.STRICT);
  }

  /**
   * Gets the value of a wrapped field from a {@link RecordTemplate} using reflection.
   *
   * @param recordTemplate the {@link RecordTemplate} to get the value from
   * @param fieldName the name of the field
   * @param valueClass the expected type for the value
   * @return the value for the field
   */
  @Nonnull
  public static <T extends RecordTemplate, V extends DataTemplate> V getRecordTemplateWrappedField(
      @Nonnull T recordTemplate, @Nonnull String fieldName, @Nonnull Class<V> valueClass) {

    final RecordDataSchema.Field field = getRecordDataSchemaField(recordTemplate, fieldName);
    final Method obtainWrapped =
        getProtectedMethod(RecordTemplate.class, "obtainWrapped", RecordDataSchema.Field.class, Class.class,
            GetMode.class);
    return (V) invokeProtectedMethod(recordTemplate, obtainWrapped, field, valueClass, GetMode.STRICT);
  }

  /**
   * Gets the currently selected {@link RecordTemplate} in a {@link UnionTemplate}.
   *
   * @param unionTemplate the {@link UnionTemplate} to check
   * @return the currently selected value in {@code unionTemplate}
   */
  @Nonnull
  public static <V extends RecordTemplate> RecordTemplate getSelectedRecordTemplateFromUnion(
      @Nonnull UnionTemplate unionTemplate) {

    final DataSchema dataSchema = unionTemplate.memberType();
    if (!(dataSchema instanceof RecordDataSchema)) {
      throw new InvalidSchemaException(
          "The currently selected member isn't a RecordTemplate in " + unionTemplate.getClass().getCanonicalName());
    }

    final Class<? extends RecordTemplate> clazz =
        ModelUtils.getClassFromName(((RecordDataSchema) dataSchema).getBindingName(), RecordTemplate.class);

    final Method obtainWrapped =
        getProtectedMethod(UnionTemplate.class, "obtainWrapped", DataSchema.class, Class.class, String.class);
    final List<UnionDataSchema.Member> members = ((UnionDataSchema) unionTemplate.schema()).getMembers();
    for (UnionDataSchema.Member m : members) {
      if (m.hasAlias() && m.getType()
          .getDereferencedDataSchema()
          .getUnionMemberKey()
          .equals(clazz.getName())) {
        return (V) invokeProtectedMethod(unionTemplate, obtainWrapped, dataSchema, clazz,
            m.getAlias());
      }
    }

    return (V) invokeProtectedMethod(unionTemplate, obtainWrapped, dataSchema, clazz,
        ((RecordDataSchema) dataSchema).getFullName());
  }

  /**
   * Sets the currently selected {@link RecordTemplate} in a {@link UnionTemplate}.
   *
   * @param unionTemplate the {@link UnionTemplate} to select
   * @param selectedMember the {@link RecordTemplate} to set to
   * @return the currently selected value in {@code unionTemplate}
   */
  @Nonnull
  public static <V extends RecordTemplate> RecordTemplate setSelectedRecordTemplateInUnion(
      @Nonnull UnionTemplate unionTemplate, @Nonnull RecordTemplate selectedMember) {
    final Method selectWrapped =
        getProtectedMethod(UnionTemplate.class, "selectWrapped", DataSchema.class, Class.class, String.class,
            DataTemplate.class);

    final List<UnionDataSchema.Member> members = ((UnionDataSchema) unionTemplate.schema()).getMembers();
    for (UnionDataSchema.Member m : members) {
      if (m.hasAlias() && m.getType()
          .getDereferencedDataSchema()
          .getUnionMemberKey()
          .equals(selectedMember.getClass().getName())) {
        return (V) invokeProtectedMethod(unionTemplate, selectWrapped, selectedMember.schema(),
            selectedMember.getClass(), m.getAlias(), selectedMember);
      }
    }
    return (V) invokeProtectedMethod(unionTemplate, selectWrapped, selectedMember.schema(), selectedMember.getClass(),
        selectedMember.schema().getUnionMemberKey(), selectedMember);
  }

  @Nonnull
  private static Method getProtectedMethod(@Nonnull Class clazz, @Nonnull String methodName,
      @Nonnull Class<?>... parameterTypes) {
    try {
      return clazz.getDeclaredMethod(methodName, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  private static <T> T invokeProtectedMethod(Object object, Method method, Object... args) {
    try {
      method.setAccessible(true);
      return (T) method.invoke(object, args);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    } finally {
      method.setAccessible(false);
    }
  }

  @Nonnull
  private static Map<String, Method> getMethodsFromRecordTemplate(@Nonnull RecordTemplate recordTemplate) {
    final HashMap<String, Method> methodMap = new HashMap<>();
    for (RecordDataSchema.Field field : recordTemplate.schema().getFields()) {
      final String capitalizedName = capitalizeFirst(field.getName());
      final String getMethodName =
          (field.getType().getType().equals(RecordDataSchema.Type.BOOLEAN) ? "is" : "get") + capitalizedName;
      try {
        methodMap.put(field.getName(), recordTemplate.getClass().getMethod(getMethodName));
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(String.format("Failed to get method [%s], for class [%s], field [%s]",
            getMethodName, recordTemplate.getClass().getCanonicalName(), field.getName()), e);
      }
    }
    return Collections.unmodifiableMap(methodMap);
  }

  @Nonnull
  private static Map<String, Method> getMethodsFromUnionTemplate(@Nonnull UnionTemplate unionTemplate) {
    final Pattern patternLastPeriod = Pattern.compile("^(.*)\\.");
    final HashMap<String, Method> methodMap = new HashMap<>();
    for (UnionDataSchema.Member member : ((UnionDataSchema) unionTemplate.schema()).getMembers()) {
      String unionMemberKey = member.getUnionMemberKey();
      // com.linkedin.foo => Foo
      String lastPartOfUnionMemberKey = patternLastPeriod.matcher(unionMemberKey).replaceAll("");
      String capitalizedName = capitalizeFirst(lastPartOfUnionMemberKey);
      String getMethodName = "get" + capitalizedName;
      try {
        methodMap.put(unionMemberKey, unionTemplate.getClass().getMethod(getMethodName));
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(String.format("Failed to get method [%s], for class [%s], field [%s]",
            getMethodName, unionTemplate.getClass().getCanonicalName(), unionMemberKey), e);
      }
    }
    return Collections.unmodifiableMap(methodMap);
  }

  /**
   * Given a {@link RecordTemplate} and field name, this will find and execute getFieldName/isFieldName and return the result
   * If neither getFieldName/isFieldName has been called for any of the fields of the RecordTemplate, then the get/is method
   * for all schema fields of the record will be found and subsequently cached.
   *
   * @param dataTemplate {@link RecordTemplate} or {@link UnionTemplate} whose field/member has to be referenced
   * @param fieldName field name of the record that has to be referenced
   * @return value of the field in the record
   */
  @Nullable
  private static Object invokeMethod(@Nonnull Object dataTemplate, @Nonnull String fieldName) {
    if (dataTemplate instanceof RecordTemplate) {
      return invokeMethodRecord((RecordTemplate) dataTemplate, fieldName);
    }
    if (dataTemplate instanceof UnionTemplate) {
      return invokeMethodUnion((UnionTemplate) dataTemplate, fieldName);
    }
    return null;
  }

  private static Object invokeMethodRecord(@Nonnull RecordTemplate record, @Nonnull String fieldName) {
    RECORD_METHOD_CACHE.putIfAbsent(record.getClass(), getMethodsFromRecordTemplate(record));
    try {
      return RECORD_METHOD_CACHE.get(record.getClass()).get(fieldName).invoke(record);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(String.format("Failed to execute method for class [%s], field [%s]",
          record.getClass().getCanonicalName(), fieldName), e);
    }
  }

  private static Object invokeMethodUnion(@Nonnull UnionTemplate union, @Nonnull String fieldName) {
    UNION_METHOD_CACHE.putIfAbsent(union.getClass(), getMethodsFromUnionTemplate(union));
    try {
      return UNION_METHOD_CACHE.get(union.getClass()).get(fieldName).invoke(union);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(String.format("Failed to execute method for class [%s], field [%s]",
          union.getClass().getCanonicalName(), fieldName), e);
    }
  }

  /**
   * Helper method for referencing array of RecordTemplate objects. Referencing a particular index or range of indices of an array is not supported.
   *
   * @param reference {@link AbstractArrayTemplate} corresponding to array of {@link RecordTemplate} or {@link UnionTemplate} which needs to be referenced
   * @param ps {@link PathSpec} for the entire path inside the array that needs to be referenced
   * @return {@link List} of objects from the array, referenced using the PathSpec
   */
  @Nonnull
  @SuppressWarnings("rawtypes")
  private static List<Object> getReferenceForAbstractArray(@Nonnull AbstractArrayTemplate<?> reference, @Nonnull PathSpec ps) {
    if (!reference.isEmpty()) {
      return Arrays.stream((reference).toArray()).map(x -> {
        if (x instanceof RecordTemplate) {
          return getFieldValue(((RecordTemplate) x), ps);
        }
        if (x instanceof UnionTemplate) {
          if (ps.getPathComponents().size() != 1) {
            throw new InvalidSchemaException("The currently selected member isn't a union of primitives: " + ps);
          }
          return Optional.ofNullable(((DataMap) ((UnionTemplate) x).data()).get(ps.getPathComponents().get(0)));
        }
        throw new InvalidSchemaException("The currently selected member is neither a RecordTemplate or a Union: " + ps);
      }).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty)).collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  /**
   * Returns a PathSpec as an array representation from a string representation.
   *
   * @param pathSpecAsString string representation of a path spec
   * @return array representation of a path spec
   */
  @Nonnull
  public static String[] getPathSpecAsArray(@Nonnull String pathSpecAsString) {
    pathSpecAsString = LEADING_SPACESLASH_PATTERN.matcher(pathSpecAsString).replaceAll("");
    pathSpecAsString = TRAILING_SPACESLASH_PATTERN.matcher(pathSpecAsString).replaceAll("");

    if (pathSpecAsString.isEmpty()) {
      return new String[0];
    }

    return SLASH_PATERN.split(pathSpecAsString);
  }

  /**
   * Similar to {@link #getFieldValue(RecordTemplate, PathSpec)} but takes string representation of Pegasus PathSpec as
   * input.
   */
  @Nonnull
  public static Optional<Object> getFieldValue(@Nonnull RecordTemplate recordTemplate, @Nonnull String pathSpecAsString) {
    final String[] pathSpecAsArray = getPathSpecAsArray(pathSpecAsString);

    if (pathSpecAsArray.length > 0) {
      return getFieldValue(recordTemplate, new PathSpec(pathSpecAsArray));
    }
    return Optional.empty();
  }

  /**
   * Given a {@link RecordTemplate} and {@link com.linkedin.data.schema.PathSpec} this will return value of the path from the record.
   * This handles only RecordTemplate, fields of which can be primitive types, typeRefs, unions ({@link UnionTemplate}),
   * arrays of primitive types, arrays of records, or arrays of primitive union types.
   * Fetching of values in a RecordTemplate where the field has a default value will return the field default value.
   * Referencing field corresponding to a particular index or range of indices of an array is not supported.
   * Fields corresponding to 1) multi-dimensional array 2) AbstractMapTemplate 3) FixedTemplate are currently not supported.
   *
   * @param recordTemplate {@link RecordTemplate} whose field specified by the pegasus path has to be accessed
   * @param ps {@link PathSpec} representing the path whose value needs to be returned
   * @return Referenced object of the RecordTemplate corresponding to the PathSpec
   */
  @Nonnull
  @SuppressWarnings("rawtypes")
  public static Optional<Object> getFieldValue(@Nonnull RecordTemplate recordTemplate, @Nonnull PathSpec ps) {
    Object reference = recordTemplate;
    final int pathSize = ps.getPathComponents().size();
    for (int i = 0; i < pathSize; i++) {
      final String part = ps.getPathComponents().get(i);
      if (part.equals(ARRAY_WILDCARD)) {
        continue;
      }
      if (StringUtils.isNumeric(part)) {
        throw new UnsupportedOperationException(String.format("Array indexing is not supported for %s (%s from %s)", part, ps, reference));
      }
      if (reference instanceof RecordTemplate) {
        reference = invokeMethod((RecordTemplate) reference, part);
        if (reference == null) {
          return Optional.empty();
        }
      } else if (reference instanceof AbstractArrayTemplate) {
        return Optional.of(getReferenceForAbstractArray((AbstractArrayTemplate<?>) reference, new PathSpec(ps.getPathComponents().subList(i, pathSize))));
      } else if (reference instanceof UnionTemplate) {
        if (ps.getPathComponents().size() == 1) {
          // if primitive
          return Optional.ofNullable(((DataMap) ((UnionTemplate) reference).data()).get(ps.getPathComponents().get(0)));
        }
        reference = invokeMethod((UnionTemplate) reference, part);
        if (reference == null) {
          return Optional.empty();
        }
      } else {
        throw new UnsupportedOperationException(String.format("Failed at extracting %s (%s from %s)", part, ps, recordTemplate));
      }
    }
    return Optional.of(reference);
  }
}
