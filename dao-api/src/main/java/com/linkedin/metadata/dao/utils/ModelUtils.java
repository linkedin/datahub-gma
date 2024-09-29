package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.data.template.WrappingArrayTemplate;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.dummy.DummySnapshot;
import com.linkedin.metadata.validator.AspectValidator;
import com.linkedin.metadata.validator.AssetValidator;
import com.linkedin.metadata.validator.DeltaValidator;
import com.linkedin.metadata.validator.DocumentValidator;
import com.linkedin.metadata.validator.EntityValidator;
import com.linkedin.metadata.validator.InvalidSchemaException;
import com.linkedin.metadata.validator.RelationshipValidator;
import com.linkedin.metadata.validator.SnapshotValidator;
import com.linkedin.metadata.validator.ValidationUtils;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.reflections.Reflections;


public class ModelUtils {

  private static final ClassLoader CLASS_LOADER = DummySnapshot.class.getClassLoader();
  private static final String ASPECTS_FIELD = "aspects";
  private static final String FIELD_FIELD_PREFIX = "FIELD_";
  private static final String MEMBER_FIELD_PREFIX = "MEMBER_";
  private static final String METADATA_AUDIT_EVENT_PREFIX = "METADATA_AUDIT_EVENT";
  private static final String URN_FIELD = "urn";

  private ModelUtils() {
    // Util class
  }

  /**
   * Gets the corresponding aspect name for a specific aspect type.
   *
   * @param aspectClass the aspect type
   * @param <T> must be a valid aspect type
   * @return the corresponding aspect name, which is actually the FQCN of type
   */
  public static <T extends DataTemplate> String getAspectName(@Nonnull Class<T> aspectClass) {
    return aspectClass.getCanonicalName();
  }

  /**
   * Gets the corresponding {@link Class} for a given aspect name.
   *
   * @param aspectName the name returned from {@link #getAspectName(Class)}
   * @return the corresponding {@link Class}
   */
  @Nonnull
  public static Class<? extends RecordTemplate> getAspectClass(@Nonnull String aspectName) {
    return getClassFromName(aspectName, RecordTemplate.class);
  }

  /**
   * Returns all supported aspects from an aspect union.
   *
   * @param aspectUnionClass the aspect union type to extract supported aspects from
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @return a set of supported aspects
   */
  @Nonnull
  public static <ASPECT_UNION extends UnionTemplate> Set<Class<? extends RecordTemplate>> getValidAspectTypes(
      @Nonnull Class<ASPECT_UNION> aspectUnionClass) {

    AspectValidator.validateAspectUnionSchema(aspectUnionClass);

    Set<Class<? extends RecordTemplate>> validTypes = new HashSet<>();
    for (UnionDataSchema.Member member : ValidationUtils.getUnionSchema(aspectUnionClass).getMembers()) {
      String fqcn = null;
      if (member.getType().getType() == DataSchema.Type.RECORD) {
        fqcn = ((RecordDataSchema) member.getType()).getBindingName();
      } else if (member.getType().getType() == DataSchema.Type.TYPEREF
          && member.getType().getDereferencedType() == DataSchema.Type.RECORD) {
        fqcn = ((RecordDataSchema) member.getType().getDereferencedDataSchema()).getBindingName();
      }

      if (fqcn != null) {
        try {
          validTypes.add(CLASS_LOADER.loadClass(fqcn).asSubclass(RecordTemplate.class));
        } catch (ClassNotFoundException | ClassCastException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return validTypes;
  }

  /**
   * Gets a {@link Class} from its FQCN.
   */
  @Nonnull
  public static <T> Class<? extends T> getClassFromName(@Nonnull String className, @Nonnull Class<T> parentClass) {
    try {
      return CLASS_LOADER.loadClass(className).asSubclass(parentClass);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(className + " cannot be found", e);
    }
  }

  /**
   * Gets a snapshot class given its FQCN.
   *
   * @param className FQCN of snapshot class
   * @return snapshot class that extends {@link RecordTemplate}, associated with className
   */
  @Nonnull
  public static Class<? extends RecordTemplate> getMetadataSnapshotClassFromName(@Nonnull String className) {
    Class<? extends RecordTemplate> snapshotClass = getClassFromName(className, RecordTemplate.class);
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    return snapshotClass;
  }

  /**
   * Extracts the "urn" field from a snapshot.
   *
   * @param snapshot the snapshot to extract urn from
   * @param <SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @return the extracted {@link Urn}
   */
  @Nonnull
  public static <SNAPSHOT extends RecordTemplate> Urn getUrnFromSnapshot(@Nonnull SNAPSHOT snapshot) {
    SnapshotValidator.validateSnapshotSchema(snapshot.getClass());
    final Urn urn = RecordUtils.getRecordTemplateField(snapshot, URN_FIELD, urnClassForSnapshot(snapshot.getClass()));
    if (urn == null) {
      ValidationUtils.throwNullFieldException(URN_FIELD);
    }
    return urn;
  }

  /**
   * Extracts the "urn" field from an asset.
   *
   * @param asset the asset to extract urn from
   * @param <ASSET> must be a valid asset model defined in com.linkedin.metadata.asset
   * @return the extracted {@link Urn}
   */
  @Nonnull
  public static <ASSET extends RecordTemplate> Urn getUrnFromAsset(@Nonnull ASSET asset) {
    AssetValidator.validateAssetSchema(asset.getClass());
    final Urn urn = RecordUtils.getRecordTemplateField(asset, URN_FIELD, urnClassForAsset(asset.getClass()));
    if (urn == null) {
      ValidationUtils.throwNullFieldException(URN_FIELD);
    }
    return urn;
  }

  /**
   * Get Urn based on provided urn string and urn class.
   *
   * @param <URN> must be a valid URN type that extends {@link Urn}
   * @param urn urn string
   * @param urnClass urn class
   * @return converted urn
   */
  public static <URN extends Urn> URN getUrnFromString(@Nullable String urn, @Nonnull Class<URN> urnClass) {
    if (urn == null) {
      return null;
    }

    try {
      final Method getUrn = urnClass.getMethod("createFromString", String.class);
      return urnClass.cast(getUrn.invoke(null, urn));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException("URN conversion error for " + urn, e);
    }
  }

  /**
   * Get the entity type of urn inside a snapshot class.
   * @param snapshot a snapshot class
   * @return entity type of urn
   */
  @Nonnull
  public static <SNAPSHOT extends RecordTemplate> String getUrnTypeFromSnapshot(@Nonnull Class<SNAPSHOT> snapshot) {
    try {
      return (String) snapshot.getMethod("getUrn").getReturnType().getField("ENTITY_TYPE").get(null);
    } catch (Exception ignored) {
      throw new IllegalArgumentException(String.format("The snapshot class %s is not valid.", snapshot.getCanonicalName()));
    }
  }

  /**
   * Get the asset type of urn inside a asset class.
   * @param asset an assset class
   * @return entity type of urn
   */
  @Nonnull
  public static <ASSET extends RecordTemplate> String getUrnTypeFromAsset(@Nonnull Class<ASSET> asset) {
    try {
      return (String) asset.getMethod("getUrn").getReturnType().getField("ENTITY_TYPE").get(null);
    } catch (Exception ignored) {
      throw new IllegalArgumentException(String.format("The snapshot class %s is not valid.", asset.getCanonicalName()));
    }
  }

  /**
   * Similar to {@link #getUrnFromSnapshot(RecordTemplate)} but extracts from a Snapshot union instead.
   */
  @Nonnull
  public static Urn getUrnFromSnapshotUnion(@Nonnull UnionTemplate snapshotUnion) {
    return getUrnFromSnapshot(RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion));
  }

  /**
   * Extracts the "urn" field from a delta.
   *
   * @param delta the delta to extract urn from
   * @param <DELTA> must be a valid delta model defined in com.linkedin.metadata.delta
   * @return the extracted {@link Urn}
   */
  @Nonnull
  public static <DELTA extends RecordTemplate> Urn getUrnFromDelta(@Nonnull DELTA delta) {
    DeltaValidator.validateDeltaSchema(delta.getClass());
    return RecordUtils.getRecordTemplateField(delta, URN_FIELD, urnClassForDelta(delta.getClass()));
  }

  /**
   * Similar to {@link #getUrnFromDelta(RecordTemplate)} but extracts from a delta union instead.
   */
  @Nonnull
  public static Urn getUrnFromDeltaUnion(@Nonnull UnionTemplate deltaUnion) {
    return getUrnFromDelta(RecordUtils.getSelectedRecordTemplateFromUnion(deltaUnion));
  }

  /**
   * Extracts the "urn" field from a search document.
   *
   * @param document the document to extract urn from
   * @param <DOCUMENT> must be a valid document model defined in com.linkedin.metadata.search
   * @return the extracted {@link Urn}
   */
  @Nonnull
  public static <DOCUMENT extends RecordTemplate> Urn getUrnFromDocument(@Nonnull DOCUMENT document) {
    DocumentValidator.validateDocumentSchema(document.getClass());
    final Urn urn = RecordUtils.getRecordTemplateField(document, URN_FIELD, urnClassForDocument(document.getClass()));
    if (urn == null) {
      ValidationUtils.throwNullFieldException(URN_FIELD);
    }
    return urn;
  }

  /**
   * Extracts the "urn" field from an entity.
   *
   * @param entity the entity to extract urn from
   * @param <ENTITY> must be a valid entity model defined in com.linkedin.metadata.entity
   * @return the extracted {@link Urn}
   */
  @Nonnull
  public static <ENTITY extends RecordTemplate> Urn getUrnFromEntity(@Nonnull ENTITY entity) {
    EntityValidator.validateEntitySchema(entity.getClass());
    final Urn urn = RecordUtils.getRecordTemplateField(entity, URN_FIELD, urnClassForDocument(entity.getClass()));
    if (urn == null) {
      ValidationUtils.throwNullFieldException(URN_FIELD);
    }
    return urn;
  }

  /**
   * Extracts the fields with type urn from a relationship.
   *
   * @param relationship the relationship to extract urn from
   * @param <RELATIONSHIP> must be a valid relationship model defined in com.linkedin.metadata.relationship
   * @param fieldName name of the field with type urn
   * @return the extracted {@link Urn}
   */
  @Nonnull
  private static <RELATIONSHIP extends RecordTemplate> Urn getUrnFromRelationship(@Nonnull RELATIONSHIP relationship,
      @Nonnull String fieldName) {
    RelationshipValidator.validateRelationshipSchema(relationship.getClass());
    final Urn urn = RecordUtils.getRecordTemplateField(relationship, fieldName, urnClassForRelationship(relationship.getClass(), fieldName));
    if (urn == null) {
      ValidationUtils.throwNullFieldException(URN_FIELD);
    }
    return urn;
  }

  /**
   * Similar to {@link #getUrnFromRelationship} but extracts from a delta union instead.
   */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> Urn getSourceUrnFromRelationship(
      @Nonnull RELATIONSHIP relationship) {
    return getUrnFromRelationship(relationship, "source");
  }

  /**
   * Similar to {@link #getUrnFromRelationship} but extracts from a delta union instead.
   */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> Urn getDestinationUrnFromRelationship(
      @Nonnull RELATIONSHIP relationship) {
    return getUrnFromRelationship(relationship, "destination");
  }

  /**
   * Extracts the list of aspects in a snapshot.
   *
   * @param snapshot the snapshot to extract aspects from
   * @param <SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @return the extracted list of aspects
   */
  @Nonnull
  public static <SNAPSHOT extends RecordTemplate> List<RecordTemplate> getAspectsFromSnapshot(
      @Nonnull SNAPSHOT snapshot) {
    SnapshotValidator.validateSnapshotSchema(snapshot.getClass());
    return getAspects(snapshot);
  }

  /**
   * Extracts the list of aspects in an asset.
   *
   * @param asset the asset to extract aspects from
   * @param <ASSET> must be a valid asset model defined in com.linkedin.metadata.asset
   * @return the extracted list of aspects
   */
  @Nonnull
  public static <ASSET extends RecordTemplate> List<RecordTemplate> getAspectsFromAsset(@Nonnull ASSET asset) {
    AssetValidator.validateAssetSchema(asset.getClass());
    // TODO: cache the asset methods loading
    try {
      final List<RecordTemplate> aspects = new ArrayList<>();
      final Field[] assetFields = asset.getClass().getDeclaredFields();
      for (final Field assetField : assetFields) {
        if (assetField.getName().startsWith(FIELD_FIELD_PREFIX)) {
          final String assetFieldName = assetField.getName().substring(FIELD_FIELD_PREFIX.length());
          if (assetFieldName.equalsIgnoreCase(URN_FIELD)) {
            continue;
          }
          final RecordTemplate aspect =
              (RecordTemplate) asset.getClass().getMethod("get" + assetFieldName).invoke(asset);
          if (aspect != null) {
            aspects.add(aspect);
          }
        }
      }
      return aspects;
    } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private final static ConcurrentHashMap<Class<? extends RecordTemplate>, Map<String, String>> ASPECT_ALIAS_CACHE =
      new ConcurrentHashMap<>();

  /**
   * Return aspect alias (in lower cases) from given asset class and aspect FQCN (Fully Qualified Class Name).
   * @param assetClass asset class
   * @param aspectFQCN aspect FQCN
   * @return alias names in lower cases
   * @param <ASSET> Asset class
   */
  @Nullable
  public static <ASSET extends RecordTemplate> String getAspectAlias(@Nonnull Class<ASSET> assetClass,
      @Nonnull String aspectFQCN) {
    return ASPECT_ALIAS_CACHE.computeIfAbsent(assetClass, key -> {
      AssetValidator.validateAssetSchema(assetClass);
      final Field[] declaredFields = assetClass.getDeclaredFields();
      Map<String, String> map = new HashMap<>();
      for (Field declaredField : declaredFields) {
        if (!declaredField.getName().startsWith(FIELD_FIELD_PREFIX)) {
          continue;
        }
        String fieldName = declaredField.getName().substring(FIELD_FIELD_PREFIX.length());
        if (fieldName.equalsIgnoreCase(URN_FIELD)) {
          continue;
        }
        String methodName = "get" + fieldName;
        try {
          String aspectClass = assetClass.getMethod(methodName).getReturnType().getCanonicalName();
          map.put(aspectClass, fieldName.toLowerCase());
        } catch (NoSuchMethodException e) {
          throw new RuntimeException("Method not found: " + methodName, e);
        }
      }
      return map;
    }).get(aspectFQCN);
  }

  /**
   * Extracts given aspect from a snapshot.
   *
   * @param snapshot the snapshot to extract the aspect from
   * @param <SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @param aspectClass the aspect class type to extract from snapshot
   * @return the extracted aspect
   */
  @Nonnull
  public static <SNAPSHOT extends RecordTemplate, ASPECT extends DataTemplate> Optional<ASPECT> getAspectFromSnapshot(
      @Nonnull SNAPSHOT snapshot, @Nonnull Class<ASPECT> aspectClass) {

    return getAspectsFromSnapshot(snapshot).stream()
        .filter(aspect -> aspect.getClass().equals(aspectClass))
        .findFirst()
        .map(aspectClass::cast);
  }

  /**
   * Similar to {@link #getAspectsFromSnapshot(RecordTemplate)} but extracts from a snapshot union instead.
   */
  @Nonnull
  public static List<RecordTemplate> getAspectsFromSnapshotUnion(@Nonnull UnionTemplate snapshotUnion) {
    return getAspects(RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion));
  }

  @Nonnull
  private static List<RecordTemplate> getAspects(@Nonnull RecordTemplate snapshot) {
    final Class<? extends WrappingArrayTemplate> clazz = getAspectsArrayClass(snapshot.getClass());

    final WrappingArrayTemplate aspectArray = RecordUtils.getRecordTemplateWrappedField(snapshot, ASPECTS_FIELD, clazz);
    if (aspectArray == null) {
      ValidationUtils.throwNullFieldException(ASPECTS_FIELD);
    }

    final List<RecordTemplate> aspects = new ArrayList<>();
    aspectArray.forEach(item -> aspects.add(RecordUtils.getSelectedRecordTemplateFromUnion((UnionTemplate) item)));
    return aspects;
  }

  /**
   * Creates a snapshot with its urn field set.
   *
   * @param snapshotClass the type of snapshot to create
   * @param urn value for the urn field
   * @param aspects value for the aspects field
   * @param <SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param <URN> must be a valid URN type
   * @return the created snapshot
   */
  @Nonnull
  public static <SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate, URN extends Urn> SNAPSHOT newSnapshot(
      @Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull URN urn, @Nonnull List<ASPECT_UNION> aspects) {
    return newSnapshot(snapshotClass, urn.toString(), aspects);
  }

  /**
   * Creates a snapshot with its urn field set.
   *
   * @param snapshotClass the type of snapshot to create
   * @param urn value for the urn field as a string
   * @param aspects value for the aspects field
   * @param <SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param <URN> must be a valid URN type
   * @return the created snapshot
   */
  @Nonnull
  public static <SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate, URN extends Urn> SNAPSHOT newSnapshot(
      @Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull String urn, @Nonnull List<ASPECT_UNION> aspects) {

    SnapshotValidator.validateSnapshotSchema(snapshotClass);

    final Class<? extends WrappingArrayTemplate> aspectArrayClass = getAspectsArrayClass(snapshotClass);

    try {
      final SNAPSHOT snapshot = snapshotClass.newInstance();
      if (urn == null) {
        ValidationUtils.throwNullFieldException(URN_FIELD);
      }
      if (aspects == null) {
        ValidationUtils.throwNullFieldException(ASPECTS_FIELD);
      }
      RecordUtils.setRecordTemplatePrimitiveField(snapshot, URN_FIELD, urn);
      WrappingArrayTemplate aspectArray = aspectArrayClass.newInstance();
      aspectArray.addAll(aspects);
      RecordUtils.setRecordTemplateComplexField(snapshot, ASPECTS_FIELD, aspectArray);
      return snapshot;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  private static <SNAPSHOT extends RecordTemplate> Class<? extends WrappingArrayTemplate> getAspectsArrayClass(
      @Nonnull Class<SNAPSHOT> snapshotClass) {

    try {
      return snapshotClass.getMethod("getAspects").getReturnType().asSubclass(WrappingArrayTemplate.class);
    } catch (NoSuchMethodException | ClassCastException e) {
      throw new RuntimeException((e));
    }
  }

  /**
   * Creates an asset with its urn field set.
   *
   * @param assetClass the type of asset to create
   * @param urn value for the urn field
   * @param aspects value for the aspects field
   * @param <ASSET> must be a valid asset model defined in com.linkedin.metadata.asset
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param <URN> must be a valid URN type
   * @return the created asset
   */
  @Nonnull
  public static <ASSET extends RecordTemplate, ASPECT_UNION extends UnionTemplate, URN extends Urn> ASSET newAsset(
      @Nonnull Class<ASSET> assetClass, @Nonnull URN urn, @Nonnull List<ASPECT_UNION> aspects) {
    return newAsset(assetClass, urn.toString(), aspects);
  }

  /**
   * Creates an asset with its urn field set.
   *
   * @param assetClass the type of asset to create
   * @param urn value for the urn field as a string
   * @param aspects value for the aspects field
   * @param <ASSET> must be a valid asset model defined in com.linkedin.metadata.asset
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @return the created asset
   */
  @Nonnull
  public static <ASSET extends RecordTemplate, ASPECT_UNION extends UnionTemplate> ASSET newAsset(
      @Nonnull Class<ASSET> assetClass, @Nonnull String urn, @Nonnull List<ASPECT_UNION> aspects) {

    AssetValidator.validateAssetSchema(assetClass);

    try {
      final ASSET asset = assetClass.newInstance();
      if (urn == null) {
        ValidationUtils.throwNullFieldException(URN_FIELD);
      }
      if (aspects == null) {
        ValidationUtils.throwNullFieldException(ASPECTS_FIELD);
      }
      RecordUtils.setRecordTemplatePrimitiveField(asset, URN_FIELD, urn);

      // TODO: cache the asset methods loading
      final Map<String, String> aspectTypeToAssetSetterMap = new HashMap<>();
      for (final Method assetMethod : assetClass.getDeclaredMethods()) {
        if (assetMethod.getName().startsWith("set") && assetMethod.getParameterTypes().length > 0) {
          aspectTypeToAssetSetterMap.put(assetMethod.getParameterTypes()[0].getName(), assetMethod.getName());
        }
      }

      for (final ASPECT_UNION aspect : aspects) {
        // TODO: cache the aspect union methods loading
        final Map<String, String> aspectTypeToAspectUnionGetterMap = new HashMap<>();
        for (final Method aspectUnionMethod : aspect.getClass().getMethods()) {
          if (aspectUnionMethod.getName().startsWith("get")) {
            aspectTypeToAspectUnionGetterMap.put(aspectUnionMethod.getReturnType().getName(),
                aspectUnionMethod.getName());
          }
        }
        for (final String aspectType : aspectTypeToAssetSetterMap.keySet()) {
          if (aspectTypeToAspectUnionGetterMap.containsKey(aspectType)) {
            Object aspectValue =
                aspect.getClass().getMethod(aspectTypeToAspectUnionGetterMap.get(aspectType)).invoke(aspect);
            if (aspectValue != null) {
              asset.getClass()
                  .getMethod(aspectTypeToAssetSetterMap.get(aspectType), aspectValue.getClass())
                  .invoke(asset, aspectValue);
            }
          }
        }
      }
      return asset;
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Creates an aspect union with a specific aspect set.
   *
   * @param aspectUnionClass the type of aspect union to create
   * @param aspect the aspect to set
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param <ASPECT> must be a supported aspect type in ASPECT_UNION
   * @return the created aspect union
   */
  @Nonnull
  public static <ASPECT_UNION extends UnionTemplate, ASPECT extends RecordTemplate> ASPECT_UNION newAspectUnion(
      @Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull ASPECT aspect) {

    AspectValidator.validateAspectUnionSchema(aspectUnionClass);

    try {
      ASPECT_UNION aspectUnion = aspectUnionClass.newInstance();
      RecordUtils.setSelectedRecordTemplateInUnion(aspectUnion, aspect);
      return aspectUnion;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a new {@link AspectVersion}.
   */
  @Nonnull
  public static <ASPECT extends RecordTemplate> AspectVersion newAspectVersion(@Nonnull Class<ASPECT> aspectClass,
      long version) {
    AspectVersion aspectVersion = new AspectVersion();
    aspectVersion.setAspect(ModelUtils.getAspectName(aspectClass));
    aspectVersion.setVersion(version);
    return aspectVersion;
  }

  /**
   * Gets the expected aspect class for a specific kind of snapshot.
   */
  @Nonnull
  public static Class<? extends UnionTemplate> aspectClassForSnapshot(
      @Nonnull Class<? extends RecordTemplate> snapshotClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);

    String aspectClassName = ((TyperefDataSchema) ((ArrayDataSchema) ValidationUtils.getRecordSchema(snapshotClass)
        .getField(ASPECTS_FIELD)
        .getType()).getItems()).getBindingName();

    return getClassFromName(aspectClassName, UnionTemplate.class);
  }

  /**
   * Gets the expected {@link Urn} class for a specific kind of entity.
   */
  @Nonnull
  public static Class<? extends Urn> urnClassForEntity(@Nonnull Class<? extends RecordTemplate> entityClass) {
    EntityValidator.validateEntitySchema(entityClass);
    return urnClassForField(entityClass, URN_FIELD);
  }

  /**
   * Gets the expected {@link Urn} class for a specific kind of snapshot.
   */
  @Nonnull
  public static Class<? extends Urn> urnClassForSnapshot(@Nonnull Class<? extends RecordTemplate> snapshotClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    return urnClassForField(snapshotClass, URN_FIELD);
  }

  /**
   * Gets the expected {@link Urn} class for a specific kind of asset.
   */
  @Nonnull
  public static Class<? extends Urn> urnClassForAsset(@Nonnull Class<? extends RecordTemplate> assetClass) {
    AssetValidator.validateAssetSchema(assetClass);
    return urnClassForField(assetClass, URN_FIELD);
  }

  /**
   * Gets the expected {@link Urn} class for a specific kind of delta.
   */
  @Nonnull
  public static Class<? extends Urn> urnClassForDelta(@Nonnull Class<? extends RecordTemplate> deltaClass) {
    DeltaValidator.validateDeltaSchema(deltaClass);
    return urnClassForField(deltaClass, URN_FIELD);
  }

  /**
   * Gets the expected {@link Urn} class for a specific kind of search document.
   */
  @Nonnull
  public static Class<? extends Urn> urnClassForDocument(@Nonnull Class<? extends RecordTemplate> documentClass) {
    DocumentValidator.validateDocumentSchema(documentClass);
    return urnClassForField(documentClass, URN_FIELD);
  }

  /**
   * Gets the expected {@link Urn} class for a specific kind of relationship.
   */
  @Nonnull
  private static Class<? extends Urn> urnClassForRelationship(
      @Nonnull Class<? extends RecordTemplate> relationshipClass, @Nonnull String fieldName) {
    RelationshipValidator.validateRelationshipSchema(relationshipClass);
    return urnClassForField(relationshipClass, fieldName);
  }

  /**
   * Gets the expected {@link Urn} class for the source field of a specific kind of relationship.
   */
  @Nonnull
  public static Class<? extends Urn> sourceUrnClassForRelationship(
      @Nonnull Class<? extends RecordTemplate> relationshipClass) {
    return urnClassForRelationship(relationshipClass, "source");
  }

  /**
   * Gets the expected {@link Urn} class for the destination field of a specific kind of relationship.
   */
  @Nonnull
  public static Class<? extends Urn> destinationUrnClassForRelationship(
      @Nonnull Class<? extends RecordTemplate> relationshipClass) {
    return urnClassForRelationship(relationshipClass, "destination");
  }

  @Nonnull
  private static Class<? extends Urn> urnClassForField(@Nonnull Class<? extends RecordTemplate> recordClass,
      @Nonnull String fieldName) {
    String urnClassName = ((DataMap) ValidationUtils.getRecordSchema(recordClass)
        .getField(fieldName)
        .getType()
        .getProperties()
        .get("java")).getString("class");

    return getClassFromName(urnClassName, Urn.class);
  }

  /**
   * Validates a specific snapshot-aspect combination.
   */
  public static <SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate> void validateSnapshotAspect(
      @Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    AspectValidator.validateAspectUnionSchema(aspectUnionClass);

    // Make sure that SNAPSHOT's "aspects" array field contains ASPECT_UNION type.
    if (!aspectClassForSnapshot(snapshotClass).equals(aspectUnionClass)) {
      throw new InvalidSchemaException(aspectUnionClass.getCanonicalName() + " is not a supported aspect class of "
          + snapshotClass.getCanonicalName());
    }
  }

  /**
   * Validates a specific snapshot-URN combination.
   */
  public static <SNAPSHOT extends RecordTemplate, URN extends Urn> void validateSnapshotUrn(
      @Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<URN> urnClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);

    // Make sure that SNAPSHOT's "urn" field uses the correct class or subclasses
    if (!urnClassForSnapshot(snapshotClass).isAssignableFrom(urnClass)) {
      throw new InvalidSchemaException(
          urnClass.getCanonicalName() + " is not a supported URN class of " + snapshotClass.getCanonicalName());
    }
  }

  /**
   * Creates a relationship union with a specific relationship set.
   *
   * @param relationshipUnionClass the type of relationship union to create
   * @param relationship the relationship to set
   * @param <RELATIONSHIP_UNION> must be a valid relationship union defined in com.linkedin.metadata.relationship
   * @param <RELATIONSHIP> must be a supported relationship type in ASPECT_UNION
   * @return the created relationship union
   */
  @Nonnull
  public static <RELATIONSHIP_UNION extends UnionTemplate, RELATIONSHIP extends RecordTemplate> RELATIONSHIP_UNION newRelationshipUnion(
      @Nonnull Class<RELATIONSHIP_UNION> relationshipUnionClass, @Nonnull RELATIONSHIP relationship) {

    RelationshipValidator.validateRelationshipUnionSchema(relationshipUnionClass);

    try {
      RELATIONSHIP_UNION relationshipUnion = relationshipUnionClass.newInstance();
      RecordUtils.setSelectedRecordTemplateInUnion(relationshipUnion, relationship);
      return relationshipUnion;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns all entity classes.
   */
  @Nonnull
  public static Set<Class<? extends RecordTemplate>> getAllEntities() {
    return new Reflections("com.linkedin.metadata.entity").getSubTypesOf(RecordTemplate.class)
        .stream()
        .filter(EntityValidator::isValidEntitySchema)
        .collect(Collectors.toSet());
  }

  /**
   * Get entity type from urn class.
   */
  @Nonnull
  public static String getEntityTypeFromUrnClass(@Nonnull Class<? extends Urn> urnClass) {
    try {
      return urnClass.getDeclaredField("ENTITY_TYPE").get(null).toString();
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get aspect specific kafka topic name from urn and aspect classes.
   */
  @Nonnull
  public static <URN extends Urn, ASPECT extends RecordTemplate> String getAspectSpecificMAETopicName(@Nonnull URN urn,
      @Nonnull ASPECT newValue) {
    return String.format("%s_%s_%s", METADATA_AUDIT_EVENT_PREFIX, urn.getEntityType().toUpperCase(),
        newValue.getClass().getSimpleName().toUpperCase());
  }

  /**
   * Return true if the aspect is defined in common namespace.
   */
  public static boolean isCommonAspect(@Nonnull Class<? extends RecordTemplate> clazz) {
    return clazz.getPackage().getName().startsWith("com.linkedin.common");
  }

  /**
   * Creates an entity union with a specific entity set.
   *
   * @param entityUnionClass the type of entity union to create
   * @param entity the entity to set
   * @param <ENTITY_UNION> must be a valid enity union defined in com.linkedin.metadata.entity
   * @param <ENTITY> must be a supported entity in entity union
   * @return the created entity union
   */
  @Nonnull
  public static <ENTITY_UNION extends UnionTemplate, ENTITY extends RecordTemplate> ENTITY_UNION newEntityUnion(
      @Nonnull Class<ENTITY_UNION> entityUnionClass, @Nonnull ENTITY entity) {

    EntityValidator.validateEntityUnionSchema(entityUnionClass);

    try {
      ENTITY_UNION entityUnion = entityUnionClass.newInstance();
      RecordUtils.setSelectedRecordTemplateInUnion(entityUnion, entity);
      return entityUnion;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get all aspects' class canonical names from a aspect union.
   * @param unionClass the union class contains all the aspects
   * @return A list of aspect canonical names.
   */
  public static <ASPECT_UNION extends UnionTemplate> List<String> getAspectClassNames(Class<ASPECT_UNION> unionClass) {
    try {
      final UnionTemplate unionTemplate = unionClass.newInstance();
      final UnionDataSchema unionDataSchema = (UnionDataSchema) unionTemplate.schema();
      return unionDataSchema.getMembers().stream().map(UnionDataSchema.Member::getUnionMemberKey).collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Derive the aspect union class from a snapshot class.
   * @param snapshotClass the snapshot class contains the aspect union.
   * @return Aspect union class
   */
  public static <SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate> Class<ASPECT_UNION> getUnionClassFromSnapshot(
      Class<SNAPSHOT> snapshotClass) {
    try {
      Class<?> innerClass = ClassUtils.loadClass(snapshotClass.getMethod("getAspects").getReturnType().getCanonicalName() + "$Fields");
      return (Class<ASPECT_UNION>) innerClass.newInstance().getClass().getMethod("items").getReturnType().getEnclosingClass();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert an entity snapshot to an asset.
   * @param assetClass the type of asset to create
   * @param snapshot the snapshot to convert to asset from
   * @param <ASSET> must be a valid asset model defined in com.linkedin.metadata.asset
   * @param <SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @return the created asset
   */
  @Nonnull
  public static <ASSET extends RecordTemplate, SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate> ASSET convertSnapshotToAsset(
      @Nonnull Class<ASSET> assetClass, @Nonnull SNAPSHOT snapshot) {
    final List<ASPECT_UNION> aspectUnion = new ArrayList<>();
    final List<RecordTemplate> aspects = getAspectsFromSnapshot(snapshot);
    for (RecordTemplate aspect : aspects) {
      aspectUnion.add(newAspectUnion(getUnionClassFromSnapshot(snapshot.getClass()), aspect));
    }
    return newAsset(assetClass, getUrnFromSnapshot(snapshot), aspectUnion);
  }

  /**
   * Convert an entity asset to an entity internal snapshot, the created internal snapshot,
   * which has "equal number of" aspects in asset.
   * @param internalSnapshotClass the type of snapshot to create
   * @param asset the asset to convert to snapshot from
   * @param <ASSET> must be a valid asset model defined in com.linkedin.metadata.asset
   * @param <INTERNAL_SNAPSHOT> must be a valid internal snapshot model defined in com.linkedin.metadata.snapshot
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @return the created internal snapshot
   */
  @Nonnull
  public static <ASSET extends RecordTemplate, INTERNAL_SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate>
  INTERNAL_SNAPSHOT convertAssetToInternalSnapshot(
      @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass, @Nonnull ASSET asset) {
    final List<ASPECT_UNION> aspectUnion = new ArrayList<>();
    final Class<ASPECT_UNION> aspectUnionClass = getUnionClassFromSnapshot(internalSnapshotClass);
    final Set<Class<? extends RecordTemplate>> validAspectTypes = getValidAspectTypes(aspectUnionClass);
    for (final RecordTemplate aspect : getAspectsFromAsset(asset)) {
      addAspectUnion(validAspectTypes, aspectUnionClass, aspect, aspectUnion);
    }
    return newSnapshot(internalSnapshotClass, getUrnFromAsset(asset), aspectUnion);
  }

  /**
   * Convert an entity snapshot to an entity internal snapshot, and vice versa,
   * the created snapshot, which has "less or equal" aspects in internal snapshot.
   * @param toSnapshotClass the type of snapshot to create
   * @param fromSnapshot the snapshot to convert
   * @param <TO_SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @param <FROM_SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @return the created internal snapshot
   */
  @Nonnull
  public static <TO_SNAPSHOT extends RecordTemplate, FROM_SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate>
  TO_SNAPSHOT convertSnapshots(
      @Nonnull Class<TO_SNAPSHOT> toSnapshotClass, @Nonnull FROM_SNAPSHOT fromSnapshot) {
    final List<ASPECT_UNION> aspectUnion = new ArrayList<>();
    final Class<ASPECT_UNION> aspectUnionClass = getUnionClassFromSnapshot(toSnapshotClass);
    final Set<Class<? extends RecordTemplate>> validAspectTypes = getValidAspectTypes(aspectUnionClass);
    for (final RecordTemplate aspect : getAspectsFromSnapshot(fromSnapshot)) {
      addAspectUnion(validAspectTypes, aspectUnionClass, aspect, aspectUnion);
    }
    return newSnapshot(toSnapshotClass, getUrnFromSnapshot(fromSnapshot), aspectUnion);
  }

  /**
   * Convert an internal aspect union to an aspect union.
   * @param aspectUnionClass the type of aspect union to create
   * @param internalAspectUnions the internal aspect union to convert
   * @param <INTERNAL_ASPECT_UNION> must be a valid internal aspect union model defined in com.linkedin.metadata.aspect
   * @param <ASPECT_UNION> must be a valid aspect union model defined in com.linkedin.metadata.aspect
   * @return the created aspect union
   */
  @Nonnull
  public static <INTERNAL_ASPECT_UNION extends UnionTemplate, ASPECT_UNION extends UnionTemplate>
  List<ASPECT_UNION> convertInternalAspectUnionToAspectUnion(
      @Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull List<INTERNAL_ASPECT_UNION> internalAspectUnions) {
    final List<ASPECT_UNION> aspectUnions = new ArrayList<>();
    final Set<Class<? extends RecordTemplate>> validAspectTypes = getValidAspectTypes(aspectUnionClass);
    for (final INTERNAL_ASPECT_UNION internalAspectUnion : internalAspectUnions) {
      addAspectUnion(validAspectTypes, aspectUnionClass,
          RecordUtils.getSelectedRecordTemplateFromUnion(internalAspectUnion), aspectUnions);
    }
    return aspectUnions;
  }

  /**
   * Add an aspect union to aspect union list if it is a validated aspect type.
   * @param validAspectTypes a set of supported aspects
   * @param aspectUnionClass the type of aspect union to add
   * @param aspect the type of aspect to be added
   * @param aspectUnions the aspect union to add
   * @param <ASPECT_UNION> must be a valid aspect union model defined in com.linkedin.metadata.aspect
   */
  private static <ASPECT_UNION extends UnionTemplate> void addAspectUnion(
      @Nonnull Set<Class<? extends RecordTemplate>> validAspectTypes, @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull RecordTemplate aspect, @Nonnull List<ASPECT_UNION> aspectUnions) {
    if (validAspectTypes.contains(aspect.getClass())) {
      aspectUnions.add(newAspectUnion(aspectUnionClass, aspect));
    }
  }

  /**
   * Decorate extra value fields from internal snapshot.
   * @param internalSnapshot internal snapshot to decorate
   * @param value value to be decorated
   * @param <INTERNAL_SNAPSHOT> must be a valid internal snapshot model defined in com.linkedin.metadata.snapshot
   * @param <VALUE> resource's value
   * @return decorated resource's value
   */
  @Nonnull
  public static <INTERNAL_SNAPSHOT extends RecordTemplate, VALUE extends RecordTemplate> VALUE decorateValue(
      @Nonnull INTERNAL_SNAPSHOT internalSnapshot, @Nonnull VALUE value) {
    try {
      final Map<Class<?>, RecordTemplate> aspectClasses = new HashMap<>();
      for (final RecordTemplate aspect : getAspectsFromSnapshot(internalSnapshot)) {
        aspectClasses.put(aspect.getClass(), aspect);
      }
      for (final Method valueMethod : value.getClass().getMethods()) {
        final String valueMethodName = valueMethod.getName();
        if (valueMethodName.startsWith("set")) {
          final Optional<Class<?>> valueAspectClass = Arrays.stream(valueMethod.getParameterTypes()).findFirst();

          // setDatasetRecommendationsInfo() --> hasDatasetRecommendationsInfo()
          final String hasMethodName = valueMethodName.replaceFirst("set", "has");

          if (valueAspectClass.isPresent() && aspectClasses.containsKey(valueAspectClass.get())
              && !(boolean) value.getClass().getMethod(hasMethodName).invoke(value)) {
            value.getClass()
                .getMethod(valueMethodName, valueAspectClass.get())
                .invoke(value, aspectClasses.get(valueAspectClass.get()));
          }
        }
      }
      return value;
    } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static <URN extends Urn> String getEntityType(@Nullable URN urn) {
    return urn == null ? null : urn.getEntityType();
  }
}
