package com.linkedin.metadata.dao.utils;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.avro2pegasus.events.UUID;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.annotations.AspectIngestionAnnotation;
import com.linkedin.metadata.annotations.AspectIngestionAnnotationArray;
import com.linkedin.metadata.annotations.GmaAnnotation;
import com.linkedin.metadata.annotations.GmaAnnotationParser;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.events.IngestionTrackingContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class IngestionUtils {

  private IngestionUtils() {
    //Utils class
  }

  /**
   * This method provides the bidirectional mapping between {@link IngestionMode} and {@link BackfillMode}. Only
   * user-allowed ingestion modes are included in the mapping.
   */
  public static final BiMap<IngestionMode, BackfillMode> ALLOWED_INGESTION_BACKFILL_BIMAP = createBiMap();
  private static BiMap<IngestionMode, BackfillMode> createBiMap() {
    BiMap<IngestionMode, BackfillMode> biMap = HashBiMap.create();
    biMap.put(IngestionMode.BACKFILL, BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX);
    biMap.put(IngestionMode.BOOTSTRAP, BackfillMode.BACKFILL_ALL);
    return biMap;
  }

  /**
   * Build IngestionTrackingContext.
   */
  @Nonnull
  public static IngestionTrackingContext buildIngestionTrackingContext(@Nonnull UUID uuid, @Nonnull String emitter, long timestamp) {
    return new IngestionTrackingContext()
        .setTrackingId(uuid)
        .setEmitter(emitter)
        .setEmitTime(timestamp);
  }

  /**
   * Parse the ingestion mode annotation given an aspect class.
   */
  @Nonnull
  public static <ASPECT extends RecordTemplate> AspectIngestionAnnotationArray parseIngestionModeFromAnnotation(
      @Nonnull final Class<ASPECT> aspectClass) {

    try {
      final RecordDataSchema schema = (RecordDataSchema) DataTemplateUtil.getSchema(aspectClass);
      final Optional<GmaAnnotation> gmaAnnotation = new GmaAnnotationParser().parse(schema);

      // Return empty array if user did not specify any ingestion annotation on the aspect.
      if (!gmaAnnotation.isPresent() || !gmaAnnotation.get().hasAspect() || !gmaAnnotation.get().getAspect().hasIngestion()) {
        return new AspectIngestionAnnotationArray();
      }

      return gmaAnnotation.get().getAspect().getIngestion();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to parse the annotations for aspect %s", aspectClass.getCanonicalName()), e);
    }
  }

  @Nullable
  public static AspectIngestionAnnotation findIngestionAnnotationForEntity(@Nonnull AspectIngestionAnnotationArray ingestionAnnotations,
      Urn urn) {
    for (AspectIngestionAnnotation ingestionAnnotation : ingestionAnnotations) {
      System.out.println("At least we found the annotation " + ingestionAnnotation.toString() + " and urn " + urn.toString());
      if (!ingestionAnnotation.hasUrn() || !ingestionAnnotation.hasMode()) {
        continue;
      }

      final String urnFromAnnotation = getLastElementsInUrnString(ingestionAnnotation.getUrn());
      final String urnFromInput = getLastElementsInUrnString(urn.getClass().getCanonicalName());

      if (urnFromAnnotation.equals(urnFromInput)) {
        return ingestionAnnotation;
      }
    }

    return null;
  }

  /**
   * Get last element from urnStr.
   * for example, if urnStr is com.linkedin.common.FooUrn, then last element is FooUrn.
   */
  private static String getLastElementsInUrnString(String urnStr) {
    final String[] urnParts = urnStr.split("\\.");
    return urnParts[urnParts.length - 1];
  }
}
