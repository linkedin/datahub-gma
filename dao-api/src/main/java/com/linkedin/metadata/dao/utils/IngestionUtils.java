package com.linkedin.metadata.dao.utils;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.events.IngestionMode;


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
}
