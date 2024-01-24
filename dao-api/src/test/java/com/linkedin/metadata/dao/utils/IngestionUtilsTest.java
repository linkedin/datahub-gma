package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.events.IngestionMode;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.IngestionUtils.*;
import static org.testng.Assert.*;


public class IngestionUtilsTest {

  @Test
  public void testAllowedIngestionModeBackfillModeBimap() {
    IngestionMode ingestionMode = IngestionMode.BACKFILL;
    BackfillMode backfillMode = ALLOWED_INGESTION_BACKFILL_BIMAP.get(ingestionMode);
    assertEquals(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX, backfillMode);

    assertEquals(ingestionMode, ALLOWED_INGESTION_BACKFILL_BIMAP.inverse().get(backfillMode));
    assertNull(ALLOWED_INGESTION_BACKFILL_BIMAP.get(IngestionMode.LIVE));
    assertNull(ALLOWED_INGESTION_BACKFILL_BIMAP.inverse().get(BackfillMode.MAE_ONLY_WITH_OLD_VALUE_NULL));
  }
}
