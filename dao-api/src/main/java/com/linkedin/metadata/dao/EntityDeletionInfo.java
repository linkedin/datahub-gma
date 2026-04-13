package com.linkedin.metadata.dao;

import java.sql.Timestamp;
import java.util.Map;
import lombok.Builder;
import lombok.Value;


/**
 * A value class that holds deletion-relevant fields for a single entity, used by batch deletion validation.
 * Contains status flags for deletion eligibility checks and all aspect column values for Kafka archival.
 */
@Value
@Builder
public class EntityDeletionInfo {

  Timestamp deletedTs;

  boolean statusRemoved;

  String statusLastModifiedOn;

  /**
   * All aspect column values (column name → raw JSON string) for Kafka archival by the service layer.
   */
  Map<String, String> aspectColumns;
}
