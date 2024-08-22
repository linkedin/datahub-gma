package com.linkedin.metadata.dao.ingestion;

/**
 * The status of the ingestion process.
 */
public enum ExitRoutingStatus {
  /**
   * Proceed with other update processes.
   */
  PROCEED,
  /**
   * Skip other update processes.
   */
  SKIP
}
