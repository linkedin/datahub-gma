package com.linkedin.metadata.dao.internal;

import lombok.Builder;
import lombok.Data;


@Builder
@Data
public final class Neo4jQueryResult {
  private final long tookMs;
  private final int retries;
}
