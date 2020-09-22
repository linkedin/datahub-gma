package com.linkedin.metadata.utils.elasticsearch;

import lombok.Data;
import org.elasticsearch.common.xcontent.XContentBuilder;


@Data
public abstract class ElasticEvent {
  /**
   * Descriptor for a change action.
   */
  public enum ChangeType {
    CREATE,
    DELETE,
    UPDATE
  }

  private String index;
  private String type;
  private String id;
  private ChangeType actionType;

  public XContentBuilder buildJson() {
    return null;
  }
}